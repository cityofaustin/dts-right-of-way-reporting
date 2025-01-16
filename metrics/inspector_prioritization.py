import boto3
import pandas as pd
import numpy as np
from sodapy import Socrata

import os
import logging

from utils import get_logger, s3_csv_to_df, df_to_socrata_dataset

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Credentials
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("PRIORITY_DATASET")
SEGMENT_DATASET = os.getenv("SEGMENT_DATASET")

PERMITS_FILE = "row_inspector_permit_list.csv"
SEGMENTS_FILE = "row_inspector_segment_list.csv"


def number_of_segments_scoring(permits, segments):
    """
    10 points for permits with more than 1 segment, 5 points otherwise.

    Returns
    -------
    permits - with a count_segment_scoring column with the scores based on the number of segments

    """

    counts = segments.groupby("FOLDERRSN")["PROPERTYRSN"].count()
    counts = counts.rename("count_segments")
    permits = permits.merge(counts, left_on="FOLDERRSN", right_index=True, how="left")
    permits["count_segments"] = permits["count_segments"].fillna(0)

    # 10 points for permits with more than 1 segment, 5 points otherwise:
    permits["count_segment_scoring"] = np.where(permits["count_segments"] > 1, 10, 5)
    return permits


def duration_scoring(row):
    """

    Parameters
    ----------
    row - works row-wise on the permits data

    Returns
    -------
    scoring based on the number of days the permit is active

    """
    # each permit type uses a different column from the query for the duration (in days).
    if row["FOLDERTYPE"] == "RW":
        duration_column = "TOTAL_DAYS"
    if row["FOLDERTYPE"] == "DS":
        duration_column = "WZ_DURATION"
    if row["FOLDERTYPE"] == "EX":
        start = pd.to_datetime(row["START_DATE"])
        end = pd.to_datetime(row["END_DATE"])
        delta = end - start
        row["duration"] = delta.days
        duration_column = "duration"

    # Scoring based on number of days the permit is active:
    if row[duration_column] <= 6:
        return 10
    elif row[duration_column] <= 15:
        return 5
    elif row[duration_column] <= 30:
        return 3
    return 1


def batch_list(data, batch_size=100):
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]


def retrieve_road_segment_data(segments, soda):
    data = soda.get(SEGMENT_DATASET, limit=999999)
    segment_data = pd.DataFrame(data)
    segment_data["segment_id"] = segment_data["segment_id"].astype(int)
    segment_data["inspector_zone"] = segment_data["inspector_zone"].astype(float)

    segments = segments.merge(
        segment_data, left_on="PROPERTYRSN", right_on="segment_id", how="inner"
    )

    # Flagging segments if they are in the DAPCZ for later scoring
    segments["is_dapcz"] = segments["dapcz_zone"].notnull()

    segments.rename(columns={"inspector_zone": "row_inspector_zone"}, inplace=True)
    return segments


def road_class_scoring(row):
    """
    Our score criteria: Critical = 10, Arterial = 7, Collector = 5, Residential = 3

    1, 2, 4 =  10 (Interstate, US and State Highways, Major Arterials)
    5 = 7 (Minor arterials)
    8 = 5 (city collector)
    else = 3 (local city/county streets, whatever else)

    """
    if row["road_class"] in (1, 2, 4):
        return 10
    elif row["road_class"] == 5:
        return 7
    elif row["road_class"] == 8:
        return 5
    return 3


def main():
    s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )
    permits = s3_csv_to_df(s3_client, BUCKET, PERMITS_FILE)
    logger.info(f"{len(permits)} Permits retrieved from S3")
    segments = s3_csv_to_df(s3_client, BUCKET, SEGMENTS_FILE)
    logger.info(f"{len(segments)} Segments retrieved from S3")

    # Socrata credentials
    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    # number of segments scoring:
    permits = number_of_segments_scoring(permits, segments)

    # permit duration scoring:
    permits["duration_scoring"] = permits.apply(duration_scoring, axis=1)

    # downloading road segment data
    logger.info("Retrieving road segment data...")
    segments = retrieve_road_segment_data(segments, soda)

    # road segment class scoring
    logger.info("Scoring permits based on road segments data")
    segments["segment_road_class_scoring"] = segments.apply(road_class_scoring, axis=1)

    # get maximum scoring for each permit's segments
    road_class = segments.groupby("FOLDERRSN")["segment_road_class_scoring"].max()
    road_class = road_class.rename("road_class_scoring")
    permits = permits.merge(
        road_class, left_on="FOLDERRSN", right_index=True, how="left"
    )
    permits["road_class_scoring"] = permits["road_class_scoring"].fillna(0)

    # Downtown Project Coordination Zone (DAPCZ) segment scoring, 10 points if any segment is in the DAPCZ, 0 otherwise
    segments["dapcz_segment_scoring"] = np.where(segments["is_dapcz"], 10, 0)
    dapcz = segments.groupby("FOLDERRSN")["dapcz_segment_scoring"].max()
    dapcz = dapcz.rename("dapcz_scoring")
    permits = permits.merge(dapcz, left_on="FOLDERRSN", right_index=True, how="left")
    permits["dapcz_scoring"] = permits["dapcz_scoring"].fillna(0)

    # Merging ROW inspector zone to permits
    row_inspector_zones = segments.groupby("FOLDERRSN")["row_inspector_zone"].max()
    row_inspector_zones = row_inspector_zones.rename("row_inspector_zone")
    permits = permits.merge(
        row_inspector_zones, left_on="FOLDERRSN", right_index=True, how="left"
    )

    # Total Scoring
    cols = permits.columns
    scoring_cols = []
    for col in cols:
        if col.endswith("_scoring"):
            scoring_cols.append(col)
    permits["total_score"] = permits[scoring_cols].sum(axis=1)

    # cleaning up timestamps
    date_fields = [
        "EXPIRY_DATE",
        "START_DATE",
        "END_DATE",
    ]
    for field in date_fields:
        permits[field] = pd.to_datetime(permits[field], format="mixed")
        permits[field] = permits[field].dt.strftime("%Y-%m-%dT%H:%M:00.000")

    # replacing NaN's with None (Socrata doesn't like)
    permits = permits.replace(np.nan, None)

    logger.info(
        f"Replacing data in Socrata dataset: datahub.austintexas.gov/d/{DATASET}"
    )
    response = df_to_socrata_dataset(soda, DATASET, permits, method="replace")
    logger.info(response)


logger = get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main()
