import boto3
import pandas as pd
import numpy as np
from sodapy import Socrata

import os

from config import DAPCZ_SHAPE

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Credentials
SO_TOKEN = os.getenv("SO_TOKEN")

PERMITS_FILE = "row_inspector_permit_list.csv"
SEGMENTS_FILE = "row_inspector_segment_list.csv"


def s3_to_df(s3, filename):
    response = s3.get_object(Bucket=BUCKET, Key=filename)
    return pd.read_csv(response.get("Body"))


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
        yield data[i: i + batch_size]


def retrieve_road_segment_data(segments):
    segment_ids = list(segments["PROPERTYRSN"].unique())
    segment_batches = batch_list(segment_ids)
    segment_data = []
    for segment_batch in segment_batches:
        segment_batch = ", ".join(map(str, segment_batch))
        client = Socrata("data.austintexas.gov", app_token=SO_TOKEN)
        segment_data += client.get(
            "8hf2-pdmb", where=f"segment_id in ({segment_batch})", limit=999999
        )
    segment_data = pd.DataFrame(segment_data)
    segment_data["segment_id"] = segment_data["segment_id"].astype(int)
    segment_data["road_class"] = segment_data["road_class"].astype(int)
    segments = segments.merge(
        segment_data, left_on="PROPERTYRSN", right_on="segment_id", how="inner"
    )

    # Flagging segments if they are in the DAPCZ for later scoring
    dapcz_segments = retrieve_dapcz_segments(client)
    segments["is_dapcz"] = segments["segment_id"].isin(dapcz_segments)
    return segments


def retrieve_dapcz_segments(client):
    """
    Gets the list of street segments within the Downtown Project Coordination Zone (DAPCZ).
    DAPCZ_SHAPE is an approximation of a more detailed geometry.
    """
    dapcz_segments = client.get(
        "8hf2-pdmb", select="segment_id", where=f'intersects(the_geom, "{DAPCZ_SHAPE}")', limit=999999
    )
    return [int(i["segment_id"]) for i in dapcz_segments]


def road_class_scoring(row):
    """
    Our score criteria: Critical = 10, Arterial = 7, Collector = 5, Residential = 3

    1, 2, 4 =  10 (Interstate, US and State Highways, Major Arterials)
    5 = 7 (Minor arterials)
    8 = 5 (city collector)
    else = 3 (local city/county streets, whatever else)

    ### CTM GIS Street Segment Road Classes
    1   A10 Interstate, Fwy, Expy
    2   A20 US and State Highways
    4   A30 Major Arterials and County Roads (FM)
    5   A31 Minor Arterials
    6   A40 Local City/County Street
    8   A45 City Collector
    9   A61 Cul-de-sac
    10  A63 Ramps and Turnarounds
    11  A73 Alley
    12  A74 Driveway
    13  ROW Jurisdiction Border Segment
    14  A50 Unimporoved Public Road
    15  A60 Private Road
    16  A70 Routing Driveway/Service Road
    17  A72 Platted ROW/Unbuilt
    53  A25 (Not used - Old SH)
    57  A41 (Not used - Old Collector)
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
    permits = s3_to_df(s3_client, PERMITS_FILE)
    segments = s3_to_df(s3_client, SEGMENTS_FILE)

    # number of segments scoring:
    permits = number_of_segments_scoring(permits, segments)

    # permit duration scoring:
    permits["duration_scoring"] = permits.apply(duration_scoring, axis=1)

    # road segment class scoring
    segments = retrieve_road_segment_data(segments)
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
    permits = permits.merge(
        dapcz, left_on="FOLDERRSN", right_index=True, how="left"
    )
    permits["dapcz_scoring"] = permits["dapcz_scoring"].fillna(0)

    # Total Scoring
    cols = permits.columns
    scoring_cols = []
    for col in cols:
        if col.endswith("_scoring"):
            scoring_cols.append(col)
    permits["total_score"] = permits[scoring_cols].sum(axis=1)
    permits


if __name__ == "__main__":
    main()
