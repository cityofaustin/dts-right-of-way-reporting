import boto3
import pandas as pd
import numpy as np
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry.filters import intersects
from sodapy import Socrata

import os
import logging

from config import (
    SEGMENTS_FEATURE_SERVICE_URL,
    DAPCZ_FEATURE_SERVICE_URL,
    ROW_INSPECTOR_SERVICE_URL,
)
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

# AGOL Credentials
AGOL_USERNAME = os.getenv("AGOL_USERNAME")
AGOL_PASSWORD = os.getenv("AGOL_PASSWORD")

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


def retrieve_road_segment_data(segments, gis):
    segments_layer = FeatureLayer(SEGMENTS_FEATURE_SERVICE_URL, gis)
    segment_ids = list(segments["PROPERTYRSN"].unique())
    segment_batches = batch_list(segment_ids)
    segment_data = []
    for segment_batch in segment_batches:
        segment_batch = ", ".join(map(str, segment_batch))
        query_result = segments_layer.query(
            where=f"segment_id in ({segment_batch})",
            out_fields="*",
            return_geometry=True,
        )
        segment_data.append(query_result.df)
    segment_data = pd.concat(segment_data)
    segments = segments.merge(
        segment_data, left_on="PROPERTYRSN", right_on="SEGMENT_ID", how="inner"
    )

    # Flagging segments if they are in the DAPCZ for later scoring
    dapcz_segments = retrieve_dapcz_segments(gis, segments_layer)
    segments["is_dapcz"] = segments["SEGMENT_ID"].isin(dapcz_segments)

    # Tagging (primary) segments with the appropriate ROW inspector zone
    inspector_zones = FeatureLayer(ROW_INSPECTOR_SERVICE_URL, gis)
    primary_segments = segments[segments["IS_PRIMARY"]]
    primary_segments.drop_duplicates(subset=["SEGMENT_ID"], inplace=True)
    primary_segments = primary_segments.to_dict(orient="records")
    row_inspector_lookup = {}
    for segment in primary_segments:
        geom = segment["SHAPE"]
        row_inspector_lookup[
            segment["SEGMENT_ID"]
        ] = retrieve_row_inspector_segments_by_segment(geom, gis, inspector_zones)
    segments["row_inspector_zone"] = segments["SEGMENT_ID"].map(row_inspector_lookup)

    return segments


def retrieve_dapcz_segments(gis, segments_layer):
    """
    Gets the list of street segments within the Downtown Project Coordination Zone (DAPCZ) polygon.
    """
    dapcz_features = FeatureLayer(DAPCZ_FEATURE_SERVICE_URL, gis)
    dapcz_features = dapcz_features.query(where="1=1", return_geometry=True)
    dapcz_segments = []
    for polygon in dapcz_features.features:
        filter = intersects(polygon.geometry)
        response = segments_layer.query(geometry_filter=filter, out_fields="SEGMENT_ID")
        dapcz_segments.append(response.df)
    dapcz_segments = pd.concat(dapcz_segments)
    return list(dapcz_segments["SEGMENT_ID"])


def retrieve_row_inspector_segments_by_segment(geom, gis, inspector_zones):
    """
    Tags roadway segments with appropriate ROW inspector zone.
    If a segment is in multiple, only the last ROW inspector zone is used.
    """
    filter = intersects(geom)
    response = inspector_zones.query(
        geometry_filter=filter,
        out_fields="ROW_INSPECTOR_ZONE_ID",
        return_geometry=False,
        gis=gis,
    )
    if response.features:
        # It is possible a segment could intersect multiple zones, but we only take the first one.
        return response.features[0].attributes["ROW_INSPECTOR_ZONE_ID"]
    return None


def road_class_scoring(row):
    """
    Our score criteria: Critical = 10, Arterial = 7, Collector = 5, Residential = 3

    1, 2, 4 =  10 (Interstate, US and State Highways, Major Arterials)
    5 = 7 (Minor arterials)
    8 = 5 (city collector)
    else = 3 (local city/county streets, whatever else)

    """
    if row["ROAD_CLASS"] in (1, 2, 4):
        return 10
    elif row["ROAD_CLASS"] == 5:
        return 7
    elif row["ROAD_CLASS"] == 8:
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

    # number of segments scoring:
    permits = number_of_segments_scoring(permits, segments)

    # permit duration scoring:
    permits["duration_scoring"] = permits.apply(duration_scoring, axis=1)

    # downloading road geometry data from AGOL
    gis = GIS(
        "https://austin.maps.arcgis.com", username=AGOL_USERNAME, password=AGOL_PASSWORD
    )
    logger.info("Retrieving road segment data from AGOL...")
    segments = retrieve_road_segment_data(segments, gis)

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
    permits = permits.merge(row_inspector_zones, left_on="FOLDERRSN", right_index=True, how="left")

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

    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    logger.info(f"Replacing data in Socrata dataset: datahub.austintexas.gov/d/{DATASET}")
    response = df_to_socrata_dataset(soda, DATASET, permits, method="replace")
    logger.info(response)


logger = get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main()
