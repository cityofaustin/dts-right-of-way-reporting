import boto3
import pandas as pd
import numpy as np
from sodapy import Socrata
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry.filters import intersects

import os

from config import DAPCZ_SHAPE, SEGMENTS_FEATURE_SERVICE_URL, DAPCZ_FEATURE_SERVICE_URL, ROW_INSPECTOR_SERVICE_URL

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Credentials
SO_TOKEN = os.getenv("SO_TOKEN")

# AGOL Credentials
AGOL_USERNAME = os.getenv("AGOL_USERNAME")
AGOL_PASSWORD = os.getenv("AGOL_PASSWORD")

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


def retrieve_road_segment_data(segments, gis):
    segments_layer = FeatureLayer(SEGMENTS_FEATURE_SERVICE_URL, gis)
    segment_ids = list(segments["PROPERTYRSN"].unique())
    segment_batches = batch_list(segment_ids)
    segment_data = []
    for segment_batch in segment_batches:
        segment_batch = ", ".join(map(str, segment_batch))
        query_result = segments_layer.query(where=f"segment_id in ({segment_batch})", out_fields="*",
                                            return_geometry=True)
        segment_data.append(query_result.df)
    segment_data = pd.concat(segment_data)
    segments = segments.merge(
        segment_data, left_on="PROPERTYRSN", right_on="SEGMENT_ID", how="inner"
    )

    # Flagging segments if they are in the DAPCZ for later scoring
    dapcz_segments = retrieve_dapcz_segments(gis, segments_layer)
    segments["is_dapcz"] = segments["SEGMENT_ID"].isin(dapcz_segments)

    # Tagging segments with the appropriate ROW inspector zone
    inspector_zone_mapping = retrieve_row_inspector_segments(gis, segments_layer)
    segments["row_inspector_zone"] = segments["SEGMENT_ID"].map(inspector_zone_mapping)

    return segments


def retrieve_dapcz_segments(gis, segments_layer):
    """
    Gets the list of street segments within the Downtown Project Coordination Zone (DAPCZ).
    DAPCZ_SHAPE is an approximation of a more detailed geometry.
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


def retrieve_row_inspector_segments(gis, segments_layer):
    """
    Tags roadway segments with appropriate ROW inspector zone.
    If a segment is in multiple, only the last ROW inspector zone is used.
    """
    dapcz_features = FeatureLayer(ROW_INSPECTOR_SERVICE_URL, gis)
    dapcz_features = dapcz_features.query(where="1=1", return_geometry=True)
    inspector_zone_segments = {}
    for polygon in dapcz_features.features:
        zone_id = polygon.attributes["ROW_INSPECTOR_ZONE_ID"]
        filter = intersects(polygon.geometry)
        response = segments_layer.query(geometry_filter=filter, out_fields="SEGMENT_ID")
        for seg in list(response.df["SEGMENT_ID"]):
            inspector_zone_segments[seg] = zone_id
    return inspector_zone_segments


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
    gis = GIS("https://austin.maps.arcgis.com", username=AGOL_USERNAME, password=AGOL_PASSWORD)
    segments = retrieve_road_segment_data(segments, gis)
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
