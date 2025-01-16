from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry.filters import intersects
from sodapy import Socrata

import argparse
import datetime
import os
import logging

from config import (
    SEGMENTS_FEATURE_SERVICE_URL,
    DAPCZ_FEATURE_SERVICE_URL,
    ROW_INSPECTOR_SERVICE_URL,
)
from utils import get_logger


# AGOL Credentials
AGOL_USERNAME = os.getenv("AGOL_USERNAME")
AGOL_PASSWORD = os.getenv("AGOL_PASSWORD")

# Socrata Credentials
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("SEGMENT_DATASET")


def batch_list(data, batch_size=100):
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]


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
        # It is possible a segment could intersect multiple ROW inspector zones, but we only take the first one.
        return response.features[0].attributes["ROW_INSPECTOR_ZONE_ID"]
    return None


def retrieve_dapcz_segments_by_segment(geom, gis, dapcz_features):
    """
    Tags roadway segments with appropriate ROW inspector zone.
    If a segment is in multiple, only the last ROW inspector zone is used.
    """
    filter = intersects(geom)
    response = dapcz_features.query(
        geometry_filter=filter,
        out_fields="DAPCZ_ZONES_ID",
        return_geometry=False,
        gis=gis,
    )
    if response.features:
        # It is possible a segment could intersect DAPCZ zones, but we only take the first one.
        return response.features[0].attributes["DAPCZ_ZONES_ID"]
    return None


def main(args):
    gis = GIS(
        "https://austin.maps.arcgis.com", username=AGOL_USERNAME, password=AGOL_PASSWORD
    )
    segments_layer = FeatureLayer(SEGMENTS_FEATURE_SERVICE_URL, gis)
    inspector_zones = FeatureLayer(ROW_INSPECTOR_SERVICE_URL, gis)
    dapcz_features = FeatureLayer(DAPCZ_FEATURE_SERVICE_URL, gis)

    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    # We search for new segment updates in the last 30 days
    search_date = datetime.datetime.now() - datetime.timedelta(days=30)
    formatted_time = search_date.strftime("%Y-%m-%d %H:%M:%S")

    if args.replace:
        # If we need to replace all the data:
        formatted_time = "1970-01-01 00:00:00"
        res = soda.replace(DATASET, [])

    # Retrieving segment data from AGOL
    query_result = segments_layer.query(
        where=f"MODIFIED_DATE >= TIMESTAMP '{formatted_time}'",
        out_fields="SEGMENT_ID, FULL_STREET_NAME, ROAD_CLASS, MODIFIED_DATE",
        return_geometry=True,
    )
    segments = query_result.df.to_dict("records")

    if not segments:
        logger.info("No recently updated segments, did nothing.")
        return 0
    logger.info(f"Tagging {len(segments)} recently updated segments.")
    segment_batches = batch_list(segments)

    for batch in segment_batches:
        for segment in batch:
            # Tag with inspector zone
            segment["INSPECTOR_ZONE"] = retrieve_row_inspector_segments_by_segment(
                geom=segment["SHAPE"], gis=gis, inspector_zones=inspector_zones
            )
            # Tag with DAPCZ zone
            segment["DAPCZ_ZONE"] = retrieve_dapcz_segments_by_segment(
                geom=segment["SHAPE"], gis=gis, dapcz_features=dapcz_features
            )
            # Removing unwanted fields for Socrata export
            segment.pop("SHAPE", None)
            segment.pop("MODIFIED_DATE", None)
            segment.pop("OBJECTID", None)
        res = soda.upsert(DATASET, batch)
        logger.info(f"Batch uploaded to Socrata with result: {res}")


# CLI argument definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "-r",
    "--replace",
    action="store_true",
    required=False,
    help="Provide --replace to truncate and replace existing segment data stored in socrata",
)

args = parser.parse_args()

logger = get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main(args)
