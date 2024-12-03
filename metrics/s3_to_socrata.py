"""
Uploads a CSV file stored in an S3 Bucket to a desired dataset 
in Socrata AKA the open data portal AKA city datahub
"""
import argparse
import boto3
import logging
from sodapy import Socrata
import numpy as np

import os

from utils import df_to_socrata_dataset, get_logger, s3_csv_to_df
from socrata_config import DATASETS

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")

DATASET = os.getenv("SO_DATASET")


def main(args):
    dataset = DATASETS[args.dataset]

    logger.info(f"Downloading csv file from S3: {dataset['file_name']}")
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )
    df = s3_csv_to_df(s3, BUCKET, dataset["file_name"])
    # replacing NaN's with None (Socrata doesn't like)
    df = df.replace(np.nan, None)

    if "sodapy_method" not in dataset:
        method = None
    else:
        method = dataset["sodapy_method"]

    logger.info(
        f"Uploading to dataset: datahub.austintexas.gov/d/{dataset['resource_id']}, method: {method}"
    )

    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=60,
    )

    response = df_to_socrata_dataset(soda, dataset["resource_id"], df, method=method)
    logger.info(response)


# CLI argument definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--dataset",
    choices=list(DATASETS.keys()),
    required=True,
    help="Name of the dataset defined in socrata_config.py. Ex: license_agreements_timeline",
)

args = parser.parse_args()

logger = get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main(args)
