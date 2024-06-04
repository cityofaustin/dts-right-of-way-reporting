"""
Uploads a CSV file stored in an S3 Bucket to a desired dataset 
in Socrata AKA the open data portal AKA city datahub
"""
import argparse
import boto3
import csv
import logging
from sodapy import Socrata
import io

import os

import utils
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


def download_csv(file_name):
    """
    downloads the CSV file from S3 and returns it as a list of dictonaries
    """
    logger.info(f"Downloading csv file from S3: {file_name}")
    s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )

    response = s3_client.get_object(Bucket=BUCKET, Key=file_name)
    lines = response["Body"].read().decode("utf-8")
    buf = io.StringIO(lines)
    reader = csv.DictReader(buf)
    return list(reader)

def cleanup_empty_strings(data):
    """
    Replaces emptry string with None in our list of dictonaries
    """
    for row in data:
        for key in row:
            if row[key] == "":
                row[key] = None

    return data

def upload_to_socrata(payload, dataset, method="upsert"):
    """
    uploads a list of dictionaries to a socrata dataset
    """
    logger.info(
        f"Uploading to dataset: datahub.austintexas.gov/d/{dataset}, method: {method}"
    )
    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=60,
    )

    if method == "upsert":
        res = soda.upsert(dataset, payload)
    elif method == "replace":
        res = soda.replace(dataset, payload)

    return res


def main(args):
    dataset = DATASETS[args.dataset]

    data = download_csv(dataset["file_name"])
    data = cleanup_empty_strings(data)

    if "sodapy_method" not in dataset:
        method = None
    else:
        method = dataset["sodapy_method"]

    response = upload_to_socrata(data, dataset["resource_id"], method=method)
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

logger = utils.get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main(args)
