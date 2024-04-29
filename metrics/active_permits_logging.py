"""
Get the currently active permits data and publish it to a
socrata dataset that is a rolling log of currently active permits.
"""

import boto3
from sodapy import Socrata
import pandas as pd
import pytz

import os

tz = "US/Central"

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("ACTIVE_DATASET")


def s3_to_df(s3, filename):
    """
    Returns a dataframe of the file from S3 and
        a string formatted datetime when the file was last modified

    Parameters
    ----------
    s3 : boto3 S3 client object
    filename (str): name of the file to access in the S3 bucket

    Returns
    ----------
    (dataframe) : dataframe of the csv file stored in S3
    (str) : string of the date/time the file was last modified
    """
    response = s3.get_object(Bucket=BUCKET, Key=filename)
    return (
        pd.read_csv(response.get("Body")),
        response["LastModified"].astimezone(pytz.timezone(tz)).strftime("%Y-%m-%dT%H:%M:00.000"),
    )

def prepare_data(df, modified_date):
    # Rotate our dataframe to have the columns be the permit type
    df = pd.pivot_table(df, columns="FOLDERTYPE")
    # Create published date column
    df['published_date'] = modified_date
    return df

def socrata_columns(df):
    # Renames a dataframe's columns to align with what Socrata is expecting
    df.columns = [c.lower() for c in df.columns]
    df.columns = [c.replace(" ", "_") for c in df.columns]
    return df

def df_to_socrata(soda, df):
    """
    Upserts the data in the socrata dataset with data in the dataframe. Must have a row identifier created.

    Parameters
    ----------
    soda: sodapy client object
    df : Pandas Dataframe

    """
    payload = df.to_dict("records")
    soda.upsert(DATASET, payload)

def main():
    s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )
    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_KEY, password=SO_SECRET, timeout=500,)

    # Get data from S3 bucket and the time it was published
    df, time = s3_to_df(s3_client, "active_permits.csv")
    # Get our data in the right shape
    df = prepare_data(df, time)
    # Format columns
    df = socrata_columns(df)
    # Upsert to socrata
    df_to_socrata(soda, df)

if __name__ == "__main__":
    main()
