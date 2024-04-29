"""
Downloads a Smartsheet and uploads a summary of the permit count by date to S3
"""

import boto3
import pandas as pd
import smartsheet

import tempfile
from io import StringIO
import os

from sheets import FILES

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")


def download_file(smart, id, temp_dir, name):
    smart.Sheets.get_sheet_as_csv(
        id, download_path=temp_dir.name, alternate_file_name=f"{name}.csv",
    )


def df_groupby_date(df, date_column):
    # Returns the count of the number of permits for each date in our dataframe
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.groupby([df[date_column].dt.date])[date_column].count()
    df = pd.DataFrame(df)
    df = df.rename(columns={date_column: "Count Permits"})
    return df


def df_to_s3(df, resource, filename):
    """
    Send pandas dataframe to an S3 bucket as a CSV
    h/t https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python

    Parameters
    ----------
    df : Pandas Dataframe
    resource : boto3 s3 resource
    filename : String of the file that will be created in the S3 bucket ex:

    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=True)
    resource.Object(BUCKET, f"{filename}.csv").put(Body=csv_buffer.getvalue())


def main():
    # Create a temporary directory where we will store the data from smartsheet
    temp_dir = tempfile.TemporaryDirectory()

    smart = smartsheet.Smartsheet()
    s3_resource = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )

    # Download the sheet and then send to s3
    for f in FILES:
        download_file(smart, f["id"], temp_dir, f["name"])
        df = pd.read_csv(f"{temp_dir.name}/{f['name']}.csv")
        df = df_groupby_date(df, f["date_column"])
        df_to_s3(df, s3_resource, f["name"])

    # Delete temporary directory
    temp_dir.cleanup()

if __name__ == "__main__":
    main()
