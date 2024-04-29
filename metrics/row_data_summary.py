"""
Summarizes data CSVs for ROW permits stored in S3 and publishes it to Socrata
"""
import boto3
import pandas as pd
from sodapy import Socrata

import os


# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")
DATASET = os.getenv("WEEK_DATASET")

FILES = [
    {
        "name": "Applications Received",
        "fname": "applications_received.csv",
        "date_col": "TO_CHAR(ROUND(INDATE,'DDD'),'YYYY-MM-DD')",
        "count_col": "ISSUEDROWPERMITS",
        "summary_cols": ["DS", "EX", "RW"],
    },
    {
        "name": "Permits Issued",
        "fname": "issued_permits.csv",
        "date_col": "TO_CHAR(ROUND(ISSUEDATE,'DDD'),'YYYY-MM-DD')",
        "count_col": "ISSUEDROWPERMITS",
        "summary_cols": ["DS", "EX", "RW"],
    },
    {
        "name": "Commercial DS Permit Requests",
        "fname": "Commercial DS Permit Requests.csv",
        "date_col": "Date Created",
        "summary_cols": "Count Permits",
    },
    {
        "name": "Extension Requests",
        "fname": "Extension and Revision Requests.csv",
        "date_col": "Created",
        "summary_cols": "Count Permits",
    },
    {
        "name": "Residential DS Permit Requests",
        "fname": "Residential DS Permits.csv",
        "date_col": "Date Created",
        "summary_cols": "Count Permits",
    },
]


def s3_to_df(s3, filename):
    response = s3.get_object(Bucket=BUCKET, Key=filename)
    return pd.read_csv(response.get("Body"))


def summarize_weekly(dfs):
    """
    Groups ROW counts by week and returns the count for each type
    Parameters
    ----------
    dfs (list of dicts): Pandas dataframes stored in 'data' and metadata

    Returns
    ----------
    output (dataframe): A dataframe where each row is one week and the
                        columns are the totals for each type of ROW permit

    """
    weekly = []
    for file in dfs:
        df = file["data"]
        # Some data from AMANDA we want to summarize by FOLDERTYPE (DS, EX, RW)
        if "FOLDERTYPE" in df.columns:
            df = pd.pivot_table(
                df,
                index=file["date_col"],
                columns="FOLDERTYPE",
                values=file["count_col"],
                aggfunc="sum",
            )
            df = df.reset_index()
        # Convert date column to datetime type
        df[file["date_col"]] = pd.to_datetime(df[file["date_col"]])
        # Grouping by week (starting on sunday)
        week = df.resample("W-SAT", on=file["date_col"])[file["summary_cols"]].sum()
        week = pd.DataFrame(week)
        week["Measure"] = file["name"]
        weekly.append(week)

    # Combining dataframes
    output = pd.concat(weekly)
    output = output.reset_index()
    output = pd.pivot_table(output, index="index", columns="Measure")
    # Renaming columns to "FOLDERTYPE Measure"
    output.columns = output.columns.map(" ".join)
    output = output.fillna(0)

    # Adding smartsheet data to AMANDA Totals
    output["DS Applications Received"] = (
        output["DS Applications Received"]
        + output["Count Permits Commercial DS Permit Requests"]
        + output["Count Permits Residential DS Permit Requests"]
    )
    # Drop smartsheet columns
    output = output.drop(
        columns=[
            "Count Permits Commercial DS Permit Requests",
            "Count Permits Residential DS Permit Requests",
        ]
    )
    output = output.reset_index()
    output["index"] = output["index"].dt.strftime("%Y-%m-%d")
    output["index"] = output["index"] + "T00:00:00.000"
    output = output.rename(
        columns={
            "Count Permits Extension Requests": "Extension Requests",
            "index": "date",
        }
    )
    return output


def socrata_columns(df):
    # Renames a dataframe's columns to align with what Socrata is expecting
    df.columns = [c.lower() for c in df.columns]
    df.columns = [c.replace(" ", "_") for c in df.columns]
    return df


def df_to_socrata(soda, df):
    """
    Replaces all the data in the socrata dataset with data in the dataframe.

    Parameters
    ----------
    soda: sodapy client object
    df : Pandas Dataframe

    """
    payload = df.to_dict("records")
    soda.replace(DATASET, payload)


def main():
    s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )
    soda = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_KEY,
        password=SO_SECRET,
        timeout=500,
    )

    # Load in data from S3
    dfs = []
    for f in FILES:
        row = f
        row["data"] = s3_to_df(s3_client, f["fname"])
        dfs.append(row)

    # Create a weekly summary of the data
    weekly = summarize_weekly(dfs)
    # Cleanup column names
    weekly = socrata_columns(weekly)
    # Upload to socrata
    df_to_socrata(soda, weekly)


if __name__ == "__main__":
    main()
