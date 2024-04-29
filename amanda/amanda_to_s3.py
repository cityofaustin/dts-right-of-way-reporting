"""
Queries the AMANDA read-replica DB and sents the result as a CSV in S3
"""

import argparse
from io import StringIO
import os
import logging

import oracledb as cx_Oracle
import pandas as pd
import boto3

import utils
from queries import QUERIES

# AMANDA RR DB Credentials
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
SERVICE_NAME = os.getenv("SERVICE_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

# AWS Credentials
AWS_ACCESS_ID = os.getenv("EXEC_DASH_ACCESS_ID")
AWS_PASS = os.getenv("EXEC_DASH_PASS")
BUCKET = os.getenv("BUCKET_NAME")

"""
Queries:

applications_received: gets the count of permits requested grouped by type, date, and subcode. Additional filtering is added to 
remove subcodes that are not used by the ROW team.

active_permits: gets the count of the number of permits that are currently active for each type of permit. Does not include
those DS or EX permits that start with LA- since those are also not part of ROW division's work. This should be treated 
as a snapshot in time as permits will enter and leave the active status all the time.

issued_permits: similar to applications_received but is now for counting those permits that were actually issued.

"""


def get_conn():
    """
    Get connected to the AMANDA Read replica database

    Returns
    -------
    cx_Oracle Connection Object

    """
    dsn_tns = cx_Oracle.makedsn(HOST, PORT, service_name=SERVICE_NAME)
    return cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)


def row_factory(cursor):
    """
    Define cursor row handler which returns each row as a dict
    h/t https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary

    Parameters
    ----------
    cursor : cx_Oracle Cursor object

    Returns
    -------
    function: the rowfactory.

    """
    return lambda *args: dict(zip([d[0] for d in cursor.description], args))


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
    df.to_csv(csv_buffer, index=False)
    resource.Object(BUCKET, f"{filename}.csv").put(Body=csv_buffer.getvalue())


def main(args):
    # Connect to AMANDA RR DB
    conn = get_conn()
    cursor = conn.cursor()

    # Execute our query
    logger.info(f"Executing query: {args.query}")
    cursor.execute(QUERIES[args.query])
    cursor.rowfactory = row_factory(cursor)
    rows = cursor.fetchall()
    conn.close()

    # Upload to S3
    logger.info(f"Uploading {len(rows)} rows to S3")
    s3_resource = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )
    df = pd.DataFrame(rows)
    df_to_s3(df, s3_resource, args.query)


# CLI argument definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--query",
    choices=list(QUERIES.keys()),
    required=True,
    help="Name of the query defined by the dict at the top of this script. Ex: applications_received",
)

args = parser.parse_args()

logger = utils.get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main(args)
