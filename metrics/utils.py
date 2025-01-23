import logging
import sys
import pandas as pd


def get_logger(name, level):
    """Return a module logger that streams to stdout"""
    logger = logging.getLogger(name)
    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def s3_csv_to_df(s3, bucket, filename):
    """Returns a pandas dataframe from a CSV stored in an S3 bucket"""
    response = s3.get_object(Bucket=bucket, Key=filename)
    return pd.read_csv(response.get("Body"))


def df_to_socrata_dataset(soda, dataset_id, df, method="upsert"):
    """
    Upserts the data in the socrata dataset with data in the dataframe. Must have a row identifier created.

    Parameters
    ----------
    method: if set to "replace" it will replace the overwrite dataset each time this method is called
    dataset_id: resource ID of the dataset
    soda: sodapy client object
    df : Pandas Dataframe

    """
    payload = df.to_dict("records")
    if method == "replace":
        response = soda.replace(dataset_id, payload)
    else:
        response = soda.upsert(dataset_id, payload)
    return response
