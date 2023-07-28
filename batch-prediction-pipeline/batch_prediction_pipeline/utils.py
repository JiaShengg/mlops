import json
import logging
import joblib
import pandas as pd
import boto3

from io import BytesIO
from pathlib import Path
from typing import Optional, Union
from botocore.exceptions import ClientError


from batch_prediction_pipeline import settings


def get_logger(name: str) -> logging.Logger:
    """
    Template for getting a logger.

    Args:
        name: Name of the logger.

    Returns: Logger.
    """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)

    return logger


def load_model(model_path: Union[str, Path]):
    """
    Template for loading a model.

    Args:
        model_path: Path to the model.

    Returns: Loaded model.
    """

    return joblib.load(model_path)


def save_json(data: dict, file_name: str, save_dir: str = settings.OUTPUT_DIR):
    """
    Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_name: Name of the JSON file.
        save_dir: Directory to save the JSON file.

    Returns: None
    """

    data_path = Path(save_dir) / file_name
    with open(data_path, "w") as f:
        json.dump(data, f)


def load_json(file_name: str, save_dir: str = settings.OUTPUT_DIR) -> dict:
    """
    Load a JSON file.

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: Dictionary with the data.
    """

    data_path = Path(save_dir) / file_name
    with open(data_path, "r") as f:
        return json.load(f)



def get_bucket(
    bucket_name: str=settings.SETTINGS["S3_CLOUD_BUCKET_NAME"],
):
    """Get an AWS S3 bucket.

    This function returns an AWS S3 bucket that can be used to upload and download
    files from Amazon S3.

    Args:
        bucket_name : str
            The name of the bucket to connect to.

    Returns:
        # boto3.resources.factory.s3.Bucket
        #     An AWS S3 bucket that can be used to upload and download files from Amazon S3.
        bucket name only
    """

    # s3_resource = boto3.resource('s3')
    # bucket = s3_resource.Bucket(bucket_name)

    return bucket_name

# def get_bucket(
#     bucket_name: str = settings.SETTINGS["GOOGLE_CLOUD_BUCKET_NAME"],
#     bucket_project: str = settings.SETTINGS["GOOGLE_CLOUD_PROJECT"],
#     json_credentials_path: str = settings.SETTINGS[
#         "GOOGLE_CLOUD_SERVICE_ACCOUNT_JSON_PATH"
#     ],
# ) -> storage.Bucket:
#     """Get a Google Cloud Storage bucket.

#     This function returns a Google Cloud Storage bucket that can be used to upload and download
#     files from Google Cloud Storage.

#     Args:
#         bucket_name : str
#             The name of the bucket to connect to.
#         bucket_project : str
#             The name of the project in which the bucket resides.
#         json_credentials_path : str
#             Path to the JSON credentials file for your Google Cloud Project.

#     Returns
#         storage.Bucket
#             A storage bucket that can be used to upload and download files from Google Cloud Storage.
#     """

#     storage_client = storage.Client.from_service_account_json(
#         json_credentials_path=json_credentials_path,
#         project=bucket_project,
#     )
#     bucket = storage_client.bucket(bucket_name=bucket_name)

#     return bucket

def write_blob_to(bucket_name: str, blob_name: str, data: pd.DataFrame):
    """Write a dataframe to an S3 bucket as a parquet file.

    Args:
        bucket_name (str): The name of the S3 bucket to write to.
        blob_name (str): The name of the blob to write to. Must be a parquet file.
        data (pd.DataFrame): The dataframe to write to S3.
    """

    # Configure AWS credentials and region
    aws_access_key_id = settings.SETTINGS["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = settings.SETTINGS["AWS_SECRET_ACCESS_KEY"]
    aws_region = settings.SETTINGS["AWS_DEFAULT_REGION"]

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

    # Convert DataFrame to bytes
    parquet_data = data.to_parquet(index=True)

    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=blob_name, Body=parquet_data)


# def read_blob_from(bucket: storage.Bucket, blob_name: str) -> Optional[pd.DataFrame]:
#     """Reads a blob from a bucket and returns a dataframe.

#     Args:
#         bucket: The bucket to read from.
#         blob_name: The name of the blob to read.

#     Returns:
#         A dataframe containing the data from the blob.
#     """

#     blob = bucket.blob(blob_name=blob_name)
#     if not blob.exists():
#         return None

#     with blob.open("rb") as f:
#         return pd.read_parquet(f)

def read_blob_from(bucket_name: str, blob_name: str) -> Optional[pd.DataFrame]:
    """Reads a blob from an S3 bucket and returns a dataframe.

    Args:
        bucket_name (str): The name of the S3 bucket to read from.
        blob_name (str): The name of the blob to read.

    Returns:
        A dataframe containing the data from the blob, or None if the blob doesn't exist.
    """

    # Configure AWS credentials and region
    aws_access_key_id = settings.SETTINGS["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = settings.SETTINGS["AWS_SECRET_ACCESS_KEY"]
    aws_region = settings.SETTINGS["AWS_DEFAULT_REGION"]

    s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    
    bucket = s3_resource.Bucket(bucket_name)

    try:
        # Read the Parquet file and return as DataFrame
        response = bucket.Object(blob_name).get()
        data = response['Body'].read()
        dataframe = pd.read_parquet(BytesIO(data))
        return dataframe
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            raise