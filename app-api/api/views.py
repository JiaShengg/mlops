import pandas as pd
from typing import Any, List

import boto3
from io import BytesIO
from fastapi import APIRouter, HTTPException

from api import schemas
from api.config import get_settings

# Create an S3 client using AWS credentials
aws_access_key_id = str(get_settings().AWS_ACCESS_KEY_ID)
aws_secret_access_key = str(get_settings().AWS_SECRET_ACCESS_KEY)
aws_region = str(get_settings().AWS_DEFAULT_REGION
)


s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

api_router = APIRouter()


@api_router.get("/health", response_model=schemas.Health, status_code=200)
def health() -> dict:
    """
    Health check endpoint.
    """
    health_data = schemas.Health(
        name=get_settings().PROJECT_NAME, api_version=get_settings().VERSION
    )

    return health_data.dict()


@api_router.get(
    "/consumer_type_values", response_model=schemas.UniqueConsumerType, status_code=200
)
def consumer_type_values() -> List:
    """
    Retrieve unique consumer types.
    """

    # Download the data from S3
    bucket_name = get_settings().AWS_BUCKET 
    blob_name = "X.parquet"
    response = s3_client.get_object(Bucket=bucket_name, Key=blob_name)
    data = response['Body'].read()
    X = pd.read_parquet(BytesIO(data))

    unique_consumer_type = list(X.index.unique(level="consumer_type"))

    return {"values": unique_consumer_type}


@api_router.get("/area_values", response_model=schemas.UniqueArea, status_code=200)
def area_values() -> List:
    """
    Retrieve unique areas.
    """

    # Download the data from AWS.
    bucket_name = get_settings().AWS_BUCKET 
    blob_name = "X.parquet"
    response = s3_client.get_object(Bucket=bucket_name, Key=blob_name)
    data = response['Body'].read()
    X = pd.read_parquet(BytesIO(data))

    unique_area = list(X.index.unique(level="area"))

    return {"values": unique_area}


@api_router.get(
    "/predictions/{area}/{consumer_type}",
    response_model=schemas.PredictionResults,
    status_code=200,
)
async def get_predictions(area: int, consumer_type: int) -> Any:
    """
    Get forecasted predictions based on the given area and consumer type.
    """

    # Download the data from S3
    bucket_name = get_settings().AWS_BUCKET

    # Read train_df and preds_df from S3
    train_blob_name = "y.parquet"
    preds_blob_name = "predictions.parquet"
    train_response = s3_client.get_object(Bucket=bucket_name, Key=train_blob_name)
    preds_response = s3_client.get_object(Bucket=bucket_name, Key=preds_blob_name)

    # Convert the S3 object contents to DataFrames
    train_data = train_response['Body'].read()
    preds_data = preds_response['Body'].read()
    train_df = pd.read_parquet(BytesIO(train_data))
    preds_df = pd.read_parquet(BytesIO(preds_data))

    # Query the data for the given area and consumer type.
    try:
        train_df = train_df.xs((area, consumer_type), level=["area", "consumer_type"])
        preds_df = preds_df.xs((area, consumer_type), level=["area", "consumer_type"])
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for the given area and consumer type: {area}, {consumer_type}",
        )

    if len(train_df) == 0 or len(preds_df) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for the given area and consumer type: {area}, {consumer_type}",
        )

    # Return only the latest week of observations.
    train_df = train_df.sort_index().tail(24 * 7)

    # Prepare data to be returned.
    datetime_utc = train_df.index.get_level_values("datetime_utc").to_list()
    energy_consumption = train_df["energy_consumption"].to_list()

    preds_datetime_utc = preds_df.index.get_level_values("datetime_utc").to_list()
    preds_energy_consumption = preds_df["energy_consumption"].to_list()

    results = {
        "datetime_utc": datetime_utc,
        "energy_consumption": energy_consumption,
        "preds_datetime_utc": preds_datetime_utc,
        "preds_energy_consumption": preds_energy_consumption,
    }

    return results


@api_router.get(
    "/monitoring/metrics",
    response_model=schemas.MonitoringMetrics,
    status_code=200,
)
async def get_metrics() -> Any:
    """
    Get monitoring metrics.
    """

    # Download the data from S3
    bucket_name = get_settings().AWS_BUCKET
    metrics_blob_name = "metrics_monitoring.parquet"
    metrics_response = s3_client.get_object(Bucket=bucket_name, Key=metrics_blob_name)

    # Convert the S3 object contents to a DataFrame
    metrics_data = metrics_response['Body'].read()
    metrics_df = pd.read_parquet(BytesIO(metrics_data))

    datetime_utc = metrics_df.index.to_list()
    mape = metrics_df["MAPE"].to_list()

    return {
        "datetime_utc": datetime_utc,
        "mape": mape,
    }


@api_router.get(
    "/monitoring/values/{area}/{consumer_type}",
    response_model=schemas.MonitoringValues,
    status_code=200,
)
async def get_predictions(area: int, consumer_type: int) -> Any:
    """
    Get forecasted predictions based on the given area and consumer type.
    """

    # Download the data from S3
    bucket_name = get_settings().AWS_BUCKET
    y_monitoring_blob_name = "y_monitoring.parquet"
    predictions_monitoring_blob_name = "predictions_monitoring.parquet"
    y_monitoring_response = s3_client.get_object(Bucket=bucket_name, Key=y_monitoring_blob_name)
    predictions_monitoring_response = s3_client.get_object(Bucket=bucket_name, Key=predictions_monitoring_blob_name)

    # Convert the S3 object contents to DataFrames
    y_monitoring_data = y_monitoring_response['Body'].read()
    predictions_monitoring_data = predictions_monitoring_response['Body'].read()
    y_monitoring = pd.read_parquet(BytesIO(y_monitoring_data))
    predictions_monitoring = pd.read_parquet(BytesIO(predictions_monitoring_data))

    # Query the data for the given area and consumer type.
    try:
        y_monitoring = y_monitoring.xs(
            (area, consumer_type), level=["area", "consumer_type"]
        )
        predictions_monitoring = predictions_monitoring.xs(
            (area, consumer_type), level=["area", "consumer_type"]
        )
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for the given area and consumer typefrontend: {area}, {consumer_type}",
        )

    if len(y_monitoring) == 0 or len(predictions_monitoring) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for the given area and consumer type: {area}, {consumer_type}",
        )

    # Prepare data to be returned.
    y_monitoring_datetime_utc = y_monitoring.index.get_level_values(
        "datetime_utc"
    ).to_list()
    y_monitoring_energy_consumption = y_monitoring["energy_consumption"].to_list()

    predictions_monitoring_datetime_utc = predictions_monitoring.index.get_level_values(
        "datetime_utc"
    ).to_list()
    predictions_monitoring_energy_consumptionc = predictions_monitoring[
        "energy_consumption"
    ].to_list()

    results = {
        "y_monitoring_datetime_utc": y_monitoring_datetime_utc,
        "y_monitoring_energy_consumption": y_monitoring_energy_consumption,
        "predictions_monitoring_datetime_utc": predictions_monitoring_datetime_utc,
        "predictions_monitoring_energy_consumptionc": predictions_monitoring_energy_consumptionc,
    }

    return results