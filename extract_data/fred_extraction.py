import sys
import json
import time
import requests
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(
    sys.argv,
    [
        "FRED_API_KEY",
        "S3_BUCKET",
        "OBSERVATION_START",
        "OBSERVATION_END",
        "RUN_DATE",
        "AWS_REGION"
    ]
)

API_KEY = args["FRED_API_KEY"]
BUCKET_NAME = args["S3_BUCKET"]
OBSERVATION_START = args["OBSERVATION_START"]
OBSERVATION_END = args["OBSERVATION_END"]
RUN_DATE = args["RUN_DATE"]
AWS_REGION = args["AWS_REGION"]

# setting FRED API endpoint
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# define which FRED series to download
SERIES = {
    "UNRATE": "unemployment_rate",
    "CPIAUCSL": "consumer_price_index",
    "FEDFUNDS": "federal_funds_rate",
    "GDP": "gross_domestic_product"
}

# s3 folder names
RAW_PREFIX = f"raw/fred/run_date={RUN_DATE}/"
PROCESSED_PREFIX = f"processed/fred/run_date={RUN_DATE}/"

# create s3 client object
s3 = boto3.client("s3", region_name=AWS_REGION)

# helper functions
def validate_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Connected to bucket: {bucket_name}")
    except ClientError as e:
        raise Exception(f"Cannot access bucket: {e}")


def fetch_fred_series(series_id):
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": OBSERVATION_START,
        "observation_end": OBSERVATION_END,
        "sort_order": "asc"
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def upload_json(key, data):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )


def upload_csv(key, df):
    csv_data = df.to_csv(index=False)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_data,
        ContentType="text/csv"
    )


def process_dataframe(data, series_id, series_name):
    df = pd.DataFrame(data.get("observations", []))

    if df.empty:
        return df

    df = df[["date", "value"]]
    df["series_id"] = series_id
    df["series_name"] = series_name

    # Cleaning
    df["value"] = df["value"].replace(".", pd.NA)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df


def main():
    print("Starting Glue FRED ingestion job...")

    validate_bucket(BUCKET_NAME)

    all_data = []

    for series_id, series_name in SERIES.items():
        print(f"Fetching {series_id}...")

        try:
            data = fetch_fred_series(series_id)

            # Upload raw JSON
            raw_key = f"{RAW_PREFIX}{series_id}.json"
            upload_json(raw_key, data)

            # Transform
            df = process_dataframe(data, series_id, series_name)

            if df.empty:
                print(f"No data for {series_id}")
                continue

            # Upload processed CSV
            processed_key = f"{PROCESSED_PREFIX}{series_id}.csv"
            upload_csv(processed_key, df)

            all_data.append(df)

            time.sleep(0.5)

        except Exception as e:
            print(f"Error processing {series_id}: {e}")

    if not all_data:
        raise Exception("No data fetched")

    # Combine datasets
    combined = pd.concat(all_data)

    # Upload long format
    upload_csv(
        f"{PROCESSED_PREFIX}combined_long.csv",
        combined
    )

    # Create wide format
    wide = combined.pivot_table(
        index="date",
        columns="series_id",
        values="value",
        aggfunc="first"
    ).reset_index()

    wide.columns.name = None

    upload_csv(
        f"{PROCESSED_PREFIX}combined_wide.csv",
        wide
    )

    print("Glue ingestion job completed successfully.")


if __name__ == "__main__":
    main()