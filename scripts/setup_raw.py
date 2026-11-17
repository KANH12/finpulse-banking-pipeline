"""
One-time setup script:
  1. Convert raw CSVs to Parquet (Snappy)
  2. Upload to s3://finpulse-lake/raw/

Run once locally before the pipeline starts.
"""

import sys
import io
import polars as pl
import boto3
from pathlib import Path
from botocore.config import Config
from dotenv import load_dotenv
import os

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET", "finpulse-lake")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    config=Config(retries={"max_attempts": 3, "mode": "adaptive"}),
)

DATA_DIR = Path("data")


def upload_parquet(df: pl.DataFrame, s3_key: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    size_mb = buf.getbuffer().nbytes / 1024 / 1024
    client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.read())
    print(f"Uploaded {s3_key} ({size_mb:.1f} MB, {len(df):,} rows)")


def process_transactions() -> None:
    print("Processing transactions CSV (2.35 GB)...")
    df = pl.read_csv(
        DATA_DIR / "credit_card_transactions-ibm_v2.csv",
        infer_schema_length=500,
    )

    # Build transaction_timestamp from Year/Month/Day/Time columns
    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("Year").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("Month").cast(pl.Utf8).str.zfill(2),
                pl.lit("-"),
                pl.col("Day").cast(pl.Utf8).str.zfill(2),
                pl.lit("T"),
                pl.col("Time"),
                pl.lit(":00"),
            ]
        )
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S", time_zone="UTC")
        .alias("transaction_timestamp")
    )

    # Clean Amount: strip "$" and cast to Float64
    df = df.with_columns(
        pl.col("Amount").str.strip_chars("$").cast(pl.Float64)
    )

    # Rename Errors? → errors, Is Fraud? → is_fraud for cleaner column names
    df = df.rename({"Errors?": "errors", "Is Fraud?": "is_fraud"})

    # Sort by timestamp for efficient batch scanning
    df = df.sort("transaction_timestamp")

    upload_parquet(df, "raw/transactions.parquet")


def process_users() -> None:
    print("Processing users CSV...")
    df = pl.read_csv(DATA_DIR / "sd254_users.csv", infer_schema_length=200)
    upload_parquet(df, "raw/users.parquet")


def process_cards() -> None:
    print("Processing cards CSV...")
    df = pl.read_csv(DATA_DIR / "sd254_cards.csv", infer_schema_length=200)
    upload_parquet(df, "raw/cards.parquet")


def init_state() -> None:
    print("Initializing state.json...")
    state = '{"last_timestamp": null, "total_processed": 0, "last_run": null}'
    client.put_object(
        Bucket=S3_BUCKET,
        Key="state/state.json",
        Body=state.encode("utf-8"),
        ContentType="application/json",
    )
    print("state/state.json initialized")


if __name__ == "__main__":
    print(f"Target bucket: s3://{S3_BUCKET}/")
    print()
    process_users()
    process_cards()
    process_transactions()
    init_state()
    print()
    print("Setup complete.")
