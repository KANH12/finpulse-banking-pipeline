import io
import duckdb
import polars as pl
from datetime import datetime, timezone

from src.config.settings import get_settings
from src.config.aws_client import configure_duckdb_s3, get_s3_client
from src.config.logger import get_logger

log = get_logger("finpulse.lambda3")


def _setup_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    configure_duckdb_s3(con)


def _read_new_silver(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    settings = get_settings()
    result = con.execute(f"""
        SELECT *
        FROM read_parquet('s3://{settings.s3_bucket}/{settings.s3_prefix_silver}*.parquet')
    """).pl()
    log.info("Silver loaded", extra={"rows": len(result)})
    return result


def _build_fact_transactions(silver: pl.DataFrame) -> pl.DataFrame:
    return silver.select([
        "transaction_id",
        "User",
        "Card",
        "transaction_timestamp",
        "ingestion_timestamp",
        "Amount",
        "Use Chip",
        "Merchant Name",
        "Merchant City",
        "Merchant State",
        "MCC",
        "errors",
        "is_fraud",
        "detection_summary",
        "flag_null_field",
        "flag_negative_amount",
        "flag_future_timestamp",
        "flag_high_velocity",
        "flag_subprime",
        "flag_high_cur",
        "cur_ratio",
        "risk_label",
        "tier1_error_code",
        "tier2_error_code",
    ]).with_columns([
        pl.col("transaction_timestamp").dt.year().alias("partition_year"),
        pl.col("transaction_timestamp").dt.month().alias("partition_month"),
    ])


def _build_dim_users(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    settings = get_settings()
    return con.execute(f"""
        SELECT *
        FROM read_parquet('s3://{settings.s3_bucket}/raw/users.parquet')
    """).pl()


def _build_dim_cards(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    settings = get_settings()
    return con.execute(f"""
        SELECT *
        FROM read_parquet('s3://{settings.s3_bucket}/raw/cards.parquet')
    """).pl()


def _build_dim_merchants(silver: pl.DataFrame) -> pl.DataFrame:
    return (
        silver.select(["Merchant Name", "Merchant City", "Merchant State", "MCC"])
        .unique()
        .rename({
            "Merchant Name": "merchant_name",
            "Merchant City": "merchant_city",
            "Merchant State": "merchant_state",
        })
    )


def _build_dim_date(silver: pl.DataFrame) -> pl.DataFrame:
    return (
        silver.select("transaction_timestamp")
        .unique()
        .with_columns([
            pl.col("transaction_timestamp").dt.date().alias("date"),
            pl.col("transaction_timestamp").dt.year().alias("year"),
            pl.col("transaction_timestamp").dt.month().alias("month"),
            pl.col("transaction_timestamp").dt.day().alias("day"),
            pl.col("transaction_timestamp").dt.weekday().alias("weekday"),
            pl.col("transaction_timestamp").dt.quarter().alias("quarter"),
        ])
        .select(["date", "year", "month", "day", "weekday", "quarter"])
        .unique()
        .sort("date")
    )


def _write_gold_partitioned(fact: pl.DataFrame) -> int:
    settings = get_settings()
    client = get_s3_client()
    total_written = 0

    for (year, month), group in fact.group_by(["partition_year", "partition_month"]):
        key = f"{settings.s3_prefix_gold}year={year}/month={month:02d}/fact_transactions.parquet"
        buf = io.BytesIO()
        group.write_parquet(buf, compression="snappy")
        buf.seek(0)
        client.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=buf.read(),
            ContentType="application/octet-stream",
        )
        total_written += len(group)
        log.info("Gold partition written", extra={"key": key, "rows": len(group)})

    return total_written


def _write_dim(df: pl.DataFrame, name: str) -> None:
    settings = get_settings()
    client = get_s3_client()
    key = f"{settings.s3_prefix_gold}dims/{name}.parquet"
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    client.put_object(Bucket=settings.s3_bucket, Key=key, Body=buf.read())
    log.info("Dim written", extra={"key": key, "rows": len(df)})


def handler(event: dict, context) -> dict:
    log.info("Lambda 3 started")
    con = duckdb.connect()
    _setup_duckdb_s3(con)

    silver = _read_new_silver(con)
    if len(silver) == 0:
        log.info("No silver data available yet")
        return {"status": "OK", "gold_rows": 0}

    fact = _build_fact_transactions(silver)
    gold_rows = _write_gold_partitioned(fact)

    dim_users = _build_dim_users(con)
    dim_cards = _build_dim_cards(con)
    dim_merchants = _build_dim_merchants(silver)
    dim_date = _build_dim_date(silver)

    _write_dim(dim_users, "dim_users")
    _write_dim(dim_cards, "dim_cards")
    _write_dim(dim_merchants, "dim_merchants")
    _write_dim(dim_date, "dim_date")

    log.info("Lambda 3 complete", extra={"gold_rows": gold_rows})
    return {"status": "OK", "gold_rows": gold_rows}
