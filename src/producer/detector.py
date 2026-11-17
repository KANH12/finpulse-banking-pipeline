import polars as pl
from datetime import datetime, timezone
from src.config.settings import get_settings

settings = get_settings()


def attach_detection_flags(df: pl.DataFrame) -> pl.DataFrame:
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    df = df.with_columns([
        pl.lit(ingestion_ts).alias("ingestion_timestamp"),
        pl.lit(False).alias("flag_null_field"),
        pl.lit(False).alias("flag_negative_amount"),
        pl.lit(False).alias("flag_future_timestamp"),
        pl.lit(False).alias("flag_high_velocity"),
        pl.lit(False).alias("flag_subprime"),
        pl.lit("CLEAN").alias("detection_summary"),
    ])

    # BR-T1-01: Null in critical fields (User, Card, Amount)
    df = df.with_columns(
        (
            pl.col("User").is_null()
            | pl.col("Card").is_null()
            | pl.col("Amount").is_null()
        ).alias("flag_null_field")
    )

    # BR-T1-02: Amount must be positive
    df = df.with_columns(
        (pl.col("Amount") <= 0).alias("flag_negative_amount")
    )

    # BR-T1-03: Events from the future are invalid source records.
    current_time = pl.lit(ingestion_ts).str.to_datetime(
        format="%Y-%m-%dT%H:%M:%S%.f%z", time_zone="UTC"
    )
    df = df.with_columns(
        (pl.col("transaction_timestamp") > current_time).alias("flag_future_timestamp")
    )

    # BR-RK-02: flag a card with excessive activity inside the replayed batch.
    velocity_cards = (
        df.group_by("Card")
        .agg(pl.len().alias("transaction_count"))
        .filter(pl.col("transaction_count") > settings.velocity_max_txn_per_window)
        .select("Card")
        .to_series()
        .to_list()
    )
    df = df.with_columns(pl.col("Card").is_in(velocity_cards).alias("flag_high_velocity"))

    # BR-RK-01: FICO subprime pre-flag (if dim data is already present)
    if "FICO Score" in df.columns:
        df = df.with_columns(
            (pl.col("FICO Score") < settings.fico_subprime_cutoff).alias("flag_subprime")
        )

    # Detection summary label
    df = df.with_columns(
        pl.when(pl.col("flag_null_field"))
        .then(pl.lit("TECHNICAL_ANOMALY"))
        .when(pl.col("flag_negative_amount"))
        .then(pl.lit("TECHNICAL_ANOMALY"))
        .when(pl.col("flag_future_timestamp"))
        .then(pl.lit("TECHNICAL_ANOMALY"))
        .otherwise(pl.lit("CLEAN"))
        .alias("detection_summary")
    )

    return df
