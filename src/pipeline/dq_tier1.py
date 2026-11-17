import duckdb
import polars as pl
from src.config.settings import get_settings
from src.config.aws_client import configure_duckdb_s3
from src.config.logger import get_logger

log = get_logger("finpulse.dq_tier1")


def _load_gold_transaction_ids(con: duckdb.DuckDBPyConnection) -> None:
    settings = get_settings()
    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW gold_txn AS
            SELECT transaction_id
            FROM read_parquet('s3://{settings.s3_bucket}/{settings.s3_prefix_gold}year=*/month=*/fact_transactions.parquet',
                              hive_partitioning=true)
        """)
    except Exception:
        con.execute("CREATE OR REPLACE VIEW gold_txn AS SELECT NULL::VARCHAR AS transaction_id WHERE false")
        log.info("Gold layer empty — dedup view initialized as empty")


def run_tier1(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    settings = get_settings()

    con = duckdb.connect()
    configure_duckdb_s3(con)

    _load_gold_transaction_ids(con)

    # Build transaction_id as composite key
    df = df.with_columns(
        (
            pl.col("User").cast(pl.Utf8)
            + "_"
            + pl.col("Card").cast(pl.Utf8)
            + "_"
            + pl.col("transaction_timestamp").cast(pl.Utf8)
            + "_"
            + pl.col("Amount").cast(pl.Utf8)
        ).alias("transaction_id")
    )

    # Dedup: find IDs already in Gold
    batch_ids = df.select("transaction_id").to_pandas()
    con.register("batch_ids", batch_ids)
    duplicates = con.execute("""
        SELECT b.transaction_id
        FROM batch_ids b
        INNER JOIN gold_txn g ON b.transaction_id = g.transaction_id
    """).fetchdf()
    dup_set = set(duplicates["transaction_id"].tolist())

    df = df.with_columns(
        pl.col("transaction_id").is_in(list(dup_set)).alias("flag_duplicate")
    )

    # BR-T1-01, BR-T1-02, BR-T1-03: three active DQ rules
    df = df.with_columns(
        pl.when(pl.col("flag_null_field"))
        .then(pl.lit("NULL_CRITICAL_FIELD"))
        .when(pl.col("flag_negative_amount"))
        .then(pl.lit("INVALID_AMOUNT"))
        .when(pl.col("flag_future_timestamp"))
        .then(pl.lit("FUTURE_TIMESTAMP"))
        .when(pl.col("flag_duplicate"))
        .then(pl.lit("DUPLICATE_TRANSACTION"))
        .otherwise(pl.lit(None))
        .alias("tier1_error_code")
    )

    passed = df.filter(pl.col("tier1_error_code").is_null())
    failed = df.filter(pl.col("tier1_error_code").is_not_null())

    log.info(
        "Tier 1 DQ complete",
        extra={"passed": len(passed), "failed": len(failed)},
    )
    return passed, failed
