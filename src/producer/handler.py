import io
import os
import duckdb
import polars as pl
from datetime import datetime, timezone

from src.config.settings import get_settings
from src.config.aws_client import configure_duckdb_s3, get_s3_client, get_events_client
from src.config.logger import get_logger
from src.producer.state_manager import load_state, save_state
from src.producer.detector import attach_detection_flags

log = get_logger("finpulse.lambda1")


def _disable_eventbridge_rule() -> None:
    if os.getenv("SCHEDULER_MANAGED", "false").lower() == "true":
        log.info("Dataset exhausted — Scheduler remains enabled for future data")
        return
    settings = get_settings()
    client = get_events_client()
    client.disable_rule(Name=settings.eventbridge_rule_name)
    log.info("EventBridge rule disabled — dataset exhausted", extra={"rule": settings.eventbridge_rule_name})


def _read_next_batch(last_timestamp: str | None, batch_size: int | None = None) -> pl.DataFrame:
    """Read only the next micro-batch from S3.

    ``setup_raw.py`` writes the source Parquet in timestamp order. DuckDB uses
    HTTP range requests and Parquet row-group metadata, avoiding a full-file
    download for every producer invocation.
    """
    settings = get_settings()
    size     = batch_size or settings.batch_size
    source   = f"s3://{settings.s3_bucket}/{settings.raw_transactions_key}"
    con      = duckdb.connect()

    try:
        configure_duckdb_s3(con)
        con.execute("SET TimeZone='UTC';")

        if last_timestamp is None:
            query  = f"SELECT * FROM read_parquet('{source}') LIMIT ?"
            params = [size]
        else:
            query  = f"""
                SELECT *
                FROM read_parquet('{source}')
                WHERE transaction_timestamp > CAST(? AS TIMESTAMPTZ)
                LIMIT ?
            """
            params = [last_timestamp, size]

        return con.execute(query, params).pl()
    finally:
        con.close()


def _write_bronze(df: pl.DataFrame, batch_ts: str) -> str:
    settings = get_settings()
    client = get_s3_client()

    safe_ts = batch_ts.replace(":", "-").replace("+", "Z")
    key = f"{settings.s3_prefix_bronze}batch_{safe_ts}.parquet"

    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)

    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=buffer.read(),
        ContentType="application/octet-stream",
    )
    log.info("Bronze batch written", extra={"key": key, "rows": len(df)})
    return key


def handler(event: dict, context) -> dict:
    log.info("Lambda 1 started", extra={"event": event})
    settings = get_settings()

    # Allow simulate_stream.py to override batch size for local demo
    batch_size = event.get("batch_size_override") or settings.batch_size

    state = load_state()
    last_timestamp = state.get("last_timestamp")
    total_processed = state.get("total_processed", 0)

    batch = _read_next_batch(last_timestamp, batch_size)

    if len(batch) == 0:
        log.info("No new rows — dataset exhausted, disabling trigger")
        _disable_eventbridge_rule()
        return {"status": "EXHAUSTED", "total_processed": total_processed}

    batch_ts = datetime.now(timezone.utc).isoformat()
    batch = attach_detection_flags(batch)
    bronze_key = _write_bronze(batch, batch_ts)

    new_last_timestamp = batch["transaction_timestamp"].max()
    if hasattr(new_last_timestamp, "isoformat"):
        new_last_timestamp = new_last_timestamp.isoformat()
    else:
        new_last_timestamp = str(new_last_timestamp)

    total_processed += len(batch)
    save_state(new_last_timestamp, total_processed)

    log.info(
        "Lambda 1 complete",
        extra={
            "rows_ingested": len(batch),
            "total_processed": total_processed,
            "bronze_key": bronze_key,
        },
    )
    return {
        "status": "OK",
        "rows_ingested": len(batch),
        "total_processed": total_processed,
        "bronze_key": bronze_key,
    }
