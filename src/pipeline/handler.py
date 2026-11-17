import io
import json
from datetime import datetime, timezone

import polars as pl

from src.config.settings import get_settings
from src.config.aws_client import get_s3_client
from src.config.logger import get_logger
from src.pipeline.dq_tier1 import run_tier1
from src.pipeline.dq_tier2 import run_tier2

log = get_logger("finpulse.lambda2")


def _read_bronze(bronze_key: str) -> pl.DataFrame:
    settings = get_settings()
    client = get_s3_client()
    obj = client.get_object(Bucket=settings.s3_bucket, Key=bronze_key)
    return pl.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_quarantine(failed_df: pl.DataFrame, bronze_key: str, error_code_col: str) -> None:
    settings = get_settings()
    client = get_s3_client()

    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    tier = "tier1" if "tier1" in error_code_col else "tier2"
    key = f"{settings.s3_prefix_quarantine}{tier}/batch_{batch_ts}.json"

    records = []
    for row in failed_df.iter_rows(named=True):
        records.append({
            "error_code": row.get(error_code_col),
            "source_bronze_key": bronze_key,
            "quarantine_timestamp": datetime.now(timezone.utc).isoformat(),
            "record": {k: str(v) if v is not None else None for k, v in row.items()},
        })

    payload = json.dumps(records, indent=2, default=str).encode("utf-8")
    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )
    log.info("Quarantine written", extra={"key": key, "rows": len(failed_df), "tier": tier})


def _write_silver(passed_df: pl.DataFrame) -> str:
    settings = get_settings()
    client = get_s3_client()

    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"{settings.s3_prefix_silver}batch_{batch_ts}.parquet"

    buf = io.BytesIO()
    passed_df.write_parquet(buf, compression="snappy")
    buf.seek(0)

    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )
    log.info("Silver batch written", extra={"key": key, "rows": len(passed_df)})
    return key


def handler(event: dict, context) -> dict:
    log.info("Lambda 2 started", extra={"event": event})

    # S3 event gives us the bronze key that just landed
    bronze_key = event.get("bronze_key") or (
        event.get("Records", [{}])[0]
        .get("s3", {})
        .get("object", {})
        .get("key")
    )

    if not bronze_key:
        log.error("No bronze_key in event", extra={"event": event})
        return {"status": "ERROR", "reason": "missing bronze_key"}

    df = _read_bronze(bronze_key)
    log.info("Bronze loaded", extra={"rows": len(df), "bronze_key": bronze_key})

    # Tier 1 — Technical DQ
    t1_passed, t1_failed = run_tier1(df)
    if len(t1_failed) > 0:
        _write_quarantine(t1_failed, bronze_key, "tier1_error_code")

    if len(t1_passed) == 0:
        log.info("All records failed Tier 1 — nothing to process in Tier 2")
        return {"status": "OK", "t1_passed": 0, "t1_failed": len(t1_failed), "silver_rows": 0}

    # Tier 2 — Business Logic
    t2_passed, t2_failed = run_tier2(t1_passed)
    if len(t2_failed) > 0:
        _write_quarantine(t2_failed, bronze_key, "tier2_error_code")

    silver_key = None
    if len(t2_passed) > 0:
        silver_key = _write_silver(t2_passed)

    log.info(
        "Lambda 2 complete",
        extra={
            "t1_passed": len(t1_passed),
            "t1_failed": len(t1_failed),
            "t2_passed": len(t2_passed),
            "t2_failed": len(t2_failed),
            "silver_key": silver_key,
        },
    )
    return {
        "status": "OK",
        "t1_passed": len(t1_passed),
        "t1_failed": len(t1_failed),
        "t2_passed": len(t2_passed),
        "t2_failed": len(t2_failed),
        "silver_key": silver_key,
    }
