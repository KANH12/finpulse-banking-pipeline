"""Export compact, presentation-ready samples of real FinPulse flagged records.

The script reads the current Gold and quarantine layers from S3.  It exports
only records that genuinely occurred; it never synthesizes a risk flag.
"""

from __future__ import annotations

import csv
import json
import sys
import tempfile
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.settings import get_settings


OUTPUT_DIR = ROOT / "docs" / "report" / "data_samples"
RISK_LIMIT = 12
QUARANTINE_PER_ERROR_LIMIT = 6


def configured_connection() -> duckdb.DuckDBPyConnection:
    """Open DuckDB with the same S3 credentials used by the application."""
    settings = get_settings()
    con = duckdb.connect()
    con.execute("SET home_directory=?", [tempfile.gettempdir()])
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute("SET s3_region=?", [settings.aws_region])
    con.execute("SET s3_access_key_id=?", [settings.aws_access_key_id])
    con.execute("SET s3_secret_access_key=?", [settings.aws_secret_access_key])
    if settings.aws_session_token:
        con.execute("SET s3_session_token=?", [settings.aws_session_token])
    return con


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    settings = get_settings()
    gold_path = (
        f"s3://{settings.s3_bucket}/{settings.s3_prefix_gold}"
        "year=*/month=*/fact_transactions.parquet"
    )
    quarantine_path = (
        f"s3://{settings.s3_bucket}/{settings.s3_prefix_quarantine}**/*.json"
    )
    cards_path = f"s3://{settings.s3_bucket}/{settings.raw_cards_key}"
    users_path = f"s3://{settings.s3_bucket}/{settings.raw_users_key}"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = configured_connection()

    risk_rows = con.execute(
        f"""
        WITH fact AS (
            SELECT *
            FROM read_parquet('{gold_path}', hive_partitioning=true)
        ), dims AS (
            SELECT
                c."User" AS user_id,
                c."CARD INDEX" AS card_id,
                TRY_CAST(REPLACE(c."Credit Limit", '$', '') AS DOUBLE) AS credit_limit
            FROM read_parquet('{cards_path}') c
        ), users AS (
            SELECT *, ROW_NUMBER() OVER () - 1 AS user_id
            FROM read_parquet('{users_path}')
        )
        SELECT
            f.transaction_id,
            f.transaction_timestamp,
            f.ingestion_timestamp,
            f."User" AS user_id,
            f."Card" AS card_id,
            f."Merchant Name" AS merchant_name,
            f.MCC AS merchant_category_code,
            f.Amount AS transaction_amount,
            d.credit_limit,
            u."FICO Score" AS fico_score,
            ROUND(f.cur_ratio * 100, 2) AS cur_percent,
            f.risk_label,
            f.flag_subprime,
            f.flag_high_cur,
            f.flag_high_velocity,
            CASE
                WHEN f.flag_subprime AND f.flag_high_cur THEN
                    'FICO below 620 and utilisation above 85%'
                WHEN f.flag_high_cur THEN 'Utilisation above 85%'
                WHEN f.flag_subprime THEN 'FICO below 620'
                WHEN f.flag_high_velocity THEN 'More than 3 transactions in this micro-batch'
                ELSE 'No configured risk flag'
            END AS why_flagged
        FROM fact f
        LEFT JOIN dims d
            ON CAST(f."User" AS VARCHAR) = CAST(d.user_id AS VARCHAR)
           AND CAST(f."Card" AS VARCHAR) = CAST(d.card_id AS VARCHAR)
        LEFT JOIN users u
            ON CAST(f."User" AS VARCHAR) = CAST(u.user_id AS VARCHAR)
        WHERE f.flag_subprime OR f.flag_high_cur OR f.flag_high_velocity
        ORDER BY
            f.flag_high_cur DESC,
            f.flag_subprime DESC,
            f.flag_high_velocity DESC,
            f.ingestion_timestamp DESC
        LIMIT {RISK_LIMIT}
        """
    ).fetchdf().to_dict(orient="records")
    risk_columns = [
        "transaction_id", "transaction_timestamp", "ingestion_timestamp", "user_id",
        "card_id", "merchant_name", "merchant_category_code", "transaction_amount",
        "credit_limit", "fico_score", "cur_percent", "risk_label", "flag_subprime",
        "flag_high_cur", "flag_high_velocity", "why_flagged",
    ]
    write_csv(OUTPUT_DIR / "flagged_risk_records_sample.csv", risk_columns, risk_rows)

    quarantine_rows = con.execute(
        f"""
        WITH numbered AS (
            SELECT
                error_code,
                quarantine_timestamp,
                source_bronze_key,
                to_json(record) AS record_json,
                ROW_NUMBER() OVER (
                    PARTITION BY error_code ORDER BY quarantine_timestamp DESC
                ) AS row_num
            FROM read_json_auto('{quarantine_path}')
        )
        SELECT error_code, quarantine_timestamp, source_bronze_key, record_json
        FROM numbered
        WHERE row_num <= {QUARANTINE_PER_ERROR_LIMIT}
        ORDER BY error_code, quarantine_timestamp DESC
        """
    ).fetchall()
    descriptions = {
        "DUPLICATE_TRANSACTION": "Transaction ID already exists in the accepted Gold layer.",
        "INVALID_AMOUNT": "Amount is zero or negative.",
        "NULL_CRITICAL_FIELD": "User, Card, or Amount is missing.",
        "FUTURE_TIMESTAMP": "Transaction timestamp is later than ingestion time.",
        "EXCEEDS_CREDIT_LIMIT": "Transaction amount is above the card credit limit.",
    }
    quarantine_export = []
    for error_code, timestamp, source_key, record_json in quarantine_rows:
        record = json.loads(record_json) if record_json else {}
        quarantine_export.append({
            "error_code": error_code,
            "why_quarantined": descriptions.get(error_code, "Pipeline data-quality rule failed."),
            "quarantine_timestamp": timestamp,
            "source_batch": str(source_key).split("/")[-1],
            "transaction_id": record.get("transaction_id"),
            "user_id": record.get("User"),
            "card_id": record.get("Card"),
            "transaction_timestamp": record.get("transaction_timestamp"),
            "transaction_amount": record.get("Amount"),
            "merchant_name": record.get("Merchant Name"),
            "merchant_city": record.get("Merchant City"),
            "merchant_state": record.get("Merchant State"),
            "merchant_category_code": record.get("MCC"),
            "flag_duplicate": record.get("flag_duplicate"),
            "flag_negative_amount": record.get("flag_negative_amount"),
            "flag_null_field": record.get("flag_null_field"),
            "flag_future_timestamp": record.get("flag_future_timestamp"),
        })
    quarantine_columns = [
        "error_code", "why_quarantined", "quarantine_timestamp", "source_batch",
        "transaction_id", "user_id", "card_id", "transaction_timestamp",
        "transaction_amount", "merchant_name", "merchant_city", "merchant_state",
        "merchant_category_code", "flag_duplicate", "flag_negative_amount",
        "flag_null_field", "flag_future_timestamp",
    ]
    write_csv(
        OUTPUT_DIR / "quarantine_records_sample.csv",
        quarantine_columns,
        quarantine_export,
    )

    print(f"Risk sample: {len(risk_rows)} rows")
    print(f"Quarantine sample: {len(quarantine_export)} rows")
    print(f"Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
