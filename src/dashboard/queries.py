import json
import logging

import duckdb
import streamlit as st
from src.config.settings import get_settings
from src.config.aws_client import configure_duckdb_s3


logger = logging.getLogger(__name__)


def _get_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    configure_duckdb_s3(con)
    return con


def _gold_fact_path() -> str:
    settings = get_settings()
    return (
        f"s3://{settings.s3_bucket}/{settings.s3_prefix_gold}"
        "year=*/month=*/fact_transactions.parquet"
    )


@st.cache_data(ttl=60)
def load_pipeline_health() -> dict:
    settings = get_settings()
    con = _get_con()
    try:
        bronze = con.execute(f"""
            SELECT COUNT(*) AS total
            FROM read_parquet('s3://{settings.s3_bucket}/{settings.s3_prefix_bronze}*.parquet')
        """).fetchone()[0]
        silver = con.execute(f"""
            SELECT COUNT(*) AS total
            FROM read_parquet('s3://{settings.s3_bucket}/{settings.s3_prefix_silver}*.parquet')
        """).fetchone()[0]
        return {"bronze_total": bronze, "silver_total": silver, "health_score": round(silver / bronze * 100, 1) if bronze else 0}
    except Exception:
        logger.exception("Failed to load pipeline health metrics")
        return {"bronze_total": 0, "silver_total": 0, "health_score": 0}


@st.cache_data(ttl=60)
def load_error_breakdown() -> list[dict]:
    settings = get_settings()
    con = _get_con()
    try:
        result = con.execute(f"""
            SELECT
                COALESCE(error_code, 'UNKNOWN') AS error_code,
                COUNT(*) AS count
            FROM read_json_auto('s3://{settings.s3_bucket}/{settings.s3_prefix_quarantine}**/*.json')
            GROUP BY 1
            ORDER BY 2 DESC
        """).fetchdf()
        return result.to_dict(orient="records")
    except Exception:
        logger.exception("Failed to load quarantine error breakdown")
        return []


@st.cache_data(ttl=60)
def load_quarantine_records(limit: int = 200, filter_error: str | None = None) -> list[dict]:
    """
    Load quarantine records and flatten nested 'record' field for display.
    Each row returned has: error_code, error_description, quarantine_timestamp,
    source_bronze_key, plus all transaction fields from record.*.
    """
    settings = get_settings()
    con = _get_con()

    ERROR_DESCRIPTIONS = {
        "NULL_CRITICAL_FIELD":      "Missing required field (User / Card / Amount is null)",
        "INVALID_AMOUNT":           "Amount is zero or negative — invalid transaction value",
        "FUTURE_TIMESTAMP":         "Transaction timestamp is in the future — clock skew or bad data",
        "DUPLICATE_TRANSACTION":    "Duplicate detected — same transaction already exists in Gold",
        "EXCEEDS_CREDIT_LIMIT":     "Transaction amount exceeds card credit limit",
    }

    try:
        where_clause = f"WHERE error_code = '{filter_error}'" if filter_error else ""
        raw = con.execute(f"""
            SELECT
                error_code,
                quarantine_timestamp,
                source_bronze_key,
                to_json(record) AS record_json
            FROM read_json_auto('s3://{settings.s3_bucket}/{settings.s3_prefix_quarantine}**/*.json')
            {where_clause}
            ORDER BY quarantine_timestamp DESC
            LIMIT {limit}
        """).fetchall()

        rows = []
        for error_code, quarantine_timestamp, source_bronze_key, record_json in raw:
            try:
                rec = json.loads(record_json) if record_json else {}
            except (TypeError, json.JSONDecodeError):
                rec = {}
            row = {
                "error_code":        error_code,
                "error_description": ERROR_DESCRIPTIONS.get(error_code, "Unknown error"),
                "quarantine_time":   quarantine_timestamp,
                "source_batch":      str(source_bronze_key).split("/")[-1],
            }
            if isinstance(rec, dict):
                row.update(rec)
            rows.append(row)
        return rows
    except Exception:
        logger.exception("Failed to load quarantine records")
        return []


@st.cache_data(ttl=60)
def load_risk_summary() -> dict:
    con = _get_con()
    try:
        result = con.execute(f"""
            SELECT
                risk_label,
                COUNT(*) AS count,
                SUM(Amount) AS total_amount
            FROM read_parquet('{_gold_fact_path()}',
                              hive_partitioning=true)
            GROUP BY risk_label
            ORDER BY count DESC
        """).fetchdf()
        return result.to_dict(orient="records")
    except Exception:
        return []


@st.cache_data(ttl=60)
def load_risk_drivers() -> dict:
    """Return the signals that explain the current review queue."""
    con = _get_con()
    try:
        return con.execute(f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE risk_label IN ('HIGH_RISK', 'CRITICAL_RISK')
                ) AS review_queue,
                COALESCE(SUM(Amount) FILTER (
                    WHERE risk_label IN ('HIGH_RISK', 'CRITICAL_RISK')
                ), 0) AS review_exposure,
                COUNT(*) FILTER (WHERE flag_high_cur) AS high_cur_count,
                COUNT(*) FILTER (WHERE flag_subprime) AS subprime_count,
                COUNT(*) FILTER (
                    WHERE flag_high_cur AND flag_subprime
                ) AS dual_factor_count,
                COUNT(*) FILTER (WHERE flag_high_velocity) AS velocity_signal_count
            FROM read_parquet('{_gold_fact_path()}', hive_partitioning=true)
        """).fetchdf().iloc[0].to_dict()
    except Exception:
        logger.exception("Failed to load risk driver summary")
        return {}


@st.cache_data(ttl=60)
def load_recent_high_risk(limit: int = 50) -> list[dict]:
    con = _get_con()
    try:
        result = con.execute(f"""
            SELECT
                transaction_id,
                transaction_timestamp,
                ingestion_timestamp,
                Amount,
                "Merchant Name",
                MCC,
                risk_label,
                cur_ratio,
                flag_subprime,
                flag_high_velocity
            FROM read_parquet('{_gold_fact_path()}',
                              hive_partitioning=true)
            WHERE risk_label IN ('HIGH_RISK', 'CRITICAL_RISK')
            ORDER BY ingestion_timestamp DESC
            LIMIT {limit}
        """).fetchdf()
        return result.to_dict(orient="records")
    except Exception:
        return []


@st.cache_data(ttl=60)
def load_mcc_breakdown() -> list[dict]:
    con = _get_con()
    try:
        result = con.execute(f"""
            SELECT
                MCC,
                COUNT(*) AS txn_count,
                SUM(Amount) AS total_amount,
                AVG(cur_ratio) AS avg_cur
            FROM read_parquet('{_gold_fact_path()}',
                              hive_partitioning=true)
            GROUP BY MCC
            ORDER BY txn_count DESC
            LIMIT 20
        """).fetchdf()
        return result.to_dict(orient="records")
    except Exception:
        return []


@st.cache_data(ttl=60)
def load_daily_trend() -> list[dict]:
    con = _get_con()
    try:
        result = con.execute(f"""
            SELECT
                DATE_TRUNC('day', CAST(ingestion_timestamp AS TIMESTAMPTZ)) AS day,
                COUNT(*) AS txn_count,
                SUM(Amount) AS total_amount,
                COUNT(*) FILTER (WHERE risk_label IN ('HIGH_RISK', 'CRITICAL_RISK')) AS risk_count
            FROM read_parquet('{_gold_fact_path()}',
                              hive_partitioning=true)
            GROUP BY 1
            ORDER BY 1
        """).fetchdf()
        return result.to_dict(orient="records")
    except Exception:
        return []
