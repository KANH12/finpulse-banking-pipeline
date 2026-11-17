import duckdb
import streamlit as st
from src.config.settings import get_settings
from src.config.aws_client import configure_duckdb_s3


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
                record
            FROM read_json_auto('s3://{settings.s3_bucket}/{settings.s3_prefix_quarantine}**/*.json')
            {where_clause}
            ORDER BY quarantine_timestamp DESC
            LIMIT {limit}
        """).fetchdf()

        rows = []
        for _, r in raw.iterrows():
            rec = r.get("record", {}) or {}
            if isinstance(rec, str):
                import json as _json
                try:
                    rec = _json.loads(rec)
                except Exception:
                    rec = {}
            error_code = r["error_code"]
            row = {
                "error_code":        error_code,
                "error_description": ERROR_DESCRIPTIONS.get(error_code, "Unknown error"),
                "quarantine_time":   r["quarantine_timestamp"],
                "source_batch":      str(r["source_bronze_key"]).split("/")[-1],
            }
            row.update({k: v for k, v in rec.items()})
            rows.append(row)
        return rows
    except Exception:
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
