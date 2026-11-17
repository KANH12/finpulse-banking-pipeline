import duckdb
import polars as pl
from src.config.settings import get_settings
from src.config.aws_client import configure_duckdb_s3
from src.config.logger import get_logger

log = get_logger("finpulse.dq_tier2")


def _setup_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    configure_duckdb_s3(con)


def _compute_monthly_balance(
    con: duckdb.DuckDBPyConnection,
    composite_keys: list[str],
    txn_year: int,
    txn_month: int,
) -> dict[str, float]:
    """
    [FP-101] Compute current-cycle balance per composite card key (User_Card).
    Scoped to the same billing month as the incoming batch to simulate a
    monthly billing cycle — avoids unbounded balance accumulation across years.
    """
    settings = get_settings()
    if not composite_keys:
        return {}
    key_list = ", ".join(f"'{k}'" for k in composite_keys)
    try:
        result = con.execute(f"""
            SELECT
                (CAST("User" AS VARCHAR) || '_' || CAST("Card" AS VARCHAR)) AS composite_key,
                SUM("Amount") AS total_spent
            FROM read_parquet(
                's3://{settings.s3_bucket}/{settings.s3_prefix_gold}year=*/month=*/fact_transactions.parquet',
                hive_partitioning=true
            )
            WHERE partition_year  = {txn_year}
              AND partition_month = {txn_month}
              AND (CAST("User" AS VARCHAR) || '_' || CAST("Card" AS VARCHAR)) IN ({key_list})
            GROUP BY 1
        """).fetchdf()
        return dict(zip(result["composite_key"].tolist(), result["total_spent"].tolist()))
    except Exception:
        log.info("Gold layer empty or no matching cards — monthly balances default to 0")
        return {}


def _load_dims(con: duckdb.DuckDBPyConnection) -> None:
    settings = get_settings()
    con.execute(f"""
        CREATE OR REPLACE VIEW dim_cards AS
        SELECT * FROM read_parquet('s3://{settings.s3_bucket}/raw/cards.parquet')
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW dim_users AS
        SELECT *, ROW_NUMBER() OVER () - 1 AS user_id
        FROM read_parquet('s3://{settings.s3_bucket}/raw/users.parquet')
    """)


def run_tier2(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    settings = get_settings()
    con = duckdb.connect()
    _setup_duckdb_s3(con)
    _load_dims(con)

    # [FP-101] Composite key: User_Card — uniquely identifies a card across all users
    df = df.with_columns(
        (pl.col("User").cast(pl.Utf8) + "_" + pl.col("Card").cast(pl.Utf8)).alias("composite_key")
    )

    # Derive billing cycle from the batch's transaction timestamps
    txn_year  = df["transaction_timestamp"].dt.year().mode().item()
    txn_month = df["transaction_timestamp"].dt.month().mode().item()

    composite_keys = df["composite_key"].unique().to_list()
    monthly_balances = _compute_monthly_balance(con, composite_keys, txn_year, txn_month)

    # Enrich: join credit limit + FICO score from dims
    con.register("batch_df", df.to_pandas())
    enriched = con.execute("""
        SELECT
            b.*,
            TRY_CAST(REPLACE(c."Credit Limit", '$', '') AS DOUBLE) AS credit_limit,
            u."FICO Score" AS fico_score,
            u."Person"     AS person_name
        FROM batch_df b
        LEFT JOIN dim_cards c
            ON CAST(b."Card" AS VARCHAR) = CAST(c."CARD INDEX" AS VARCHAR)
           AND CAST(b."User" AS VARCHAR) = CAST(c."User"       AS VARCHAR)
        LEFT JOIN dim_users u
            ON CAST(b."User" AS VARCHAR) = CAST(u.user_id AS VARCHAR)
    """).pl()

    # [FP-101] CUR keyed on composite_key to avoid cross-user balance collision
    def compute_cur(row) -> float:
        key   = row["composite_key"]
        spent = monthly_balances.get(key, 0.0) + (row["Amount"] or 0.0)
        limit = row["credit_limit"]
        if not limit or limit <= 0:
            return 0.0
        return spent / limit

    cur_values = [compute_cur(r) for r in enriched.iter_rows(named=True)]
    enriched = enriched.with_columns(pl.Series("cur_ratio", cur_values, dtype=pl.Float64))

    # Risk flags — Tier 2 Business Logic (BR-T2-01, BR-RK-03)
    enriched = enriched.with_columns([
        (pl.col("cur_ratio") > settings.cur_high_risk_threshold).alias("flag_high_cur"),
        (
            pl.col("fico_score").is_not_null()
            & (pl.col("fico_score") < settings.fico_subprime_cutoff)
        ).alias("flag_subprime"),
    ])

    # Dual-factor risk matrix: FICO × CUR (BR-RK-03, CFPB 2023 / Basel II Principle 9)
    enriched = enriched.with_columns(
        pl.when(pl.col("flag_high_cur") & pl.col("flag_subprime"))
        .then(pl.lit("CRITICAL_RISK"))
        .when(pl.col("flag_high_cur"))
        .then(pl.lit("HIGH_RISK"))
        .when(pl.col("flag_subprime"))
        .then(pl.lit("SUBPRIME_RISK"))
        .otherwise(pl.lit("NORMAL"))
        .alias("risk_label")
    )

    # Hard reject: single transaction exceeds the card's total credit limit (BR-T2-01)
    enriched = enriched.with_columns(
        pl.when(
            pl.col("credit_limit").is_not_null()
            & (pl.col("Amount") > pl.col("credit_limit"))
        )
        .then(pl.lit("EXCEEDS_CREDIT_LIMIT"))
        .otherwise(pl.lit(None))
        .alias("tier2_error_code")
    )

    passed = enriched.filter(pl.col("tier2_error_code").is_null())
    failed = enriched.filter(pl.col("tier2_error_code").is_not_null())

    log.info(
        "Tier 2 DQ complete",
        extra={
            "passed": len(passed),
            "failed": len(failed),
            "critical_risk": enriched.filter(pl.col("risk_label") == "CRITICAL_RISK").height,
            "high_risk":     enriched.filter(pl.col("risk_label") == "HIGH_RISK").height,
        },
    )
    return passed, failed
