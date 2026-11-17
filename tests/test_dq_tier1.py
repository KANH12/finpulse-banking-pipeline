from datetime import datetime, timezone

import duckdb
import polars as pl

from src.pipeline import dq_tier1


class _LocalConnection:
    """DuckDB wrapper that ignores S3 extension/settings statements."""

    def __init__(self):
        self._con = duckdb.connect()

    def execute(self, query, *args, **kwargs):
        if query.strip().upper().startswith(("INSTALL", "LOAD", "SET S3_")):
            return self
        return self._con.execute(query, *args, **kwargs)

    def register(self, *args, **kwargs):
        return self._con.register(*args, **kwargs)


def _batch() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "User": [1, 2, None, 4],
            "Card": [0, 1, 2, 3],
            "Amount": [20.0, -5.0, 10.0, 30.0],
            "transaction_timestamp": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 4,
            "flag_null_field": [False, False, True, False],
            "flag_negative_amount": [False, True, False, False],
            "flag_future_timestamp": [False, False, False, False],
        }
    )


def test_tier1_routes_null_and_negative_transactions_to_quarantine(monkeypatch):
    con = _LocalConnection()
    monkeypatch.setattr(dq_tier1.duckdb, "connect", lambda: con)
    monkeypatch.setattr(
        dq_tier1,
        "_load_gold_transaction_ids",
        lambda connection: connection.execute(
            "CREATE VIEW gold_txn AS SELECT NULL::VARCHAR AS transaction_id WHERE false"
        ),
    )

    passed, failed = dq_tier1.run_tier1(_batch())

    assert passed.height == 2
    assert failed["tier1_error_code"].to_list() == ["INVALID_AMOUNT", "NULL_CRITICAL_FIELD"]


def test_tier1_blocks_transaction_already_in_gold(monkeypatch):
    con = _LocalConnection()
    monkeypatch.setattr(dq_tier1.duckdb, "connect", lambda: con)

    batch = _batch().head(1)
    transaction_id = (
        batch.with_columns(
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
        .item(0, "transaction_id")
    )
    monkeypatch.setattr(
        dq_tier1,
        "_load_gold_transaction_ids",
        lambda connection: connection.execute(
            f"CREATE VIEW gold_txn AS SELECT '{transaction_id}' AS transaction_id"
        ),
    )

    passed, failed = dq_tier1.run_tier1(batch)

    assert passed.height == 0
    assert failed["tier1_error_code"].to_list() == ["DUPLICATE_TRANSACTION"]
