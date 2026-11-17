from datetime import datetime, timezone

import polars as pl

from src.pipeline import dq_tier2


class _QueryResult:
    def __init__(self, dataframe):
        self._dataframe = dataframe

    def pl(self):
        return self._dataframe


class _Tier2Connection:
    def __init__(self, enriched):
        self.enriched = enriched

    def register(self, *_args, **_kwargs):
        return None

    def execute(self, _query):
        return _QueryResult(self.enriched)


def _batch() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "User": [1, 2, 3],
            "Card": [0, 0, 0],
            "Amount": [20.0, 50.0, 150.0],
            "transaction_timestamp": [datetime(2024, 1, 15, tzinfo=timezone.utc)] * 3,
        }
    )


def test_tier2_assigns_risk_labels_and_rejects_over_limit(monkeypatch):
    enriched = _batch().with_columns(
        [
            pl.Series("composite_key", ["1_0", "2_0", "3_0"]),
            pl.Series("credit_limit", [100.0, 100.0, 100.0]),
            pl.Series("fico_score", [700, 600, 700]),
            pl.Series("person_name", ["A", "B", "C"]),
        ]
    )
    connection = _Tier2Connection(enriched)
    monkeypatch.setattr(dq_tier2.duckdb, "connect", lambda: connection)
    monkeypatch.setattr(dq_tier2, "_setup_duckdb_s3", lambda _con: None)
    monkeypatch.setattr(dq_tier2, "_load_dims", lambda _con: None)
    monkeypatch.setattr(
        dq_tier2,
        "_compute_monthly_balance",
        lambda *_args: {"1_0": 70.0, "2_0": 40.0, "3_0": 0.0},
    )

    passed, failed = dq_tier2.run_tier2(_batch())

    assert passed["risk_label"].to_list() == ["HIGH_RISK", "CRITICAL_RISK"]
    assert failed["tier2_error_code"].to_list() == ["EXCEEDS_CREDIT_LIMIT"]
