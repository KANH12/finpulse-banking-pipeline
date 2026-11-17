from datetime import datetime, timezone

import polars as pl

from src.analytics.handler import _build_fact_transactions, _build_dim_date, _build_dim_merchants


def _silver_batch() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "transaction_id": ["txn-1"],
            "User": [1],
            "Card": [2],
            "transaction_timestamp": [datetime(2024, 2, 3, 10, 30, tzinfo=timezone.utc)],
            "ingestion_timestamp": ["2024-02-03T10:31:00+00:00"],
            "Amount": [99.5],
            "Use Chip": ["Swipe Transaction"],
            "Merchant Name": ["Merchant A"],
            "Merchant City": ["Sydney"],
            "Merchant State": ["NSW"],
            "MCC": [5411],
            "errors": [None],
            "is_fraud": ["No"],
            "detection_summary": ["CLEAN"],
            "flag_null_field": [False],
            "flag_negative_amount": [False],
            "flag_future_timestamp": [False],
            "flag_high_velocity": [False],
            "flag_high_cur": [False],
            "flag_subprime": [False],
            "cur_ratio": [0.1],
            "risk_label": ["NORMAL"],
            "tier1_error_code": [None],
            "tier2_error_code": [None],
        }
    )


def test_analytics_fact_keeps_transaction_and_derives_partition_columns():
    fact = _build_fact_transactions(_silver_batch())

    assert fact.height == 1
    assert fact["transaction_id"].to_list() == ["txn-1"]
    assert fact["partition_year"].to_list() == [2024]
    assert fact["partition_month"].to_list() == [2]


def test_analytics_dimensions_have_expected_grain():
    silver = _silver_batch()

    merchants = _build_dim_merchants(silver)
    dates = _build_dim_date(silver)

    assert merchants.columns == ["merchant_name", "merchant_city", "merchant_state", "MCC"]
    assert dates.select("date").n_unique() == 1
    assert dates["year"].to_list() == [2024]
