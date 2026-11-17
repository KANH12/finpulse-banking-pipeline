from datetime import datetime, timezone

import polars as pl

from src.producer.detector import attach_detection_flags


def _transactions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "User": [1, None, 3, 4],
            "Card": [0, 1, 2, 3],
            "Amount": [50.0, 12.0, -1.0, 10.0],
            "transaction_timestamp": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2999, 1, 1, tzinfo=timezone.utc),
            ],
        }
    )


def test_detector_adds_ingestion_metadata_and_marks_critical_input_errors():
    result = attach_detection_flags(_transactions())

    assert "ingestion_timestamp" in result.columns
    assert result["flag_null_field"].to_list() == [False, True, False, False]
    assert result["flag_negative_amount"].to_list() == [False, False, True, False]
    assert result["flag_future_timestamp"].to_list() == [False, False, False, True]
    assert result["flag_high_velocity"].to_list() == [False, False, False, False]
    assert result["detection_summary"].to_list() == ["CLEAN", "TECHNICAL_ANOMALY", "TECHNICAL_ANOMALY", "TECHNICAL_ANOMALY"]
