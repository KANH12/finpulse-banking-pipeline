from datetime import datetime, timezone
from types import SimpleNamespace

import importlib
import polars as pl
import pytest

producer = importlib.import_module("src.producer.handler")


def _batch() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "User": [1, 2],
            "Card": [0, 1],
            "Amount": [10.0, 20.0],
            "transaction_timestamp": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
            ],
        }
    )


def test_producer_writes_bronze_then_advances_state(monkeypatch):
    saved = []
    monkeypatch.setattr(producer, "get_settings", lambda: SimpleNamespace(batch_size=10))
    monkeypatch.setattr(producer, "load_state", lambda: {"last_timestamp": None, "total_processed": 4})
    monkeypatch.setattr(producer, "_read_next_batch", lambda last, size: _batch())
    monkeypatch.setattr(producer, "attach_detection_flags", lambda dataframe: dataframe)
    monkeypatch.setattr(producer, "_write_bronze", lambda dataframe, timestamp: "bronze/batch.parquet")
    monkeypatch.setattr(producer, "save_state", lambda timestamp, total: saved.append((timestamp, total)))

    result = producer.handler({"batch_size_override": 2}, None)

    assert result == {
        "status": "OK",
        "rows_ingested": 2,
        "total_processed": 6,
        "bronze_key": "bronze/batch.parquet",
    }
    assert saved == [("2024-01-02T00:00:00+00:00", 6)]


def test_producer_does_not_advance_state_when_bronze_write_fails(monkeypatch):
    monkeypatch.setattr(producer, "get_settings", lambda: SimpleNamespace(batch_size=10))
    monkeypatch.setattr(producer, "load_state", lambda: {"last_timestamp": None, "total_processed": 0})
    monkeypatch.setattr(producer, "_read_next_batch", lambda last, size: _batch())
    monkeypatch.setattr(producer, "attach_detection_flags", lambda dataframe: dataframe)
    monkeypatch.setattr(producer, "_write_bronze", lambda *_args: (_ for _ in ()).throw(OSError("S3 unavailable")))
    monkeypatch.setattr(producer, "save_state", lambda *_args: (_ for _ in ()).throw(AssertionError("state must not advance")))

    with pytest.raises(OSError, match="S3 unavailable"):
        producer.handler({}, None)


def test_producer_disables_schedule_when_source_is_exhausted(monkeypatch):
    disabled = []
    monkeypatch.setattr(producer, "get_settings", lambda: SimpleNamespace(batch_size=10))
    monkeypatch.setattr(producer, "load_state", lambda: {"last_timestamp": "2024-01-01T00:00:00+00:00", "total_processed": 12})
    monkeypatch.setattr(producer, "_read_next_batch", lambda last, size: pl.DataFrame())
    monkeypatch.setattr(producer, "_disable_eventbridge_rule", lambda: disabled.append(True))

    result = producer.handler({}, None)

    assert result == {"status": "EXHAUSTED", "total_processed": 12}
    assert disabled == [True]
