import importlib

import polars as pl

pipeline = importlib.import_module("src.pipeline.handler")


def test_pipeline_rejects_event_without_bronze_key():
    assert pipeline.handler({}, None) == {"status": "ERROR", "reason": "missing bronze_key"}


def test_pipeline_routes_both_dq_failure_types_and_writes_only_tier2_passes(monkeypatch):
    raw = pl.DataFrame({"id": [1, 2, 3]})
    t1_passed = pl.DataFrame({"id": [1, 2]})
    t1_failed = pl.DataFrame({"id": [3]})
    t2_passed = pl.DataFrame({"id": [1]})
    t2_failed = pl.DataFrame({"id": [2]})
    quarantines = []

    monkeypatch.setattr(pipeline, "_read_bronze", lambda _key: raw)
    monkeypatch.setattr(pipeline, "run_tier1", lambda _df: (t1_passed, t1_failed))
    monkeypatch.setattr(pipeline, "run_tier2", lambda _df: (t2_passed, t2_failed))
    monkeypatch.setattr(
        pipeline,
        "_write_quarantine",
        lambda dataframe, _key, error_col: quarantines.append((dataframe.height, error_col)),
    )
    monkeypatch.setattr(pipeline, "_write_silver", lambda dataframe: "silver/test.parquet")

    result = pipeline.handler({"bronze_key": "bronze/test.parquet"}, None)

    assert quarantines == [(1, "tier1_error_code"), (1, "tier2_error_code")]
    assert result == {
        "status": "OK",
        "t1_passed": 2,
        "t1_failed": 1,
        "t2_passed": 1,
        "t2_failed": 1,
        "silver_key": "silver/test.parquet",
    }


def test_pipeline_does_not_write_silver_when_every_row_fails_tier1(monkeypatch):
    monkeypatch.setattr(pipeline, "_read_bronze", lambda _key: pl.DataFrame({"id": [1]}))
    monkeypatch.setattr(pipeline, "run_tier1", lambda _df: (pl.DataFrame({"id": []}), pl.DataFrame({"id": [1]})))
    monkeypatch.setattr(pipeline, "_write_quarantine", lambda *_args: None)
    monkeypatch.setattr(pipeline, "_write_silver", lambda _df: (_ for _ in ()).throw(AssertionError("must not write Silver")))

    result = pipeline.handler({"bronze_key": "bronze/test.parquet"}, None)

    assert result["silver_rows"] == 0
