import importlib

orchestrator = importlib.import_module("src.orchestrator.handler")


def test_orchestrator_runs_pipeline_in_order(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestrator, "producer_handler", lambda event, context: calls.append("producer") or {"bronze_key": "bronze/x.parquet"})
    monkeypatch.setattr(orchestrator, "pipeline_handler", lambda event, context: calls.append(event["bronze_key"]) or {"status": "OK"})
    monkeypatch.setattr(orchestrator, "analytics_handler", lambda event, context: calls.append("analytics") or {"status": "OK"})

    result = orchestrator.handler({"batch_size_override": 10}, None)

    assert result["status"] == "OK"
    assert calls == ["producer", "bronze/x.parquet", "analytics"]


def test_orchestrator_stops_when_producer_is_exhausted(monkeypatch):
    monkeypatch.setattr(orchestrator, "producer_handler", lambda event, context: {"status": "EXHAUSTED"})
    monkeypatch.setattr(orchestrator, "pipeline_handler", lambda *_args: (_ for _ in ()).throw(AssertionError("must not run")))

    assert orchestrator.handler({}, None) == {"status": "EXHAUSTED", "producer": {"status": "EXHAUSTED"}}
