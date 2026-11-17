import json
from types import SimpleNamespace

from src.producer import state_manager


class _NoSuchKey(Exception):
    pass


class _FakeClient:
    class exceptions:
        NoSuchKey = _NoSuchKey

    def __init__(self, state=None):
        self.state = state
        self.put_calls = []

    def get_object(self, **_kwargs):
        if self.state is None:
            raise _NoSuchKey()
        return {"Body": self}

    def read(self):
        return json.dumps(self.state).encode("utf-8")

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def test_load_state_initializes_defaults_when_object_does_not_exist(monkeypatch):
    monkeypatch.setattr(state_manager, "get_settings", lambda: SimpleNamespace(s3_bucket="bucket", s3_state_key="state/state.json"))
    monkeypatch.setattr(state_manager, "get_s3_client", lambda: _FakeClient())

    assert state_manager.load_state() == {"last_timestamp": None, "total_processed": 0, "last_run": None}


def test_save_state_persists_cursor_and_total(monkeypatch):
    client = _FakeClient()
    monkeypatch.setattr(state_manager, "get_settings", lambda: SimpleNamespace(s3_bucket="bucket", s3_state_key="state/state.json"))
    monkeypatch.setattr(state_manager, "get_s3_client", lambda: client)

    state_manager.save_state("2024-01-01T00:00:00+00:00", 42)

    assert len(client.put_calls) == 1
    payload = json.loads(client.put_calls[0]["Body"])
    assert payload["last_timestamp"] == "2024-01-01T00:00:00+00:00"
    assert payload["total_processed"] == 42
