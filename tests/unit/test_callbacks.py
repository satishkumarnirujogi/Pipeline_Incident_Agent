from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import respx
from common.callbacks import build_incident, notify_incident


def _context(exc: Exception | None = None) -> dict:
    return {
        "ti": SimpleNamespace(dag_id="d", run_id="r", task_id="t", try_number=2),
        "dag_run": SimpleNamespace(conf={"month": "2026-06"}),
        "logical_date": datetime(2026, 6, 1, tzinfo=UTC),
        "exception": exc,
    }


def test_build_incident_truncates_exception():
    payload = build_incident(_context(ValueError("x" * 5000)))
    assert payload["conf"] == {"month": "2026-06"}
    assert payload["try_number"] == 2
    assert len(payload["exception"]) == 2000
    assert payload["exception"].startswith("ValueError: ")


@respx.mock
def test_notify_posts_once(monkeypatch):
    monkeypatch.setenv("INCIDENT_WEBHOOK_URL", "http://api/incidents")
    route = respx.post("http://api/incidents").mock(return_value=httpx.Response(202))
    notify_incident(_context(FileNotFoundError("gone")))
    assert route.call_count == 1


@respx.mock
def test_notify_never_raises(monkeypatch):
    monkeypatch.setenv("INCIDENT_WEBHOOK_URL", "http://api/incidents")
    respx.post("http://api/incidents").mock(side_effect=httpx.ConnectError("down"))
    notify_incident(_context(RuntimeError("boom")))  # must not raise
