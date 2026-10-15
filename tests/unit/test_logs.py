"""JSON logs: one object per line, merged agent events, incident id on every line."""

import json
import logging

from agent import telemetry
from agent.logs import JsonFormatter


def render(msg, *args, extra=None, exc=None):
    record = logging.getLogger("t").makeRecord(
        "t", logging.INFO, "f.py", 1, msg, args, exc, extra=extra
    )
    return json.loads(JsonFormatter().format(record))


def test_plain_message():
    out = render("hello %s", "world")
    assert out["msg"] == "hello world" and out["level"] == "info" and "ts" in out


def test_agent_event_json_is_merged_not_nested():
    out = render(json.dumps({"event": "route", "incident_id": "i1", "next": "reporter"}))
    assert out["event"] == "route" and out["next"] == "reporter" and "msg" not in out


def test_bound_incident_is_added_to_every_line():
    with telemetry.bind("inc-7", run_id=42):
        out = render("celery says something")
    assert out["incident_id"] == "inc-7" and out["agent_run_id"] == 42
    assert "incident_id" not in render("outside any incident")


def test_extra_fields_and_exceptions():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        out = render("failed", extra={"incident_id": "i9", "tool": "x"}, exc=sys.exc_info())
    assert out["incident_id"] == "i9" and out["tool"] == "x"
    assert "ValueError: boom" in out["exc"]
