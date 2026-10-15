"""JSON logs for the API and the worker: one object per line, with the incident id.

    docker compose logs worker | grep <incident_id>

then tells one incident's whole story. A message that is itself a JSON object (the agent's
log_event lines) is merged into the record instead of being nested as a string.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

from agent import telemetry

_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
        }
        message = record.getMessage()
        parsed = None
        if message.startswith("{"):
            try:
                parsed = json.loads(message)
            except ValueError:
                parsed = None
        if isinstance(parsed, dict):
            out.update(parsed)
        else:
            out["msg"] = message
        for key, value in vars(record).items():  # logger.info(..., extra={...})
            if key not in _STANDARD and not key.startswith("_"):
                out[key] = value
        incident = out.get("incident_id") or telemetry.current_incident()
        if incident:
            out["incident_id"] = incident
            run = telemetry.current_run()
            if run is not None:
                out.setdefault("agent_run_id", run)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure(level: str = "INFO") -> None:
    """Route everything (including uvicorn and celery) through one JSON handler on stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "celery", "celery.task"):
        lg = logging.getLogger(name)
        lg.handlers = []
        lg.propagate = True
    for noisy in ("httpx", "httpcore", "openai", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
