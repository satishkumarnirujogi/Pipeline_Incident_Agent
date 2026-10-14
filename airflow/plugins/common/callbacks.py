"""on_failure_callback: report a task's final failure to the incident API."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

log = logging.getLogger(__name__)

MAX_EXCEPTION_CHARS = 2000


def build_incident(context: dict[str, Any]) -> dict[str, Any]:
    ti = context["ti"]
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date")
    exception = context.get("exception")
    return {
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "task_id": ti.task_id,
        "try_number": ti.try_number,
        "logical_date": logical_date.isoformat() if logical_date else None,
        "conf": dict(getattr(dag_run, "conf", None) or {}),
        "exception": f"{type(exception).__name__}: {exception}"[:MAX_EXCEPTION_CHARS]
        if exception
        else "",
    }


def notify_incident(context: dict[str, Any]) -> None:
    """Never raises: a callback must not crash the task runner."""
    url = os.environ.get("INCIDENT_WEBHOOK_URL")
    if not url:
        log.warning("INCIDENT_WEBHOOK_URL not set; incident not reported")
        return
    try:
        payload = build_incident(context)
        httpx.post(url, json=payload, timeout=5).raise_for_status()
        log.info("incident reported for %s.%s", payload["dag_id"], payload["task_id"])
    except Exception:  # noqa: BLE001
        log.exception("failed to report incident to %s", url)
