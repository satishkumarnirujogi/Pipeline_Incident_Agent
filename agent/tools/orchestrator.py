"""Airflow 3 REST adapter. The only module in the agent that knows Airflow exists."""

from __future__ import annotations

import re
import threading
import time
from datetime import datetime
from functools import lru_cache
from typing import Any

import httpx

from agent.settings import get_settings

TERMINAL = {"success", "failed"}
DAG_IDS = ("ingest_taxi_trips", "ingest_weather", "transform_dbt")

# Values that look like secrets are replaced before a log line reaches the model.
_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key)(\s*[=:]\s*)\S+"),
        r"\1\2[REDACTED]",
    ),
    (re.compile(r"(?i)(postgres(?:ql)?(?:\+\w+)?://[^:/\s@]+:)[^@\s]+@"), r"\1[REDACTED]@"),
    (re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._\-]+"), r"\1[REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9\-_]{12,}"), "[REDACTED]"),
]
_NOISE = ("::group::", "::endgroup::")


def redact(text: str) -> str:
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def format_log_events(events: list[Any]) -> list[str]:
    """Airflow 3 structured log events -> readable lines; exceptions with their last frames."""
    lines: list[str] = []
    for e in events:
        if isinstance(e, str):
            lines.append(redact(e))
            continue
        event = str(e.get("event", ""))
        if any(event.startswith(n) for n in _NOISE):
            continue
        ts = str(e.get("timestamp", ""))[:19].replace("T", " ")
        level = str(e.get("level", "info")).upper()
        logger = e.get("logger", "")
        lines.append(redact(f"{ts} {level} {logger}: {event}".rstrip()))
        for err in e.get("error_detail") or []:
            lines.append(redact(f"    {err.get('exc_type')}: {err.get('exc_value')}"))
            for fr in (err.get("frames") or [])[-3:]:
                lines.append(
                    f"      at {fr.get('filename')}:{fr.get('lineno')} in {fr.get('name')}"
                )
    return lines


class Airflow:
    def __init__(
        self, base_url: str | None = None, user: str | None = None, password: str | None = None
    ) -> None:
        s = get_settings()
        self.base_url = (base_url or s.airflow_url).rstrip("/")
        self._user = user or s.airflow_user
        self._password = password if password is not None else s.airflow_password
        self._token: str | None = None
        self._lock = threading.Lock()
        self.http = httpx.Client(base_url=f"{self.base_url}/api/v2", timeout=15)

    def _auth(self) -> dict[str, str]:
        with self._lock:
            if self._token is None:
                r = httpx.post(
                    f"{self.base_url}/auth/token",
                    timeout=10,
                    json={"username": self._user, "password": self._password},
                )
                r.raise_for_status()
                self._token = r.json()["access_token"]
            return {"Authorization": f"Bearer {self._token}"}

    def _get(self, path: str, **kw: Any) -> Any:
        r = self.http.get(path, headers=self._auth(), **kw)
        if r.status_code == 401:  # token expired
            self._token = None
            r = self.http.get(path, headers=self._auth(), **kw)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> Any:
        r = self.http.post(path, json=body, headers=self._auth())
        if r.status_code == 401:
            self._token = None
            r = self.http.post(path, json=body, headers=self._auth())
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------ reads

    def task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> list[str]:
        data = self._get(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
            params={"full_content": True},
        )
        return format_log_events(data.get("content") or [])

    def dag_runs(self, dag_id: str, limit: int) -> list[dict]:
        runs = self._get(
            f"/dags/{dag_id}/dagRuns", params={"order_by": "-run_after", "limit": limit}
        )["dag_runs"]
        return [_run_summary(r) for r in runs]

    def run(self, dag_id: str, run_id: str) -> dict:
        return _run_summary(self._get(f"/dags/{dag_id}/dagRuns/{run_id}"))

    def task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        tis = self._get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]
        return {t["task_id"]: t["state"] for t in tis}

    # ----------------------------------------------------------------- writes

    def trigger(self, dag_id: str, conf: dict) -> dict:
        run = self._post(f"/dags/{dag_id}/dagRuns", {"logical_date": None, "conf": conf})
        return _run_summary(run)

    # ------------------------------------------------------------------ waits

    def wait(self, dag_id: str, run_id: str, timeout_s: float, poll_s: float = 5) -> dict:
        deadline = time.monotonic() + timeout_s
        while True:
            run = self.run(dag_id, run_id)
            if run["state"] in TERMINAL:
                run["tasks"] = self.task_states(dag_id, run_id)
                return run
            if time.monotonic() >= deadline:
                run["tasks"] = self.task_states(dag_id, run_id)
                return run
            time.sleep(poll_s)

    def triggered_transform(self, after: str, timeout_s: float = 60) -> dict | None:
        """The transform_dbt run that an ingest_taxi_trips run triggered (started after `after`)."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            runs = self.dag_runs("transform_dbt", 5)
            for r in runs:
                if r["run_after"] and r["run_after"] >= after:
                    return r
            time.sleep(3)
        return None


def _run_summary(r: dict) -> dict:
    start, end = r.get("start_date"), r.get("end_date")
    duration = None
    if start and end:
        duration = round(
            (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(), 1
        )
    return {
        "dag_id": r["dag_id"],
        "run_id": r["dag_run_id"],
        "state": r.get("state"),
        "run_after": r.get("run_after"),
        "start_date": start,
        "end_date": end,
        "duration_s": duration,
        "conf": r.get("conf") or {},
    }


@lru_cache
def airflow() -> Airflow:
    return Airflow()
