"""Minimal Airflow 3 REST client for scripts (bootstrap, backfill, later the injector).

The agent gets its own adapter in agent/tools/orchestrator.py; this one is for
operator scripts that run on the host.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from agent.settings import get_settings

TERMINAL = {"success", "failed"}


class AirflowClient:
    def __init__(self, base_url: str | None = None) -> None:
        s = get_settings()
        # the published port on the host, the compose name in Docker
        self.base_url = base_url or s.airflow_url
        token = httpx.post(
            f"{self.base_url}/auth/token",
            json={"username": s.airflow_user, "password": s.airflow_password},
            timeout=10,
        )
        token.raise_for_status()
        self.http = httpx.Client(
            base_url=f"{self.base_url}/api/v2",
            headers={"Authorization": f"Bearer {token.json()['access_token']}"},
            timeout=30,
        )

    def _req(self, method: str, path: str, **kw: Any) -> Any:
        r = self.http.request(method, path, **kw)
        r.raise_for_status()
        return r.json() if r.content else None

    # ---------------------------------------------------------- connections

    def upsert_connection(self, conn: dict[str, Any]) -> None:
        r = self.http.get(f"/connections/{conn['connection_id']}")
        if r.status_code == 404:
            self._req("POST", "/connections", json=conn)
        else:
            r.raise_for_status()
            self._req("PATCH", f"/connections/{conn['connection_id']}", json=conn)

    # ----------------------------------------------------------------- dags

    def wait_for_dag(self, dag_id: str, timeout_s: int = 120) -> None:
        """Wait until the DAG processor has parsed the DAG."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            r = self.http.get(f"/dags/{dag_id}")
            if r.status_code == 200:
                return
            time.sleep(3)
        raise TimeoutError(f"DAG {dag_id} not parsed after {timeout_s}s")

    def import_errors(self) -> list[dict]:
        return self._req("GET", "/importErrors")["import_errors"]

    def unpause(self, dag_id: str) -> None:
        self._req("PATCH", f"/dags/{dag_id}", json={"is_paused": False})

    def trigger(self, dag_id: str, conf: dict[str, Any]) -> str:
        run = self._req(
            "POST", f"/dags/{dag_id}/dagRuns", json={"logical_date": None, "conf": conf}
        )
        return run["dag_run_id"]

    def run_state(self, dag_id: str, run_id: str) -> str:
        return self._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}")["state"]

    def wait_for_run(self, dag_id: str, run_id: str, timeout_s: int = 900) -> str:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            state = self.run_state(dag_id, run_id)
            if state in TERMINAL:
                return state
            time.sleep(5)
        raise TimeoutError(f"{dag_id} {run_id} still running after {timeout_s}s")

    def latest_run(self, dag_id: str) -> dict | None:
        runs = self._req(
            "GET", f"/dags/{dag_id}/dagRuns", params={"order_by": "-run_after", "limit": 1}
        )["dag_runs"]
        return runs[0] if runs else None

    def task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        tis = self._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        return {t["task_id"]: t["state"] for t in tis["task_instances"]}
