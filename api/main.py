"""FastAPI: incidents, approvals, health.

POST /incidents only validates and logs for now; storing, deduping and
enqueueing arrive with the incidents table and Celery tasks (Phases 6 and 10).
"""

import logging
from typing import Any

import psycopg
import redis
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from agent.settings import get_settings

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="Pipeline Incident Agent API")


class IncidentIn(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    try_number: int
    logical_date: str | None = None
    conf: dict[str, Any] = Field(default_factory=dict)
    exception: str = Field(default="", max_length=2000)


@app.post("/incidents", status_code=202)
def create_incident(incident: IncidentIn) -> dict:
    log.info(
        "incident received dag_id=%s run_id=%s task_id=%s try=%s exception=%r",
        incident.dag_id,
        incident.run_id,
        incident.task_id,
        incident.try_number,
        incident.exception[:200],
    )
    return {"status": "accepted"}


@app.get("/health")
def health() -> JSONResponse:
    s = get_settings()
    checks: dict[str, str] = {}
    try:
        with psycopg.connect(s.agent_dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 - report any failure as unhealthy
        checks["postgres"] = f"error: {type(exc).__name__}"
    try:
        redis.Redis.from_url(s.redis_url, socket_timeout=3).ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {type(exc).__name__}"

    healthy = all(v == "ok" for v in checks.values())
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", **checks}, status_code=200 if healthy else 503
    )
