"""FastAPI: incidents, approvals, health. Phase 0 only exposes /health."""

import psycopg
import redis
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from agent.settings import get_settings

app = FastAPI(title="Pipeline Incident Agent API")


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
