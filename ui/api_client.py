"""The UI's only way to the system: the FastAPI service. Never the database directly."""

from __future__ import annotations

import os
from typing import Any

import httpx

API_URL = os.environ.get("API_URL", "http://api:8000")


class ApiError(Exception):
    pass


def _call(method: str, path: str, **kw: Any) -> Any:
    try:
        r = httpx.request(method, f"{API_URL}{path}", timeout=10, **kw)
    except httpx.HTTPError as exc:
        raise ApiError(f"API unreachable: {exc}") from None
    if r.status_code >= 400:
        detail = r.json().get("detail") if "json" in r.headers.get("content-type", "") else r.text
        raise ApiError(str(detail))
    return r.json()


def incidents(status: str | None = None) -> list[dict]:
    return _call("GET", "/incidents", params={"status": status} if status else {})


def incident(incident_id: str) -> dict:
    return _call("GET", f"/incidents/{incident_id}")


def approvals(status: str = "pending") -> list[dict]:
    return _call("GET", "/approvals", params={"status": status})


def decide(approval_id: str, decision: str, args: dict | None = None,
           comment: str | None = None) -> dict:
    return _call("POST", f"/approvals/{approval_id}/decision",
                 json={"decision": decision, "args": args, "comment": comment})
