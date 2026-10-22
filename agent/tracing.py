"""Langfuse tracing: one trace per incident with nested spans for nodes, LLM calls and tools.

Off unless LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set. The trace id is derived
from the incident id, so the first run, a resume after approval hours later and a crash
recovery all land in the same trace. Postgres stays the source of truth for KPIs; this is
for looking at one run. Tracing never breaks a run: every call here swallows its errors.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from typing import Any

from agent.settings import get_settings

log = logging.getLogger(__name__)
_ctx: dict[str, Any] = {"trace": None}


def enabled() -> bool:
    s = get_settings()
    return bool(s.langfuse_public_key and s.langfuse_secret_key)


@lru_cache
def client() -> Any:
    from langfuse import Langfuse

    s = get_settings()
    return Langfuse(
        public_key=s.langfuse_public_key, secret_key=s.langfuse_secret_key, host=s.langfuse_host
    )


def trace_id_for(incident_id: str) -> str:
    from langfuse import Langfuse

    return Langfuse.create_trace_id(seed=incident_id)


@lru_cache
def _project_id() -> str:
    return client().api.projects.get().data[0].id


def trace_url(incident_id: str | None) -> str | None:
    """Link to the incident's trace in the Langfuse UI, or None without keys or a connection."""
    if not incident_id or not enabled():
        return None
    try:
        host = get_settings().langfuse_host.rstrip("/")
        return f"{host}/project/{_project_id()}/traces/{trace_id_for(incident_id)}"
    except Exception:  # noqa: BLE001 - a missing link must never break a page or report
        return None


def current_trace_context() -> Any:
    """{"trace_id", "parent_span_id"} of the job span, for spans made in other threads."""
    return _ctx["trace"]


@contextmanager
def incident_trace(incident_id: str, kind: str, incident: dict | None = None) -> Iterator[None]:
    if not enabled():
        yield
        return
    try:
        from langfuse import propagate_attributes

        lf = client()
        attrs = propagate_attributes(
            session_id=incident_id,
            trace_name=f"incident {incident_id}",
            tags=[t for t in ((incident or {}).get("dag_id"), kind) if t],
        )
        span_cm = lf.start_as_current_observation(
            name=f"agent {kind}",
            as_type="agent",
            trace_context={"trace_id": trace_id_for(incident_id)},
            metadata=incident or {},
        )
    except Exception:  # noqa: BLE001
        log.exception("langfuse: could not start the trace")
        yield
        return
    with attrs, span_cm as span:
        _ctx["trace"] = {"trace_id": span.trace_id, "parent_span_id": span.id}
        try:
            yield
        finally:
            _ctx["trace"] = None
    try:
        lf.flush()
    except Exception:  # noqa: BLE001
        log.exception("langfuse: flush failed")


def callbacks() -> list:
    """LangChain callback for graph nodes and LLM calls, attached to the job span."""
    ctx = current_trace_context()
    if not enabled() or ctx is None:
        return []
    try:
        from langfuse.langchain import CallbackHandler

        return [CallbackHandler(trace_context=ctx)]
    except Exception:  # noqa: BLE001
        log.exception("langfuse: no callback handler")
        return []


def tool_span(name: str, args: dict, result: Any, latency_ms: int) -> None:
    """One span per tool call, under the job span (works from any thread)."""
    ctx = current_trace_context()
    if not enabled() or ctx is None:
        return
    try:
        text = json.dumps(result.model_dump(exclude_none=True), default=str)
        output = json.loads(text) if len(text) <= 8000 else {"truncated": text[:8000]}
        span = client().start_observation(
            trace_context=ctx,
            name=name,
            as_type="tool",
            input=args,
            output=output,
            level="DEFAULT" if result.ok else "ERROR",
            status_message=result.message or None,
            metadata={"latency_ms": latency_ms, "replayed": result.replayed},
        )
        span.end()
    except Exception:  # noqa: BLE001
        log.exception("langfuse: tool span failed")
