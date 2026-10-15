"""Failure injection into the agent itself. Evaluation only: CHAOS_MODE=on.

Rates come from eval/chaos.yaml (or the file named by CHAOS_CONFIG):

    tool_failure_rate        the tool envelope raises a timeout or a 503-style error
                             before the real tool runs
    llm_failure_rate         a primary-model call raises a rate-limit error
    primary_down_scenarios   for this share of incidents the primary provider fails every
                             call, forcing the fallback
    worker_kill_scenarios    share of scenarios in which the eval runner kills the worker
                             mid-run (eval/run_eval.py does the killing)

Draws are deterministic per incident, so a chaos run can be replayed. Every injected
failure carries "(chaos)" in its message and is logged with chaos_injected=True.
"""

from __future__ import annotations

import hashlib
import os
import random
import threading
from functools import lru_cache
from pathlib import Path

import yaml

from agent.settings import get_settings
from agent.tools.errors import ToolTimeout, UpstreamError

DEFAULT_CONFIG = Path(__file__).parents[1] / "eval" / "chaos.yaml"


class ChaosRateLimit(Exception):  # noqa: N818 - mirrors a provider's RateLimitError
    """Injected provider rate limit (HTTP 429)."""


@lru_cache
def rates() -> dict[str, float]:
    path = Path(os.environ.get("CHAOS_CONFIG") or DEFAULT_CONFIG)
    if not path.exists():
        return {}
    return {
        k: float(v) for k, v in (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).items()
    }


def enabled() -> bool:
    return get_settings().chaos_mode == "on"


_lock = threading.Lock()
_rngs: dict[str, random.Random] = {}
_current_incident: str | None = None


def set_incident(incident_id: str | None) -> None:
    """The worker processes one incident at a time; LLM hooks use this to pick the draw."""
    global _current_incident
    _current_incident = incident_id


def _seed(incident_id: str, salt: str) -> int:
    return int(hashlib.sha256(f"{incident_id}|{salt}".encode()).hexdigest()[:12], 16)


def _draw(incident_id: str | None, stream: str) -> float:
    key = f"{incident_id or 'none'}|{stream}"
    with _lock:
        rng = _rngs.setdefault(key, random.Random(_seed(incident_id or "none", stream)))
        return rng.random()


def scenario_flag(incident_id: str, name: str) -> bool:
    """A per-incident yes/no (e.g. primary_down), stable across calls and restarts."""
    share = rates().get(name, 0.0)
    return (_seed(incident_id, name) % 10_000) / 10_000 < share


def before_tool(tool: str, incident_id: str | None = None) -> None:
    """Raise a timeout or a 503-style error before the real tool runs, at the configured rate."""
    if not enabled():
        return
    if _draw(incident_id, "tool") < rates().get("tool_failure_rate", 0.0):
        if _draw(incident_id, "tool_kind") < 0.5:
            raise ToolTimeout(f"{tool}: timed out (chaos)")
        raise UpstreamError(f"{tool}: HTTP 503 Service Unavailable (chaos)")


def before_primary_llm(node: str) -> None:
    """Raise a rate-limit error on the primary model: always when this incident's primary is
    'down', otherwise at llm_failure_rate."""
    if not enabled():
        return
    incident = _current_incident
    if incident and scenario_flag(incident, "primary_down_scenarios"):
        raise ChaosRateLimit(f"{node}: primary provider unavailable (chaos)")
    if _draw(incident, "llm") < rates().get("llm_failure_rate", 0.0):
        raise ChaosRateLimit(f"{node}: 429 rate limit (chaos)")
