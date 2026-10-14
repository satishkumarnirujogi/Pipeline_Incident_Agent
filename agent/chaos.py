"""Failure injection into the agent itself. Evaluation only (CHAOS_MODE=on).

Phase 5 wires the hook into the tool envelope; Phase 7 adds the LLM hooks,
per-scenario rates from eval/chaos.yaml and the worker kills.
"""

from __future__ import annotations

import random
from pathlib import Path

import yaml

from agent.settings import get_settings
from agent.tools.errors import ToolTimeout, UpstreamError

CHAOS_FILE = Path("eval/chaos.yaml")
_rng = random.Random()


def _rates() -> dict[str, float]:
    if not CHAOS_FILE.exists():
        return {}
    return yaml.safe_load(CHAOS_FILE.read_text(encoding="utf-8")) or {}


def before_tool(tool: str) -> None:
    """Raise a timeout or a 503-style error before the real tool runs, at the configured rate."""
    if get_settings().chaos_mode != "on":
        return
    rate = float(_rates().get("tool_failure_rate", 0.0))
    if _rng.random() < rate:
        if _rng.random() < 0.5:
            raise ToolTimeout(f"{tool}: timed out (chaos)")
        raise UpstreamError(f"{tool}: HTTP 503 Service Unavailable (chaos)")
