"""Step and cost limits, and loop detection. Checked by the Supervisor before every route."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

from agent.settings import get_settings

LOOP_REPEATS = 3  # same tool with the same arguments this often = a loop


def over_budget(state: Mapping[str, Any]) -> str | None:
    """Why the run must stop, or None."""
    s = get_settings()
    if state.get("steps", 0) >= s.max_steps:
        return f"step budget of {s.max_steps} exhausted"
    if state.get("cost_eur", 0.0) >= s.max_cost_eur_per_incident:
        return f"cost budget of EUR {s.max_cost_eur_per_incident:.2f} exhausted"
    return None


def loop_detected(state: Mapping[str, Any]) -> str | None:
    """Repeating a call that keeps failing is a loop. Repeated successful reads are not (the
    step and cost budgets bound those), and repeated actions are capped by the tier-1 limit
    and idempotency."""
    counts = Counter(
        (t["tool"], t["args_hash"]) for t in state.get("tool_log", []) if not t.get("ok")
    )
    for (tool, _), n in counts.items():
        if n >= LOOP_REPEATS:
            return f"loop: {tool} called {n} times with the same arguments"
    return None
