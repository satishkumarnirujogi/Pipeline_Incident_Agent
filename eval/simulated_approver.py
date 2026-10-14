"""The on-call human, simulated for evaluation.

Approves a proposal when (tool, args) matches one of the scenario's required
actions on the scenario's match keys (after normalisation), and rejects it
otherwise with "not the expected fix". Every rejection counts as an
approval-precision miss in the report. Phase 6 adds the polling loop.
"""

from __future__ import annotations

from typing import Any

from faults.scenario import ExpectedAction, Scenario

REJECT_COMMENT = "not the expected fix"


def _get(args: dict[str, Any], path: str) -> Any:
    value: Any = args
    for part in path.split("."):
        value = value.get(part) if isinstance(value, dict) else None
    return value


def _norm(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int | float):
        return float(value)
    return value


def matches(expected: ExpectedAction, tool: str, args: dict[str, Any]) -> bool:
    if tool != expected.tool:
        return False
    keys = expected.match or list(expected.args)
    return all(_norm(_get(args, k)) == _norm(_get(expected.args, k)) for k in keys)


def decide(scenario: Scenario, tool: str, args: dict[str, Any]) -> dict[str, Any]:
    if any(matches(e, tool, args) for e in scenario.expected.required_actions):
        return {"decision": "approve", "comment": None}
    return {"decision": "reject", "comment": REJECT_COMMENT}
