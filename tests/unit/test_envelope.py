"""The tool envelope: tiers, approvals, idempotency, limits, timeouts, error conversion."""

import time

import httpx
import pytest
from pydantic import Field

from agent.tools import registry
from agent.tools.common import Args
from agent.tools.registry import (
    REGISTRY,
    TIER_CHANGES_DATA,
    TIER_READONLY,
    TIER_REVERSIBLE,
    ToolContext,
    args_hash,
    call_tool,
    tool_spec,
)
from agent.tools.store import InMemoryToolStore

calls: list[str] = []


class EchoIn(Args):
    value: int = Field(ge=0, le=100)


@pytest.fixture(autouse=True)
def fake_tools():
    calls.clear()
    names = []

    def add(name, tier, fn, timeout_s=5, **kw):
        REGISTRY.pop(name, None)  # a monkeypatch undo may have put an old copy back
        fn.__name__ = name
        tool_spec(tier=tier, input_model=EchoIn, agents={"remediation"}, timeout_s=timeout_s, **kw)(
            fn
        )
        names.append(name)

    def echo(a, ctx):
        calls.append("echo")
        return {"value": a.value}

    def slow(a, ctx):
        time.sleep(2)
        return {}

    def boom(a, ctx):
        raise httpx.HTTPStatusError(
            "x",
            request=httpx.Request("GET", "http://api/thing"),
            response=httpx.Response(503),
        )

    add("t_read", TIER_READONLY, echo)
    add("t_rerun", TIER_REVERSIBLE, echo, idempotent=True)
    add("t_change", TIER_CHANGES_DATA, echo, idempotent=True, dry_run=lambda a, c: {"n": 1})
    add("t_slow", TIER_READONLY, slow, timeout_s=0.2)
    add("t_boom", TIER_READONLY, boom)
    yield
    for n in names:
        REGISTRY.pop(n, None)


@pytest.fixture
def ctx():
    return ToolContext(incident_id="inc-1", agent="remediation", store=InMemoryToolStore())


def approve(ctx, tool="t_change", args=None, decision="approve", incident="inc-1"):
    args = args or {"value": 1}
    ctx.store.approvals["ap-1"] = {
        "incident_id": incident,
        "tool": tool,
        "args_hash": args_hash(args),
        "decision": decision,
    }
    ctx.approval_id = "ap-1"


def test_readonly_tool_runs_and_is_logged(ctx):
    r = call_tool("t_read", {"value": 3}, ctx)
    assert r.ok and r.data == {"value": 3}
    assert ctx.store.calls[-1]["tool"] == "t_read" and ctx.store.calls[-1]["ok"]


def test_invalid_args_fail_at_the_boundary(ctx):
    r = call_tool("t_read", {"value": 999}, ctx)
    assert not r.ok and r.error_type == "validation"
    assert "value" in r.message
    assert calls == []


def test_unknown_arguments_are_rejected(ctx):
    r = call_tool("t_read", {"value": 1, "sql": "drop table x"}, ctx)
    assert r.error_type == "validation"


def test_unknown_tool_is_a_logged_safety_violation(ctx):
    r = call_tool("drop_table", {"table": "raw.yellow_trips"}, ctx)
    assert not r.ok and r.error_type == "permission"
    assert ctx.store.calls[-1]["safety_violation"] is True


def test_agent_cannot_use_another_agents_tool(ctx):
    ctx.agent = "diagnostics"
    r = call_tool("t_read", {"value": 1}, ctx)
    assert r.error_type == "permission" and calls == []


def test_tier2_without_approval_returns_approval_required(ctx):
    r = call_tool("t_change", {"value": 1}, ctx)
    assert not r.ok and r.error_type == "approval_required"
    assert calls == []


@pytest.mark.parametrize(
    ("kwargs", "why"),
    [
        ({"decision": "reject"}, "not granted"),
        ({"decision": None}, "not granted"),
        ({"incident": "inc-2"}, "different incident"),
        ({"tool": "t_other"}, "different incident or tool"),
        ({"args": {"value": 2}}, "differ from the approved"),
    ],
)
def test_tier2_with_wrong_approval_is_refused(ctx, kwargs, why):
    approve(ctx, **kwargs)
    r = call_tool("t_change", {"value": 1}, ctx)
    assert r.error_type == "approval_required" and why in r.message
    assert calls == []


def test_tier2_with_matching_approval_runs(ctx):
    approve(ctx)
    r = call_tool("t_change", {"value": 1}, ctx)
    assert r.ok and calls == ["echo"]


def test_same_action_twice_executes_once(ctx):
    approve(ctx)
    first = call_tool("t_change", {"value": 1}, ctx)
    second = call_tool("t_change", {"value": 1}, ctx)
    assert first.ok and second.ok and second.replayed
    assert second.data == first.data
    assert calls == ["echo"]
    assert ctx.store.calls[-1]["replayed"] is True


def test_new_proposal_step_runs_again(ctx):
    call_tool("t_rerun", {"value": 1}, ctx)
    ctx.step = 1  # a deliberate second rerun, e.g. after an API outage ended
    r = call_tool("t_rerun", {"value": 1}, ctx)
    assert r.ok and not r.replayed and calls == ["echo", "echo"]


def test_tier1_limited_per_incident(ctx):
    for step in range(registry.MAX_TIER1_PER_INCIDENT):
        ctx.step = step
        assert call_tool("t_rerun", {"value": 1}, ctx).ok
    ctx.step = 99
    r = call_tool("t_rerun", {"value": 1}, ctx)
    assert r.error_type == "permission" and "limit" in r.message


def test_failed_action_can_be_retried(ctx, monkeypatch):
    import dataclasses

    approve(ctx)
    good = REGISTRY["t_change"]
    monkeypatch.setitem(REGISTRY, "t_change", dataclasses.replace(good, fn=lambda a, c: 1 / 0))
    assert call_tool("t_change", {"value": 1}, ctx).error_type == "unknown"
    key = next(iter(ctx.store.actions))
    assert ctx.store.actions[key]["status"] == "failed"

    monkeypatch.setitem(REGISTRY, "t_change", good)  # same action, now it works
    r = call_tool("t_change", {"value": 1}, ctx)
    assert r.ok and not r.replayed and calls == ["echo"]


def test_timeout_becomes_a_result(ctx):
    r = call_tool("t_slow", {"value": 1}, ctx)
    assert not r.ok and r.error_type == "timeout" and r.retryable


def test_http_503_becomes_retryable_upstream(ctx):
    r = call_tool("t_boom", {"value": 1}, ctx)
    assert r.error_type == "upstream" and r.retryable
    assert "Traceback" not in r.message and len(r.message) <= 300


def test_chaos_failure_is_marked(ctx, monkeypatch):
    from agent import chaos
    from agent.tools.errors import ToolTimeout

    def fail(tool, incident_id=None):
        raise ToolTimeout(f"{tool}: timed out (chaos)")

    monkeypatch.setattr(chaos, "before_tool", fail)
    r = call_tool("t_read", {"value": 1}, ctx)
    assert r.error_type == "timeout"
    assert ctx.store.calls[-1]["chaos_injected"] is True
    assert calls == []


def test_dry_run_is_readonly_preview(ctx):
    r = registry.dry_run("t_change", {"value": 1}, ctx)
    assert r.ok and r.data == {"n": 1} and calls == []


def test_every_real_tier2_tool_has_a_dry_run():
    import agent.tools  # noqa: F401 - registers the real tools

    tier2 = [
        s for s in REGISTRY.values() if s.tier == TIER_CHANGES_DATA and not s.name.startswith("t_")
    ]
    assert {s.name for s in tier2} == {
        "upsert_column_mapping",
        "quarantine_rows",
        "restore_quarantined",
    }
    assert all(s.dry_run for s in tier2)
