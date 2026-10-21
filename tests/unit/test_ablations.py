"""Ablation switches (agent/ablation.py): no memory, no recovery, single agent."""

from __future__ import annotations

import httpx
import pytest
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from agent import llm as llm_module
from agent.agents import common, diagnostics
from agent.graph import build_graph, compile_graph, run_config
from agent.llm import LLM, LLMUnavailable
from agent.settings import get_settings
from agent.tools.registry import ToolResult, call_tool
from agent.tools.store import InMemoryToolStore
from tests.fakes import POSTMORTEM, RERUN, FakeLLM, call, diagnosis, done
from tests.unit.test_resilience import StubModel, ctx, flaky, http_503  # noqa: F401


@pytest.fixture
def ablation(monkeypatch):
    def set_(name: str) -> None:
        monkeypatch.setenv("ABLATION", name)
        get_settings.cache_clear()

    return set_


# ------------------------------------------------------------------ memory


def test_full_system_offers_incident_memory():
    assert "search_incident_memory" in diagnostics.tools()
    assert "search_incident_memory" in diagnostics._prompt()


def test_no_memory_removes_the_tool_and_its_prompt_step(ablation):
    ablation("no_memory")
    assert "search_incident_memory" not in diagnostics.tools()
    assert "search_incident_memory" not in diagnostics._prompt()
    assert "get_task_log" in diagnostics.tools()


# ---------------------------------------------------------------- recovery


def test_no_recovery_tries_a_tool_once(flaky, ablation):  # noqa: F811
    ablation("no_recovery")
    flaky["r_read"] = [http_503(), {"ok": 1}]
    c = ctx()
    assert not call_tool("r_read", {}, c).ok
    assert len(c.store.calls) == 1


def test_no_recovery_has_no_model_fallback(ablation):
    ablation("no_recovery")
    primary = StubModel(error=httpx.ConnectError("provider down"))
    fallback = StubModel(AIMessage("f"))
    with pytest.raises(LLMUnavailable):
        LLM(primary, fallback).with_tools("diagnostics", [], [])
    assert fallback.calls == 0


def test_no_recovery_graph_has_no_node_retry_policies(ablation):
    assert build_graph().nodes["diagnostics"].retry_policy is not None
    ablation("no_recovery")
    assert all(n.retry_policy is None for n in build_graph().nodes.values())


# ------------------------------------------------------------ single agent

INCIDENT = {
    "dag_id": "ingest_taxi_trips",
    "run_id": "manual__1",
    "task_id": "validate_contract",
    "try_number": 2,
    "conf": {"month": "2026-06"},
    "exception": "ContractViolation: null rate 0.71 > 0.35 for passenger_count",
}
QUARANTINE = {
    "table": "raw.yellow_trips",
    "month": "2026-06",
    "predicate": {"column": "passenger_count", "op": "is_null", "value": None},
}


@pytest.fixture
def tools(monkeypatch):
    calls: list[tuple[str, dict, str | None]] = []

    def run_tool(name, args, c):
        calls.append((name, args, c.approval_id))
        if name == "rerun_dag":
            return ToolResult(ok=True, data={"dag_id": args["dag_id"], "run_id": "r-new"})
        if name == "wait_for_dag_run":
            return ToolResult(ok=True, data={"chain_state": "success"})
        return ToolResult(ok=True, data={"tool": name})

    monkeypatch.setattr(common, "run_tool", run_tool)
    monkeypatch.setattr(
        common, "run_dry", lambda n, a, c: ToolResult(ok=True, data={"effect": "preview"})
    )
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    return calls


def run_single(script, decisions=()):
    fake = FakeLLM(script)
    llm_module.set_llm(fake)
    try:
        graph = compile_graph(InMemorySaver())
        config = run_config("inc-s")
        state = graph.invoke({"incident_id": "inc-s", "incident": INCIDENT}, config)
        interrupts = []
        for decision in decisions:
            assert "__interrupt__" in state
            interrupts.append(state["__interrupt__"][0].value)
            state = graph.invoke(Command(resume=decision), config)
        assert "__interrupt__" not in state
    finally:
        llm_module.set_llm(None)
    return state, interrupts, fake


def test_single_agent_graph_has_no_supervisor(ablation):
    ablation("single_agent")
    nodes = set(build_graph().nodes)
    assert "single_agent" in nodes and "supervisor" not in nodes and "diagnostics" not in nodes


def test_single_agent_actions_still_pass_the_approval_gate(ablation, tools):
    ablation("single_agent")
    state, interrupts, fake = run_single(
        {
            "single": [
                call("get_task_log"),
                call("quarantine_rows", QUARANTINE, "c2"),
                call("rerun_dag", RERUN, "c3"),
                done("verified"),
            ],
            "single_finalize": [diagnosis("NULL_SPIKE")],
            "reporter": [POSTMORTEM],
        },
        decisions=[{"decision": "approve", "approval_id": "ap-1"}],
    )
    assert interrupts[0]["tool"] == "quarantine_rows"  # tier 2 paused for a human
    executed = [(n, a) for n, a, _ in tools if n in ("quarantine_rows", "rerun_dag")]
    assert executed == [("quarantine_rows", QUARANTINE), ("rerun_dag", RERUN)]
    assert [n for n, _, ap in tools if n == "quarantine_rows" and ap == "ap-1"]
    assert state["status"] == "resolved" and state["diagnosis"]["root_cause"] == "NULL_SPIKE"
    # the model saw the verified rerun as the answer to its rerun_dag call
    answer = fake.asked["single"][3][-1].content
    assert '"verified": true' in answer


def test_single_agent_sees_a_rejection_and_can_escalate(ablation, tools):
    ablation("single_agent")
    state, _, fake = run_single(
        {
            "single": [
                call("quarantine_rows", QUARANTINE),
                call("escalate_to_human", {"summary": "nulls", "reason": "human said no"}, "c2"),
            ],
            "single_finalize": [diagnosis("NULL_SPIKE")],
            "reporter": [POSTMORTEM],
        },
        decisions=[{"decision": "reject", "approval_id": "ap-1", "comment": "too many rows"}],
    )
    assert "rejected by a human: too many rows" in fake.asked["single"][1][-1].content
    assert state["status"] == "escalated"
    assert state["escalation"]["reason"] == "human said no"
    assert "quarantine_rows" not in [n for n, _, _ in tools]


def test_single_agent_that_stops_without_a_fix_is_escalated(ablation, tools):
    ablation("single_agent")
    state, _, _ = run_single(
        {
            "single": [done("not sure")],
            "single_finalize": [diagnosis("NULL_SPIKE")],
            "reporter": [POSTMORTEM],
        }
    )
    assert state["status"] == "escalated"
    assert state["escalation"]["reason"] == "agent stopped without a verified fix"


def test_single_agent_tools_are_granted_to_the_roles_that_run_them():
    """Regression: reads ran as an unknown role and every one was refused (permission)."""
    from agent import single
    from agent.tools.registry import REGISTRY

    for name in single.tools():
        role = "remediation" if name in single.ACTIONS else single.READ_ROLE
        assert role in REGISTRY[name].agents, name
