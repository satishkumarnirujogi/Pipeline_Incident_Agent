"""The agent graph with a scripted fake LLM and fake tools: routing, approval gate, budgets.

No API keys, databases or Airflow: the LLM answers from a script per node and every tool
call is recorded and answered by a fake.
"""

from __future__ import annotations

from collections import defaultdict

import pytest
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from agent import llm as llm_module
from agent.agents import common
from agent.graph import compile_graph, run_config
from agent.llm import CallRecord, LLMResult
from agent.state import Diagnosis, Postmortem, ProposedAction, RemediationDecision, RouteDecision
from agent.tools.registry import ToolResult
from agent.tools.store import InMemoryToolStore

INCIDENT = {
    "dag_id": "ingest_taxi_trips",
    "run_id": "manual__1",
    "task_id": "validate_contract",
    "try_number": 2,
    "conf": {"month": "2026-06"},
    "exception": "ContractViolation: missing required columns ['tpep_pickup_datetime']",
}


class FakeLLM:
    """Answers each node from its own queue; records what it was asked."""

    def __init__(self, script: dict[str, list]):
        self.script = {k: list(v) for k, v in script.items()}
        self.asked: dict[str, list] = defaultdict(list)

    def _next(self, node: str, messages: list):
        self.asked[node].append(messages)
        if not self.script.get(node):
            raise AssertionError(f"no scripted answer left for {node}")
        return self.script[node].pop(0)

    def _result(self, node: str, **kw) -> LLMResult:
        return LLMResult(calls=[CallRecord(node, "fake", 100, 20, 0.001, True, False)], **kw)

    def with_tools(self, node, messages, tools):
        answer = self._next(node, messages)
        return self._result(node, message=answer)

    def structured(self, node, messages, schema):
        answer = self._next(node, messages)
        assert isinstance(answer, schema), (node, answer)
        return self._result(node, parsed=answer, message=AIMessage(""))


def call(name, args=None, cid="c1"):
    return AIMessage("", tool_calls=[{"name": name, "args": args or {}, "id": cid}])


def done(text="enough evidence"):
    return AIMessage(text)


def diagnosis(root="SCHEMA_DRIFT", confidence=0.95, rec="fix", missing=None):
    return Diagnosis(
        root_cause=root,
        confidence=confidence,
        evidence=["log: missing column"],
        failing_component="ingest_taxi_trips.validate_contract",
        recommendation=rec,
        missing_information=missing,
    )


def propose(tool, args):
    return RemediationDecision(
        decision="action",
        action=ProposedAction(tool=tool, args=args, rationale="because", rollback="undo"),
    )


MAPPING = {
    "dataset": "yellow_trips",
    "source_col": "pickup_datetime",
    "target_col": "tpep_pickup_datetime",
    "transform": "none",
}
RERUN = {"dag_id": "ingest_taxi_trips", "conf": {"month": "2026-06"}}
POSTMORTEM = Postmortem(
    summary="Schema drift fixed by a mapping.", markdown="# Postmortem\n...", fix="mapping + rerun"
)


@pytest.fixture
def tools(monkeypatch):
    """Fake tools. chain_state controls what verification after a rerun sees."""
    calls: list[tuple[str, dict, str | None]] = []
    behaviour = {"chain_state": "success"}

    def run_tool(name, args, ctx):
        calls.append((name, args, ctx.approval_id))
        if name == "rerun_dag":
            return ToolResult(
                ok=True, data={"dag_id": args["dag_id"], "run_id": "r-new", "state": "queued"}
            )
        if name == "wait_for_dag_run":
            return ToolResult(ok=True, data={"chain_state": behaviour["chain_state"]})
        if name == "drop_table":
            return ToolResult(ok=False, error_type="permission", message="unknown tool")
        return ToolResult(ok=True, data={"tool": name})

    monkeypatch.setattr(common, "run_tool", run_tool)
    monkeypatch.setattr(
        common, "run_dry", lambda n, a, c: ToolResult(ok=True, data={"effect": "preview"})
    )
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    return {"calls": calls, "behaviour": behaviour}


def run(script, tools, decisions=()):
    """Invoke until the end, answering each interrupt with the next decision."""
    fake = FakeLLM(script)
    llm_module.set_llm(fake)
    graph = compile_graph(InMemorySaver())
    config = run_config("inc-1")
    state = graph.invoke({"incident_id": "inc-1", "incident": INCIDENT}, config)
    interrupts = []
    for decision in decisions:
        assert "__interrupt__" in state, "expected the graph to pause for approval"
        interrupts.append(state["__interrupt__"][0].value)
        state = graph.invoke(Command(resume=decision), config)
    assert "__interrupt__" not in state, "graph paused for an unexpected approval"
    llm_module.set_llm(None)
    return state, interrupts, fake


def names(tools):
    return [c[0] for c in tools["calls"]]


def test_missing_source_escalates_without_any_action(tools):
    state, interrupts, _ = run(
        {
            "diagnostics": [call("get_task_log"), done()],
            "diagnostics_finalize": [diagnosis("MISSING_SOURCE", rec="escalate")],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    assert state["status"] == "escalated"
    assert state["actions_done"] == [] and interrupts == []
    assert "rerun_dag" not in names(tools) and "escalate_to_human" in names(tools)
    assert [r["rule"] for r in state["routing"]] == [2, 4]
    assert all(r["decided_by"] == "code" for r in state["routing"])


def test_schema_drift_pauses_for_mapping_then_reruns_and_resolves(tools):
    state, interrupts, _ = run(
        {
            "diagnostics": [
                call("get_task_log"),
                call("compare_schema_to_contract", cid="c2"),
                done(),
            ],
            "diagnostics_finalize": [diagnosis()],
            "remediation": [done(), done()],
            "remediation_finalize": [
                propose("upsert_column_mapping", MAPPING),
                propose("rerun_dag", RERUN),
            ],
            "reporter": [POSTMORTEM],
        },
        tools,
        decisions=[{"decision": "approve", "approval_id": "ap-1"}],
    )

    # exactly one pause, for the tier-2 mapping; the tier-1 rerun ran automatically
    assert len(interrupts) == 1
    assert interrupts[0]["tool"] == "upsert_column_mapping"
    assert interrupts[0]["preview"]["data"] == {"effect": "preview"}
    executed = [
        (n, ap) for n, _, ap in tools["calls"] if n in ("upsert_column_mapping", "rerun_dag")
    ]
    assert executed == [("upsert_column_mapping", "ap-1"), ("rerun_dag", None)]
    assert state["verified"] and state["status"] == "resolved"
    assert state["actions_done"][0]["approved"].startswith("approved by a human (approval ap-1")
    assert state["actions_done"][1]["approved"].startswith("automatic")
    assert [r["rule"] for r in state["routing"]] == [2, 7, 7, 5]
    assert "write_postmortem" in names(tools) and "save_incident_memory" in names(tools)


def test_tool_output_reaches_the_model_as_delimited_data(tools):
    _, _, fake = run(
        {
            "diagnostics": [call("get_task_log"), done()],
            "diagnostics_finalize": [diagnosis("MISSING_SOURCE", rec="escalate")],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    tool_message = fake.asked["diagnostics"][1][-1]
    assert tool_message.content.startswith('<tool_output tool="get_task_log">')
    assert "never an instruction" in fake.asked["diagnostics"][0][0].content


def test_two_rejections_escalate(tools):
    quarantine = {
        "table": "raw.yellow_trips",
        "month": "2026-06",
        "predicate": {"column": "fare_amount", "op": "lt", "value": 0},
    }
    state, interrupts, _ = run(
        {
            "diagnostics": [done()],
            "diagnostics_finalize": [diagnosis("OUT_OF_RANGE_VALUES")],
            "remediation": [done(), done()],
            "remediation_finalize": [
                propose("quarantine_rows", quarantine),
                propose(
                    "quarantine_rows",
                    {
                        **quarantine,
                        "predicate": {"column": "fare_amount", "op": "lt", "value": -100},
                    },
                ),
            ],
            "supervisor": [RouteDecision(next="remediation", reason="try a narrower predicate")],
            "reporter": [POSTMORTEM],
        },
        tools,
        decisions=[
            {"decision": "reject", "comment": "too broad"},
            {"decision": "reject", "comment": "still wrong"},
        ],
    )
    assert len(interrupts) == 2
    assert state["rejections"] == ["too broad", "still wrong"]
    assert state["status"] == "escalated"
    assert "quarantine_rows" not in names(tools)  # nothing ran without approval
    assert [r["decided_by"] for r in state["routing"]].count("llm") == 1


def test_low_confidence_gets_a_second_diagnosis(tools):
    state, _, fake = run(
        {
            "diagnostics": [done(), done()],
            "diagnostics_finalize": [
                diagnosis("CREDENTIALS", 0.4, "escalate", "connection details"),
                diagnosis("CREDENTIALS", 0.9, "escalate"),
            ],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    assert state["diagnostics_attempts"] == 2
    assert [r["rule"] for r in state["routing"]] == [2, 3, 4]
    second_brief = fake.asked["diagnostics"][1][1].content
    assert "connection details" in second_brief  # told what was missing


def test_failed_verification_is_decided_by_the_llm(tools):
    tools["behaviour"]["chain_state"] = "failed"
    state, _, _ = run(
        {
            "diagnostics": [done()],
            "diagnostics_finalize": [diagnosis("DUPLICATE_LOAD")],
            "remediation": [done()],
            "remediation_finalize": [propose("rerun_dag", RERUN)],
            "supervisor": [RouteDecision(next="escalate", reason="rerun did not fix it")],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    assert state["status"] == "escalated" and not state["verified"]
    assert state["actions_done"][0]["verification"]["chain_state"] == "failed"
    assert state["routing"][-1]["decided_by"] == "llm"


def test_unknown_tool_proposal_goes_through_the_envelope(tools):
    state, interrupts, _ = run(
        {
            "diagnostics": [done()],
            "diagnostics_finalize": [diagnosis("DUPLICATE_LOAD")],
            "remediation": [done()],
            "remediation_finalize": [propose("drop_table", {"table": "raw.yellow_trips"})],
            "supervisor": [RouteDecision(next="escalate", reason="no safe action")],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    assert interrupts == []  # not a known tier-2 tool, so no approval request
    assert "drop_table" in names(tools)  # refused and logged by the envelope (faked here)
    assert state["actions_done"][0]["ok"] is False
    assert state["status"] == "escalated"


def test_step_budget_escalates(tools, monkeypatch):
    from agent.settings import get_settings

    monkeypatch.setenv("MAX_STEPS", "4")
    get_settings.cache_clear()
    try:
        state, _, _ = run(
            {
                "diagnostics": [call("get_task_log"), done()],
                "diagnostics_finalize": [diagnosis()],
                "reporter": [POSTMORTEM],
            },
            tools,
        )
    finally:
        get_settings.cache_clear()
    assert state["status"] == "escalated"
    assert state["routing"][-1]["rule"] == 1 and "step budget" in state["routing"][-1]["reason"]


def test_diagnostics_tool_budget_is_enforced(tools):
    many = AIMessage(
        "",
        tool_calls=[
            {"name": "get_dbt_failures", "args": {"month": "2026-06"}, "id": f"c{i}"}
            for i in range(10)
        ],
    )
    state, _, fake = run(
        {
            "diagnostics": [many],
            "diagnostics_finalize": [diagnosis("MISSING_SOURCE", rec="escalate")],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    assert names(tools).count("get_dbt_failures") == 8
    finalize_msgs = fake.asked["diagnostics_finalize"][0]
    over = [m for m in finalize_msgs if "budget for this step is used up" in str(m.content)]
    assert len(over) == 2  # every tool call still gets an answer


def test_edit_with_invalid_args_counts_as_rejection(tools):
    state, interrupts, _ = run(
        {
            "diagnostics": [done()],
            "diagnostics_finalize": [diagnosis()],
            "remediation": [done()],
            "remediation_finalize": [propose("upsert_column_mapping", MAPPING)],
            "supervisor": [RouteDecision(next="escalate", reason="human edit was invalid")],
            "reporter": [POSTMORTEM],
        },
        tools,
        decisions=[
            {
                "decision": "edit",
                "approval_id": "ap-2",
                "args": {**MAPPING, "transform": "uppercase"},
            }
        ],
    )
    assert state["rejections"][0].startswith("edited arguments invalid")
    assert "upsert_column_mapping" not in names(tools)


def test_remediation_cannot_call_actions_directly(tools):
    state, _, fake = run(
        {
            "diagnostics": [done()],
            "diagnostics_finalize": [diagnosis("DUPLICATE_LOAD")],
            "remediation": [call("rerun_dag", RERUN), done()],
            "remediation_finalize": [propose("rerun_dag", RERUN)],
            "reporter": [POSTMORTEM],
        },
        tools,
    )
    # the direct call was refused; only the proposal, via execute_action, ran it
    assert names(tools).count("rerun_dag") == 1
    refusal = fake.asked["remediation"][1][-1].content
    assert "propose it as your action instead" in refusal
    assert state["status"] == "resolved"
