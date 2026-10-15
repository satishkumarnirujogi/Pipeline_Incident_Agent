"""Retry layers and chaos: tool retries, LLM fallback under injected failures, loops."""

from __future__ import annotations

import httpx
import pytest
from langchain_core.messages import AIMessage

from agent import chaos
from agent.llm import LLM
from agent.settings import get_settings
from agent.tools.common import Args
from agent.tools.registry import (
    REGISTRY,
    TIER_READONLY,
    TIER_REVERSIBLE,
    ToolContext,
    call_tool,
    tool_spec,
)
from agent.tools.store import InMemoryToolStore


class In(Args):
    n: int = 0


@pytest.fixture
def flaky(monkeypatch):
    """Tools whose behaviour each test scripts: a list of outcomes, consumed per call."""
    script: dict[str, list] = {}
    names = []

    def make(name, tier, idempotent):
        def fn(a, ctx):
            outcome = script[name].pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        fn.__name__ = name
        REGISTRY.pop(name, None)
        tool_spec(
            tier=tier, input_model=In, agents={"diagnostics", "remediation"}, idempotent=idempotent
        )(fn)
        names.append(name)

    make("r_read", TIER_READONLY, False)
    make("r_unsafe", TIER_REVERSIBLE, False)
    yield script
    for n in names:
        REGISTRY.pop(n, None)


def ctx(agent="diagnostics", incident="inc-r"):
    return ToolContext(incident_id=incident, agent=agent, store=InMemoryToolStore())


def http_503():
    return httpx.HTTPStatusError(
        "x", request=httpx.Request("GET", "http://u/p"), response=httpx.Response(503)
    )


def test_retryable_failures_are_retried_and_each_attempt_logged(flaky):
    flaky["r_read"] = [http_503(), httpx.ReadTimeout("slow"), {"ok": 1}]
    c = ctx()
    r = call_tool("r_read", {}, c)
    assert r.ok and r.data == {"ok": 1}
    rows = [(row["attempt"], row["ok"], row["error_type"]) for row in c.store.calls]
    assert rows == [(1, False, "upstream"), (2, False, "timeout"), (3, True, None)]


def test_retries_stop_after_three_attempts(flaky):
    flaky["r_read"] = [http_503(), http_503(), http_503(), {"never": 1}]
    c = ctx()
    r = call_tool("r_read", {}, c)
    assert not r.ok and r.error_type == "upstream" and r.retryable
    assert [row["attempt"] for row in c.store.calls] == [1, 2, 3]


def test_non_retryable_errors_are_not_retried(flaky):
    flaky["r_read"] = [ValueError("bad data"), {"ok": 1}]
    c = ctx()
    assert not call_tool("r_read", {}, c).ok
    assert len(c.store.calls) == 1


def test_non_idempotent_actions_are_not_retried(flaky):
    flaky["r_unsafe"] = [http_503(), {"ok": 1}]
    c = ctx("remediation")
    r = call_tool("r_unsafe", {}, c)
    assert not r.ok and len(c.store.calls) == 1


# -------------------------------------------------------------------- chaos


@pytest.fixture
def chaos_on(monkeypatch, tmp_path):
    def configure(**rates):
        path = tmp_path / "chaos.yaml"
        path.write_text("\n".join(f"{k}: {v}" for k, v in rates.items()), encoding="utf-8")
        monkeypatch.setenv("CHAOS_CONFIG", str(path))
        monkeypatch.setenv("CHAOS_MODE", "on")
        get_settings.cache_clear()
        chaos.rates.cache_clear()
        chaos._rngs.clear()

    yield configure
    chaos.rates.cache_clear()
    chaos._rngs.clear()
    chaos.set_incident(None)


def test_chaos_tool_failures_are_retried_and_flagged(flaky, chaos_on):
    chaos_on(tool_failure_rate=1.0)
    flaky["r_read"] = [{"ok": 1}] * 3
    c = ctx()
    r = call_tool("r_read", {}, c)
    assert not r.ok and "(chaos)" in r.message
    assert [row["chaos_injected"] for row in c.store.calls] == [True, True, True]


def test_chaos_is_deterministic_per_incident(flaky, chaos_on):
    def outcomes(incident):
        chaos_on(tool_failure_rate=0.5)
        flaky["r_read"] = [{"ok": 1}] * 30
        c = ctx(incident=incident)
        return [call_tool("r_read", {}, c).ok for _ in range(10)]

    assert outcomes("inc-a") == outcomes("inc-a")
    assert outcomes("inc-a") != outcomes("inc-b")


class StubModel:
    """Minimal chat model: answers every call with the same message, or raises."""

    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.calls = reply, error, 0

    def bind_tools(self, tools):
        return self

    def with_structured_output(self, schema, **kw):
        stub = self

        class Structured:
            def invoke(self, messages, config=None):
                stub.calls += 1
                return {
                    "raw": AIMessage(""),
                    "parsed": schema(next="escalate", reason="r"),
                    "parsing_error": None,
                }

        return Structured()

    def invoke(self, messages, config=None):
        self.calls += 1
        if self.error:
            raise self.error
        return self.reply


def test_llm_rate_limit_chaos_falls_back(chaos_on):
    chaos_on(llm_failure_rate=1.0)
    primary, fallback = StubModel(AIMessage("p")), StubModel(AIMessage("f"))
    result = LLM(primary, fallback).with_tools("diagnostics", [], [])
    assert result.message.content == "f" and primary.calls == 0
    first, second = result.calls
    assert (first.ok, first.error_type, first.chaos_injected) == (False, "rate_limit", True)
    assert (second.ok, second.fallback_used) == (True, True)


def test_primary_down_incident_uses_fallback_for_every_call(chaos_on):
    from agent.state import RouteDecision

    chaos_on(primary_down_scenarios=1.0)
    chaos.set_incident("inc-down")
    primary, fallback = StubModel(AIMessage("p")), StubModel(AIMessage("f"))
    llm = LLM(primary, fallback)
    for _ in range(3):
        assert llm.with_tools("diagnostics", [], []).message.content == "f"
    assert llm.structured("supervisor", [], RouteDecision).calls[-1].fallback_used
    assert primary.calls == 0


def test_real_provider_error_also_falls_back():
    primary = StubModel(error=httpx.ConnectError("provider down"))
    fallback = StubModel(AIMessage("f"))
    result = LLM(primary, fallback).with_tools("diagnostics", [], [])
    assert result.message.content == "f"
    assert result.calls[0].error_type == "ConnectError"
    assert not result.calls[0].chaos_injected


def test_both_providers_down_raises_for_the_node_retry_policy():
    from langgraph.types import default_retry_on

    from agent.llm import LLMUnavailable

    assert default_retry_on(LLMUnavailable("x"))  # the node retry policy will retry it
    with pytest.raises(LLMUnavailable, match="both failed"):
        LLM(StubModel(error=RuntimeError("a")), StubModel(error=RuntimeError("b"))).with_tools(
            "diagnostics", [], []
        )


# --------------------------------------------------------------------- loop


def test_scenario_forced_into_a_loop_escalates_within_budget(monkeypatch):
    from langgraph.checkpoint.memory import InMemorySaver

    from agent import llm as llm_module
    from agent.agents import common
    from agent.graph import compile_graph, run_config
    from agent.tools.registry import ToolResult
    from tests.fakes import POSTMORTEM, FakeLLM, call, diagnosis, done

    monkeypatch.setattr(
        common,
        "run_tool",
        lambda n, a, c: ToolResult(
            ok=False, error_type="upstream", retryable=True, message="HTTP 503"
        ),
    )
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    same = {"dag_id": "ingest_weather", "limit": 5}
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [call("get_dag_run_history", same, f"c{i}") for i in range(3)]
                + [done()],
                "diagnostics_finalize": [diagnosis("UPSTREAM_API_OUTAGE", 0.8)],
                "reporter": [POSTMORTEM],
            }
        )
    )
    try:
        state = compile_graph(InMemorySaver()).invoke(
            {
                "incident_id": "inc-loop",
                "incident": {
                    "dag_id": "ingest_weather",
                    "run_id": "r",
                    "task_id": "fetch_weather",
                    "try_number": 3,
                    "conf": {"month": "2026-06"},
                },
            },
            run_config("inc-loop"),
        )
    finally:
        llm_module.set_llm(None)
    assert state["status"] == "escalated"
    assert state["routing"][-1]["rule"] == 1
    assert "loop: get_dag_run_history" in state["routing"][-1]["reason"]
    assert state["steps"] < get_settings().max_steps


def test_hung_model_call_times_out_and_falls_back(monkeypatch):
    import time as _time

    from agent import llm as llm_mod

    monkeypatch.setattr(llm_mod, "CALL_TIMEOUT_S", 0.3)

    class Hanging(StubModel):
        def invoke(self, messages, config=None):
            _time.sleep(2)
            return AIMessage("too late")

    result = LLM(Hanging(), StubModel(AIMessage("f"))).with_tools("reporter", [], [])
    assert result.message.content == "f"
    assert result.calls[0].error_type == "timeout"


def test_reporter_writes_a_template_postmortem_when_models_are_down(monkeypatch):
    from langgraph.checkpoint.memory import InMemorySaver

    from agent import llm as llm_module
    from agent.agents import common
    from agent.graph import compile_graph, run_config
    from agent.llm import LLMUnavailable
    from agent.tools.registry import ToolResult
    from tests.fakes import FakeLLM, diagnosis, done

    written = {}

    def run_tool(name, args, ctx):
        written[name] = args
        return ToolResult(ok=True, data={})

    monkeypatch.setattr(common, "run_tool", run_tool)
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)

    class DownForReporter(FakeLLM):
        def structured(self, node, messages, schema):
            if node == "reporter":
                raise LLMUnavailable("reporter: both down")
            return super().structured(node, messages, schema)

    llm_module.set_llm(
        DownForReporter(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("MISSING_SOURCE", rec="escalate")],
            }
        )
    )
    try:
        state = compile_graph(InMemorySaver()).invoke(
            {
                "incident_id": "inc-pm",
                "incident": {
                    "dag_id": "ingest_taxi_trips",
                    "run_id": "r",
                    "task_id": "check_landing_file",
                    "try_number": 2,
                    "conf": {"month": "2026-06"},
                    "exception": "FileNotFoundError: gone",
                },
            },
            run_config("inc-pm"),
        )
    finally:
        llm_module.set_llm(None)
    assert state["status"] == "escalated"
    assert "written without an LLM" in written["write_postmortem"]["markdown"]
    assert written["save_incident_memory"]["root_cause"] == "MISSING_SOURCE"


def test_remediation_does_not_see_missing_information():
    from agent.agents.remediation import _history

    state = {
        "incident_id": "i",
        "incident": {"dag_id": "d", "task_id": "t", "month": "m"},
        "diagnosis": {
            "root_cause": "DUPLICATE_LOAD",
            "confidence": 0.85,
            "missing_information": "confirm before any rerun",
        },
    }
    assert "missing_information" not in _history(state)["diagnosis"]
