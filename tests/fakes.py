"""Shared fakes: a scripted LLM and builders for its answers."""

from __future__ import annotations

from collections import defaultdict

from langchain_core.messages import AIMessage

from agent.llm import CallRecord, LLMResult
from agent.state import Diagnosis, Postmortem, ProposedAction, RemediationDecision


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
