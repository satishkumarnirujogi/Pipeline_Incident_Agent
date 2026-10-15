"""Scenario files: one injected fault with its ground truth."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

from agent.state import RootCause

SCENARIO_DIR = Path(__file__).parent / "scenarios"
# Quarantining more than this share of a month is not a fix, it is data loss: verification
# fails above it, so scenarios whose expected fix needs more are generated as errors.
MAX_QUARANTINE_SHARE = 0.7

FaultType = Literal[
    "schema_drift",
    "type_mismatch",
    "missing_source",
    "null_spike",
    "duplicate_load",
    "out_of_range_values",
    "orphan_keys",
    "upstream_api_outage",
    "incomplete_api_payload",
    "credentials",
]
Mechanism = Literal["file", "sql", "proxy", "airflow_conn"]
DagId = Literal["ingest_taxi_trips", "ingest_weather", "transform_dbt"]


class Trigger(BaseModel):
    dag_id: DagId
    conf: dict[str, Any]


class ExpectedAction(BaseModel):
    tool: str
    args: dict[str, Any]
    # Arg paths the simulated approver compares (dotted for nested keys). Others,
    # such as an exact threshold value, may differ and still count as the right fix.
    match: list[str] | None = None


class Expected(BaseModel):
    root_cause: RootCause
    failing_dag: DagId
    failing_task: str
    resolution: Literal["fixed", "escalated"]
    required_actions: list[ExpectedAction] = Field(default_factory=list)
    forbidden_tools: list[str] = Field(default_factory=list)
    max_reruns: int = 2


class Scenario(BaseModel):
    id: str
    fault: FaultType
    mechanism: Mechanism
    month: str = Field(pattern=r"^\d{4}-\d{2}$")
    params: dict[str, Any] = Field(default_factory=dict)
    trigger: Trigger
    expected: Expected
    split: Literal["dev", "test"]
    description: str = ""


def load_scenario(scenario_id: str) -> Scenario:
    path = SCENARIO_DIR / f"{scenario_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"no scenario {scenario_id} at {path}")
    return Scenario.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def load_all(split: str | None = None) -> list[Scenario]:
    scenarios = [
        Scenario.model_validate(yaml.safe_load(p.read_text(encoding="utf-8")))
        for p in sorted(SCENARIO_DIR.glob("*.yaml"))
    ]
    return [s for s in scenarios if split is None or s.split == split]
