"""Ablation table (build guide 9.4): compare eval runs by configuration.

    uv run python -m eval.ablations <eval_run_id> [<eval_run_id> ...] [--out PATH]

Runs are grouped by configuration (eval.runs.config: ablation + chaos). A group with
several runs (the test split is run 3 times, guide 9.3) shows the mean and the worst run.
The recovery rate of a chaos run is measured against the full system's run without chaos
(the first one listed), over the same scenarios. Every number is a named query in
eval/queries.sql, as in the report.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agent.settings import get_settings
from eval.report import REPORTS, _table, run

LABELS = {
    ("single_agent", False): "A. Single agent",
    ("no_recovery", True): "B. No recovery, chaos on",
    ("no_memory", False): "C. No memory",
    ("full", False): "D. Full system",
    ("full", True): "D. Full system, chaos on",
}
ORDER = list(LABELS.values())


def _config(eval_run_id: str) -> tuple[str, bool]:
    with psycopg.connect(get_settings().dsn("agent_app", "agent"), row_factory=dict_row) as conn:
        row = conn.execute("SELECT config FROM eval.runs WHERE id=%s", (eval_run_id,)).fetchone()
    if row is None:
        raise SystemExit(f"unknown eval run {eval_run_id}")
    return row["config"].get("ablation", "full"), bool(row["config"].get("chaos"))


def metrics(eval_run_id: str, baseline: str | None) -> dict[str, Any]:
    p = {"eval_run_id": eval_run_id, "baseline_run_id": baseline}
    head = run("headline", p)[0]
    tools = run("tool_call_success", p)[0]
    esc = run("escalation", p)[0]
    out = {
        "scenarios": head["scenarios"],
        "task_completion": head["task_completion_rate"],
        "root_cause_accuracy": head["root_cause_accuracy"],
        "tool_call_success": tools["tool_call_success_rate"],
        "safety_violations": head["safety_violations"],
        "cost_per_completed_eur": head["cost_per_completed_eur"],
        "escalation_precision": esc["escalation_precision"],
        "mttr_median_min": head["mttr_median_min"],
        "recovery_rate": None,
    }
    if baseline and baseline != eval_run_id:
        out["recovery_rate"] = run("recovery_rate", p)[0]["recovery_rate"]
    return out


def _mean_and_worst(rows: list[dict]) -> dict[str, Any]:
    """'mean (worst)' per metric; worst = the run with the lowest task completion."""
    worst = min(rows, key=lambda r: float(r["task_completion"] or 0))
    out: dict[str, Any] = {}
    for key in rows[0]:
        values = [float(r[key]) for r in rows if r[key] is not None]
        if not values:
            out[key] = None
        elif len(rows) == 1:
            out[key] = rows[0][key]
        else:
            out[key] = f"{statistics.mean(values):.3f} ({worst[key]})"
    return out


def build(run_ids: list[str]) -> str:
    configs = {r: _config(r) for r in run_ids}
    baseline = next((r for r in run_ids if configs[r] == ("full", False)), None)
    groups: dict[str, list[dict]] = {}
    for r in run_ids:
        label = LABELS.get(configs[r], f"{configs[r][0]}{', chaos on' if configs[r][1] else ''}")
        chaos = configs[r][1]
        groups.setdefault(label, []).append(metrics(r, baseline if chaos else None))
    rows = [
        {"config": label, "runs": len(groups[label]), **_mean_and_worst(groups[label])}
        for label in sorted(groups, key=lambda x: ORDER.index(x) if x in ORDER else 99)
    ]
    lines = [
        "# Ablations",
        "",
        "Runs: " + ", ".join(f"`{r}`" for r in run_ids),
        "",
        "Several runs per configuration show `mean (worst run)`. Recovery rate: scenarios "
        "still solved under chaos / scenarios the full system solves without chaos"
        + (f" (`{baseline}`)." if baseline else "; needs a full-system run without chaos."),
        "",
        _table(rows),
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.ablations")
    parser.add_argument("runs", nargs="+")
    parser.add_argument("--out", type=Path, default=REPORTS / "ablations.md")
    args = parser.parse_args()
    md = build(args.runs)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(md)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
