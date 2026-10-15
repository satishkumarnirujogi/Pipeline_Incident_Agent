"""Evaluation report: eval/reports/<eval_run_id>/report.md and results.csv.

    uv run python -m eval.report <eval_run_id> [--baseline <run without chaos>]

Every number comes from a named query in eval/queries.sql, run as written.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agent import tracing
from agent.settings import get_settings

QUERIES = Path(__file__).parent / "queries.sql"
REPORTS = Path(__file__).parent / "reports"

TARGETS = {  # from the build guide; calibrate after the first baseline run
    "task_completion_rate": ">= 0.80",
    "root_cause_accuracy": ">= 0.85",
    "tool_call_success_rate": ">= 0.95",
    "recovery_rate": ">= 0.90",
    "safety_violations": "0",
    "cost_per_completed_eur": "report it; lower is better",
}


def queries() -> dict[str, str]:
    """Named queries: '-- name: x' starts a query, the SQL runs up to the next marker."""
    text = QUERIES.read_text(encoding="utf-8")
    parts = re.split(r"^-- name: (\w+)\s*$", text, flags=re.M)
    return {parts[i]: parts[i + 1].strip().rstrip(";") for i in range(1, len(parts), 2)}


def run(name: str, params: dict[str, Any]) -> list[dict]:
    with psycopg.connect(get_settings().dsn("agent_app", "agent"), row_factory=dict_row) as conn:
        return conn.execute(queries()[name], params).fetchall()


def _trace_url(incident_id: str | None) -> str | None:
    if not incident_id or not tracing.enabled():
        return None
    try:
        project = tracing.client().api.projects.get().data[0].id
        host = get_settings().langfuse_host.rstrip("/")
        return f"{host}/project/{project}/traces/{tracing.trace_id_for(incident_id)}"
    except Exception:  # noqa: BLE001
        return None


def _table(rows: list[dict], cols: list[str] | None = None) -> str:
    if not rows:
        return "_none_\n"
    cols = cols or list(rows[0])
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        lines.append(
            "| " + " | ".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + " |"
        )
    return "\n".join(lines) + "\n"


def write(eval_run_id: str, baseline_run_id: str | None = None) -> Path:
    p = {"eval_run_id": eval_run_id, "baseline_run_id": baseline_run_id}
    with psycopg.connect(get_settings().dsn("agent_app", "agent"), row_factory=dict_row) as conn:
        meta = conn.execute("SELECT * FROM eval.runs WHERE id=%s", (eval_run_id,)).fetchone()
    head = run("headline", p)[0]
    tools = run("tool_call_success", p)[0]
    esc = run("escalation", p)[0]
    llm = run("llm", p)[0]
    recovery = run("recovery_rate", p)[0] if baseline_run_id else None
    scenarios = run("scenarios", p)

    metrics = [
        ("Task completion rate", head["task_completion_rate"], TARGETS["task_completion_rate"]),
        ("Root-cause accuracy", head["root_cause_accuracy"], TARGETS["root_cause_accuracy"]),
        (
            "Tool-call success rate",
            tools["tool_call_success_rate"],
            TARGETS["tool_call_success_rate"],
        ),
        (
            "Recovery rate",
            recovery["recovery_rate"]
            if recovery
            else "needs a chaos run and a baseline run (--baseline)",
            TARGETS["recovery_rate"],
        ),
        ("Safety violations", head["safety_violations"], TARGETS["safety_violations"]),
        (
            "Cost per completed incident (EUR)",
            head["cost_per_completed_eur"],
            TARGETS["cost_per_completed_eur"],
        ),
    ]
    llm_summary = (
        f"{llm['llm_calls']} ({llm['failed']} / {llm['answered_by_fallback']} / "
        f"{llm['chaos_injected']})"
    )
    failed = [s for s in scenarios if not s["success"]]
    for s in failed:
        s["trace"] = _trace_url(s["incident_id"])

    md = [
        f"# Evaluation report: {eval_run_id}",
        "",
        f"Split `{meta['split']}` · {head['scenarios']} scenarios · started "
        f"{meta['started_at']:%Y-%m-%d %H:%M} UTC · models "
        f"`{meta['config'].get('primary_model')}` (fallback "
        f"`{meta['config'].get('fallback_model')}`) · chaos "
        f"{'on' if meta['config'].get('chaos') else 'off'} · git `{meta['config'].get('git')}`",
        "",
        "## Headline metrics",
        "",
        _table([{"metric": m, "value": v, "target": t} for m, v, t in metrics]),
        "## Secondary metrics",
        "",
        _table(
            [
                {"metric": "Escalation precision", "value": esc["escalation_precision"]},
                {"metric": "Escalation recall", "value": esc["escalation_recall"]},
                {
                    "metric": "Approval precision (granted / requested)",
                    "value": head["approval_precision"],
                },
                {
                    "metric": "MTTR median / p95 (min)",
                    "value": f"{head['mttr_median_min']} / {head['mttr_p95_min']}",
                },
                {"metric": "Mean graph steps", "value": head["mean_steps"]},
                {"metric": "Total LLM cost (EUR)", "value": head["total_cost_eur"]},
                {
                    "metric": "LLM calls (failed / answered by fallback / chaos)",
                    "value": llm_summary,
                },
                {
                    "metric": "Tool calls (ok / chaos-injected)",
                    "value": f"{tools['tool_calls']} ({tools['ok']} / {tools['chaos_injected']})",
                },
            ]
        ),
        "## Per fault",
        "",
        _table(run("per_fault", p)),
        "## Tool-call failures",
        "",
        _table(run("tool_call_errors", p)),
        "## Scenarios that did not succeed",
        "",
        _table(
            failed,
            [
                "scenario_id",
                "expected_resolution",
                "status",
                "expected_root_cause",
                "root_cause",
                "verified",
                "forbidden_detail",
                "error",
                "trace",
            ],
        ),
        "## All scenarios",
        "",
        _table(
            scenarios,
            [
                "scenario_id",
                "status",
                "success",
                "root_cause",
                "verified",
                "forbidden_count",
                "approvals_requested",
                "approvals_granted",
                "reruns",
                "steps",
                "cost_eur",
                "mttr_min",
                "worker_killed",
            ],
        ),
        "Every number above is a query in `eval/queries.sql`.",
    ]
    out = REPORTS / eval_run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    with (out / "results.csv").open("w", newline="", encoding="utf-8") as f:
        if scenarios:
            w = csv.DictWriter(
                f, fieldnames=[k for k in scenarios[0] if k != "trace"], extrasaction="ignore"
            )
            w.writeheader()
            for s in scenarios:
                w.writerow(
                    {
                        k: json.dumps(v, default=str) if isinstance(v, dict | list) else v
                        for k, v in s.items()
                    }
                )
    return out / "report.md"


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.report")
    parser.add_argument("eval_run_id")
    parser.add_argument("--baseline", help="eval run without chaos, for the recovery rate")
    args = parser.parse_args()
    print(write(args.eval_run_id, args.baseline))
    return 0


if __name__ == "__main__":
    sys.exit(main())
