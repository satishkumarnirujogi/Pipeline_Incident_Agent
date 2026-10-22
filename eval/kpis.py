"""KPI data for the API's /metrics endpoints (and so the Streamlit KPI page).

The numbers come from the same named queries as the markdown report (eval/queries.sql),
so the page and the report always agree.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from agent.settings import get_settings
from eval import ablations
from eval.report import TARGETS, run

MIN_SCENARIOS = 10  # smaller runs are re-checks of a few scenarios, not evaluations


def _runs() -> list[dict]:
    """Finished eval runs, oldest first, with their configuration and size."""
    with psycopg.connect(get_settings().dsn("agent_app", "agent"), row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT r.id, r.split, r.config, r.started_at, r.finished_at, count(e.id) AS size "
            "FROM eval.runs r JOIN eval.results e ON e.eval_run_id = r.id "
            "WHERE r.finished_at IS NOT NULL GROUP BY r.id ORDER BY r.started_at"
        ).fetchall()


def _is_headline(r: dict) -> bool:
    """The full system without chaos: the configuration the headline numbers describe."""
    cfg = r["config"] or {}
    return (
        cfg.get("ablation", "full") == "full"
        and not cfg.get("chaos")
        and r["size"] >= MIN_SCENARIOS
    )


def summary(eval_run_id: str) -> dict[str, Any]:
    meta = next((r for r in _runs() if r["id"] == eval_run_id), None)
    if meta is None:
        raise LookupError(eval_run_id)
    cfg = meta["config"] or {}
    p = {"eval_run_id": eval_run_id, "baseline_run_id": cfg.get("baseline")}
    head = run("headline", p)[0]
    tools = run("tool_call_success", p)[0]
    esc = run("escalation", p)[0]
    recovery = run("recovery_rate", p)[0] if cfg.get("baseline") else None
    return {
        "eval_run_id": eval_run_id,
        "split": meta["split"],
        "ablation": cfg.get("ablation", "full"),
        "chaos": bool(cfg.get("chaos")),
        "git": cfg.get("git"),
        "started_at": meta["started_at"],
        "finished_at": meta["finished_at"],
        "headline": {
            "task_completion_rate": head["task_completion_rate"],
            "root_cause_accuracy": head["root_cause_accuracy"],
            "tool_call_success_rate": tools["tool_call_success_rate"],
            "recovery_rate": recovery["recovery_rate"] if recovery else None,
            "safety_violations": head["safety_violations"],
            "cost_per_completed_eur": head["cost_per_completed_eur"],
        },
        "targets": TARGETS,
        "secondary": {
            "scenarios": head["scenarios"],
            "successes": head["successes"],
            "escalation_precision": esc["escalation_precision"],
            "escalation_recall": esc["escalation_recall"],
            "approval_precision": head["approval_precision"],
            "mttr_median_min": head["mttr_median_min"],
            "mttr_p95_min": head["mttr_p95_min"],
            "mean_steps": head["mean_steps"],
            "total_cost_eur": head["total_cost_eur"],
        },
        "per_fault": run("per_fault", p),
    }


def latest(eval_run_id: str | None = None) -> dict[str, Any] | None:
    """The given run, or the newest full-system run without chaos; None before any eval."""
    if eval_run_id is None:
        headline_runs = [r for r in _runs() if _is_headline(r)]
        if not headline_runs:
            return None
        eval_run_id = headline_runs[-1]["id"]
    return summary(eval_run_id)


def trend() -> list[dict[str, Any]]:
    """Headline numbers of every evaluation run (re-check runs left out), oldest first."""
    out = []
    for r in _runs():
        if r["size"] < MIN_SCENARIOS:
            continue
        head = run("headline", {"eval_run_id": r["id"], "baseline_run_id": None})[0]
        cfg = r["config"] or {}
        out.append(
            {
                "eval_run_id": r["id"],
                "started_at": r["started_at"],
                "split": r["split"],
                "ablation": cfg.get("ablation", "full"),
                "chaos": bool(cfg.get("chaos")),
                "scenarios": head["scenarios"],
                "task_completion_rate": head["task_completion_rate"],
                "root_cause_accuracy": head["root_cause_accuracy"],
                "cost_per_completed_eur": head["cost_per_completed_eur"],
            }
        )
    return out


def ablation_table(split: str = "test") -> dict[str, Any]:
    """The ablation table over every evaluation run on the split (guide 9.4)."""
    run_ids = [r["id"] for r in _runs() if r["split"] == split and r["size"] >= MIN_SCENARIOS]
    if not run_ids:
        return {"split": split, "runs": [], "baseline": None, "rows": []}
    table, baseline = ablations.rows(run_ids)
    return {"split": split, "runs": run_ids, "baseline": baseline, "rows": table}
