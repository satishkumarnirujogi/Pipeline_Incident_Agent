"""Prove the fault catalog: apply + trigger fails exactly the expected task,
revert + trigger turns every DAG green again.

    python -m faults.verify F01-v1 F05-v1 ...
    python -m faults.verify --one-per-fault        # first dev variant of each fault type
    python -m faults.verify --all

Per scenario: make sure the month is loaded clean, apply, trigger the
scenario's DAG (following ingest -> transform), check the failed task and that
exactly one incident was posted for it, revert, and run the clean chain again.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass

from faults import injector
from faults.scenario import Scenario, load_all, load_scenario
from scripts.airflow_client import AirflowClient


@dataclass
class Outcome:
    scenario: str
    ok: bool
    detail: str


def _wait_triggered_transform(af: AirflowClient, after: str | None) -> tuple[str, str]:
    """The transform run that ingest_taxi_trips triggered (newer than `after`)."""
    for _ in range(60):
        run = af.latest_run("transform_dbt")
        if run and run["dag_run_id"] != after:
            return run["dag_run_id"], af.wait_for_run("transform_dbt", run["dag_run_id"])
        time.sleep(3)
    raise TimeoutError("transform_dbt was not triggered")


def run_dag(af: AirflowClient, dag_id: str, month: str) -> tuple[str, str, str]:
    """Trigger dag_id; if it is a successful trips ingest, follow the triggered transform.
    Returns (dag_id, run_id, state) of the last run in the chain."""
    before = (af.latest_run("transform_dbt") or {}).get("dag_run_id")
    run_id = af.trigger(dag_id, {"month": month})
    state = af.wait_for_run(dag_id, run_id)
    if dag_id == "ingest_taxi_trips" and state == "success":
        t_run, t_state = _wait_triggered_transform(af, before)
        return "transform_dbt", t_run, t_state
    return dag_id, run_id, state


def clean_chain(af: AirflowClient, month: str) -> bool:
    for dag in ("ingest_weather", "ingest_taxi_trips"):
        dag_id, run_id, state = run_dag(af, dag, month)
        if state != "success":
            print(f"    clean chain failed at {dag_id} {run_id}: {af.task_states(dag_id, run_id)}")
            return False
    return True


def incidents_posted(run_id: str, task_id: str) -> int:
    logs = subprocess.run(
        ["docker", "compose", "logs", "api", "--no-log-prefix"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    ).stdout
    needle = f"run_id={run_id} task_id={task_id} "
    return sum(1 for line in logs.splitlines() if "incident received" in line and needle in line)


def verify(af: AirflowClient, sc: Scenario, clean_months: set[str]) -> Outcome:
    exp = sc.expected
    t0 = time.time()
    if sc.month not in clean_months:
        injector.revert(sc)
        if not clean_chain(af, sc.month):
            return Outcome(sc.id, False, "baseline not green before apply")
        clean_months.add(sc.month)

    injector.apply(sc)
    clean_months.discard(sc.month)
    dag_id, run_id, state = run_dag(af, sc.trigger.dag_id, sc.month)
    tasks = af.task_states(dag_id, run_id)
    failed = sorted(t for t, s in tasks.items() if s == "failed")
    problems = []
    if state != "failed":
        problems.append(f"{dag_id} ended {state}, expected failure")
    if dag_id != exp.failing_dag or failed != [exp.failing_task]:
        problems.append(
            f"failed {dag_id}.{failed}, expected {exp.failing_dag}.[{exp.failing_task}]"
        )
    time.sleep(2)
    n_incidents = incidents_posted(run_id, exp.failing_task)
    if n_incidents != 1:
        problems.append(f"{n_incidents} incidents posted, expected 1")

    injector.revert(sc)
    if clean_chain(af, sc.month):
        clean_months.add(sc.month)
    else:
        problems.append("not green after revert")

    took = f"{time.time() - t0:.0f}s"
    if problems:
        return Outcome(sc.id, False, "; ".join(problems) + f" ({took})")
    return Outcome(
        sc.id, True, f"{dag_id}.{failed[0]} failed, 1 incident, green after revert ({took})"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario_ids", nargs="*")
    parser.add_argument("--one-per-fault", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        selected = load_all()
    elif args.one_per_fault:
        seen: dict[str, Scenario] = {}
        for sc in load_all("dev"):
            seen.setdefault(sc.fault, sc)
        selected = list(seen.values())
    else:
        selected = [load_scenario(i) for i in args.scenario_ids]
    # Group by month so the clean baseline is reused between scenarios.
    selected.sort(key=lambda s: (s.month, s.id))

    af = AirflowClient()
    clean: set[str] = set()
    results = []
    for sc in selected:
        print(f"{sc.id} [{sc.month}] {sc.description}", flush=True)
        try:
            outcome = verify(af, sc, clean)
        except Exception as exc:  # noqa: BLE001 - report and keep going
            injector.revert(sc)
            clean.discard(sc.month)
            outcome = Outcome(sc.id, False, f"{type(exc).__name__}: {exc}")
        print(f"  {'PASS' if outcome.ok else 'FAIL'}  {outcome.detail}", flush=True)
        results.append(outcome)

    passed = sum(r.ok for r in results)
    print(f"\n{passed}/{len(results)} scenarios verified")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
