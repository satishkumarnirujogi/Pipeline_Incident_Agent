"""Evaluation harness: run scenarios unattended and score them.

    make eval SPLIT=dev                                   # all 55 dev scenarios (~5 h)
    uv run python -m eval.run_eval --one-per-fault        # 10 scenarios, smoke test
    uv run python -m eval.run_eval --scenarios F01-v1 F05-v2
    uv run python -m eval.run_eval --resume dev-20261015T1200   # continue an interrupted run
    uv run python -m eval.run_eval --split dev --chaos    # worker must run with CHAOS_MODE=on
    uv run python -m eval.run_eval --split test --ablation no_memory   # worker ABLATION too

Per scenario (one at a time: parallel runs would share one warehouse):
 1. revert to a clean state and verify it (the month's DAG chain green)
 2. apply the fault, recorded in eval.injections
 3. trigger the scenario's DAG
 4. wait for the incident the failure callback creates; tag it with scenario and run ids
 5. let the system work; the simulated approver answers approvals; chaos may kill the worker
 6. wait for a terminal status, at most 10 minutes (timeout = failed)
 7. verify independently of the agent
 8. score and write one eval.results row
 9. revert
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import UTC, datetime

import httpx
import psycopg

from agent.ablation import ABLATIONS
from agent.chaos import scenario_flag
from agent.settings import get_settings
from eval import observe, report
from eval.scoring import TERMINAL, Facts, score
from eval.simulated_approver import decide
from faults import injector
from faults.scenario import Scenario, load_all, load_scenario
from faults.verify import clean_chain
from scripts.airflow_client import AirflowClient

log = logging.getLogger("eval")
API = "http://localhost:8000"
INCIDENT_WAIT_S = 300
RESOLVE_TIMEOUT_S = 1500  # an API outage may wait ~15 min (OUTAGE_CHECKS x OUTAGE_WAIT_S)
WORKER = "pipeline-incident-agent-worker-1"


def _agent() -> psycopg.Connection:
    return psycopg.connect(get_settings().dsn("agent_app", "agent"), autocommit=True)


def worker_env(name: str, default: str) -> str:
    return (
        subprocess.run(
            ["docker", "exec", WORKER, "printenv", name], capture_output=True, text=True
        ).stdout.strip()
        or default
    )


def worker_chaos() -> str:
    return worker_env("CHAOS_MODE", "off")


def git_sha() -> str:
    return subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True
    ).stdout.strip()


# ---------------------------------------------------------------- approvals


def answer_approvals(sc: Scenario, incident_id: str) -> int:
    """The simulated on-call human: approve the expected fix, reject anything else."""
    pending = [
        a
        for a in httpx.get(f"{API}/approvals", timeout=10).json()
        if a["incident_id"] == incident_id
    ]
    for a in pending:
        d = decide(sc, a["tool"], a["args"])
        httpx.post(
            f"{API}/approvals/{a['id']}/decision",
            timeout=10,
            json={
                "decision": d["decision"],
                "comment": d["comment"],
                "decided_by": "simulated_approver",
            },
        )
        log.info("approval %s %s -> %s", a["tool"], json.dumps(a["args"]), d["decision"])
    return len(pending)


def kill_worker() -> None:
    subprocess.run(["docker", "kill", WORKER], capture_output=True, check=False)
    time.sleep(3)
    subprocess.run(["docker", "start", WORKER], capture_output=True, check=False)


def wait_until_done(sc: Scenario, incident_id: str, kill: bool) -> tuple[str, bool]:
    """Poll until a terminal status (or timeout), answering approvals on the way."""
    deadline = time.time() + RESOLVE_TIMEOUT_S
    killed = False
    while time.time() < deadline:
        try:
            status = observe.incident_status(incident_id)
            if status in TERMINAL:
                return status, killed
            if status == "awaiting_approval":
                answer_approvals(sc, incident_id)
            if kill and not killed and status == "running":
                with _agent() as conn:
                    calls = conn.execute(
                        "SELECT count(*) FROM tool_calls WHERE incident_id=%s", (incident_id,)
                    ).fetchone()[0]
                if calls:
                    log.info("chaos: killing the worker mid-run")
                    kill_worker()
                    killed = True
        except (httpx.HTTPError, psycopg.OperationalError) as exc:
            log.warning("poll failed: %s", exc)
        time.sleep(3)
    return "timeout", killed


# ----------------------------------------------------------------- scenario


def run_scenario(sc: Scenario, eval_run_id: str, af: AirflowClient, chaos: bool) -> dict:
    started = datetime.now(UTC)
    row: dict = {
        "eval_run_id": eval_run_id,
        "scenario_id": sc.id,
        "fault": sc.fault,
        "split": sc.split,
        "month": sc.month,
        "chaos_mode": chaos,
        "expected_resolution": sc.expected.resolution,
        "expected_root_cause": sc.expected.root_cause.value,
        "started_at": started,
    }
    try:
        injector.revert(sc, eval_run_id)
        if not clean_chain(af, sc.month):
            raise RuntimeError("baseline not green before the fault")
        base = observe.baseline(sc.month)

        injector.apply(sc, eval_run_id)
        triggered_at = datetime.now(UTC)
        af.trigger(sc.trigger.dag_id, {"month": sc.month})
        deadline = time.time() + INCIDENT_WAIT_S
        incident = None
        while incident is None and time.time() < deadline:
            time.sleep(5)
            incident = observe.incident_since(triggered_at)
        if incident is None:
            row |= score(sc, Facts(status="no_incident"), verified=False)
            row["error"] = "the fault produced no incident"
            return row
        row["incident_id"] = incident["id"]
        observe.tag_incident(incident["id"], sc.id, eval_run_id)

        kill = chaos and scenario_flag(f"{eval_run_id}|{sc.id}", "worker_kill_scenarios")
        status, killed = wait_until_done(sc, incident["id"], kill)
        row["worker_killed"] = killed
        verified, detail = observe.verify(sc, af, base, incident["id"], status)
        row["verification"] = detail
        row |= score(sc, observe.facts(incident["id"], status), verified)
    except Exception as exc:  # noqa: BLE001 - one broken scenario must not stop the run
        log.exception("scenario %s failed", sc.id)
        row.setdefault("status", "error")
        row |= {
            k: v
            for k, v in score(sc, Facts(status="error"), verified=False).items()
            if k not in row
        }
        row["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        try:
            injector.revert(sc, eval_run_id)
        except Exception:  # noqa: BLE001
            log.exception("revert of %s failed", sc.id)
    return row


def save(row: dict) -> None:
    cols = list(row)
    values = [json.dumps(v, default=str) if isinstance(v, dict | list) else v for v in row.values()]
    with _agent() as conn:
        conn.execute(
            f"INSERT INTO eval.results ({', '.join(cols)}) VALUES "
            f"({', '.join(['%s'] * len(cols))})",
            values,
        )


def select(args: argparse.Namespace) -> list[Scenario]:
    if args.scenarios:
        return [load_scenario(s) for s in args.scenarios]
    scenarios = load_all(None if args.split == "all" else args.split)
    if args.one_per_fault:
        seen: dict[str, Scenario] = {}
        for sc in scenarios:
            seen.setdefault(sc.fault, sc)
        scenarios = list(seen.values())
    return scenarios[: args.limit] if args.limit else scenarios


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m eval.run_eval")
    parser.add_argument("--split", choices=["dev", "test", "all"], default="dev")
    parser.add_argument("--scenarios", nargs="*")
    parser.add_argument("--one-per-fault", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--chaos", action="store_true", help="expect the worker in chaos mode")
    parser.add_argument(
        "--ablation", choices=ABLATIONS, default="full", help="expect the worker in this config"
    )
    parser.add_argument(
        "--baseline", help="for a chaos run: the run without chaos, for the recovery rate"
    )
    parser.add_argument("--resume", help="continue an eval run, skipping finished scenarios")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    mode = worker_chaos()
    if args.chaos != (mode == "on"):
        print(
            f"worker CHAOS_MODE is {mode!r} but --chaos is {args.chaos}; start the worker "
            "accordingly (make chaos-on / make chaos-off)",
            file=sys.stderr,
        )
        return 2
    ablation = worker_env("ABLATION", "full")
    if args.ablation != ablation:
        print(
            f"worker ABLATION is {ablation!r} but --ablation is {args.ablation!r}; recreate it "
            f"with make worker ABLATION={args.ablation}",
            file=sys.stderr,
        )
        return 2

    scenarios = select(args)
    s = get_settings()
    eval_run_id = args.resume or (
        f"{args.split}{'' if args.ablation == 'full' else '-' + args.ablation}"
        f"{'-chaos' if args.chaos else ''}-{datetime.now(UTC):%Y%m%dT%H%M}"
    )
    with _agent() as conn:
        done = {
            r[0]
            for r in conn.execute(
                "SELECT scenario_id FROM eval.results WHERE eval_run_id=%s", (eval_run_id,)
            )
        }
        conn.execute(
            "INSERT INTO eval.runs (id, split, config) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING",
            (
                eval_run_id,
                args.split,
                json.dumps(
                    {
                        "primary_model": s.primary_model,
                        "fallback_model": s.fallback_model,
                        "chaos": args.chaos,
                        "ablation": args.ablation,
                        "git": git_sha(),
                        "scenarios": [sc.id for sc in scenarios],
                    }
                ),
            ),
        )

    af = AirflowClient()
    todo = [sc for sc in scenarios if sc.id not in done]
    print(f"eval run {eval_run_id}: {len(todo)} to run, {len(done)} already done", flush=True)
    for i, sc in enumerate(todo, 1):
        t0 = time.time()
        row = run_scenario(sc, eval_run_id, af, args.chaos)
        save(row)
        print(
            f"[{i}/{len(todo)}] {sc.id:8} {row.get('status'):18} "
            f"success={row.get('success')} rc={row.get('root_cause')} "
            f"verified={row.get('verified')} forbidden={row.get('forbidden_count')} "
            f"EUR {row.get('cost_eur', 0):.4f} {time.time() - t0:.0f}s"
            + (f" error={row['error']}" if row.get("error") else ""),
            flush=True,
        )

    with _agent() as conn:
        conn.execute("UPDATE eval.runs SET finished_at=now() WHERE id=%s", (eval_run_id,))
    path = report.write(eval_run_id, args.baseline)
    print(f"report: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
