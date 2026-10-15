# Pipeline Incident Agent

A small local data platform that breaks on purpose, and a LangGraph multi-agent system that diagnoses each failure, proposes a fix, asks a human before any risky action, and records the outcome.

> Work in progress. Done: environment, data, pipelines, fault injection, tools and risk tiers, the agent graph, and human-in-the-loop approvals. Next: retries and chaos, metrics, evaluation.

## How an incident flows

1. An Airflow task fails for good (after its retries); `on_failure_callback` posts it to `POST /incidents`.
2. The API stores it (deduped on dag, run and task) and queues `run_incident` on Celery.
3. The worker runs the LangGraph graph (Postgres checkpoints, `thread_id` = incident id):
   the **Supervisor** routes by fixed rules, **Diagnostics** investigates with read-only tools,
   **Remediation** proposes one action at a time.
4. Data-changing (tier-2) actions pause the graph. The request appears in the Streamlit
   **Approval inbox** with the rationale, evidence, a dry-run preview and the rollback path.
   No worker is blocked while a human decides; a worker restart changes nothing.
5. Approve, edit or reject: the API validates the decision and queues `resume_incident`.
   Reruns are verified across the whole DAG chain; the **Reporter** writes a postmortem
   and saves the case to incident memory. Approvals left 24 h escalate automatically.

## Quickstart

```bash
cp .env.example .env      # then fill in passwords and OPENROUTER_API_KEY
make up                   # or: docker compose up -d --build
make check-llm            # one plain call + one tool call per model
make seed                 # download, 20% sample, manifest, seed landing (no-op when done)
make bootstrap            # Airflow warehouse connection + unpause DAGs (after every make reset)
make backfill             # first clean runs for month 1
make test                 # unit + integration (no API keys needed)
make e2e                  # against the running stack, real models
```

Run one scenario end to end from the laptop, approvals answered by the simulated approver:

```bash
uv run python -m scripts.run_incident F01-v1
```

## Breaking things on purpose

```bash
uv run python -m faults.injector apply F01-v1   # see faults/catalog.md for the 10 fault types
uv run python -m faults.injector revert F01-v1
make reset-data                                 # revert everything
make verify-faults                              # prove one variant per fault fails as expected
```

69 scenarios in `faults/scenarios/` (55 dev, 14 held-out test), each with its ground truth:
root cause, failing task, correct fix or escalation, and forbidden actions.

| Service | URL |
| --- | --- |
| Airflow UI / REST API | http://localhost:8080 (user `admin`, password in `airflow/config/simple_auth_manager_passwords.json`) |
| API | http://localhost:8000/docs |
| Weather proxy | http://localhost:8091/health |
| Streamlit | http://localhost:8501 |
| Postgres | `localhost:5433` |

## Low-memory mode

Built for an 8 GB laptop: Airflow runs in **standalone mode** (development only, one container), every service has a memory limit, and Docker is capped at 5 GB through `%UserProfile%\.wslconfig`. `make up-pipeline` starts only what pipeline work needs.

## LLMs

All models are called through [OpenRouter](https://openrouter.ai) with `langchain-openai`. Primary `anthropic/claude-haiku-5.5`, fallback `deepseek/deepseek-v4-flash` (a different vendor, so one provider outage does not stop the run). Prices live in `config/pricing.yaml`.

## Data credits

Weather data by [Open-Meteo](https://open-meteo.com/), CC BY 4.0. Trip data from the NYC Taxi & Limousine Commission.
