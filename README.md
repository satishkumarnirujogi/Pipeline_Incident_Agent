# Pipeline Incident Agent

A small local data platform that breaks on purpose, and a LangGraph multi-agent system that diagnoses each failure, proposes a fix, asks a human before any risky action, and records the outcome.

> Work in progress. Phase 0 (environment) is done.

## Quickstart

```bash
cp .env.example .env      # then fill in passwords and OPENROUTER_API_KEY
make up                   # or: docker compose up -d --build
make check-llm            # one plain call + one tool call per model
```

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
