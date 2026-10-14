.PHONY: up up-pipeline down reset reset-data ps logs stats seed profile bootstrap backfill scenarios verify-faults eval test lint fmt check-llm

COMPOSE := docker compose

up:            ## start every service
	$(COMPOSE) up -d --build

up-pipeline:   ## only what pipeline work needs (low-memory mode)
	$(COMPOSE) up -d --build postgres airflow weather-proxy api redis

down:
	$(COMPOSE) down

reset:         ## drop volumes: Postgres starts empty and init.sql runs again
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

stats:
	docker stats --no-stream

seed:          ## Phase 1
	uv run python -m scripts.seed

profile:       ## baselines -> docs/data_profile.md
	uv run python -m scripts.profile_data

bootstrap:     ## Airflow warehouse connection + unpause DAGs
	uv run python -m scripts.bootstrap_airflow

backfill:      ## first clean runs for month 1
	uv run python -m scripts.backfill

scenarios:     ## regenerate faults/scenarios/*.yaml
	uv run python -m faults.generate_scenarios

reset-data:    ## revert every fault: clean landing, proxy normal, connection restored
	uv run python -m faults.injector reset

verify-faults: ## apply + revert one dev variant per fault type
	uv run python -m faults.verify --one-per-fault

eval:          ## Phase 9
	uv run python -m eval.run_eval --split $(or $(SPLIT),dev)

test:
	uv run pytest tests/unit tests/integration

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy agent api worker

fmt:
	uv run ruff format .
	uv run ruff check --fix .

check-llm:
	uv run python -m scripts.check_llm
