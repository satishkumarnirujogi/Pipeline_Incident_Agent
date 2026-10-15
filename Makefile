.PHONY: chaos-on chaos-off e2e up up-pipeline down reset reset-data migrate ps logs stats seed profile bootstrap backfill scenarios verify-faults eval test lint fmt check-llm

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

migrate:       ## warehouse SQL migrations + Alembic for the agent DB
	uv run python -m scripts.migrate

bootstrap: migrate  ## + Airflow warehouse connection, unpause DAGs
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
	uv run python -m eval.run_eval --split $(or $(SPLIT),dev) $(if $(CHAOS),--chaos,) $(if $(RESUME),--resume $(RESUME),)

test:
	uv run pytest tests/unit tests/integration

chaos-on:      ## worker with chaos injection (eval only); CHAOS_CONFIG=eval/chaos_demo.yaml for a demo
	CHAOS_MODE=on CHAOS_CONFIG=$(CHAOS_CONFIG) $(COMPOSE) up -d worker

chaos-off:
	$(COMPOSE) up -d --force-recreate worker

e2e:           ## against the running stack (make up-pipeline + make bootstrap first)
	uv run pytest tests/e2e -m e2e -v

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy agent api worker

fmt:
	uv run ruff format .
	uv run ruff check --fix .

check-llm:
	uv run python -m scripts.check_llm
