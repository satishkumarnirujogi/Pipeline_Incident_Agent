.PHONY: up up-pipeline down reset ps logs stats seed profile backfill eval test lint fmt check-llm

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

backfill:      ## Phase 2
	uv run python -m scripts.backfill

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
