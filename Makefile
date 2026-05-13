.PHONY: install dev test lint typecheck format db-start db-stop db-reset db-diff db-push dev-account dev-account-reset

install:
	uv sync

dev:
	uv run uvicorn upstream_api.main:app --host 0.0.0.0 --port $${UPSTREAM_PORT:-4000} --reload

test:
	uv run pytest -q

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

typecheck:
	uv run mypy src

db-start:
	supabase start

db-stop:
	supabase stop

db-reset:
	supabase db reset

db-diff:
	supabase db diff

db-push:
	supabase db push

db-types:
	supabase gen types typescript --local > /dev/null

dev-account:
	uv run python scripts/seed_dev_account.py

dev-account-reset:
	uv run python scripts/seed_dev_account.py --reset
