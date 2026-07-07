.PHONY: install dev test ci lint typecheck format db-start db-stop db-reset db-diff db-push dev-account dev-account-reset admin-seed admin-list

install:
	uv sync

dev:
	uv run uvicorn upstream_api.main:app --host 0.0.0.0 --port $${UPSTREAM_PORT:-4000} --reload

test:
	uv run pytest -q

# Throwaway secrets GitHub Actions injects for the test job (see
# .github/workflows/ci.yml). `make ci` runs pytest under THESE instead of your
# local .env, so tests that only pass because of a value in your .env (a
# configured fanout secret, a matching webhook secret, ...) fail here the same
# way they'd fail in CI — before you push, not after. Keep in sync with the
# workflow's env block.
CI_ENV = \
	UPSTREAM_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:54322/postgres' \
	UPSTREAM_SUPABASE_URL='http://127.0.0.1:54321' \
	UPSTREAM_SUPABASE_ANON_KEY=ci-anon \
	UPSTREAM_SUPABASE_SERVICE_ROLE_KEY=ci-service \
	UPSTREAM_SUPABASE_JWT_SECRET='ci-supabase-jwt-secret-min-32-bytes!' \
	UPSTREAM_SESSION_JWT_SECRET='ci-session-jwt-secret-min-32-bytes!!' \
	UPSTREAM_STREAMID_HMAC_KEY='ci-hmac-key-min-32-bytes-yes-please!' \
	UPSTREAM_MEDIAMTX_WEBHOOK_SECRET=ci-mediamtx \
	UPSTREAM_PROCESSOR_WEBHOOK_SECRET=ci-processor \
	UPSTREAM_PROMETHEUS_AUTH_TOKEN=ci-prom \
	UPSTREAM_FANOUT_TOKEN_SECRET=

# Reproduce the full GitHub Actions gate locally. Run before every push.
ci:
	uv run ruff check src tests
	uv run ruff format --check src tests
	uv run mypy src tests
	env $(CI_ENV) uv run pytest -q

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

typecheck:
	uv run mypy src tests

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

# Grant /v1/admin/* access to an auth user: make admin-seed EMAIL=you@example.com
admin-seed:
	uv run python scripts/seed_platform_admin.py --email $(EMAIL)

admin-list:
	uv run python scripts/seed_platform_admin.py --list
