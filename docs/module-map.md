# Module map

What each file and directory owns. Read this before adding new code so you reuse what's already there.

## Top level

- `pyproject.toml` — dependencies (managed with `uv`), `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`. Python ≥ 3.11.
- `Makefile` — `install`, `dev`, `test`, `lint`, `typecheck`, `format`, `db-start`, `db-stop`, `db-reset`, `db-diff`, `db-push`, `db-types`.
- `Dockerfile` — multi-stage build (uv-based builder, slim runtime, non-root user). Runs `gunicorn` with `uvicorn.workers.UvicornWorker`.
- `.env.example` — every `UPSTREAM_*` env var the service needs. `.env` is gitignored.
- `.github/workflows/ci.yml` — ruff + ruff format check + mypy strict + pytest on push/PR.

## `supabase/`

Supabase CLI layout. `supabase start` boots the local stack; the Supabase GitHub integration auto-applies anything new in `migrations/` to the hosted DB on push to `main`.

- `config.toml` — Supabase CLI config (project id, ports, schemas).
- `migrations/<timestamp>_<name>.sql` — schema migrations, applied in lexical order.
- `seed.sql` — local-dev seed (Local Dev account + permissive limits). **Only runs on `supabase db reset`** — never applied to the hosted project.

### Migration files (current)

- `20260512130000_init_extensions.sql` — `pgcrypto`, `citext`, `set_updated_at()` trigger fn.
- `20260512130100_accounts_and_auth.sql` — `accounts`, `account_members`, `account_limits`, `api_tokens` + RLS.
- `20260512130200_agents_and_cameras.sql` — `agents`, `cameras` + RLS.
- `20260512130300_stream_sessions.sql` — `stream_session_state` enum, `stream_sessions`, `stream_validations` + RLS.

## `src/upstream_api/`

### `app.py` — FastAPI app factory

`create_app(settings)` is the only thing the rest of the system needs. It:
- Configures structlog (JSON in prod, console in dev).
- Builds the FastAPI app with the right lifespan.
- Adds middleware: `CORSMiddleware`, `AccessLogMiddleware`, `RequestIdMiddleware`.
- Registers exception handlers from `errors.py`.
- Mounts every router.

`lifespan()` opens the asyncpg pool on startup and closes it on shutdown.

### `main.py` — uvicorn entrypoint

A single `app = create_app()`. Imported by `uvicorn upstream_api.main:app …`.

### `config.py` — `Settings(BaseSettings)`

Reads `UPSTREAM_*` env vars. Uses pydantic-settings v2. `get_settings()` is `lru_cache`d.

Notable design choices:
- All secrets are `SecretStr` to avoid accidental logs.
- `allowed_origins` is a `list[str]` exposed as a `@property` over a CSV string field (`UPSTREAM_ALLOWED_ORIGINS=a,b,c`), because pydantic-settings tries to JSON-decode `list[str]` env values by default.
- `session_token_ttl_seconds` defaults to 14400 (4h).

### `errors.py` — error envelope and exception handlers

- `ApiError` (base) with `status_code`, `code`, `message`, `details` — these are the stable error codes in the contract.
- Concrete: `NotFoundError`, `UnauthorizedError`, `ForbiddenError`, `ConflictError`, `LimitExceeded`.
- `api_error_handler`, `http_exception_handler`, `validation_exception_handler`, `unhandled_exception_handler` — all emit the same envelope shape `{ "error": { "code", "message", "details", "request_id" } }`.

### `db/pool.py` — asyncpg pool

- `create_pool(settings)` — opens the pool with `application_name="upstream-api"`, lifespan-managed.
- `get_pool(request)` — FastAPI dep; reads from `request.app.state.db_pool`.

### `security/` — auth and token machinery

- `api_tokens.py` — `issue_token()`, `split_token()`, `verify_secret()`, `ALLOWED_SCOPES`. Plaintext shape is `ups_<env>_<8 base32 id>_<22 base32 secret>`; the `prefix` (everything except the last segment) is the unique DB lookup key and the last segment is argon2id-hashed. `ALLOWED_SCOPES = frozenset({"agent"})` — used by the api-tokens router to reject scope escalation.
- `session_jwt.py` — `mint_session_token()`, `verify_session_token()`, `SessionLimits`, `SessionTokenClaims`. Issues HS256 JWTs with `iss=upstream-api`, `aud=srt-ingest`, plus `acc`/`sub`/`cam`/`sid`/`jti`/`exp`/`iat`/`lim`. The `lim` claim mirrors the session row's `lim_*` columns and is the contract the streaming server enforces.
- `supabase_jwt.py` — `verify_supabase_token()`, returns `SupabaseUserClaims(user_id, email, role)`. Validates with `aud="authenticated"`.
- `shared_secret.py` — `verify_shared_secret(presented, expected)` using `secrets.compare_digest`.
- `streamid.py` — `build_streamid(jwt)` returns `"publish:<jwt>"`. `extract_session_token(streamid)` reverses it. Used by `start_session` and (later) `/v1/ingest/validate-stream`.
- `deps.py` — the four FastAPI deps + their `Annotated` aliases:
  - `current_agent` → `AgentAuth(account_id, token_id, scopes)` + binds `account_id` into the logging contextvar.
  - `current_user` → `UserAuth(user_id, memberships: dict[UUID, role])`.
  - `current_ingest` → `IngestAuth` placeholder.
  - `current_processor` → `ProcessorAuth` placeholder.

### `services/` — business logic, no HTTP

- `limits.py` — `load_account_limits()`, `check_session_start()`, `check_camera_defaults()`, `check_camera_slot_available()`. Raises `LimitExceeded(code="limit_exceeded.*")` with stable codes the agent SDK can switch on.
- `session_lifecycle.py` — `start_session()`, `stop_session()`. Owns the state machine. `start_session` fetches the camera once, derives `agent_id`, checks busy + limits, mints the JWT, inserts the row, writes `usage_events`.
- `usage.py` — `record_event()`. Wraps the insert in a `conn.transaction()` savepoint so a missing-`usage_events`-table error (until Phase 5) doesn't abort the parent transaction.

### `routers/`

Each router owns its own path prefix. They consume `services.*` and never write SQL directly.

- `admin.py` — `GET /healthz`, `GET /readyz`.
- `agents.py` — `POST /v1/agents/register`. Upserts on `(account_id, name)`.
- `cameras.py` — `POST /v1/cameras/register`. Upserts on `(agent_id, external_id)` if provided.
- `stream_sessions.py` — `POST /v1/stream-sessions/start`, `POST /v1/stream-sessions/{id}/stop`, `GET /v1/stream-sessions/{id}`.
- `api_tokens.py` — `POST /v1/api-tokens` (issue with one-time plaintext), `DELETE /v1/api-tokens/{id}` (soft-revoke).

### `schemas/`

Pydantic v2 request/response models, one file per resource: `agents.py`, `cameras.py`, `stream_sessions.py`, `api_tokens.py`.

### `observability/`

- `context.py` — `request_id_var`, `account_id_var`, `agent_id_var`, `session_id_var`. Bound by middleware and the auth deps; auto-included in every log line.
- `logging.py` — `configure_logging(level, json_logs)`, `get_logger(name)`. Secret-key denylist scrubs `authorization`, `*token*`, `*secret*`, `*_key`, `password`.
- `middleware.py` — `RequestIdMiddleware` (reads/sets `X-Request-ID`), `AccessLogMiddleware` (one log line per request with duration).

## `tests/`

- `conftest.py` — populates dummy `UPSTREAM_*` env vars before settings load; clears the `get_settings` cache between tests.
- `test_health.py` — TestClient hits `/healthz` and `/readyz` without entering the lifespan (avoids needing a real DB).
- `test_app_routes.py` — enumerates the mounted route table and asserts the Phase 1 endpoints are present.
- `test_api_tokens.py` — token format, uniqueness of prefixes, argon2 round-trip.
- `test_session_jwt.py` — JWT mint/verify, expiry, tampering, audience/issuer enforcement.
- `test_streamid.py` — `publish:` round-trip + rejection of unknown prefixes.
- `test_shared_secret.py` — constant-time compare semantics.

## `scripts/`

- `seed_dev_token.py` — mints a real API token using `issue_token()` and prints SQL to insert it for the seeded local-dev account (`00000000-0000-0000-0000-000000000001`). The plaintext is echoed to stderr only.
- `smoke_local.py` — end-to-end smoke test: register agent → register camera → start session → decode JWT claims → stop session. Reads `UPSTREAM_API_URL` and `UPSTREAM_DEV_TOKEN` from env.
