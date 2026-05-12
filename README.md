# upstream-api-server

The control plane for the Upstream multi-camera streaming platform. FastAPI + Supabase Postgres. Owns accounts, agents, cameras, stream sessions, and account limits. Does not carry video — that's `upstream-streaming-server`.

Production: `https://api.upstreamcv.com` (us-east-2). See `docs/deployment.md` for the EC2 inventory.

## Where this fits

```
upstream-dashboard (Next.js)             ◄── browser
        │ Supabase JWT
        ▼
┌─────────────────────┐
│  upstream-api       │  ◄── this repo
│  (FastAPI)          │      auth, limits, session minting
└──┬───────────────┬──┘
   │ asyncpg       │ webhook secret
   ▼               ▼
Supabase Postgres   upstream-streaming-server (MediaMTX)
                    │ SRT in, WebRTC out
                    ▼
                    upstream-processor (CV workers)

upstream-agent (public pip lib) ──API token──► upstream-api
                                ──SRT publish──► upstream-streaming-server
```

This service is the only one that talks to the database. Every other service either calls the API or is called back by it (signed webhooks).

## Stack

- Python 3.11+, FastAPI, pydantic v2
- asyncpg for the hot path; SQLAlchemy 2.0 async reserved for dashboard reads
- Supabase Postgres (managed), migrations in `supabase/migrations/`
- argon2id for API token hashing, HS256 JWTs for session tokens
- structlog (JSON in prod, console in dev)
- `uv` for dependency management

## Quickstart

Prerequisites: Python 3.11+, [uv](https://docs.astral.sh/uv/), Docker, the [Supabase CLI](https://supabase.com/docs/guides/cli) (`brew install supabase/tap/supabase`).

```sh
# 1. Install deps
uv sync

# 2. Boot the local Supabase stack (Postgres + Auth + Studio + …)
supabase start

# 3. Apply migrations + seed
supabase db reset

# 4. Copy env template and fill in values from `supabase status -o env`
cp .env.example .env
$EDITOR .env

# 5. Mint a local API token for the seeded dev account, insert it into the DB
uv run python scripts/seed_dev_token.py | docker exec -i supabase_db_upstream-api-server psql -U postgres
# (The plaintext token is printed to stderr — copy it.)

# 6. Run the API
make dev

# 7. Walk the full happy path end-to-end
UPSTREAM_API_URL=http://localhost:4000 \
UPSTREAM_DEV_TOKEN=<plaintext from step 5> \
uv run python scripts/smoke_local.py
```

`http://localhost:4000/healthz` should return `{"status":"ok"}`. The smoke script reports:

```
ok  agent_id=…
ok  camera_id=…
ok  srt_url=srt://ingest.upstream.tv:8890?streamid=publish:…
ok  jwt claims session_id=…
ok  session stopped
PASS
```

## Project layout

```
src/upstream_api/
  app.py              # FastAPI app factory; mounts middleware, handlers, routers
  main.py             # uvicorn entrypoint
  config.py           # UPSTREAM_* settings
  errors.py           # ApiError family + envelope-shaped handlers
  db/pool.py          # asyncpg pool lifespan
  security/           # api_tokens, session_jwt, supabase_jwt, shared_secret, deps
  services/           # limits, session_lifecycle, usage
  routers/            # admin, agents, cameras, stream_sessions, api_tokens
  schemas/            # pydantic request/response models
  observability/      # structlog config + request_id middleware
supabase/
  migrations/         # SQL migrations (GitHub-integrated; auto-applied on push to main)
  seed.sql            # local-dev seed (Local Dev account + permissive limits)
  config.toml         # Supabase CLI config
tests/                # pytest, async-mode auto
scripts/              # seed_dev_token.py, smoke_local.py
docs/                 # see "Documentation" below
```

Detail per directory and per file lives in `docs/module-map.md`.

## Make targets

| Command | What it does |
|---|---|
| `make install` | `uv sync` |
| `make dev` | `uvicorn upstream_api.main:app --reload` on port `UPSTREAM_PORT` |
| `make test` | `pytest -q` |
| `make lint` | `ruff check` + `ruff format --check` (matches CI exactly) |
| `make format` | `ruff format` + `ruff check --fix` |
| `make typecheck` | `mypy --strict` |
| `make db-start` / `db-stop` / `db-reset` / `db-diff` / `db-push` | thin wrappers around the Supabase CLI |

## Testing

`pytest` runs 26 unit tests covering token primitives, JWT round-trips, streamid handling, shared-secret compare, and route mounting. They don't need a running database — `tests/conftest.py` sets dummy env vars and tests avoid the lifespan.

End-to-end coverage is `scripts/smoke_local.py` against a running server with `supabase start` up.

## API surface

Endpoint shapes, error codes, auth headers, and the session-JWT claim format are documented in **`docs/api-contracts.md`**. Read that before integrating a client.

Three identity classes, three auth headers:
- Agent → `Authorization: Bearer ups_<env>_<id>_<secret>` (argon2-hashed)
- Dashboard user → `Authorization: Bearer <Supabase JWT>`
- Streaming server / processor → `X-Upstream-Webhook-Secret: <secret>`

All errors share the envelope `{ "error": { "code", "message", "details", "request_id" } }`. `code` strings are stable; `message` is not.

## Deployment

Production runs on a single t4g.small EC2 in `us-east-2`. See **`docs/deployment.md`** for the resource inventory, SSH command, DNS record, and teardown playbook.

The migration pipeline is one-way: pushing to `main` triggers Supabase's GitHub integration, which applies any new `supabase/migrations/*.sql` to the hosted DB. The hosted API server itself does **not** auto-deploy yet — that's a planned follow-up.

## Documentation

The repo's docs are the source of truth. Read these in roughly this order:

| File | What's in it |
|---|---|
| `docs/architecture.md` | The big picture: service responsibilities, request flow, state machines, phased delivery |
| `docs/module-map.md` | File-by-file walk-through of `src/upstream_api/` |
| `docs/api-contracts.md` | Request/response shapes, error codes, env vars, claim formats |
| `docs/deployment.md` | Production AWS inventory + SSH + teardown |
| `docs/troubleshooting.md` | Common errors and their fixes |
| `docs/commit-log.md` | Reverse-chronological summary of every meaningful commit |

## Commit / doc policy

**Every meaningful commit updates `docs/commit-log.md` in the same commit**, plus any other doc affected by the change (architecture, module-map, api-contracts, deployment, troubleshooting). The template lives at the bottom of `commit-log.md`. The goal is that a future contributor (human or coding agent) can ramp up from the docs alone, without grepping the code.

If a change feels too small to deserve an entry (typo fix, dependency bump), batch it with the next real commit.

## Environment

Every var the service reads:

| Variable | Required | Notes |
|---|---|---|
| `UPSTREAM_DATABASE_URL` | yes | `postgres://…` — port 5432 (direct) for long-running, 6543 (pooler) for serverless |
| `UPSTREAM_SUPABASE_URL` / `_ANON_KEY` / `_SERVICE_ROLE_KEY` / `_JWT_SECRET` | yes | From `supabase status -o env` (local) or Project Settings → API (hosted) |
| `UPSTREAM_SESSION_JWT_SECRET` | yes | Independent of Supabase. Generate: `python -c "import secrets;print(secrets.token_urlsafe(32))"` |
| `UPSTREAM_STREAMID_HMAC_KEY` | yes | Reserved for the compact streamid form (unused today) |
| `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` | yes | Shared with `upstream-streaming-server` |
| `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` | yes | Shared with `upstream-processor` |
| `UPSTREAM_PROMETHEUS_AUTH_TOKEN` | yes | Will gate `/admin/metrics` in Phase 6 |
| `UPSTREAM_ALLOWED_ORIGINS` | no | CSV (default `http://localhost:3000`) |
| `UPSTREAM_PORT`, `UPSTREAM_ENVIRONMENT`, `UPSTREAM_LOG_LEVEL` | no | sensible defaults |
| `UPSTREAM_SENTRY_DSN` | no | Sentry exception reporting (Phase 6) |

`.env.example` is the source list. `.env` is gitignored.

## Status

Phase 1 ships. Agent auth + camera registration + session creation work end-to-end against the local stack. Migrations are applied. CI runs ruff + mypy strict + pytest.

Not yet implemented:
- Phase 2 — `/v1/ingest/*` webhooks for MediaMTX.
- Phase 3 — `/v1/heartbeats` (batched) and `/v1/snapshots` (HTTPS frame uploads).
- Phase 4 — dashboard read endpoints with cursor pagination.
- Phase 5 — processor surface + `usage_events` rollups.
- Phase 6 — Prometheus metrics, Sentry, slowapi rate limiting.
- Deployment automation (Docker on the EC2 box, Caddy, secrets in SSM).

See `docs/architecture.md` for the full phased delivery plan.

## License

Private. All rights reserved.
