# Commit log

Reverse chronological. Newest at top. Every meaningful commit gets an entry following the template at the bottom of this file.

---

## 2026-05-12 - ruff format pass + recorded api.upstreamcv.com DNS pointer

### Summary
CI's `ruff format --check` step caught five files that I had only run `ruff check` against locally. Applied `ruff format` (no semantic changes, only line breaks / argument wrapping). Also recorded that `api.upstreamcv.com` now has an A record pointing at the EC2 Elastic IP `<api-ip>`.

### Files touched
- `src/upstream_api/routers/agents.py` — formatter line-wrapping only.
- `src/upstream_api/routers/api_tokens.py` — formatter line-wrapping only.
- `src/upstream_api/routers/cameras.py` — formatter line-wrapping only.
- `src/upstream_api/routers/stream_sessions.py` — formatter line-wrapping only.
- `src/upstream_api/services/session_lifecycle.py` — formatter line-wrapping only.
- `docs/deployment.md` — recorded `api.upstreamcv.com` A record + updated the "Pointing a domain" section.

### System interaction notes
None — no runtime behavior changed. The DNS pointer is informational; nothing on the EC2 box is actually serving on 80/443 yet, so `https://api.upstreamcv.com` won't resolve to anything useful until Caddy is installed and the API is deployed.

### Testing/checks
- `ruff format --check src tests` → clean.
- `ruff check src tests` → clean.
- `mypy --strict src tests` → clean (41 files).
- `pytest -q` → 26 passed.

### Follow-up TODOs
- Locally I should make a habit of running `make format` (or `ruff format`) before pushing; CI was the wrong place to catch this.

---

## 2026-05-12 - Provisioned production EC2 host on AWS

### Summary
Stood up the production hosting target on AWS us-east-2: one t4g.small ARM instance running AL2023, fronted by a stable Elastic IP, behind a fresh security group that allows SSH/HTTP/HTTPS from the public internet. Nothing is installed on the box yet — this commit is just the bare infrastructure. Full resource inventory lives in `docs/deployment.md`.

### Files touched
- `docs/deployment.md` — new. Source of truth for the deployed infra: instance ID, AMI, IP, security group, key pair, SSH command, teardown playbook.

### Resources created (AWS)
- Key pair `upstream-api` (ed25519). Private key at `<ssh-key>` mode 0400.
- Security group `<security-group-id>` (`upstream-api-sg`): ingress 22/80/443 from `0.0.0.0/0`.
- Elastic IP `<eip-allocation-id>` → `<api-ip>`.
- Instance `<instance-id>` (t4g.small, AL2023 ARM64, 16 GiB gp3, IMDSv2-only).

### System interaction notes
The instance is empty — no Docker, Caddy, or app code yet. Follow-up commits will install the runtime stack, deploy the API, and document each step here. Custom domain is not yet pointed at the EIP; pointing it is a DNS-side change documented in `deployment.md`.

### Testing/checks
- SSH verified: `ssh -i <ssh-key> ec2-user@<api-ip>` returns AL2023 kernel info.
- Instance state: `running` in `us-east-2b`.
- No services exposed yet — port scans on 80/443 will refuse connection until the runtime is installed.

### Follow-up TODOs
- Install Docker + docker-compose plugin on the box.
- Install Caddy and write the Caddyfile pointing at uvicorn.
- Move `UPSTREAM_*` secrets into AWS Systems Manager Parameter Store and load them at service start.
- Add a systemd unit (or rely on `docker compose --restart=always`) so the stack survives reboots.
- Stop using the AWS root account — create an `upstream-deploy` IAM user with scoped permissions.
- Point a real DNS A record at `<api-ip>` once a domain is chosen.

---

## 2026-05-12 - Audit pass: token-prefix fix, transaction-safety fix, local verification

### Summary
Re-audited the Phase 1 implementation, fixed one critical bug (every prod API token shared the same DB-unique prefix, so the second create would have failed), one transaction-poisoning bug (`record_event` aborted the parent transaction when `usage_events` table did not exist), and several smaller issues. Stood up the local Supabase stack, applied all four migrations, and ran an end-to-end smoke that confirmed the canonical agent flow works.

### Files touched
- `src/upstream_api/security/api_tokens.py` — plaintext format changed from `ups_<env>_<26 chars>` to `ups_<env>_<8 id>_<22 secret>`; added `ALLOWED_SCOPES = frozenset({"agent"})`.
- `tests/test_api_tokens.py` — updated parametrize cases for the new shape; added assertion that two issued tokens have distinct prefixes.
- `src/upstream_api/services/session_lifecycle.py` — `start_session()` no longer takes `agent_id`; it derives it from a single camera fetch. Dropped `serialize_session`.
- `src/upstream_api/routers/stream_sessions.py` — removed redundant camera pre-fetch in the `/start` handler; builds `StreamSessionResponse` directly from the dataclass (no JSON round-trip).
- `src/upstream_api/routers/api_tokens.py` — rejects scopes outside `ALLOWED_SCOPES`.
- `src/upstream_api/services/usage.py` — `record_event()` wraps the insert in `async with conn.transaction()` so the missing-table swallow becomes a SAVEPOINT and the parent transaction is not aborted.
- `src/upstream_api/security/deps.py` — moved inline `from datetime import datetime` to module top.
- `src/upstream_api/errors.py` — dropped `assert isinstance(...)` calls; handlers accept `Exception` (Starlette's signature) and narrow with `isinstance` guards.
- `src/upstream_api/config.py` — `allowed_origins` is now a `@property` over a CSV `str` env field aliased to `ALLOWED_ORIGINS` so `UPSTREAM_ALLOWED_ORIGINS=a,b,c` works.
- `supabase/migrations/20260512130100_accounts_and_auth.sql` — simplified `account_members_self_select` policy to `using (user_id = auth.uid())` to avoid self-referential RLS recursion risk.
- `supabase/seed.sql` — added Local Dev account + permissive `account_limits`.
- `scripts/seed_dev_token.py` — new. Mints a real token and prints SQL to insert.
- `scripts/smoke_local.py` — new. Walks the full happy path against a running server.
- `.github/workflows/ci.yml` — new. ruff + ruff format check + mypy strict + pytest on push/PR.

### Functions/classes changed
- `issue_token`, `split_token` (security/api_tokens.py) — token shape change; prefix is now per-token unique. `ALLOWED_SCOPES` added.
- `start_session` (services/session_lifecycle.py) — single-fetch camera lookup; drops the `agent_id` parameter.
- `record_event` (services/usage.py) — SAVEPOINT isolation around the insert.
- `api_error_handler` and siblings (errors.py) — accept `Exception`, guard with `isinstance`, no asserts.
- `Settings` (config.py) — `allowed_origins_csv: str` field + `allowed_origins: list[str]` property.

### System interaction notes
The transaction-safety fix is the load-bearing one for production correctness. The `start_session` flow calls `record_event` inside the same `async with pool.acquire() as conn, conn.transaction()` block as the `stream_sessions` insert. Previously, when `usage_events` didn't exist (anywhere before Phase 5), the bare `except asyncpg.UndefinedTableError` caught the error but left the asyncpg transaction in an aborted state; the COMMIT at the end of the `async with` silently produced no rows. The smoke test surfaced this: `/v1/stream-sessions/start` returned 200 with a valid response shape, but `select count(*) from stream_sessions` returned 0. Wrapping the `record_event` insert in a nested `conn.transaction()` makes asyncpg create a SAVEPOINT, scope the rollback there, and leave the parent transaction healthy.

The token-prefix fix is mandatory for any second token to issue. Without it the unique constraint on `api_tokens.prefix` blocks the second `POST /v1/api-tokens` for the same environment.

### Testing/checks
- 26 unit tests passing; `ruff check`, `ruff format --check`, `mypy --strict` all clean.
- `supabase db reset` applies all 4 migrations cleanly (verified by `\dt public.*` → 8 tables, plus the `stream_session_state` enum).
- `scripts/smoke_local.py` runs PASS end-to-end: registers `smoke-agent`, registers `smoke-cam`, starts session (gets `srt_url` with `streamid=publish:<jwt>`), decodes JWT and asserts `acc`/`cam`/`sub`/`lim` claims match, stops session. Post-run, the `stream_sessions` row persists in `state=ended` with `ended_at` set.

### Follow-up TODOs
- Push to GitHub so the Supabase integration applies migrations to the hosted DB.
- Phase 2: implement `/v1/ingest/*` webhooks for MediaMTX.
- Phase 5: drop the SAVEPOINT + `UndefinedTableError` swallow in `record_event` once migration 0005 ships the `usage_events` table.

---

## 2026-05-12 - Stack swap and Phase 1 build-out: Express → FastAPI

### Summary
Removed the prior Express/TypeScript scaffold and built out the API server in FastAPI per the architecture spec. Implemented migrations 0–3 (accounts, members, limits, api_tokens, agents, cameras, stream_sessions, stream_validations), the auth layer (four identity classes), the limits/lifecycle services, and the Phase 1 routers (agents, cameras, stream-sessions, api-tokens, admin). Established structured logging with secret redaction and a stable error envelope.

### Files touched
Removed: `package.json`, `package-lock.json`, `tsconfig.json`, `node_modules/`, every `src/*.ts`.

Added everything currently under `src/upstream_api/`, all four SQL migrations under `supabase/migrations/`, `pyproject.toml`, `Makefile`, `Dockerfile`, `.env.example` (rewritten with `UPSTREAM_*` keys), and the initial test suite.

### Functions/classes added
- `Settings` (config.py) — pydantic-settings v2 with the `UPSTREAM_` env prefix.
- `ApiError` family (errors.py) — base + `NotFoundError`, `UnauthorizedError`, `ForbiddenError`, `ConflictError`, `LimitExceeded`; the envelope is `{ "error": { "code", "message", "details", "request_id" } }`.
- `current_agent`, `current_user`, `current_ingest`, `current_processor` (security/deps.py) — the four FastAPI deps. `Annotated` aliases (`CurrentAgent`, `CurrentUser`, …) let handlers consume them with one type hint.
- `issue_token`, `verify_secret` (security/api_tokens.py) — argon2id token primitives (initial shape; revised in the audit-pass commit above).
- `mint_session_token`, `verify_session_token` (security/session_jwt.py) — HS256 with `iss=upstream-api`, `aud=srt-ingest`, `lim` claim.
- `build_streamid`, `extract_session_token` (security/streamid.py) — `publish:<jwt>` carrier.
- `load_account_limits`, `check_session_start`, `check_camera_defaults`, `check_camera_slot_available` (services/limits.py) — limit engine.
- `start_session`, `stop_session` (services/session_lifecycle.py) — state machine and JWT minting in a single transaction.
- Routers: `admin`, `agents`, `cameras`, `stream_sessions`, `api_tokens`.

### System interaction notes
Canonical Phase 1 flow:

```
agent --Bearer ups_<env>_…--> POST /v1/stream-sessions/start
  -> current_agent (prefix lookup + argon2 verify)
  -> services.session_lifecycle.start_session
       -> fetch camera, derive agent_id
       -> services.limits.check_session_start
       -> mint_session_token(secret, claims) -> HS256 JWT
       -> build_streamid(jwt) -> "publish:<jwt>"
       -> INSERT stream_sessions (state=approved, lim_* snapshot)
       -> record_event(session_started)
  <- { srt_url, session_token, session: {...} }
```

The agent then opens an SRT connection with `streamid=publish:<jwt>`. The streaming server's validation hook (Phase 2) calls back into `/v1/ingest/validate-stream` to verify the JWT before accepting the stream.

### Testing/checks
- 25 unit tests at the time of this commit (route enumeration, token primitives, JWT, streamid, shared secret, health).
- ruff + mypy strict clean.
- No live DB verification at this point; that was added in the follow-up audit-pass commit above.

### Follow-up TODOs
- Apply migrations against a real Postgres and confirm schema. (done in follow-up)
- Add local seed data and CI workflow. (done in follow-up)
- Phase 2 ingest webhooks.

---

## Template

Copy this block for each new entry. Put new entries at the top of the file, not the bottom.

```markdown
## YYYY-MM-DD - Short commit title

### Summary
One paragraph: what changed and why.

### Files touched
- `path/to/file.py` — what changed.

### Functions/classes changed
- `Name` — what it does and how it differs.

### System interaction notes
How this change moves through the request flow (e.g. `current_agent → services.session_lifecycle → asyncpg pool → stream_sessions row → response`).

### Testing/checks
What was run and what passed; what could not be run and why.

### Follow-up TODOs
What this commit deferred.
```
