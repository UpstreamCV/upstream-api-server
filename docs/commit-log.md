# Commit log

Reverse chronological. Newest at top. Every meaningful commit gets an entry following the template at the bottom of this file.

---

## 2026-05-12 - Caddy IP rate limiting (custom build with caddy-ratelimit plugin)

### Summary
Built a small custom Caddy image with the official `caddy-ratelimit` plugin baked in via `xcaddy`, swapped it in for `caddy:2-alpine` in `deploy/docker-compose.yml`, and wired two IP-keyed zones in the Caddyfile: `public_burst` (30 req/min/IP on `/`, `/healthz`, `/readyz`, `/docs`, `/openapi.json`) and `api_per_ip` (600 req/min/IP on `/v1/*`). This is coarse defense-in-depth so a single bot can't burn CPU by hammering anonymous endpoints; per-token application limits will land in Phase 6 via slowapi.

### Files touched
- `deploy/caddy/Dockerfile` — new. Two-stage build using `caddy:2-builder` + `xcaddy build --with github.com/mholt/caddy-ratelimit`, output copied into `caddy:2-alpine`.
- `deploy/docker-compose.yml` — `caddy` service now `build`s from `./caddy` and tags `upstream-caddy:latest` instead of pulling `caddy:2-alpine`.
- `deploy/Caddyfile` — added `order rate_limit before basicauth` in the global block; declared `@public` and `@api` matchers with their own `rate_limit { zone … }` directives.

### System interaction notes
Rate limiting happens at Caddy before the request ever reaches the FastAPI container. The plugin uses in-process storage — fine for one Caddy instance; needs a distributed backend (Redis) once we horizontally scale Caddy. Zone keys are `{http.request.remote.host}` (the client's real public IP at the edge). Caddy returns 429 with no body on limit exceeded — clients can inspect `Retry-After` header (the plugin sets it automatically).

### Testing/checks
- `docker compose build caddy` succeeded; image `upstream-caddy:latest` produced.
- `docker compose up -d` swapped Caddy with no API downtime; api container stayed healthy throughout.
- Burst test against `/healthz`: requests 1–30 → HTTP 200, requests 31–32 → HTTP 429 (matches the 30 events / 1 min config).
- `/v1/*` request from the same caller IP during the burst still routed through to FastAPI (returned 404 because `/v1/anything-here` doesn't exist) — confirming the two zones are independent.
- Local `ruff` + `mypy --strict` + `pytest -q` still clean (no app-side change).

### Follow-up TODOs
- Per-token application rate limits via slowapi (Phase 6).
- When we go multi-Caddy, point `rate_limit` at a Redis backend (the plugin supports it).

---

## 2026-05-12 - Add GET / service-info endpoint

### Summary
Anyone hitting `https://api.upstreamcv.com/` used to get a 404. Added a small landing endpoint that returns the service name, version, environment, and pointers to `/docs`, `/openapi.json`, `/healthz`, `/readyz`.

### Files touched
- `src/upstream_api/routers/admin.py` — added `GET /` returning a service-info dict. Pulls version from `upstream_api.__version__` and environment from `app.state.settings`.
- `tests/test_app_routes.py` — added `/:GET` to the expected-routes set.

### System interaction notes
No new dependencies, no DB access. Lives on the existing `admin` router, so it inherits the same middleware chain. Doesn't reveal anything beyond what FastAPI's OpenAPI doc already would. Cached as a normal handler — no special caching needed.

### Testing/checks
- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest -q` all clean (26 tests).
- After rebuild on the box: `curl https://api.upstreamcv.com/` returns the JSON above, HTTP 200.

### Follow-up TODOs
None.

---

## 2026-05-12 - Fix gunicorn control-socket permission warning

### Summary
gunicorn 26+ writes a control socket at `$HOME/.gunicorn/...` on startup. The runtime stage of the Dockerfile chowns the copied files to `app:app` but not `/app` itself (created by `WORKDIR` as root), so the non-root `app` user couldn't write to its home. Added `RUN chown app:app /app` after the COPY. Rebuilt the image on the box, redeployed — the `Permission denied: '/app/.gunicorn'` line is replaced by `Control socket listening at /app/.gunicorn/gunicorn.ctl`.

### Files touched
- `Dockerfile` — one new `RUN chown app:app /app` in the runtime stage, with a comment explaining the WORKDIR vs COPY ownership quirk.

### System interaction notes
No behaviour change for the API itself. Cleans up startup logs and gives gunicorn's runtime control plumbing a place to live.

### Testing/checks
- `docker compose build api && docker compose up -d` on the box.
- API container restarts healthy; no permission denied line in `docker compose logs api`.
- `curl https://api.upstreamcv.com/healthz` still returns `{"status":"ok"}`.

### Follow-up TODOs
None for this one.

---

## 2026-05-12 - First production deploy — api.upstreamcv.com live behind Let's Encrypt

### Summary
With `deploy/.env` filled in from the Supabase dashboard (project URL, publishable key, secret key, asymmetric JWT key id, session-pooler DB URL), `docker compose build api && docker compose up -d` brought the stack up on the EC2 host. Caddy obtained a Let's Encrypt cert for `api.upstreamcv.com` on the first request; `/healthz` and `/readyz` return 200 from the public internet. `/readyz` confirms the box reaches Supabase Postgres via the session pooler.

### Files touched
None. This is the runtime bring-up of artifacts already committed (`deploy/docker-compose.yml`, `deploy/Caddyfile`, `Dockerfile`).

### System interaction notes
- Build path: `uv sync --frozen --no-dev` in the builder stage, then `COPY --from=builder` into the slim runtime layer. Image tag `upstream-api:latest` on the host.
- Stack: Caddy (host ports 80/443) → docker network `internal` → `api:4000` (gunicorn + 2 uvicorn workers). Caddy waits on the api healthcheck before accepting traffic.
- Lifespan opens the asyncpg pool against the Supabase session-pooler URL successfully → DB reachable.
- Non-fatal log at startup: `Control server error: [Errno 13] Permission denied: '/app/.gunicorn'`. gunicorn 26 tries to create a control socket in `$HOME`; the non-root `app` user can't write to `/app` because the Dockerfile chowns `/app/*` but not `/app` itself. Harmless — gunicorn still serves traffic.

### Testing/checks
- `docker compose ps`: `upstream-api-api-1` Up healthy, `upstream-api-caddy-1` Up.
- `curl https://api.upstreamcv.com/healthz` → `{"status":"ok"}`, HTTP 200, TLS verified.
- `curl https://api.upstreamcv.com/readyz` → `{"status":"ok"}` (DB ping ≤500 ms).

### Follow-up TODOs
- Silence the gunicorn control-server warning (`RUN chown app:app /app` in the runtime stage, or `gunicorn.conf.py` pointing pidfile/socket at a writable path).
- Add a systemd unit so `docker compose up -d` runs on host boot.
- Migrate `deploy/.env` secrets into AWS Systems Manager Parameter Store; source at container start.
- Update `security/supabase_jwt.py` to verify via JWKS when Phase 4 dashboard reads land (Supabase project is on asymmetric signing).
- Add CloudWatch metrics / alarm on the host (disk / cpu / memory).

---

## 2026-05-12 - Add deploy config (docker-compose + Caddy) + prep EC2 host

### Summary
Wrote the deploy configuration that runs the API in production: a `deploy/` folder containing `docker-compose.yml` (FastAPI built from the project `Dockerfile` + Caddy 2 fronting it), a `Caddyfile` for `api.upstreamcv.com` with automatic Let's Encrypt, and `.env.production.example` documenting every variable. On the EC2 host, installed git + docker + docker compose v2, cloned the repo via the new deploy key, copied the deploy config in, and generated a real `.env` with random internal secrets pre-filled. Supabase values are still placeholders pending dashboard paste.

### Files touched
- `deploy/docker-compose.yml` — new. Two services (`api`, `caddy`), internal docker network, named volumes for Caddy data/config, healthcheck on the api container so Caddy waits for it.
- `deploy/Caddyfile` — new. Reverse-proxies `api.upstreamcv.com` → `api:4000` with HSTS, request logging, zstd/gzip encode, fail-fast on backend errors.
- `deploy/.env.production.example` — new. Documents every `UPSTREAM_*` variable with where to source it.
- `.gitignore` — added `deploy/.env` so secrets never land in git.
- `docs/deployment.md` — replaced the "what is on the box" stub with a real inventory of what's installed and the first-time bring-up command.

### On the EC2 host
- `dnf install git docker` + enable docker + add `ec2-user` to the docker group.
- Installed docker compose v2.32.4 to `/usr/local/lib/docker/cli-plugins/`.
- `git clone git@github.com:UpstreamCV/upstream-api-server.git` via the deploy key (GitHub auth verified `Hi UpstreamCV/upstream-api-server!`).
- `scp`'d the three deploy files to `~ec2-user/upstream-api-server/deploy/`.
- Wrote `deploy/.env` with random `UPSTREAM_SESSION_JWT_SECRET`, `UPSTREAM_STREAMID_HMAC_KEY`, `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET`, `UPSTREAM_PROCESSOR_WEBHOOK_SECRET`, `UPSTREAM_PROMETHEUS_AUTH_TOKEN` (each `secrets.token_urlsafe(32)`). Mode 0600.

### System interaction notes
The Caddy reverse-proxy sits in front of FastAPI/gunicorn. Caddy is the only thing bound to public ports (80/443); the API container exposes 4000 only on the internal docker network. TLS is fully Caddy's responsibility — first request to `https://api.upstreamcv.com` triggers a Let's Encrypt HTTP-01 challenge over port 80, after which the cert is cached in the `caddy_data` named volume.

### Testing/checks
- `docker --version`, `docker compose version` on the box — OK.
- `ssh -T git@github.com` from the box — `Hi UpstreamCV/upstream-api-server!`.
- Box repo `git log --oneline -3` matches the GitHub remote.
- Stack not yet started — pending Supabase secrets in `.env`.

### Follow-up TODOs
- User pastes Supabase values into `deploy/.env` on the box (URL, ANON_KEY, SERVICE_ROLE_KEY, JWT_SECRET, DATABASE_URL).
- `docker compose up -d` to bring the stack up and observe Caddy issuing a Let's Encrypt cert.
- Add a systemd unit so the stack restarts on host reboot.
- Move secrets out of the on-disk `.env` and into AWS Systems Manager Parameter Store (Phase 7-ish hardening).
- Create an IAM user `upstream-deploy` and rotate the root AWS credentials currently in use.

---

## 2026-05-12 - Add top-level README

### Summary
Wrote a comprehensive README at the repo root. Acts as the entry point for new contributors and coding agents: explains the service's role in the wider Upstream architecture, the stack, a copy-pasteable quickstart, the project layout, the Make targets, where each doc lives, the env-var inventory, and the current phase status. All deep content stays in `docs/`; the README only summarizes and links.

### Files touched
- `README.md` — new.

### System interaction notes
No runtime change. The README enforces the docs-per-commit policy by pointing every reader at `docs/commit-log.md` and the per-area docs, so a newcomer follows the doc map rather than reading code.

### Testing/checks
- Eyeballed for broken doc links (all five `docs/*.md` references resolve to files that exist).
- No code touched; CI checks unaffected.

### Follow-up TODOs
- Add a CI badge once the GitHub Actions workflow has run at least once on `main`.

---

## 2026-05-12 - ruff format pass + recorded api.upstreamcv.com DNS pointer

### Summary
CI's `ruff format --check` step caught five files that I had only run `ruff check` against locally. Applied `ruff format` (no semantic changes, only line breaks / argument wrapping). Also recorded that `api.upstreamcv.com` now has an A record pointing at the EC2 Elastic IP `18.225.104.21`.

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
- Key pair `upstream-api` (ed25519). Private key at `~/.ssh/upstream-api.pem` mode 0400.
- Security group `sg-0236c95fe7b168503` (`upstream-api-sg`): ingress 22/80/443 from `0.0.0.0/0`.
- Elastic IP `eipalloc-00c601da170d052ae` → `18.225.104.21`.
- Instance `i-07434302e4b0785e8` (t4g.small, AL2023 ARM64, 16 GiB gp3, IMDSv2-only).

### System interaction notes
The instance is empty — no Docker, Caddy, or app code yet. Follow-up commits will install the runtime stack, deploy the API, and document each step here. Custom domain is not yet pointed at the EIP; pointing it is a DNS-side change documented in `deployment.md`.

### Testing/checks
- SSH verified: `ssh -i ~/.ssh/upstream-api.pem ec2-user@18.225.104.21` returns AL2023 kernel info.
- Instance state: `running` in `us-east-2b`.
- No services exposed yet — port scans on 80/443 will refuse connection until the runtime is installed.

### Follow-up TODOs
- Install Docker + docker-compose plugin on the box.
- Install Caddy and write the Caddyfile pointing at uvicorn.
- Move `UPSTREAM_*` secrets into AWS Systems Manager Parameter Store and load them at service start.
- Add a systemd unit (or rely on `docker compose --restart=always`) so the stack survives reboots.
- Stop using the AWS root account — create an `upstream-deploy` IAM user with scoped permissions.
- Point a real DNS A record at `18.225.104.21` once a domain is chosen.

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
