# Architecture — Upstream API Server

This service is the **control plane** for the Upstream multi-camera streaming platform. It is one of four sister services; the others are out of scope of this document.

Its job is to authenticate agents, gate session creation against account limits, mint short-lived SRT credentials, ingest agent heartbeats, expose read endpoints for the dashboard, and act as the policy brain for the streaming server. It **does not** carry video frames, process frames, or render UI.

## Stack

- **Python 3.11+**, FastAPI for HTTP.
- **asyncpg** for direct Postgres access on the hot path.
- **SQLAlchemy 2.0 async** is reserved for dashboard reads in Phase 4.
- **Supabase Postgres** as the database (managed). Schema migrations live in `supabase/migrations/` and are applied to the hosted DB by the Supabase GitHub integration on push to `main`.
- **argon2id** for API token hashing.
- **HS256 JWT** for session tokens, signed with our own `UPSTREAM_SESSION_JWT_SECRET` (separate from Supabase Auth).
- **structlog** with JSON output in production and secret-key redaction.

## Sister services and where this fits

```
                    upstream-dashboard (Next.js)
                            │
                            ▼ Supabase JWT
                    ┌───────────────┐
                    │ upstream-api  │  <-- this repo
                    │   (FastAPI)   │
                    └───┬───────────┘
   API token │          │ webhook secret
   ┌─────────┘          └──────────────┐
   ▼                                   ▼
upstream-agent                  upstream-streaming-server
(Python, public pip lib)        (MediaMTX, SRT in / WebRTC out)
   │
   └── SRT publish, streamid=publish:<jwt>
         │
         ▼
   upstream-streaming-server
         │
         ▼ webhook secret
   upstream-api  (validate-stream, stream-started/ended)
```

`upstream-api` owns the Supabase database. All other services either consume API endpoints or (for the streaming server) call back via signed webhooks.

## Three identity classes

| Caller | Header scheme | Verified by | Used on |
|---|---|---|---|
| Agent | `Authorization: Bearer ups_<env>_<id>_<secret>` | argon2id against `api_tokens.hash`, prefix-indexed | `/v1/agents/*`, `/v1/cameras/*`, `/v1/stream-sessions/*`, `/v1/heartbeats`, `/v1/snapshots` |
| Dashboard user | `Authorization: Bearer <Supabase access token>` | HS256 against `UPSTREAM_SUPABASE_JWT_SECRET` + `account_members` lookup | dashboard reads, `/v1/api-tokens` |
| Streaming server | `X-Upstream-Webhook-Secret: <secret>` | constant-time compare against `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` | `/v1/ingest/*` (Phase 2) |
| Processor | `X-Upstream-Webhook-Secret: <secret>` | constant-time compare against `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` | `/v1/processor/*` (Phase 5) |

Each identity class has its own FastAPI dep in `security/deps.py`: `current_agent`, `current_user`, `current_ingest`, `current_processor`. Routers ask only for the dep they need; account scoping comes from the auth context, never from the request body.

## Canonical request flow — session creation

```
agent ──Bearer API token──► POST /v1/stream-sessions/start
                              │
                              ▼  current_agent
                              │  - split_token(plaintext) -> (prefix, secret)
                              │  - SELECT … FROM api_tokens WHERE prefix = $1
                              │  - argon2 verify(secret, row.hash)
                              │  - check revoked_at / expires_at
                              │
                              ▼  services.session_lifecycle.start_session
                              │  - SELECT camera (derive agent_id, assert account_id)
                              │  - busy = EXISTS active session for camera
                              │  - services.limits.check_session_start
                              │      (load account_limits, concurrent session count)
                              │  - mint_session_token(secret, claims with lim)
                              │  - build streamid = "publish:<jwt>"
                              │  - INSERT stream_sessions (state='approved', lim_* snapshot)
                              │  - record_event(session_started)
                              │
                              ▼  HTTP 200
agent ◄── { srt_url, session_token, session: {...} }
```

The agent then opens an SRT connection to `upstream-streaming-server` with `streamid=publish:<jwt>`. In Phase 2 the streaming server will POST to `/v1/ingest/validate-stream` to verify the JWT before accepting the stream.

## Session state machine

```
                ┌────────────┐
   start    ───►│  approved  │───► streaming ───► ended
                └────┬──┬────┘                 ▲
                     │  │                      │
                     │  └────► failed          │
                     │           (validation   │
                     │            mismatch)    │
                     │                         │
                     └──► expired ─────────────┘
                            (expires_at hit
                             without streaming)
```

- `approved` — set on session creation.
- `streaming` — set by `/v1/ingest/stream-started` (Phase 2) after the streaming server's ffprobe confirms codec/res/fps match the snapshot in `lim_*`.
- `ended` — set by `POST /v1/stream-sessions/{id}/stop` (agent or admin) or `/v1/ingest/stream-ended` (clean disconnect).
- `expired` — set by a background sweeper when `expires_at` passes without reaching `streaming`. **Sweeper is not yet implemented.**
- `failed` — set by `/v1/ingest/validation-failed`.

## Limit-snapshot pattern

When a session is approved, the relevant `account_limits` values are copied to `lim_*` columns on the session row. The streaming server uses the JWT's `lim` claim — which mirrors `lim_*` — as the contract for that session. If an operator raises or lowers `account_limits` mid-stream, the in-flight session is unaffected; the new limit only applies to future-session decisions.

## Concurrency safeguards on session start

Two concurrent `POST /v1/stream-sessions/start` calls could otherwise race past the application-level pre-checks. We belt-and-brace this in two places:

- **Per-camera**: a partial unique index `stream_sessions_one_active_per_camera_idx on stream_sessions(camera_id) where state in ('approved','streaming')` makes the second insert fail with a unique-violation, which `services.session_lifecycle.start_session` translates back into the same `ConflictError(code="camera_already_streaming")` the pre-check raises.
- **Per-account**: `services.limits.check_session_start` takes a transaction-scoped `pg_advisory_xact_lock(hashtextextended(account_id::text, 0))` before counting active sessions. Different accounts don't contend; concurrent starts within one account serialize and observe the same active-count snapshot. The lock releases automatically at commit/rollback because `start_session` runs inside `async with conn.transaction()`.

## Token model

There are three kinds of token in the system:

1. **API token** (long-lived, opaque, agent-facing). Plaintext format `ups_<env>_<8 id>_<22 secret>`. The `prefix` (everything except the last segment) is stored in DB and is the unique lookup index; the last segment is argon2id-hashed in `hash`. Shown to the dashboard user exactly once on creation.
2. **Session token** (4h TTL, JWT, agent-facing). Minted by `POST /v1/stream-sessions/start`. Embedded in SRT streamids. Carries `acc`/`sub`/`cam`/`sid`/`jti`/`exp`/`lim` claims. Validated by the streaming server via `/v1/ingest/validate-stream` (Phase 2).
3. **Supabase Auth JWT** (1h TTL, JWT, dashboard-user-facing). Issued by Supabase Auth; verified using `UPSTREAM_SUPABASE_JWT_SECRET`.

## Phased delivery

| Phase | Scope | Status |
|---|---|---|
| 0 | FastAPI scaffold | done |
| 1 | Agent auth + session creation | done |
| 2 | MediaMTX ingest webhooks | not started |
| 3 | Heartbeats + snapshots | not started |
| 4 | Dashboard reads (Supabase JWT, RLS) | not started |
| 5 | Processor surface + usage rollup | not started |
| 6 | Observability + rate limiting | partial (structlog + middleware in; Prometheus + slowapi not yet) |
| 7 | Hardening (token rotation, revocation list, mTLS) | not started |
