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
   └── SRT publish, streamid=publish:<stream_key>
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
                              │  - build streamid = "publish:<stream_key>" (short opaque)
                              │  - INSERT stream_sessions (state='approved', lim_* snapshot)
                              │  - record_event(session_started)
                              │
                              ▼  HTTP 200
agent ◄── { srt_url, session_token, session: {...} }
```

The agent then opens an SRT connection to `upstream-streaming-server` with `streamid=publish:<stream_key>`. The streaming server POSTs to `/v1/ingest/validate-stream` with the full streamid; the API looks up the matching session row and returns the approved `lim_*` parameters as `expected`. The session JWT (`session_token` in the response) is **not** embedded in the streamid — libsrt caps the streamid query value at ~512 bytes, and a signed JWT with `lim` claims comfortably exceeds that. The JWT is still returned at the top level for non-ingest callers.

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
2. **Session token** (4h TTL, JWT, agent-facing). Minted by `POST /v1/stream-sessions/start` and returned at the top level of the response. Carries `acc`/`sub`/`cam`/`sid`/`jti`/`exp`/`lim` claims. *Not* placed in the SRT streamid — see "stream_key" below. Currently a back-compat carrier for non-ingest callers; the live ingest validation path no longer reads it.
4. **Stream key** (4h TTL, opaque, ingest-only). Short random token (`sess_<16 hex>`) minted per session and stored on `stream_sessions.streamid` as `publish:sess_<hex>`. This is what rides in the SRT streamid query parameter and what `/v1/ingest/validate-stream` looks the session up by.
3. **Supabase Auth JWT** (1h TTL, JWT, dashboard-user-facing). Issued by Supabase Auth; verified using `UPSTREAM_SUPABASE_JWT_SECRET`.

## Phased delivery

| Phase | Scope | Status |
|---|---|---|
| 0 | FastAPI scaffold | done |
| 1 | Agent auth + session creation | done |
| 2 | MediaMTX ingest webhooks | done |
| 3 | Heartbeats + snapshots | done |
| 4 | Dashboard reads (Supabase JWT, RLS) | done — `GET /v1/agents`, `/v1/cameras`, `/v1/stream-sessions` accept either an agent token or a user JWT via `CurrentCaller` (security/deps.py) |
| 5 | Processor surface + usage rollup | not started |
| 6 | Observability + rate limiting | partial (structlog + middleware in; Prometheus + slowapi not yet) |
| 7 | Hardening (token rotation, revocation list, mTLS) | not started |

## Phase 2 — MediaMTX ingest webhook flow

```
agent ──SRT publish, streamid=publish:<stream_key>──► upstream-streaming-server
                                                        │
   POST /v1/ingest/validate-stream                      │ X-Upstream-Webhook-Secret
   { "streamid": "publish:<stream_key>" }               │
   ◄── { "accept": true, "expected": { "codec":..., "width":..., ... } }
                                                        │
                                                        │ accept publish, run ffprobe
                                                        │
   POST /v1/ingest/stream-started                       │
   { "session_id":..., "observed": { ... }, "started_at": ... }
   ◄── { "session_id":..., "state": "streaming" }       │
                                                        │
                                                        │ stream runs ...
                                                        │
   POST /v1/ingest/stream-ended                         │
   { "session_id":..., "ended_at":..., "duration_seconds":..., "bytes_sent":..., "reason":... }
   ◄── { "session_id":..., "state": "ended" }           │
```

If ffprobe finds a mismatch, the streaming server calls `validation-failed` instead of `stream-started`; the session transitions to `failed`, a row lands in `stream_validations` with `passed=false`, and the SRT connection is dropped.

## Phase 3 — Heartbeats + snapshots

- **Heartbeats**: agent posts batches of up to 100 every few seconds. Each item carries `status`, `fps`, `bitrate_kbps`, `width`, `height`, `codec`, `last_error`, `agent_version`, `reported_at`. The handler updates `agents.last_seen_at` once per batch — not per heartbeat — to avoid hot-row contention on chatty fleets.
- **Snapshots**: agent (or, optionally, the streaming server in a later phase) uploads a JPEG as multipart. Bytes go to Supabase Storage at `{account_id}/{camera_id}/{yyyymmdd}/{uuid}.jpg`; metadata + `storage_key` land in `snapshots`. Quota is `account_limits.snapshot_storage_mb` summed across the trailing 30 days. Max upload is `UPSTREAM_SNAPSHOT_MAX_BYTES` (default 5 MB). **Caddy must allow `>= snapshot_max_bytes` on `/v1/snapshots` specifically** — the global 1 MB body cap stays in place everywhere else.

## Latency modes

Cameras carry `default_latency_mode` (`low | balanced | stable`); each session snapshots it onto `req_latency_mode` and `lim_latency_mode`. The SRT URL builder maps the mode to a `latency=<ms>` parameter:

| Mode | SRT `latency` | Use when |
|---|---|---|
| low | 80 ms | low end-to-end delay over a healthy link |
| balanced (default) | 200 ms | typical web/dashboard viewing |
| stable | 1500 ms | weak Wi-Fi / cellular; tolerates jitter and packet loss |

## Supabase Realtime for dashboard push (RLS tables)

The dashboard subscribes directly to Supabase Realtime `postgres_changes` for
sub-second push (camera session state via `stream_sessions`; live detection
overlay via `latest_perception_snapshots`) instead of polling. This is the one
path where the dashboard reads Postgres **directly as the `authenticated` role**
— everything else goes through this API on the **service role**, which is
privileged and RLS-exempt. That difference is the source of a whole class of
"Realtime is silent but the API works fine" bugs: a table can be perfectly
readable via the API yet deliver zero Realtime events.

**For a browser client to receive `postgres_changes` on an RLS table, ALL of
these must hold** (we hit each one as a separate failure this took a while to
untangle — see `commit-log.md` 2026-05-28/29):

1. **Publication membership** — `alter publication supabase_realtime add table <t>;` (see `20260514120000_realtime_publication.sql`, `20260528180000_realtime_perception_snapshots.sql`).
2. **`REPLICA IDENTITY FULL`** — required for `UPDATE`/`DELETE` events to be delivered AND for a server-side `filter:` on a non-primary-key column to match on UPDATEs. With the default identity (PK only) the old tuple carries just the PK, so UPDATE events are dropped / unfilterable. Our snapshot/session tables are UPSERTed (one row per camera/session) → after the first INSERT *everything is an UPDATE*, so this is mandatory (`20260529120000_replica_identity_full_realtime.sql`).
3. **`GRANT SELECT ON <t> TO authenticated`** — hosted Supabase does **not** auto-grant table-level SELECT to `authenticated` for tables created via migration (the local CLI does, which masks it in dev). Without it Postgres rejects with `permission denied (42501)` **before RLS even runs**, so Realtime (evaluating as `authenticated`) delivers nothing — silently, with no 401. This is invisible to the API (service role), so polling/reads keep working. This was the actual root cause of the perception overlay never receiving events. See the header of `20260513150000_dashboard_grants.sql` and `20260529130000_grant_authenticated_realtime_tables.sql`.
4. **A member-select RLS policy** whose subquery the `authenticated` role can resolve — ours is `account_id in (select account_id from account_members where user_id = auth.uid())`. `account_members` is itself granted to `authenticated` and has `account_members_self_select` (`user_id = auth.uid()`), so the subquery resolves.
5. **Authed socket (client side)** — the browser must put the user JWT on the Realtime socket via `realtime.setAuth(token)` *before* `.subscribe()`; an expired token yields per-record `Error 401`. (Dashboard responsibility — see `upstream-dashboard/docs/dashboard-next.md`.)

> **Rule for new realtime/dashboard tables:** add `grant select on public.<table> to authenticated;` **and** `alter table <table> replica identity full;` (if UPDATE events matter) **in the same migration that creates the table**, and add it to the `supabase_realtime` publication. Otherwise it will read fine via the API and be silent over Realtime.

Symptom → cause cheat-sheet (full version in `troubleshooting.md`): SUBSCRIBED + `401` per record → token expired / not set on the socket; SUBSCRIBED + no 401 + no events → missing GRANT, or the RLS policy returns false; events arrive but empty (`camera_id` undefined / `keys=[]`) → payload stripped (auth-rejected record or oversized).
