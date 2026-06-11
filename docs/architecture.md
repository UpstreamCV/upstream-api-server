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

## Five identity classes

| Caller | Header scheme | Verified by | Used on |
|---|---|---|---|
| Agent | `Authorization: Bearer ups_<env>_<id>_<secret>` | argon2id against `api_tokens.hash`, prefix-indexed | `/v1/agents/*`, `/v1/cameras/*`, `/v1/stream-sessions/*`, `/v1/heartbeats`, `/v1/snapshots` |
| Dashboard user | `Authorization: Bearer <Supabase access token>` | HS256 against `UPSTREAM_SUPABASE_JWT_SECRET` + `account_members` lookup | dashboard reads, `/v1/api-tokens` |
| Streaming server | `X-Upstream-Webhook-Secret: <secret>` | constant-time compare against `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` | `/v1/ingest/*` (Phase 2) |
| Processor | `X-Upstream-Webhook-Secret: <secret>` | constant-time compare against `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` | `/v1/internal/processor/*` (M8.0) |
| Platform admin | `Authorization: Bearer <Supabase access token>` | same JWT verification as a dashboard user + `platform_admins` allowlist lookup | `/v1/admin/*` (admin panel) |

Each identity class has its own FastAPI dep in `security/deps.py`: `current_agent`, `current_user`, `current_ingest`, `current_processor`, `current_platform_admin`. Routers ask only for the dep they need; account scoping comes from the auth context, never from the request body.

The platform-admin dep differs from `current_user` deliberately: no membership lookup and no TTL cache (allowlist revocation is instant), and it never sets `account_id_var` — admin requests are cross-tenant, so target accounts are recorded in `admin_audit_log` instead of the logging context. A miss on the allowlist is a 403 `not_platform_admin` (vs 401 for a bad token) so the admin panel can distinguish "log in again" from "not an admin".

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

There are four kinds of token in the system:

1. **API token** (long-lived, opaque, agent-facing). Plaintext format `ups_<env>_<8 id>_<22 secret>`. The `prefix` (everything except the last segment) is stored in DB and is the unique lookup index; the last segment is argon2id-hashed in `hash`. Shown to the dashboard user exactly once on creation.
2. **Session token** (4h TTL, JWT, agent-facing). Minted by `POST /v1/stream-sessions/start` and returned at the top level of the response. Carries `acc`/`sub`/`cam`/`sid`/`jti`/`exp`/`lim` claims. *Not* placed in the SRT streamid — see "Stream key" below. Currently a back-compat carrier for non-ingest callers; the live ingest validation path no longer reads it.
3. **Stream key** (4h TTL, opaque, ingest-only). Short random token (`sess_<16 hex>`) minted per session and stored on `stream_sessions.streamid` as `publish:sess_<hex>`. This is what rides in the SRT streamid query parameter and what `/v1/ingest/validate-stream` looks the session up by.
4. **Supabase Auth JWT** (1h TTL, JWT, dashboard-user-facing). Issued by Supabase Auth; verified using `UPSTREAM_SUPABASE_JWT_SECRET`. Carries the user's `aal` (MFA assurance level) and `amr` (auth methods) claims, which the api-server exposes on `UserAuth` for future per-route MFA gating.

A fifth secret rides on each SRT publish but is not a "token" in the auth sense: every `/start` mints a one-shot AES-256 **SRT passphrase**, embeds it in the returned `srt_url` (`&passphrase=…&pbkeylen=32`), and PATCHes the matching MediaMTX path so the SRT handshake actually negotiates encryption. See "Per-session SRT passphrase" below.

## Phased delivery

| Phase | Scope | Status |
|---|---|---|
| 0 | FastAPI scaffold | done |
| 1 | Agent auth + session creation | done |
| 2 | MediaMTX ingest webhooks | done — plus follow-ups: per-session SRT passphrase + MediaMTX path provisioning (FU-6), WHEP read-path proxy (FU-3), `lookup-session` resolver, M4.1 path-segment auth gate, M5.1 report-only validation endpoints, M6.2-delta webhook-authed `/v1/ingest/snapshots` |
| 3 | Heartbeats + snapshots | done |
| 4 | Dashboard reads (Supabase JWT, RLS) | done — `GET /v1/agents`, `/v1/cameras`, `/v1/stream-sessions` accept either an agent token or a user JWT via `CurrentCaller` (security/deps.py) |
| 5 | Processor surface + usage rollup | processor surface done — M8.0 readiness gate, P4-B..E real observation types, P5-A recent observations read, P5-C latest perception snapshots, P6-A geometry config, P6-D latest processor status read. Usage rollup partial — `services/usage.py` writes `usage_events` rows on every billable transition but there is no read/aggregate endpoint yet |
| 6 | Observability + rate limiting | partial — structlog + `RequestIdMiddleware` + `AccessLogMiddleware` in. No `/metrics` endpoint yet (`UPSTREAM_PROMETHEUS_AUTH_TOKEN` is reserved). No slowapi rate limiting |
| 7 | Hardening (token rotation, revocation list, mTLS) | not started. Per-session SRT passphrase shipped under Phase 2 / FU-6, not here — it hardens the publish hop, not the API surface |

Two feature tracks have since landed on top of the Phase 5 surface (milestone-numbered independently of the phases above):

- **Reliability** (M0–M7) — persisted `reliability_findings` + the evaluate-on-read rule set behind `GET /v1/cameras/{id}/reliability/summary` / `findings` (five rules: processor-silent, frame-stale, default-geometry, scene-too-dark, unstable-detection-confidence), the M5 `snapshot_detection_metrics` confidence history + windowed drift, the M6 hourly rollups + retention (`run_detection_metrics_rollup`), and the M7 `confidence-trend` read API.
- **Model config** (M2–M4, M7B, M9/M9.1, M11) — the static `GET /v1/models` registry, the account-level `processing_enabled` master switch (`GET`/`PATCH /v1/account/processing-config`, enforced in the eligible-sessions SQL), per-camera `GET`/`PUT /v1/cameras/{id}/processing-config` (registry-validated), stamp-only `processing_config` carriage on eligible-sessions (M7B), applied-config dimensions on the rollup tables + the `confidence-trend/by-config` read (M9, FPS dimensions in M9.1), and the M11 `config-comparison-summary` report card.

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

The diagram above shows the original Phase 2 lifecycle. Five additional ingest routes have landed since and share the same `current_ingest` webhook-secret auth:

- `POST /v1/ingest/mediamtx-auth/{secret}` — MediaMTX 1.9.3 `authMethod: http` deny gate (M4.1). Path-segment auth because 1.9.3 has no `authHTTPHeaders`.
- `POST /v1/ingest/lookup-session` — streamid → session ids resolver for the `runOnReady` / `runOnNotReady` hooks. State-agnostic, unlike `validate-stream`.
- `GET /v1/ingest/expected/{session_id}` — state-agnostic fetch of the expected media-config block for the streaming-server's comparator (M5.1).
- `POST /v1/ingest/stream-validations` — report-only validation row insert; does NOT mutate session state (M5.1).
- `POST /v1/ingest/snapshots` — webhook-authed snapshot upload from the streaming-server (M6.2-delta). Only accepted when the session is `streaming`; tenant fields are always derived from the session row.

### Per-session SRT passphrase + MediaMTX path provisioning (FU-6)

`POST /v1/stream-sessions/start` mints an AES-256 passphrase (~32 url-safe chars) on every session, stores it on `stream_sessions.srt_publish_passphrase`, embeds it in the returned `srt_url` as `&passphrase=<X>&pbkeylen=32`, and PATCHes a matching MediaMTX path via `UPSTREAM_MEDIAMTX_API_BASE_URL` so the SRT handshake actually negotiates encryption. `POST /v1/stream-sessions/{id}/stop` and the `stream-ended` / `validation-failed` ingest hooks DELETE the path (best-effort, 404-tolerant). A leaked passphrase only exposes that one session.

Known gap: MediaMTX's path config is in-memory, so a streaming-server restart drops every dynamic path and in-flight sessions break until the agent re-`/start`s. A periodic reconciler is deferred (FU-7).

## Read-path auth — WHEP proxy

`POST /v1/stream-sessions/{id}/whep` and `DELETE /v1/stream-sessions/{id}/whep/{resource_id}` proxy WebRTC signaling between an authenticated dashboard user and MediaMTX. The api-server authenticates via `CurrentCaller` (agent token OR Supabase JWT, account-scoped) so MediaMTX's permissive `authInternalUsers` posture never reaches the public internet. UDP/ICE media still flows browser ↔ MediaMTX directly; only the HTTP signaling makes the api-server hop. Cited via `UPSTREAM_MEDIAMTX_BASE_URL`.

## Phase 3 — Heartbeats + snapshots

- **Heartbeats**: agent posts batches of up to 100 every few seconds. Each item carries `status`, `fps`, `bitrate_kbps`, `width`, `height`, `codec`, `last_error`, `agent_version`, `reported_at`. The handler updates `agents.last_seen_at` once per batch — not per heartbeat — to avoid hot-row contention on chatty fleets.
- **Snapshots**: agent (or, optionally, the streaming server in a later phase) uploads a JPEG as multipart. Bytes go to Supabase Storage at `{account_id}/{camera_id}/{yyyymmdd}/{uuid}.jpg`; metadata + `storage_key` land in `snapshots`. Quota is `account_limits.snapshot_storage_mb` summed across the trailing 30 days. Max upload is `UPSTREAM_SNAPSHOT_MAX_BYTES` (default 5 MB). **Caddy must allow `>= snapshot_max_bytes` on `/v1/snapshots` specifically** — the global 1 MB body cap stays in place everywhere else.

Snapshot upload has two variants. `POST /v1/snapshots` is agent-token authed and is what the agent SDK uses. `POST /v1/ingest/snapshots` (M6.2-delta) is webhook-secret authed and is for the streaming server to forward locally-extracted JPEGs; it only accepts sessions in `streaming` state and derives `account_id` / `agent_id` / `camera_id` from the session row, never the request body. Both share the same Storage / quota / `snapshots`-insert pipeline (`_read_validated_jpeg` / `_enforce_snapshot_quota` / `_persist_snapshot` in `routers/snapshots.py`).

## Phase 5 — Processor surface

Documented in detail in `api-contracts.md` under the M8.0 section; the high-level shape: the api-server is the source of truth for "which sessions are processable" via `GET /v1/internal/processor/eligible-sessions` (the eligibility WHERE clause is the single source of truth), accepts `POST /v1/internal/processor/status` upserts and `POST /v1/internal/processor/observations` appends, plus the P5-A / P5-C / P6-A reads. All processor routes are gated by a **separate** `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` from the ingest webhook secret. `account_id` / `camera_id` are always derived from the session row, never trusted from the request. No `srt_url` / token / passphrase is ever returned to the processor — only the internal RTSP read URL built from `UPSTREAM_MEDIAMTX_RTSP_BASE`.

## Realtime publication

Migration `20260514120000_realtime_publication.sql` adds `stream_sessions` and `cameras` to the `supabase_realtime` publication. The dashboard subscribes via the browser's Supabase client (filtered by the same RLS policies the api-server honors) instead of polling, cutting agent-connect → live-indicator latency from up to 5 s down to ~50-200 ms. The api-server is the only writer to these tables, so the publication is safe.

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

## Perception-fanout hot path (10–15 Hz detection overlays)

The Realtime path above tops out at the processor's DB-write cadence
(~1 Hz) because every snapshot rides Postgres write → WAL decode →
Realtime fan-out. For the live bounding-box overlay, a separate
**ephemeral hot path** exists: the `perception-fanout` WS sidecar in
`upstream-streaming-server` relays processor snapshots to browsers at
10–15 Hz with no DB involvement (contract:
`upstream-streaming-server/docs/perception-fanout.md`).

This api-server's role is **auth only**:
`POST /v1/cameras/{camera_id}/perception/stream-token` runs the normal
Supabase JWT + camera-ownership checks, then mints a short-lived `upt1`
HMAC token (`security/fanout_token.py`) signed with
`UPSTREAM_FANOUT_TOKEN_SECRET` (shared with the sidecar) and returns it
with `ws_url` built from `UPSTREAM_FANOUT_PUBLIC_WS_URL`. Both settings
optional — unset means 503 `perception_stream_not_configured` and the
dashboard silently stays on the Realtime + polling path above, which
remains the durable record and the universal fallback. The processor's
`POST /v1/internal/processor/perception-snapshots` cadence is unchanged.
