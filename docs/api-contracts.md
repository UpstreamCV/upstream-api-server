# API contracts

Stable contract between the API server and its callers. Paths are all under `/v1/` unless noted. Phase numbers refer to the delivery plan in `architecture.md`.

## Auth headers

| Caller | Header | Verified against |
|---|---|---|
| Agent | `Authorization: Bearer ups_<env>_<id>_<secret>` | `api_tokens` row, argon2id on the secret segment |
| Dashboard user | `Authorization: Bearer <Supabase access token>` | `UPSTREAM_SUPABASE_JWT_SECRET` (HS256, `aud=authenticated`) |
| Streaming server | `X-Upstream-Webhook-Secret: …` | `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` |
| Processor | `X-Upstream-Webhook-Secret: …` | `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` |
| Platform admin | `Authorization: Bearer <Supabase access token>` | same JWT verification as a dashboard user + `platform_admins` allowlist row |

## Error envelope

Every non-2xx response uses this shape:

```json
{
  "error": {
    "code": "limit_exceeded.max_cameras",
    "message": "Camera slot limit reached: 3 of 3.",
    "details": { "in_use": 3, "max": 3 },
    "request_id": "abc123…"
  }
}
```

`code` strings are stable. We may add new codes but never reuse one for a different meaning. The agent SDK should switch on `code`, not on `message`.

### Codes currently emitted

| HTTP | code | Where |
|---|---|---|
| 401 | `missing_credentials` | any agent route without Authorization |
| 401 | `invalid_auth_scheme` | non-Bearer auth |
| 401 | `invalid_token` | unknown prefix or argon2 mismatch |
| 401 | `token_revoked` | `revoked_at` set |
| 401 | `token_expired` | `expires_at` passed |
| 401 | `invalid_user_token` | Supabase JWT decode failed |
| 401 | `invalid_webhook_secret` | wrong `X-Upstream-Webhook-Secret` |
| 403 | `forbidden.role` | dashboard user lacks `owner` or `admin` role |
| 403 | `invalid_scopes` | requested scopes not in `ALLOWED_SCOPES` |
| 403 | `limit_exceeded.max_cameras` | camera registration past `max_cameras` |
| 403 | `limit_exceeded.max_concurrent_sessions` | session start past `max_concurrent_sessions` |
| 403 | `limit_exceeded.fps_too_high` | requested fps > `max_fps` |
| 403 | `limit_exceeded.resolution_too_high` | requested w×h > `max_width`×`max_height` |
| 403 | `limit_exceeded.bitrate_too_high` | requested bitrate > `max_bitrate_kbps` |
| 403 | `limit_exceeded.codec_not_allowed` | requested codec not in `allowed_codecs` |
| 404 | `not_found` (or specific: `camera_not_found`, `agent_not_found`, `session_not_found`, `token_not_found`) | resource missing or not owned by caller |
| 404 | `account_limits_missing` | `account_limits` row not present for the account |
| 409 | `camera_already_streaming` | another active session exists for the camera |
| 409 | `agent_account_mismatch` | agent_id given doesn't belong to the calling account |
| 409 | `stream_state_invalid` | ingest webhook fired on a session in the wrong state (e.g. `stream-started` on an already-ended session; or `POST /v1/ingest/snapshots` for a session not in `streaming`) |
| 403 | `forbidden_account` | dashboard user is not a member of the `account_id` they queried |
| 403 | `account_id_required` | dashboard user has multiple memberships but did not pass `?account_id=` |
| 403 | `forbidden_resource` | heartbeat references a `camera_id` or `session_id` not owned by the agent |
| 400 | `empty_upload` | snapshot upload had a zero-byte body |
| 400 | `invalid_resource_id` | WHEP `DELETE` path-traversal guard tripped (resource id outside `[A-Za-z0-9_~-]{1,128}`) |
| 400 | `invalid_sdp` | WHEP `POST` body was empty / not `application/sdp` |
| 400 | `streamid_malformed` | ingest webhook received a streamid that doesn't decode (also used as a `reason` code on `validate-stream`) |
| 404 | `account_not_found` | account row missing for the looked-up id |
| 404 | `perception_snapshot_not_found` | `GET /v1/cameras/{id}/perception/latest` and no snapshot has been written yet |
| 404 | `processor_status_not_found` | `GET /v1/cameras/{id}/processor-status/latest` and no processor has ever reported for this camera (or nothing matches the supplied `stream_session_id`) |
| 404 | `snapshot_not_found` | `GET /v1/cameras/{id}/latest-frame` and no snapshot row exists |
| 404 | `stream_not_ready` | WHEP `POST` while MediaMTX hasn't registered the path yet (client should retry) |
| 409 | `session_expired` | ingest webhook fired on a session whose `expires_at` has passed |
| 409 | `session_not_streaming` | WHEP `POST` while session is `approved` / `ended` / `expired` / `failed` |
| 413 | `snapshot_too_large` | snapshot upload exceeds `UPSTREAM_SNAPSHOT_MAX_BYTES` |
| 415 | `invalid_content_type` | snapshot upload was not `image/jpeg`, or WHEP body wasn't `application/sdp` |
| 429 | `snapshot_quota_exceeded` | trailing-30-day snapshot bytes would exceed `account_limits.snapshot_storage_mb` |
| 422 | `validation_error` | pydantic request validation failed |
| 500 | `internal_error` | unhandled exception |
| 502 | `mediamtx_provision_failed` | `/start` couldn't PATCH the MediaMTX path for the new session; row is left `failed` with `failure_code='mediamtx_provision_failed'` |
| 502 | `upstream_unavailable` | WHEP proxy timed out, got a 5xx, or got an unparseable response from MediaMTX |

## Endpoints — Phase 1 (implemented)

### `GET /`

No auth. Service-info landing page; returns a small JSON object identifying the service. Safe to leave on in production — useful for ops smoke tests.

### `GET /healthz`

No auth. Liveness only; doesn't touch the DB.

```json
{ "status": "ok" }
```

### `GET /readyz`

No auth. Pings the DB with a 500ms timeout.

200 → `{ "status": "ok" }`. 503 → `{ "status": "not_ready", "error": "…" }`.

### `GET /metrics`

Prometheus scrape endpoint (M9.3). Auth: `Authorization: Bearer <UPSTREAM_PROMETHEUS_AUTH_TOKEN>`, constant-time compare. With the token env **unset** the route returns 404 (it does not exist for that deployment); wrong/missing token or non-Bearer scheme → 401. 200 returns the `text/plain` exposition of the api-server registry: `ingest_webhook_total{endpoint,outcome}`, `mediamtx_auth_total{decision}`, `session_transitions_total{from_state,to_state}`, `session_start_to_streaming_seconds` (histogram), `db_pool_size` / `db_pool_in_use` (refreshed per scrape). In production the route is additionally firewalled to the monitoring box's IP.

### `POST /v1/agents/register`

Auth: `current_agent`. Upserts on `(account_id, name)`. `last_seen_at` is set to `now()`.

Request:
```json
{
  "name": "smoke-agent",
  "hostname": "macbook.local",
  "version": "0.1.0",
  "os": "darwin",
  "python_version": "3.11.5",
  "capabilities": {}
}
```

Response 200:
```json
{
  "agent": {
    "id": "uuid",
    "account_id": "uuid",
    "name": "smoke-agent",
    "hostname": "macbook.local",
    "version": "0.1.0",
    "os": "darwin",
    "python_version": "3.11.5",
    "capabilities": {},
    "status": "active",
    "last_seen_at": "2026-05-12T20:00:00Z",
    "created_at": "…",
    "updated_at": "…"
  },
  "server_time": "2026-05-12T20:00:00Z"
}
```

### `POST /v1/cameras/register`

Auth: `current_agent`. Upserts on `(agent_id, external_id)` if `external_id` is provided; otherwise creates a new row. Validates defaults against `account_limits` before insert/update. Path that creates a new row also enforces `max_cameras`.

Request:
```json
{
  "agent_id": "uuid",
  "name": "smoke-cam",
  "external_id": "smoke-1",
  "device_path": "/dev/video0",
  "default_width": 1280,
  "default_height": 720,
  "default_fps": 30,
  "default_codec": "h264",
  "default_bitrate_kbps": 3000,
  "metadata": {}
}
```

Response 200: full camera row matching `schemas/cameras.py:CameraResponse`.

Errors: `agent_not_found`, `agent_account_mismatch`, `limit_exceeded.{codec_not_allowed,fps_too_high,resolution_too_high,bitrate_too_high,max_cameras}`.

### `POST /v1/stream-sessions/start`

Auth: `current_agent`. Validates limits, mints a session JWT, inserts a `stream_sessions` row in state `approved`, returns the SRT URL with the JWT embedded.

Request:
```json
{
  "camera_id": "uuid",
  "requested": {
    "width": 1280,
    "height": 720,
    "fps": 30,
    "codec": "h264",
    "bitrate_kbps": 3000
  }
}
```

Response 200:
```json
{
  "session": {
    "id": "uuid",
    "account_id": "uuid",
    "agent_id": "uuid",
    "camera_id": "uuid",
    "state": "approved",
    "srt_url": "srt://ingest.upstreamcv.com:18890?streamid=publish:sess_<hex>&latency=200&passphrase=<urlencoded>&pbkeylen=32",
    "streamid": "publish:sess_<hex>",
    "expires_at": "2026-05-13T00:00:00Z",
    "started_at": null,
    "ended_at": null,
    "requested": { "width": 1280, "height": 720, "fps": 30, "codec": "h264", "bitrate_kbps": 3000 },
    "approved":  { "width": 1280, "height": 720, "fps": 30, "codec": "h264", "bitrate_kbps": 3000 }
  },
  "srt_url": "srt://ingest.upstreamcv.com:18890?streamid=publish:sess_<hex>&latency=200&passphrase=<urlencoded>&pbkeylen=32",
  "session_token": "<jwt>",
  "expires_at": "2026-05-13T00:00:00Z"
}
```

Errors: `camera_not_found`, `camera_already_streaming`, `limit_exceeded.*`, `upstream_unavailable` (when MediaMTX path provisioning fails — the session row is left in `failed` state with `failure_code='mediamtx_provision_failed'`; agent should retry).

Phase 2 / FU-6: every issued `srt_url` includes a unique
`&passphrase=<X>&pbkeylen=32` (~32 url-safe chars, AES-256). The api-server
PATCHes the corresponding MediaMTX path with the same passphrase before
returning. SRT publishes that drop the passphrase parameter will fail at
the SRT handshake. The api-server DELETEs the MediaMTX path at session
end (`/stop`, `/stream-ended`, `/validation-failed` — best-effort, with
404 tolerance for MediaMTX restarts in between).

### `POST /v1/stream-sessions/{session_id}/stop`

Auth: `current_agent`. Idempotent. Transitions `approved|streaming` → `ended` with `ended_at = now()`. No-op on terminal states.

Response 200: the updated session row (same shape as the `session` object above).

Errors: `session_not_found`.

### `GET /v1/stream-sessions/{session_id}`

Auth: `current_agent`. Owner-only.

Response 200: session row.

Errors: `session_not_found`.

### `POST /v1/api-tokens`

Auth: `current_user`. Caller must have role `owner` or `admin` for `body.account_id`.

Request:
```json
{
  "account_id": "uuid",
  "name": "production agent",
  "scopes": ["agent"],
  "expires_at": null
}
```

`scopes` must be a subset of `ALLOWED_SCOPES` (currently `{"agent"}`).

Response 201:
```json
{
  "token": {
    "id": "uuid",
    "account_id": "uuid",
    "name": "production agent",
    "prefix": "ups_live_AB12CDEF",
    "last_four": "WXYZ",
    "scopes": ["agent"],
    "last_used_at": null,
    "expires_at": null,
    "revoked_at": null,
    "created_at": "…"
  },
  "plaintext": "ups_live_AB12CDEF_FULLSECRET22CHARSWXYZ"
}
```

**`plaintext` is shown exactly once.** Never returned by any other endpoint.

Errors: `forbidden.role`, `invalid_scopes`.

### `DELETE /v1/api-tokens/{token_id}`

Auth: `current_user`. Caller must have role `owner` or `admin` for the token's account. Soft-delete: sets `revoked_at = now()`. Idempotent.

Response 204.

Errors: `token_not_found`, `forbidden.role`.

## Session JWT claims

The session JWT returned at `session_token` on the response (not embedded in the SRT streamid — see below):

```json
{
  "iss": "upstream-api",
  "aud": "srt-ingest",
  "sub": "<agent_id>",
  "acc": "<account_id>",
  "sid": "<session_id>",
  "cam": "<camera_id>",
  "jti": "<unique JWT id>",
  "iat": 1778617300,
  "exp": 1778631700,
  "lim": { "w": 1280, "h": 720, "fps": 30, "codec": "h264", "kbps": 3000 }
}
```

Signed HS256 with `UPSTREAM_SESSION_JWT_SECRET`. TTL is `UPSTREAM_SESSION_TOKEN_TTL_SECONDS` (default 14400 = 4h). The ingest validation path no longer reads this token; it remains for non-ingest callers that already consume the field.

## SRT streamid format

Ingest connections set `streamid=publish:sess_<16 hex>` — the literal prefix `publish:` followed by a short opaque random key (~21 chars total). The full streamid is unique-indexed on `stream_sessions.streamid` and is what `/v1/ingest/validate-stream` looks up. The `publish:` prefix is reserved for ingest; subscribe-style connections will use a different prefix in Phase 4/5.

Why opaque, not the JWT: libsrt's `streamid` query parameter is capped at ~512 bytes, and a signed JWT carrying the `lim` claim object exceeds that — FFmpeg surfaces this as "Query string option 'streamid' value is too long". A short DB-backed key sidesteps the limit and removes a round of JWT verification from the ingest hot path; the API row is the source of truth either way.

### Consumed by the api-server WHEP proxy (internal)

The api-server itself derives the MediaMTX path from `streamid` for its read-path proxy endpoints (see "WHEP proxy" below). The convention: a session with `streamid=publish:sess_abc123` plays back as MediaMTX path `sess_abc123`. The dashboard no longer derives this client-side — it just calls the proxy endpoint by session id.

## WHEP proxy (read-path playback for the dashboard)

The api-server forwards WebRTC signaling between authenticated dashboard users and MediaMTX. Only the HTTP signaling is proxied; the actual UDP/ICE media flows browser ↔ MediaMTX directly via ICE candidates carried in the SDP answer.

This exists because MediaMTX's read path has no native auth — proxying through the api-server lets us reuse the existing `CurrentCaller` (Supabase JWT or agent token) + `account_id` scoping that already gates every other read endpoint. Production deploys MUST firewall MediaMTX's `8889/tcp` / `8554/tcp` / `9997/tcp` to the api-server's egress IP only — those ports are not behind any auth.

### `POST /v1/stream-sessions/{session_id}/whep`

Authenticated via `CurrentCaller`. Multi-account users may need `?account_id=...` like other read endpoints.

Request:
- Header: `Content-Type: application/sdp` (other types → 415)
- Body: SDP offer bytes (browser-generated). Empty bodies → 400.

Response:
- 201 + `Content-Type: application/sdp` + `Location: <resource_id>` + body=MediaMTX's SDP answer. The Location header is a **bare resource id**, not a URL — the dashboard appends it to the POST URL to form the DELETE target.
- 404 `session_not_found` — session doesn't exist or belongs to a different account (does not leak existence)
- 404 `stream_not_ready` — MediaMTX hasn't registered the path yet (race; client should retry)
- 409 `session_not_streaming` — session is in `approved` / `ended` / `expired` / `failed` state
- 415 `invalid_content_type` — wrong Content-Type
- 400 `invalid_sdp` — empty body
- 502 `upstream_unavailable` — MediaMTX timeout, 5xx, or unparseable response

### `DELETE /v1/stream-sessions/{session_id}/whep/{resource_id}`

Authenticated via `CurrentCaller`. The `resource_id` is the value the client received from the POST's Location header.

- 204 No Content on success or any MediaMTX outcome (DELETE is best-effort — MediaMTX cleans up orphans via ICE timeout anyway)
- 400 `invalid_resource_id` — `resource_id` contains characters outside `[A-Za-z0-9_~-]` (path-traversal defense)
- 404 `session_not_found` — same as POST

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `UPSTREAM_MEDIAMTX_BASE_URL` | `http://localhost:8889` | Base URL for MediaMTX's WHEP endpoint. Reachable only from the api-server in production. |

## Environment variables (consumed by this service)

| Variable | Required | Purpose |
|---|---|---|
| `UPSTREAM_DATABASE_URL` | yes | Direct Postgres connection (use port 5432 for long-running, 6543 for pooled) |
| `UPSTREAM_SUPABASE_URL` | yes | Supabase project URL (used when supabase-py client is wired up) |
| `UPSTREAM_SUPABASE_ANON_KEY` | yes | Supabase anon key (currently unused in hot path) |
| `UPSTREAM_SUPABASE_SERVICE_ROLE_KEY` | yes | Supabase service role key (for future storage uploads) |
| `UPSTREAM_SUPABASE_JWT_SECRET` | yes | Verifies dashboard user JWTs |
| `UPSTREAM_SESSION_JWT_SECRET` | yes | Signs session JWTs returned at `session_token` (independent of Supabase) |
| `UPSTREAM_STREAMID_HMAC_KEY` | yes | Reserved for a future signed streamid form (currently unused; ingest uses an opaque DB-backed key) |
| `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` | yes | Verifies `/v1/ingest/*` callers |
| `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` | yes | Verifies `/v1/internal/processor/*` callers (M8.0) |
| `UPSTREAM_MEDIAMTX_RTSP_BASE` | no | Internal MediaMTX RTSP read base advertised to a processor (default `rtsp://host.docker.internal:8554`). The read surface, **not** the SRT publish URL |
| `UPSTREAM_MEDIAMTX_BASE_URL` | no | MediaMTX WHEP base for the dashboard read-path proxy (default `http://localhost:8889`). Must be reachable only from the api-server in production |
| `UPSTREAM_MEDIAMTX_API_BASE_URL` | no | MediaMTX HTTP control-API base used to provision/deprovision per-session paths and passphrases (default `http://localhost:9997`). Reachable only from the api-server in production |
| `UPSTREAM_PROMETHEUS_AUTH_TOKEN` | no | Bearer token for the `GET /metrics` auth gate (M9.3). Unset → the endpoint 404s; keep it set in every deployed env |
| `UPSTREAM_SRT_INGEST_HOST` | no | SRT ingest host advertised in `srt_url` (default `ingest.upstreamcv.com`) |
| `UPSTREAM_SRT_INGEST_PORT` | no | SRT ingest port (default `18890`) |
| `UPSTREAM_PORT` | no | HTTP port (default 4000) |
| `UPSTREAM_ENVIRONMENT` | no | `development` \| `staging` \| `production` (default `development`) |
| `UPSTREAM_LOG_LEVEL` | no | `DEBUG`/`INFO`/`WARNING`/`ERROR` (default `INFO`) |
| `UPSTREAM_SESSION_TOKEN_TTL_SECONDS` | no | Default 14400 |
| `UPSTREAM_DB_POOL_MIN_SIZE` | no | Default 5 |
| `UPSTREAM_DB_POOL_MAX_SIZE` | no | Default 20 |
| `UPSTREAM_ALLOWED_ORIGINS` | no | CSV list of CORS origins (default `http://localhost:3000`) |
| `UPSTREAM_SUPABASE_STORAGE_BUCKET` | no | Supabase Storage bucket for snapshot uploads (default `snapshots`) |
| `UPSTREAM_SNAPSHOT_MAX_BYTES` | no | Per-request snapshot size limit (default 5 MB) |
| `UPSTREAM_SENTRY_DSN` | no | Sentry exception reporting (M9.3: initialized at app startup when set, `send_default_pii=False`; unset is a no-op) |

## Endpoints — Phase 2: ingest webhooks (auth: `current_ingest`, except mediamtx-auth)

Streaming server -> API. Most require `X-Upstream-Webhook-Secret`. The one exception is `/v1/ingest/mediamtx-auth/{secret}` — MediaMTX 1.9.3 cannot send custom headers, so that endpoint takes the secret as a URL path segment (see its section below).

### `POST /v1/ingest/validate-stream`

Preflight: should this incoming SRT connection be accepted?

Request:
```json
{ "streamid": "publish:sess_<hex>", "remote_addr": "203.0.113.42" }
```

Response 200 (accepted):
```json
{
  "accept": true,
  "session_id": "uuid",
  "expected": {
    "codec": "h264",
    "width": 1280, "height": 720, "fps": 30,
    "bitrate_kbps": 3000, "latency_mode": "balanced"
  }
}
```

Response 200 (rejected — always 200 with `accept: false`, never 4xx, because the streaming server treats this as a decision, not an error):
```json
{
  "accept": false,
  "session_id": "uuid",
  "reason": "Session is in state 'ended', expected 'approved'.",
  "reason_code": "stream_state_invalid"
}
```

Reason codes: `streamid_malformed`, `session_not_found`, `stream_state_invalid`, `session_expired`.

### `POST /v1/ingest/stream-started`

Streaming server has accepted the publish and ffprobe matched the expected params. Transition `approved → streaming`.

Request:
```json
{
  "session_id": "uuid",
  "observed": { "codec": "h264", "width": 1280, "height": 720, "fps": 29.97, "bitrate_kbps": 2980 },
  "started_at": "2026-05-12T20:00:01Z"
}
```

Response 200: `{ "session_id": "...", "state": "streaming" }`.

Errors: `session_not_found`, `stream_state_invalid`.

### `POST /v1/ingest/stream-ended`

SRT connection closed. Transition `approved|streaming → ended`. Idempotent — returns the current state if already terminal. Mirrors `cameras.status` to `'idle'` in the same transaction.

Request:
```json
{
  "session_id": "uuid",
  "ended_at": "2026-05-12T20:05:30Z",
  "duration_seconds": 329.2,
  "bytes_sent": 123456789,
  "reason": "client_disconnect"
}
```

`duration_seconds` is optional — the API derives the authoritative value from the session row's `started_at` and the supplied `ended_at`. The streaming-server hooks omit it; only legacy callers that already track started_at should send it.

Response 200: `{ "session_id": "...", "state": "ended" }`.

A `usage_events` row of type `stream_seconds` is written using the server-derived duration (falls back to the request body field, then `0`, in that order).

### `POST /v1/ingest/validation-failed`

ffprobe (or a later validator) rejected the stream. Transition `approved|streaming → failed` and write a failing `stream_validations` row. Mirrors `cameras.status` to `'error'` in the same transaction.

Request:
```json
{
  "session_id": "uuid",
  "observed": { "codec": "h265", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 3000 },
  "mismatch_fields": ["codec"],
  "failure_code": "codec_mismatch",
  "failure_message": "expected h264, observed h265"
}
```

Response 200: `{ "session_id": "...", "state": "failed" }`.

### `GET /v1/ingest/expected/{session_id}` (M5.1)

Fetch the expected media-config block for a session. Same shape as the `expected` object `validate-stream` returns when it accepts, plus `account_id` / `agent_id` / `camera_id` / `state` / `expires_at`. State-agnostic — unlike `validate-stream`, this does NOT gate on `state='approved'`, so the streaming server's M5.1 comparator can keep running after a reconnect into an already-`streaming` session.

Response 200:
```json
{
  "session_id": "uuid",
  "account_id": "uuid",
  "agent_id": "uuid",
  "camera_id": "uuid",
  "state": "streaming",
  "expires_at": "2026-05-13T00:00:00Z",
  "expected": {
    "codec": "h264",
    "width": 1280, "height": 720, "fps": 30,
    "bitrate_kbps": 3000, "latency_mode": "balanced"
  }
}
```

Errors: 404 `session_not_found`; 401 on missing/wrong webhook secret.

### `POST /v1/ingest/stream-validations` (M5.1)

Report-only validation row insert. Accepts both `passed=true` and `passed=false`; in **both** cases the API ONLY inserts a `stream_validations` row — it does NOT call `mark_streaming` / `mark_failed`, does NOT update `stream_sessions`, does NOT touch `cameras.status`, does NOT deprovision the MediaMTX path. Use this when the streaming server wants the dashboard to surface a validation outcome without changing session state. For mismatches that should drop the connection, use `validation-failed` instead.

Request:
```json
{
  "session_id": "uuid",
  "passed": false,
  "observed": { "codec": "h265", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 3000 },
  "expected": { "codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 3000 },
  "mismatch_fields": ["codec"]
}
```

Response 200: `{ "validation_id": "uuid" }`. Errors: 404 `session_not_found`; 401 on missing/wrong webhook secret.

### `POST /v1/ingest/lookup-session`

Resolve a streamid to its session identifiers without coupling to `validate-stream`'s state-gate semantics. Used by the streaming-server's `runOnReady` / `runOnNotReady` MediaMTX hooks to translate `MTX_PATH=sess_<hex>` into the `session_id` they pass to `stream-started` / `stream-ended`.

Read-only. Idempotent. Works regardless of session state — including during `streaming` (which is what `runOnNotReady` will see).

Request:
```json
{ "streamid": "publish:sess_<hex>" }
```

Response 200:
```json
{
  "session_id": "uuid",
  "account_id": "uuid",
  "agent_id": "uuid",
  "camera_id": "uuid",
  "state": "streaming"
}
```

Errors: 404 `session_not_found` if no session row matches the streamid.

### `POST /v1/ingest/mediamtx-auth/{secret}` (M4.1 — MediaMTX authHTTP deny gate)

Called by MediaMTX 1.9.3's `authMethod: http` for every SRT publish attempt. Returns **200 with empty body** to accept the publish and **401 with empty body** to deny — MediaMTX inspects only the status code, so no JSON envelope is sent.

**Path-segment auth.** The shared webhook secret is in the URL path because MediaMTX 1.9.3 has no `authHTTPHeaders` support (the field was added upstream in 1.11+). The handler does NOT require `X-Upstream-Webhook-Secret` and the `current_ingest` dependency is intentionally not used. Constant-time compared via `verify_shared_secret`.

**Deploy note (important):** The URL contains the secret. Any reverse-proxy / load-balancer in front of the api-server MUST be configured so its access logs strip the trailing path segment. The api-server's own `AccessLogMiddleware` already redacts the path to `/v1/ingest/mediamtx-auth/<redacted>` before logging (see `tests/test_access_log_redaction.py`). When MediaMTX is bumped to 1.11+, migrate the secret out of the URL path into a header and remove this endpoint's path segment auth.

Request body (verbatim from MediaMTX 1.9.3 — see `mediamtx.yml.default` lines 87-96):
```json
{
  "user": "",
  "password": "",
  "ip": "203.0.113.42",
  "action": "publish",
  "path": "sess_2a88f3405d513c05",
  "protocol": "srt",
  "id": "<connection-uuid>",
  "query": ""
}
```

Decision pipeline (fail-closed — any failure returns 401):

1. Path segment ≠ `Settings.mediamtx_webhook_secret` → 401.
2. `action != "publish"` → 401 (M4.1 gates publish only; read/playback are excluded by the streaming-server's `authHTTPExclude` but the handler is defensive).
3. `protocol != "srt"` → 401.
4. Stream key extraction: primary `body.path` (MediaMTX strips the `publish:` prefix before the auth call so `path` is already `sess_<hex>`); fallback `body.query` parsed for either `streamid=publish:<key>` or bare `publish:<key>`. Shape-validated via `is_valid_stream_key()` (`^sess_[0-9a-f]{16}\Z`).
5. `check_session_admit()` (shared with `validate-stream`): `session_not_found` / `stream_state_invalid` / `session_expired` → 401.
6. Any uncaught exception (DB error, etc.) → 401, logged as `reason_code=internal_error`.

Logging (structured via structlog at `ingest.mediamtx_auth`):

- **Logged:** `action`, `protocol`, redacted `stream_key` (first 12 chars + `…`), `reason_code`, `session_id` / `agent_id` / `camera_id` / `account_id` when resolved.
- **Never logged:** the presented secret value, the full URL, the full stream key, the `password` body field, the `query` body value.

Scope: publish only. Reader / playback / api / metrics / pprof actions are excluded in the streaming-server's `authHTTPExclude` config and should not reach this endpoint; if they do, they 401 defensively.

### Side-effect: `cameras.status` mirroring

Every successful state transition through these webhooks (and through the agent-driven `/v1/stream-sessions/start` and `/stop` paths via `mark_streaming` / `mark_ended` / `mark_failed`) also updates the parent camera row:

| Session transition | `cameras.status` after |
|---|---|
| → `streaming` | `'streaming'` |
| → `ended` | `'idle'` |
| → `failed` | `'error'` |

The cameras list view at `/dashboard/cameras` reads `cameras.status` directly via `<StatusBadge>`. The mirror runs inside the same transaction as the session UPDATE so the two columns can never diverge.

## Endpoints — Phase 3: telemetry (auth: `current_agent`)

### `POST /v1/heartbeats?agent_id=<uuid>`

Batched, max 100 per call. The query param identifies the agent within the calling token's account.

Request:
```json
{
  "heartbeats": [
    {
      "camera_id": "uuid",
      "session_id": "uuid",
      "status": "streaming",
      "fps": 29.97,
      "bitrate_kbps": 2950,
      "width": 1280, "height": 720, "codec": "h264",
      "agent_version": "0.1.4",
      "reported_at": "2026-05-12T20:00:30Z"
    }
  ]
}
```

`status` must be one of `online | streaming | error | reconnecting`. `camera_id` / `session_id` are optional but if provided must belong to this agent.

Response 200: `{ "accepted": 1, "server_time": "..." }`.

Errors: `forbidden_resource` (camera/session not owned by this agent).

### `POST /v1/snapshots`

Multipart form. Bytes are uploaded to Supabase Storage; the row references the storage key.

Form fields:
- `camera_id`: UUID
- `agent_id`: UUID (must own the camera under the calling account)
- `session_id`: UUID, optional
- `captured_at`: ISO datetime
- `width`, `height`: int, optional
- `file`: JPEG, `Content-Type: image/jpeg`, ≤ `UPSTREAM_SNAPSHOT_MAX_BYTES` (default 5 MB)

Response 201: `SnapshotUploadResponse`:
```json
{
  "id": "uuid",
  "camera_id": "uuid",
  "session_id": "uuid",
  "storage_key": "<account_id>/<camera_id>/20260512/<uuid>.jpg",
  "bytes": 187432,
  "width": 1280,
  "height": 720,
  "captured_at": "2026-05-12T20:00:30Z",
  "created_at": "2026-05-12T20:00:31Z"
}
```

Errors: `camera_not_found`, `agent_account_mismatch`, `invalid_content_type`, `snapshot_too_large`, `snapshot_quota_exceeded`, `account_limits_missing`.

### `POST /v1/ingest/snapshots`

Auth: `current_ingest` (header `X-Upstream-Webhook-Secret`). This is the
**streaming-server** producer path (M6.2) — used after the streaming server's
M6.1A local extraction. Agents do **not** use this endpoint; they use
`POST /v1/snapshots`. The dashboard continues to read frames via
`GET /v1/cameras/{camera_id}/latest-frame` (unchanged).

Multipart form. Bytes are uploaded to Supabase Storage; the row references the storage key.

Form fields:
- `session_id`: UUID (required) — the session **must be in state `streaming`**
- `captured_at`: ISO datetime
- `width`, `height`: int, optional
- `file`: JPEG, `Content-Type: image/jpeg`, ≤ `UPSTREAM_SNAPSHOT_MAX_BYTES` (default 5 MB)

`account_id`, `agent_id`, and `camera_id` are **derived server-side from the
session row** and never trusted from the request. Any `camera_id` / `agent_id`
/ `account_id` / `storage_key` form fields are ignored, so a caller cannot
write into another tenant's storage path (`storage_key` is always
`<account_id>/<camera_id>/<yyyymmdd>/<uuid>.jpg` from the session's tenant).

Response 201: `SnapshotUploadResponse` (identical shape to `POST /v1/snapshots`;
`session_id` is always populated since it is required here):
```json
{
  "id": "uuid",
  "camera_id": "uuid",
  "session_id": "uuid",
  "storage_key": "<account_id>/<camera_id>/20260518/<uuid>.jpg",
  "bytes": 187432,
  "width": 1280,
  "height": 720,
  "captured_at": "2026-05-18T20:00:30Z",
  "created_at": "2026-05-18T20:00:31Z"
}
```

Errors: `invalid_webhook_secret` (401), `session_not_found` (404),
`stream_state_invalid` (409 — session not `streaming`; `details.current_state`
carries the actual state), `invalid_content_type` (415), `empty_upload` (400),
`snapshot_too_large` (413), `snapshot_quota_exceeded` (429),
`account_limits_missing` (404).

## Endpoints — Phase 4: reads (auth: `current_agent` OR `current_user`)

These three accept either an agent token (scoped to that agent's account) or a dashboard user JWT (must pass `?account_id=` unless the user has a single membership).

### `GET /v1/agents`

Query: `account_id` (UUID, required for multi-account users), `status` (str), `limit` (1-200, default 50).

Response: `{ "agents": [AgentResponse, ...] }`.

### `GET /v1/agents/{agent_id}`

Query: `account_id` (UUID, required for multi-account users).

Response: `AgentResponse`.

### `GET /v1/cameras`

Query: `account_id`, `agent_id` (filter), `status` (filter), `limit`.

Response: `{ "cameras": [CameraResponse, ...] }`.

### `GET /v1/cameras/{camera_id}`

Query: `account_id`.

Response: `CameraResponse`. Includes `default_latency_mode`.

### `GET /v1/stream-sessions`

Query: `account_id`, `agent_id`, `camera_id`, `state` (`approved|streaming|ended|expired|failed`), `limit`.

Response: `{ "sessions": [StreamSessionResponse, ...] }`.

## Endpoints — Phase 4.5: dashboard convenience reads

Added so the dashboard doesn't have to query Supabase tables/Storage directly. All use the same `current_caller` (agent token OR Supabase user JWT) unless noted.

### `GET /v1/me`

Auth: `current_user` only (no agent token — agents don't have a "me" concept).

Response 200:
```json
{
  "user_id": "uuid",
  "aal": "aal1",
  "amr": ["password"],
  "memberships": [
    { "account_id": "uuid", "account_name": "Acme", "account_slug": "acme-a1b2c3", "role": "owner" }
  ]
}
```

### `GET /v1/account`

Query: `account_id` (UUID, required for multi-account users).

Response 200: full `accounts` row.

Errors: `account_not_found`.

### `GET /v1/account/limits`

Query: `account_id`.

Response 200: full `account_limits` row.

Errors: `account_limits_missing`.

### `GET /v1/account/processing-config`

M3. The **account-level processing master switch** — one boolean per account,
stored as a `processing_enabled` key in `accounts.metadata` JSONB (no new
table). An account that has never written the flag reads as `true`, so every
pre-M3 account keeps processing exactly as before.

Query: `account_id` (UUID, required for multi-account users).

Response 200:
```json
{
  "account_id": "uuid",
  "processing_enabled": true,
  "updated_at": "2026-06-01T12:00:00Z"
}
```

Errors: `account_not_found`.

### `PATCH /v1/account/processing-config`

M3. Flip the master switch. Body: `{ "processing_enabled": false }`.

OFF → the processor's eligible-sessions SQL drops every session under this
account (`coalesce((accounts.metadata->>'processing_enabled')::boolean, true)`
in the WHERE clause), so workers drain on their next poll. Stream sessions
stay alive — only worker-side processing pauses. ON → currently-streaming
sessions become eligible again within one poll cycle.

Response 200: same shape as the GET. Errors: `account_not_found`.

### `GET /v1/api-tokens`

Auth: `current_user` + role gate (owner/admin only — mirrors the existing POST).

Query: `account_id`.

Response 200:
```json
{
  "tokens": [
    {
      "id": "uuid",
      "account_id": "uuid",
      "name": "production agent",
      "prefix": "ups_live_AB12CDEF",
      "last_four": "WXYZ",
      "scopes": ["agent"],
      "last_used_at": null,
      "expires_at": null,
      "revoked_at": null,
      "created_at": "..."
    }
  ]
}
```

Errors: `forbidden.role`, `account_id_required`, `forbidden_account`.

### `GET /v1/agents/{agent_id}/heartbeats`

Query: `account_id`, `limit` (1-500, default 50), `before` (ISO datetime).

Response 200: `{ "heartbeats": [HeartbeatRecord, ...] }` newest first.

Errors: `agent_not_found`.

### `GET /v1/cameras/{camera_id}/heartbeats`

Same shape as above, filtered by `camera_id`.

Errors: `camera_not_found`.

### `GET /v1/cameras/{camera_id}/latest-frame`

Query: `account_id`.

Response 200:
```json
{
  "storage_key": "<account_id>/<camera_id>/20260513/<uuid>.jpg",
  "bytes": 187432,
  "width": 1280,
  "height": 720,
  "captured_at": "2026-05-13T20:00:30Z",
  "created_at": "2026-05-13T20:00:31Z",
  "signed_url": "https://...supabase.co/storage/v1/object/sign/...",
  "expires_at": "2026-05-13T20:01:31Z"
}
```

The signed URL is short-lived (~60s). Clients should re-fetch this endpoint to refresh.

Errors: `camera_not_found`, `snapshot_not_found`.

### `GET /v1/stream-sessions/{session_id}/validations`

Query: `account_id`, `limit` (1-100, default 20).

Response 200: `{ "validations": [StreamValidationResponse, ...] }` newest first.

Errors: `session_not_found`.

### `GET /v1/cameras/{camera_id}/observations/recent`

P5-A. Recent processor observations for one camera, so operators can see
processor output (line crossings, ROI occupancy, zone transitions, dwell)
without direct DB access. Read-only; no dashboard panel yet (P5-B).

Query: `account_id`, `type` (str, optional exact-match filter), `limit` (1-200, default 50).

Response 200: `{ "observations": [ObservationRecord, ...] }` newest first (by `observed_at`), where `ObservationRecord` is:
```json
{
  "id": "uuid",
  "camera_id": "uuid",
  "stream_session_id": "uuid",
  "processor_id": "proc-1",
  "package_id": "pkg.detect.v1",
  "type": "processor.dwell_exceeded",
  "payload": { "...": "as stored" },
  "observed_at": "2026-05-21T12:00:00Z",
  "created_at": "2026-05-21T12:00:01Z"
}
```

`type` is a plain string (the `observations.type` column is open vocabulary — no CHECK constraint), so older or future observation types never break a read. An unknown `type` filter returns an empty list, not an error. `payload` is returned as stored. `account_id` is not echoed (the caller is already account-scoped). The query is double-scoped by account and camera, and the camera must belong to the caller's account.

Errors: `camera_not_found` (404 — unknown camera **or** a camera in another account; identical response, no existence oracle).

### `GET /v1/cameras/{camera_id}/perception/latest`

P5-C. The latest **perception snapshot** for one camera — current visual
state (detected objects + configured ROI/line geometry) for a dashboard
overlay. Distinct from observations: a snapshot is latest-state (one upserted
row per camera), not an append-only event. No dashboard overlay yet (P5-D).

Query: `account_id`.

Response 200: `{ "snapshot": PerceptionSnapshotRecord }` where `PerceptionSnapshotRecord` is:
```json
{
  "id": "uuid",
  "camera_id": "uuid",
  "stream_session_id": "uuid",
  "processor_id": "proc-1",
  "package_id": "deepstream.analytics_probe.v0",
  "snapshot": { "frame": {}, "objects": [], "geometry": {}, "source": {}, "debug": {} },
  "observed_at": "2026-05-21T12:00:00Z",
  "updated_at": "2026-05-21T12:00:01Z"
}
```

`snapshot` is returned as stored (opaque JSON — the server does not deep-validate its shape). `account_id` is not echoed. The query is double-scoped by account and camera.

Errors: `camera_not_found` (404 — unknown camera or a camera in another account); `perception_snapshot_not_found` (404 — the processor has not posted a snapshot for this camera yet; mirrors `latest-frame`'s missing-resource convention).

### `GET /v1/cameras/{camera_id}/processor-status/latest`

P6-D. The latest **processor status** row for one camera — the full
``processor_statuses`` record including the ``payload`` JSONB where the P6-C
analytics worker stamps the ``applied_geometry`` lineage block. The read
counterpart that lets a dashboard chip or verification script confirm the
processor actually picked up the operator's saved geometry on its current
session, rather than falling back to probe defaults.

Query: `account_id`, optional `stream_session_id` (narrow to a specific run
when multiple processor restarts have stamped multiple rows for the same
camera — common in dev/verification because the supervisor bounds DeepStream
runs by a probe-timeout and cycles fresh workers).

Response 200: `{ "status": ProcessorStatusRecord }` where `ProcessorStatusRecord` is:
```json
{
  "id": "uuid",
  "camera_id": "uuid",
  "stream_session_id": "uuid",
  "processor_id": "proc-1",
  "runtime": "deepstream",
  "package_id": "deepstream.analytics_probe.v0",
  "status": "processing",
  "fps_input": 30.0,
  "fps_processed": 29.8,
  "last_frame_at": "2026-05-27T15:30:10Z",
  "last_observation_at": "2026-05-27T15:30:11Z",
  "error_code": null,
  "error_message": null,
  "payload": {
    "applied_geometry": {
      "source": "operator_saved",
      "revision": 9,
      "line_count": 2,
      "roi_count": 1
    }
  },
  "created_at": "2026-05-27T15:24:59Z",
  "updated_at": "2026-05-27T15:30:11Z"
}
```

The contract for ``payload.applied_geometry``:
- ``source`` ∈ ``{"operator_saved", "default", "fallback_default_on_error"}`` — what the processor actually applied.
- ``revision`` matches the dashboard-saved revision when ``source = operator_saved``; ``null`` for ``default`` (no operator save yet); the operator's revision is preserved for ``fallback_default_on_error`` so a bad save can be correlated.
- ``line_count`` / ``roi_count`` are the counts of enabled lines/ROIs the runtime is actually running with (probe-default contributes 1/1 on the ``default`` path).
- ``error_reason`` (optional) is set only when ``source = fallback_default_on_error``.

When operator-saved geometry parses but is invalid (e.g. all-disabled, wrong coordinate space), the processor degrades to the probe default and stamps ``error_code = "geometry_invalid"`` at the row level (alongside the ``fallback_default_on_error`` source). The camera stays up — the supervisor would otherwise thrash on a persistently-bad save.

`account_id` is not echoed (caller is already account-scoped). `payload` is returned as stored (opaque JSON — the server does not deep-validate its shape).

Errors: `camera_not_found` (404); `processor_status_not_found` (404 — no row matches; processor has never reported for this camera, or no row matches the supplied ``stream_session_id``).

### `GET /v1/cameras/{camera_id}/geometry-config`

P6-A. The dashboard-authored ROI/line **geometry config** for one camera —
the shapes the processor's analytics uses. This is **processing geometry
only**: presentation (colors, stroke, fill, opacity, CSS) is deliberately
not modelled here and is rejected (422) — overlay styling stays
frontend-owned.

Query: `account_id`.

Response 200: `{ "geometry_config": GeometryConfigRecord }` where
`GeometryConfigRecord` is:
```json
{
  "id": "uuid|null",
  "camera_id": "uuid",
  "revision": 3,
  "config": {
    "version": 1,
    "frame": {"width": 1280, "height": 720, "coordinate_space": "normalized"},
    "rois": [
      {
        "id": "entry-roi",
        "label": "Entry ROI",
        "type": "polygon",
        "points": [{"x": 0.05, "y": 0.05}, {"x": 0.95, "y": 0.05}, {"x": 0.95, "y": 0.95}],
        "enabled": true
      }
    ],
    "lines": [
      {
        "id": "entry-line",
        "label": "Entry Line",
        "points": [{"x": 0.25, "y": 0.5}, {"x": 0.75, "y": 0.5}],
        "direction": "any",
        "enabled": true
      }
    ]
  },
  "is_default": false,
  "created_at": "2026-05-22T12:00:00Z",
  "updated_at": "2026-05-22T12:00:01Z"
}
```

Two distinct, never-conflated numbers: `config.version` is the JSON **schema
version** (always `1`); `revision` is the DB **row revision** — `0` for the
unsaved probe default, `1` on first save, `+1` on every subsequent PUT.

When a camera has no saved config the endpoint returns the **probe-default**
config (near-full-frame `entry-roi` + horizontal `entry-line`) with
`is_default: true`, `revision: 0`, `id: null`, and null timestamps. A saved
config returns `is_default: false`.

Coordinates are normalized floats in `[0, 1]` (resolution independent);
`frame.width/height` record only the reference resolution the geometry was
authored against.

Errors: `camera_not_found` (404 — unknown camera or a camera in another
account).

### `PUT /v1/cameras/{camera_id}/geometry-config`

P6-A. Full-replace upsert of a camera's geometry config. Query: `account_id`.

Body: `{ "config": GeometryConfig }` — the `config` object from the record
above. `account_id` / `camera_id` are derived from the authenticated caller
and path; any tenant fields in the body are ignored.

The new config overwrites any prior one and the row `revision` bumps by one
(first save → `revision: 1`). Response 200: `{ "geometry_config":
GeometryConfigRecord }` with `is_default: false`.

Validation (→ 422 `validation_error`): `config.version` must be `1`;
`frame.coordinate_space` must be `normalized`; ROI/line `id` must match
`^[a-z0-9][a-z0-9-]{0,63}$`; ROI ids unique; line ids unique; each ROI
polygon ≥ 3 points (≤ 64); each line exactly 2 points; ≤ 20 rois and ≤ 20
lines; every point `x`/`y` in `[0, 1]`; `label` ≤ 80 chars; unknown fields
(including any styling field) rejected.

Errors: `camera_not_found` (404). No PATCH — replacement only.

Geometry saved here is **not** applied to a running processor session: the
processor picks it up on its next session (P6-C). No dashboard editor yet
(P6-B).

### `GET /v1/models`

M2. The platform's read-only **model catalog** — the detection models the
dashboard can offer in a per-camera model picker. This is **facts only**:
each entry describes a model's capabilities (task type, runtime, supported
classes/trackers, defaults) so the UI can build a picker and so the
per-camera processing config can be validated against it. The catalog is
static, in-process data — there is **no DB table** behind it.

Authenticated (any caller) but **not** account-scoped: the catalog is
platform-global. No query params.

Response 200: `{ "models": [ModelRegistryEntry, ...] }` where each
`ModelRegistryEntry` is:
```json
{
  "models": [
    {
      "model_key": "trafficcamnet_v1",
      "display_name": "TrafficCamNet",
      "description": "NVIDIA TrafficCamNet object detector for road scenes.",
      "task_type": "object_detection",
      "runtime": "deepstream",
      "default_confidence_threshold": 0.4,
      "supported_classes": ["car", "bicycle", "person", "roadsign"],
      "supported_trackers": ["iou"],
      "model_version": null,
      "detector_version": null,
      "resource_profile": "gpu_light",
      "enabled": true,
      "notes": "The processor runs this detector today but does not yet read this registry or per-camera config.",
      "limitations": [
        "Fixed to the 4 TrafficCamNet classes.",
        "Can misclassify people as cars in some scenes."
      ]
    },
    {
      "model_key": "mock_detect_v0",
      "display_name": "Mock Detector",
      "description": null,
      "task_type": "object_detection",
      "runtime": "mock",
      "default_confidence_threshold": null,
      "supported_classes": ["person", "car"],
      "supported_trackers": ["none"],
      "model_version": null,
      "detector_version": null,
      "resource_profile": "cpu",
      "enabled": true,
      "notes": "Development/testing detector; outputs are synthetic.",
      "limitations": ["Not a real detector."]
    }
  ]
}
```

`model_key` is the stable, snake_case identifier the per-camera processing
config references. `enabled: false` entries are still listed but cannot be
selected by a processing-config PUT (→ 422). Unknown fields on an entry are
rejected (`extra="forbid"`); no styling/overlay fields are modelled.

The live processor does not consume this config yet — it runs its
hard-coded detector; the catalog exists so the dashboard can author model
selection ahead of processor integration.

### `GET /v1/cameras/{camera_id}/processing-config`

M3. The dashboard-authored model/**processing config** for one camera —
which model to run, which classes to keep, the tracker, confidence
threshold and processing FPS. This is **config / facts only**: presentation
(colors, stroke, fill, opacity, CSS, overlay styling) is deliberately not
modelled here and is rejected (422) — overlay styling stays frontend-owned.
This is a **new table** (`camera_processing_configs`), separate from the
account-level `processing_enabled` switch.

Query: `account_id`.

Response 200: `{ "processing_config": CameraProcessingConfigRecord }` where
`CameraProcessingConfigRecord` is:
```json
{
  "id": "uuid|null",
  "account_id": "uuid",
  "camera_id": "uuid",
  "revision": 3,
  "config": {
    "version": 1,
    "model_key": "trafficcamnet_v1",
    "selected_classes": ["car", "person"],
    "tracker": "iou",
    "confidence_threshold": 0.5,
    "processing_fps": 10
  },
  "is_default": false,
  "created_at": "2026-06-01T12:00:00Z",
  "updated_at": "2026-06-01T12:00:01Z"
}
```

Two distinct, never-conflated numbers: `config.version` is the JSON
**schema version** (always `1`); `revision` is the DB **row revision** —
`0` for the unsaved default, `1` on first save, `+1` on every subsequent
PUT.

When a camera has no saved config the endpoint returns the **default**
config with `is_default: true`, `revision: 0`, `id: null`, and null
timestamps:
```json
{
  "processing_config": {
    "id": null,
    "account_id": "uuid",
    "camera_id": "uuid",
    "revision": 0,
    "config": {
      "version": 1,
      "model_key": "trafficcamnet_v1",
      "selected_classes": [],
      "tracker": null,
      "confidence_threshold": null,
      "processing_fps": null
    },
    "is_default": true,
    "created_at": null,
    "updated_at": null
  }
}
```

The default selects `trafficcamnet_v1` with `selected_classes: []` (which
means **all supported classes**) and leaves tracker/threshold/fps unset
(processor-side defaults apply). A saved config returns `is_default: false`
with `revision >= 1`. The base config payload re-validates structurally on
read even if the registry later changes (registry checks gate writes only).

Errors: `camera_not_found` (404 — unknown camera or a camera in another
account).

The live processor does not consume this config yet.

### `PUT /v1/cameras/{camera_id}/processing-config`

M3/M4. Full-replace upsert of a camera's processing config. Query:
`account_id`.

Body: `{ "config": CameraProcessingConfig }` — the `config` object from the
record above. `account_id` / `camera_id` are derived from the authenticated
caller and path; any tenant fields in the body are ignored.

The new config overwrites any prior one and the row `revision` bumps by one
(first save → `revision: 1`). Response 200: `{ "processing_config":
CameraProcessingConfigRecord }` with `is_default: false`.

Structural validation (→ 422 `validation_error`): `version` must be `1`;
`model_key` 1–128 chars; `selected_classes` ≤ 64 items; `tracker` ≤ 64
chars; `confidence_threshold` in `[0, 1]`; `processing_fps` in `[1, 30]`;
unknown fields (including any styling/overlay field) rejected.

Registry validation (→ 422 `validation_error`, enforced against the
`GET /v1/models` catalog): `model_key` must exist **and** be `enabled`;
every `selected_classes` item must be in that model's `supported_classes`
(an empty `selected_classes` is allowed and means **all supported
classes**); `tracker`, when set, must be in the model's
`supported_trackers`. Registry checks gate **writes only** — they are not
re-run when an existing row is read.

Errors: `camera_not_found` (404). No PATCH — replacement only.

The live processor does not consume this config yet: it is saved as the
*intended* per-camera configuration ahead of processor integration. No
dashboard editor yet.

### `GET /v1/dashboard/summary`

Query: `account_id`.

Response 200:
```json
{
  "total_cameras": 5,
  "online_cameras": 4,
  "offline_cameras": 1,
  "streaming_cameras": 3,
  "cameras_with_errors": 0,
  "active_streams": 3,
  "validation_errors_24h": 0
}
```

Errors: `account_id_required`, `forbidden_account`.

## Endpoints — M8.0 Processor Readiness Gate (auth: `current_processor`)

The safe API contract a future `upstream-processor` uses to discover only
processable streams, report status, and post mock observations. All three
require `X-Upstream-Webhook-Secret` matching `UPSTREAM_PROCESSOR_WEBHOOK_SECRET`
(a **distinct** secret from the ingest webhook secret). M8.0 does not decode
video and does not run DeepStream — it only creates the contract M8.1 (mock
processor) and M8.2 (DeepStream POC) consume.

### `GET /v1/internal/processor/eligible-sessions`

Query: `limit` (1–500, default 200).

Returns a session **only if** (filter enforced entirely in SQL):
- `stream_sessions.state = 'streaming'` (excludes approved / ended / expired / failed)
- `expires_at > now()` (excludes a streaming row past its clock TTL)
- a `stream_validations` row exists (excludes streaming-with-no-validation)
- the **latest** validation has `passed = true` (excludes streaming-then-failed,
  even if an older validation passed)
- the account's processing master switch is on (M3 —
  `coalesce((accounts.metadata->>'processing_enabled')::boolean, true) = true`;
  see `GET`/`PATCH /v1/account/processing-config`)

`internal_rtsp_url` is built as `{UPSTREAM_MEDIAMTX_RTSP_BASE}/{stream_key}` from
the session's `streamid`. The response never contains `srt_url`, an agent/API
token, a session token, a passphrase, or any secret.

Response 200:
```json
{
  "sessions": [
    {
      "session_id": "<uuid>",
      "stream_key": "sess_0123456789abcdef",
      "camera_id": "<uuid>",
      "account_id": "<uuid>",
      "internal_rtsp_url": "rtsp://host.docker.internal:8554/sess_0123456789abcdef",
      "package_id": "mock.detect.v0",
      "runtime": "mock",
      "approved": {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 3000},
      "validation": {
        "passed": true,
        "observed": {},
        "expected": {},
        "mismatch_fields": [],
        "created_at": "2026-05-18T12:00:00Z"
      },
      "geometry_config": null,
      "processing_config": null,
      "started_at": "2026-05-18T11:59:00Z",
      "processor_eligible": true
    }
  ]
}
```

`package_id` is `mock.detect.v0` unless overridden by `cameras.metadata->>'package_id'`.
No package-registry table exists yet (deferred).

`geometry_config` (P6-A) carries the camera's saved geometry config so the
processor does not need a separate fetch. It is `null` when the camera has no
saved config (the processor applies its own probe default); otherwise it is
`{"revision": <int>, "config": <GeometryConfig>}` — see
`GET /v1/cameras/{camera_id}/geometry-config`. P6-C ships the consumer: the
DeepStream analytics worker now renders the saved geometry into its
nvdsanalytics runtime config on its next eligible session, and stamps the
revision + source onto its status payload and emitted observations'
`source` block. (This field replaces the former always-null `zones_lines`
placeholder.)

`processing_config` (M7B) carries the camera's saved per-camera processing
config (cloned additively from `geometry_config`, a LEFT join — eligibility /
`processing_enabled` / `package_id` are unchanged). It is `null` when the
camera has no `camera_processing_configs` row; otherwise it is
`{"revision": <int>, "config": <CameraProcessingConfig>}` where `config` is
the `model_dump()` shape (`version`, `model_key`, `selected_classes`,
`tracker`, `confidence_threshold`, `processing_fps`) — see
`GET /v1/cameras/{camera_id}/processing-config`. M7B is STAMP-ONLY: the field
is wire-carried for lineage and nothing from it is applied. The processor
status payload gains an additive `applied_processing_config` block whose
`applied` is ALWAYS the current runtime defaults (`trafficcamnet_v1` / `[]`
all-classes / `iou` / `null` / `null`); a requested value is never echoed
into `applied`, and `partially_applied` is reserved for a later milestone.

### `POST /v1/internal/processor/status`

Upserts the current status of one processor working one session, keyed by
`(processor_id, stream_session_id)`. The session must exist; `account_id` /
`camera_id` are derived from the session row, never trusted from the body.
`status` ∈ `starting | attached | processing | lagging | failed | stopped`
(invalid → 422 `validation_error`). Unknown session → 404 `session_not_found`.

### `POST /v1/internal/processor/observations`

Appends one observation. M8.0 only accepts `type` ∈
`processor.mock_frame_seen | processor.mock_heartbeat | processor.mock_detection`
(invalid → 422 `validation_error`). Session must exist (404 otherwise);
`account_id` / `camera_id` derived from the session row. Returns `{id, count}`.

### `POST /v1/internal/processor/perception-snapshots`

P5-C. Upserts the **latest perception snapshot** for one camera — the
processor's current visual state (detected objects + configured geometry).
One row per camera: a newer snapshot overwrites the older one (latest-state,
**not** the append-only `observations` log). Session must exist (404
`session_not_found` otherwise); `account_id` / `camera_id` are derived from
the session row, never trusted from the body. Body:
`{stream_session_id, processor_id?, package_id?, observed_at, snapshot}` —
`snapshot` is an opaque JSON object (envelope validated; nested shape not
deep-validated server-side). Returns `{snapshot: PerceptionSnapshotRecord}`.
Read it back via `GET /v1/cameras/{camera_id}/perception/latest`.

Reliability M5 side-effect: on a successful upsert the API also writes one
compact detection-confidence summary row to `snapshot_detection_metrics`
(numeric stats only — never the raw snapshot; a per-row average is summarised
for any frame with ≥1 detection). This is best-effort — isolated in a savepoint
so a failure (or the table not existing yet) never fails the ingest. That
history backs the `unstable_detection_confidence` finding's drift trigger:
confidence is aggregated over rolling time windows (current = last ~10 min vs.
prior = 10–30 min ago, weighted by sample count), so drift fires when the
current window drops > 30% from the prior window regardless of per-frame object
count. The finding's `evidence` then carries `current_avg_confidence`,
`previous_avg_confidence`, `drop_ratio`, `baseline_window_seconds`,
`baseline_confidence_sample_count`, `baseline_metric_count`, and
`trigger_reasons` (null/absent until each window has enough samples). The
per-frame static thresholds (avg/ratio/stddev over the latest snapshot) keep
their original ≥5-detection gate and are unchanged.

Reliability M6 retention + rollup: `snapshot_detection_metrics` is now treated as
**raw, short-term** history (kept ~72h) and is rolled up into compact **hourly**
summaries in a new `detection_confidence_hourly_rollups` table (kept ~90d) for
future long-term confidence trend charts. There is **no new HTTP surface** — this
is a maintenance operation, not an endpoint. A SQL-set-based rollup +
coverage-guarded prune lives in `services/detection_metrics_rollup.py` and is run
via `scripts/run_detection_metrics_rollup.py` (manual / future cron; no pg_cron in
this project). One atomic pass: roll completed hours up (idempotent upsert),
prune raw rows older than the hour-aligned 72h cutoff **only where an hourly
bucket already covers them** (never delete un-rolled data), then age out 90d-old
rollups. The rollup table stores per-(account, camera, hour, package) weighted
summaries (`avg_confidence_weighted`, `min/max_confidence`,
`avg_low_confidence_ratio_weighted`, `avg_confidence_stddev_weighted`,
`metric_count`, `object_count_total`, `confidence_sample_count`) — numeric only,
never the raw snapshot. RLS member-select + `grant select to authenticated`,
same posture as the raw table. **The M5 drift evaluator is unchanged: it keeps
reading raw `snapshot_detection_metrics` (its ~30-min window sits well inside the
72h raw-retention horizon); hourly rollups are long-term history, not a drift
input.** Constants (override per run): `RAW_METRIC_RETENTION_HOURS=72`,
`HOURLY_ROLLUP_RETENTION_DAYS=90`, `ROLLUP_LOOKBACK_HOURS=96` (the 24h surplus
over retention is catch-up margin so a lapsed-then-resumed run still rolls up
prunable hours before deleting their raw rows).

`GET /v1/cameras/{camera_id}/reliability/summary` (Reliability M0) — the
categorical reliability verdict for one camera that backs the dashboard's
Reliability card. **Evaluate-on-read**: the request runs the rule set
(`services/reliability_evaluator.py` — currently five rules:
`stream_valid_processor_silent`, `latest_frame_stale`,
`processor_using_default_geometry`, `scene_too_dark_for_reliable_detection`,
`unstable_detection_confidence`), persists finding upserts/resolves, and
returns the resulting state in a single transaction — no background worker.
Auth/scoping like the other camera reads (`CurrentCaller` + camera-ownership →
404 `camera_not_found`, no existence oracle); query: `account_id`. Response:
`{camera_id, status, highest_severity, open_findings_count, top_findings,
recommended_next_action, last_evaluated_at}` where `status` ∈
`healthy | info | needs_review | critical | unknown` (unknown = not enough
data, distinct from "no open findings") and `top_findings` is the ≤3
highest-severity open `ReliabilityFindingRecord`s.

`GET /v1/cameras/{camera_id}/reliability/findings?status=open&limit=50`
(Reliability M0; `status=` filter added later) — the findings list behind the
Reliability tab. Same auth/scoping and evaluate-on-read behavior as
`summary`. Query: `account_id`, `limit` (1–200, default 50), `status` ∈
`open | resolved | suppressed | ignored` (default `open`; bogus value → 422).
`status=open` returns severity-first ordering (the M0 contract);
`status=resolved` is newest-resolved first (the "Recently resolved" section).
Response: `{findings: [ReliabilityFindingRecord, ...], last_evaluated_at}`;
each record carries `finding_type` (open vocabulary), `category`
(`stream | processor | scene_quality | geometry_config | detection`),
`severity` (`info | warning | critical`), `status`, `title` / `message` /
`likely_cause` / `recommended_action`, JSONB `evidence`, `dedupe_key`,
`detector_version`, and the first/last-seen + resolved timestamps.
`account_id` is never echoed.

`GET /v1/cameras/{camera_id}/reliability/confidence-trend?range=24h` (Reliability
M7) — hourly detection-confidence history for the camera Reliability tab's trend
chart. Same auth/scoping as the other reliability reads (`CurrentCaller` +
camera-ownership → 404 `camera_not_found` on missing/cross-account; bad `range`
→ 422). `range` is `24h` (default) or `7d`; `bucket_size` is always `1h`. It
reads the M6 `detection_confidence_hourly_rollups` and **aggregates across
packages to one camera-level point per hour**, re-weighting per-package weighted
means by `confidence_sample_count` (mirrors M6's per-field weighting). Internal
weighted column names are renamed to clean public names and no
account/session/package/source columns are exposed. Response:
`{camera_id, range, bucket_size:"1h", points:[{bucket_start, bucket_end,
avg_confidence, min_confidence, max_confidence, low_confidence_ratio,
confidence_stddev, metric_count, object_count_total, confidence_sample_count}],
summary:{point_count, latest_avg_confidence, lowest_avg_confidence,
highest_avg_confidence, total_confidence_samples, latest_bucket_start}}`. Points
are oldest-first over the last `range` of **completed** whole hours (the current
incomplete hour is excluded); confidence fields are `null` for an hour that
carried no usable confidence (the dashboard renders those as gaps). Pure read —
**does not run the evaluator and does not touch raw metrics or M5 drift**; these
rollups are long-term chart history only. The dashboard fetches it resiliently
(any failure degrades to an "unavailable" card, never breaking the tab).
Model-config M9 left this endpoint byte-identical: extra per-config rollup rows
blend into the camera-level points exactly as per-package rows already did.

`GET /v1/cameras/{camera_id}/reliability/confidence-trend/by-config`
(model-config M9) — the same hourly history split into **one series per applied
processing config** instead of one blended camera-level line. Backed by the M9
applied-processing-config dimension columns on
`detection_confidence_hourly_rollups` (extracted at snapshot-metric ingest from
M8's `snapshot.source.applied_processing_config`; rows recorded before M8 carry
all-`null` dimensions and form their own group, ordered first). Same
auth/scoping and `range` semantics as `confidence-trend`. Optional filter query
params (unset = no filter): `processing_config_revision` (int ≥ 0),
`processing_config_status` (`applied` | `partially_applied` |
`requested_not_applied` | `applied_defaults` | `fallback_default_on_error`;
bogus → 422), `applied_selected_classes_key` (canonical key: `all`, `person`,
`car,person`…), `applied_confidence_threshold` (float in [0,1]), `warning`
(groups whose warning codes include it, e.g.
`selected_classes_analytics_counts_not_filtered`), and — M9.1 —
`applied_processing_fps` (int in [1,30]; the M7E output-cadence FPS actually
applied — cadence only, not inference FPS). Response: `{camera_id,
range, bucket_size:"1h", group_count, groups:[{processing_config_source,
processing_config_revision, processing_config_status, applied_model_key,
applied_selected_classes_key, applied_confidence_threshold,
requested_processing_fps, applied_processing_fps,
processing_config_warnings, points:[...], summary:{...}}]}` — `points`/`summary`
per group use the exact `confidence-trend` point/summary shapes. Groups are
ordered ascending by revision with no-metadata/default groups first. Pure read;
no evaluator, no raw metrics. Honest read: grouped trends show association
under a config, not proven causation, and M9 applies nothing new at runtime —
confidence threshold and selected classes are applied only where supported;
model switching, tracker, processing FPS, and class-filtered line/ROI counts
remain not live-applied.

`GET /v1/cameras/{camera_id}/reliability/config-comparison-summary`
(model-config M11) — a **descriptive report card** comparing two applied-config
groups from the M9/M9.1 by-config data. NOT a recommender: no verdicts, no
"better/worse/best/recommended", no causal claims — factual metric directions
and deltas only. Same auth/scoping and `range` semantics as the other
reliability reads. Query params: `baseline_revision` / `candidate_revision`
(both → explicit selection; otherwise latest config revision vs previous, or
the two most recent groups for revision-less pre-M8 history) and `min_samples`
(int ≥ 1, default 30 — either side below it → `inconclusive`). Response:
`{camera_id, range, generated_at, comparison_mode:
"explicit"|"latest_vs_previous"|"recent_vs_prior"|"none", min_samples,
baseline, candidate, metric_deltas:{avg_confidence_delta,
avg_confidence_percent_delta, low_confidence_ratio_delta, sample_count_delta,
applied_processing_fps_delta}, comparison_status:
"comparable"|"mixed_metrics"|"inconclusive"|"no_data", metric_directions:
["avg_confidence_higher|lower|unchanged", ...], summary, bullets,
comparability_notes, caveats}` — baseline/candidate carry the group identity
(label matching the dashboard format, e.g. "All classes · confidence 0.10 ·
5 FPS"), window aggregates (sample-count-weighted avg confidence /
low-confidence ratio), and warnings. `comparable` means the population-defining
settings (threshold, classes) match; `mixed_metrics` means they changed, so the
numbers describe a different measured population (the notes say so). Standing
caveats always include "Directional comparison only, not causal proof", plus
the FPS output-cadence-only and line/ROI all-class caveats when relevant. Pure
read over rollups; no migration, no new write path.

### Intentionally deferred (not in M8.0)

- `upstream-processor` repo, DeepStream runtime, YOLO/person detection, Triton
- package-registry / model-registry tables, package marketplace
- dashboard zones/lines UI, custom line-crossing, rules engine, billing
- WebSocket event delivery, the stale-session sweeper

## Endpoints — Platform admin (auth: `current_platform_admin`)

Cross-tenant surface consumed only by `upstream-admin-panel` (which reaches it
through its own server-side proxy — these routes are proxy-only and are not
called from a browser origin, so they need no CORS method additions). A valid
Supabase JWT whose `sub` is missing from `platform_admins` gets
`403 not_platform_admin`; a bad/expired token gets `401 invalid_user_token`.

All mutations write an `admin_audit_log` row (`{before, after}` diff,
`request_id` correlation).

| Method | Path | Notes |
|---|---|---|
| GET | `/v1/admin/me` | Gate check: `{user_id, email}`. |
| GET | `/v1/admin/system/health` | Rollup: `{api, database, streaming_server, processors}`. Probes degrade (DB 0.5 s timeout, MediaMTX `GET /v3/paths/list` 1 s timeout → `unreachable`); always 200. |
| GET | `/v1/admin/streams?state=&account_id=&limit=&offset=` | Cross-account sessions, newest first; each item carries `account_name/slug`, `camera_name`, `latest_heartbeat`, `latest_validation_passed`, `processor_status` (lateral joins). `{items, total}`. |
| GET | `/v1/admin/streams/summary` | `{by_state, started_24h, validation_failures_24h, accounts_streaming, open_reliability_findings}`. |
| GET | `/v1/admin/streams/{session_id}` | Session + `requested/approved/validated` dicts + last 50 heartbeats + last 20 validations + camera findings + processor statuses. |
| GET | `/v1/admin/accounts?search=&status=&plan=&limit=&offset=` | `search` ilikes name/slug. Items carry member/agent/camera/active-session counts. `{items, total}`. |
| GET | `/v1/admin/accounts/{id}` | `{account, limits (nullable), usage_30d}`. |
| PATCH | `/v1/admin/accounts/{id}` | Body `{plan?, status?}`; `plan ∈ free\|pro\|enterprise`, `status ∈ active\|suspended` (`deleted` is NOT settable). Empty body → `400 empty_update`. Audited (`account.plan.update` / `account.status.update` / `account.update`). |
| PATCH | `/v1/admin/accounts/{id}/limits` | Partial update of any `account_limits` column; applies to FUTURE sessions only (in-flight keep their `lim_*` snapshot). Audited (`account.limits.update`). |
| GET | `/v1/admin/accounts/{id}/members` | `[{user_id, role, email?, created_at}]`. Emails best-effort from `auth.users`; degrades to null if the DB role can't read the auth schema. |
| GET | `/v1/admin/accounts/{id}/api-tokens` | Token metadata (prefix, last_four, scopes, timestamps). `hash` is never selected. |
| GET | `/v1/admin/usage?start=&end=&bucket=day\|week\|month&account_id=&event_type=` | `date_trunc`-bucketed `usage_events` aggregates. Defaults: last 30 days, `day`. `start >= end` → `400 invalid_range`; > 366 days → `400 range_too_large`. |
| GET | `/v1/admin/usage/summary?days=30` | Per-account totals over the trailing window (`days` 1–365). |
| GET | `/v1/admin/audit-log?admin_user_id=&target_id=&limit=&offset=` | `{items, total}`, newest first. |

### Editing endpoints (added 2026-06-11)

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/admin/accounts` | Provision a tenant: `{name, slug?, plan?, owner_email?, owner_password?}`. Slug derived from name when omitted (signup-trigger recipe); limits row gets the trigger's permissive defaults. Unknown owner email without password → `400 owner_not_found`; duplicate slug → `409 slug_taken`. Audited (`account.create`). |
| PATCH | `/v1/admin/accounts/{id}` | Extended: also accepts `{name?, slug?}` (slug `^[a-z0-9][a-z0-9-]*$`; conflict → `409 slug_taken`). |
| POST | `/v1/admin/accounts/{id}/members` | `{email, role, password?}` — attaches an existing auth user, or creates one (confirmed, via GoTrue Admin API) when `password` given. `400 member_user_not_found`, `409 member_exists`. Audited. |
| PATCH | `/v1/admin/accounts/{id}/members/{user_id}` | `{role}`. Demoting the only owner → `409 last_owner`. Audited. |
| DELETE | `/v1/admin/accounts/{id}/members/{user_id}` | Removing the only owner → `409 last_owner`. Audited. |
| POST | `/v1/admin/accounts/{id}/api-tokens` | `{name}` → issues an agent token; `plaintext` returned exactly once, never stored/audited. Audited (`account.api_token.create`, prefix only). |
| DELETE | `/v1/admin/accounts/{id}/api-tokens/{token_id}` | Idempotent soft-revoke. Audited. |
| POST | `/v1/admin/streams/{session_id}/stop` | Force-stop: `approved\|streaming` → `ended` (`end_reason=admin_forced`), camera mirrored to idle, usage event recorded, best-effort MediaMTX deprovision. Audited (`stream.force_stop`). |
| POST | `/v1/admin/users/recovery-link` | `{email}` → one-time GoTrue recovery `action_link` for out-of-band delivery. Link NOT audited (event is, without the link). `404 user_not_found`. |
| GET | `/v1/admin/system/timeseries?hours=24` | Hourly activity for the System graphs (`hours` 1–168): zero-filled `generate_series` buckets of `sessions_started`, `validation_failures`, `streaming_minutes` (from `session_ended` usage events), `avg_fps` (null = no heartbeats that hour). |
| GET | `/v1/admin/system/health/history?minutes=60` | Server-collected component samples (`minutes` 5–1440, cap 2000 rows) from the in-process sampler loop: db_ok/latency, MediaMTX status/paths, processor rollup, active sessions. |
| GET | `/v1/admin/platform-admins` | Allowlist with emails. |
| POST | `/v1/admin/platform-admins` | `{email, note?}` — grants an EXISTING auth user admin access (`400 user_not_found`, `409 already_admin`). Audited. |
| DELETE | `/v1/admin/platform-admins/{user_id}` | Revokes; self-removal → `409 cannot_remove_self`. Audited. |

GoTrue admin calls (`services/supabase_admin.py`) use the service-role key
against `<supabase_url>/auth/v1/admin/*` through the shared httpx client;
failures surface as `502 auth_admin_unreachable` / `auth_admin_error` or
`400 auth_user_invalid`.

**Suspension enforcement:** `POST /v1/stream-sessions/start` now rejects with
`403 account_suspended` when `accounts.status != 'active'` (checked in
`services/limits.check_account_active` before the limits load). Suspension
stops NEW sessions only; in-flight sessions are not killed.

## Endpoints — Agent smart flow (bootstrap, stream types, web cameras)

Migration: `20260611120000_stream_types_and_web_cameras.sql` — adds
`account_limits.allowed_stream_types text[]` (default `{webcam,rtsp}`),
`cameras.source_type text` (nullable), and makes `cameras.agent_id`
nullable (dashboard-created cameras start unassigned).

### `GET /v1/agents/bootstrap`

Auth: `current_agent`. One call returns everything an agent needs to
render account status and decide what it may stream. All data is scoped
to the token's account.

**No-secrets guarantee:** the response never contains `srt_url`,
`streamid`, session tokens, SRT passphrases, or token `prefix`/`hash`.
`active_sessions` is a reduced shape for exactly this reason.

Response 200:
```json
{
  "account": { "id": "uuid", "name": "…", "slug": "…", "plan": "free", "status": "active" },
  "token": { "name": "…", "scopes": ["agent"], "last_four": "ABCD", "expires_at": null },
  "agents": [ { "id": "uuid", "name": "…", "status": "active", "version": "0.1.0", "last_seen_at": "…" } ],
  "limits": {
    "max_cameras": 5, "max_concurrent_sessions": 5, "max_fps": 60,
    "max_width": 1920, "max_height": 1080, "max_bitrate_kbps": 8000,
    "allowed_codecs": ["h264"], "allowed_stream_types": ["rtsp", "webcam"],
    "monthly_minutes_quota": 600, "snapshot_storage_mb": 1024
  },
  "usage": { "camera_count": 1, "active_session_count": 0, "monthly_minutes_used": 42 },
  "cameras": [ { "…": "CameraResponse incl. source_type and nullable agent_id" } ],
  "active_sessions": [
    { "id": "uuid", "camera_id": "uuid", "agent_id": "uuid", "state": "streaming",
      "created_at": "…", "expires_at": "…", "started_at": "…",
      "approved": { "width": 1280, "height": 720, "fps": 30, "codec": "h264",
                     "bitrate_kbps": 3000, "latency_mode": "balanced" } }
  ],
  "server_time": "…"
}
```

`usage.monthly_minutes_used` is approximate: it sums `session_ended`
usage events for the current calendar month, so in-flight session time
is invisible until the session ends. `agents` is empty when no agent
has registered yet.

### Stream types (`allowed_stream_types` / `source_type`)

- `POST /v1/cameras/register` accepts an optional `source_type` (max 32
  chars). Validation: `check_stream_type_allowed` rejects values not in
  `account_limits.allowed_stream_types` with 403
  `limit_exceeded.stream_type_not_allowed` (details carry `requested` +
  `allowed`). `source_type = null` is default-allowed (pre-smart-flow
  agents). On re-register the update uses `coalesce`, so an older agent
  that omits the field cannot clear a previously declared value — and a
  declared value cannot be cleared via register at all (use PATCH).
- `POST /v1/stream-sessions/start` re-checks the camera's stored
  `source_type` against the allow-list — stream time is an enforcement
  point too, so disallowing a type takes effect on the next session.
- `GET /v1/account/limits` now includes `allowed_stream_types`.

### Dashboard camera management (auth: `current_user` via `current_caller`)

- `POST /v1/cameras` — owner/admin only (403 `forbidden.role` otherwise;
  agents get 403 `forbidden.user_required`). Body: `name`,
  `source_type` (required), `device_path`, `default_*` fields. Validated
  against limits (`check_camera_defaults`, `check_stream_type_allowed`,
  `check_camera_slot_available`). Creates the row with
  `agent_id = null`. 201 → `CameraResponse`.
- `PATCH /v1/cameras/{camera_id}` — owner/admin only. Partial update of
  `name`/`source_type`/`device_path`/`default_*`; the merged result is
  re-validated against limits. 404 `camera_not_found` on cross-account.

### `POST /v1/cameras/{camera_id}/claim`

Auth: `current_agent`. Body: `{ "agent_id": "uuid" }`. Assigns an
unassigned (dashboard-created) camera to that agent. Idempotent when the
camera is already held by the same agent. 409
`camera_claimed_by_other_agent` when another agent holds it; 409
`agent_account_mismatch` when the agent row isn't in the token's
account; 404 `camera_not_found` on cross-account.

`POST /v1/stream-sessions/start` rejects cameras with `agent_id IS NULL`
with 409 `camera_unassigned` — claim before streaming.

## Endpoints — Workflow Engine M1 (auth: `current_user`)

> Originally shipped as "Rules/Triggers M1"; re-labeled when the Workflow
> Engine track was named. Same endpoints, same numbering position: this is
> the first milestone of the Workflow Engine product track (separate from
> the reliability milestone sequence). M1 = simple rules + in-app alerts;
> M2 (below) reframes every rule match as a workflow run.

Users author rules over processor observations; matched rules raise alerts
into a dashboard inbox.

Migration: `20260612120000_rules_and_alerts.sql` — creates `rules` and
`alerts` (both RLS member-select + `GRANT SELECT TO authenticated`).
`rules.rule_type` is open vocabulary at the DB layer (same posture as
`observations.type`); the API schema layer enforces the currently-supported
set, so new types ship without a migration.

Evaluation runs inline on `POST /v1/internal/processor/observations`:
after the observation insert, the evaluator fetches enabled rules for the
camera matching the observation's rule type, runs a pure matcher per rule,
and inserts alerts. The whole step is savepoint-isolated (mirroring the M5
detection-metrics write) — it can NEVER fail observation ingest, including
when the migration has not been applied yet. Tenant columns are derived
from the session row, never the request.

Rule types and per-type `config` keys (all optional unless noted):

| `rule_type`      | fires on observation type   | config keys                          |
|------------------|-----------------------------|--------------------------------------|
| `line_crossing`  | `processor.line_crossing`   | `line_id`                            |
| `roi_occupancy`  | `processor.roi_occupancy`   | `roi_id`, `min_total_count` (int ≥1) |
| `zone_entered`   | `processor.zone_entered`    | `zone_id`                            |
| `dwell_exceeded` | `processor.dwell_exceeded`  | `zone_id`, `min_duration_seconds` (>0) |

Match semantics: a missing optional filter key means "match any"; a payload
missing the field a filter needs does NOT match (a rule scoped to line A
never fires on an observation that can't prove it came from line A).
Unknown config keys are rejected at authoring time (422 `invalid_rule_config`)
so typos surface immediately instead of as silently-never-matching rules.

Flood control: `cooldown_seconds` (0–86400, default 300) is the minimum gap
between two alerts from the same rule, enforced by a same-statement
`NOT EXISTS` guard on insert. Worst case under concurrency is one duplicate
alert across two connections (accepted for M1).

Deferred (need a periodic evaluator that does not exist yet):
`camera_offline`, `no_detections_for_n_minutes`; email/webhook delivery.

### `POST /v1/rules`

Auth: `current_user`, owner/admin on `account_id`. Camera must belong to the
account (404 `camera_not_found` otherwise — anti-enumeration).

Request:
```json
{
  "account_id": "uuid",
  "camera_id": "uuid",
  "name": "Front door crossing",
  "rule_type": "line_crossing",
  "enabled": true,
  "config": {"line_id": "line-1"},
  "severity": "warning",
  "cooldown_seconds": 300
}
```

Response 201: the rule (no `account_id` echoed — same posture as the
observations read path).
```json
{
  "id": "uuid", "camera_id": "uuid", "name": "Front door crossing",
  "rule_type": "line_crossing", "enabled": true,
  "config": {"line_id": "line-1"}, "severity": "warning",
  "cooldown_seconds": 300, "created_by": "uuid",
  "created_at": "…", "updated_at": "…"
}
```

Errors: 403 `forbidden` (viewer), 404 `camera_not_found`,
422 `invalid_rule_config` (details.problems lists each issue),
422 validation_error (unknown `rule_type`, bad `cooldown_seconds`).

### `GET /v1/rules?account_id=…&camera_id=…`

Auth: `current_user`, any member. `camera_id` optional filter. Newest first.
Response 200: `{"rules": [RuleSummary, …]}`.

### `GET /v1/rules/{rule_id}?account_id=…`

Auth: `current_user`, any member. 404 `rule_not_found` for unknown or
cross-tenant ids.

### `PATCH /v1/rules/{rule_id}?account_id=…`

Auth: `current_user`, owner/admin. Partial update of `name`, `enabled`,
`config`, `severity`, `cooldown_seconds`. `rule_type` is immutable — delete
and recreate to change it. A patched `config` is re-validated against the
stored type. Response 200: the updated rule.

### `DELETE /v1/rules/{rule_id}?account_id=…`

Auth: `current_user`, owner/admin. 204 on success, 404 `rule_not_found`.
Alerts the rule raised survive (their `rule_id` becomes null).

### `GET /v1/alerts?account_id=…&status=…&camera_id=…&limit=…`

Auth: `current_user`, any member. `status` ∈ {`open`, `resolved`} (optional),
`camera_id` optional, `limit` 1–200 default 50. Newest first.

Response 200:
```json
{
  "alerts": [{
    "id": "uuid", "camera_id": "uuid", "rule_id": "uuid",
    "observation_id": "uuid", "severity": "warning", "status": "open",
    "title": "Front door crossing",
    "message": "Line crossing detected on Front door.",
    "payload": {"line_id": "line-1", "count_delta": 1},
    "created_at": "…", "read_at": null, "resolved_at": null
  }]
}
```

`payload` is the matched observation's payload (evidence snapshot).
`observation_id` is a soft reference — the observations log may be pruned.

### `POST /v1/alerts/{alert_id}/read`

Auth: `current_user`, any member. Sets `read_at` once (idempotent — repeat
calls return the same timestamp). Response 200: the alert.
404 `alert_not_found` for unknown/cross-tenant ids.

### `POST /v1/alerts/{alert_id}/resolve`

Auth: `current_user`, owner/admin (resolution is a workflow decision, not a
view state). Sets `status='resolved'`, `resolved_at`, and `read_at` if
unset. Idempotent. Response 200: the alert.

## Endpoints — Workflow Engine M2 (auth: `current_user`)

Workflow Runs + Internal Event History. The product premise: Upstream is a
camera **workflow engine**, not an alerts product. M1 added simple rules +
in-app alerts; M2 makes every rule match a **workflow run** — the internal
system of record — recorded WHETHER OR NOT an alert was sent. Alerts are now
one action/output of a run: a cooldown-suppressed match still produces a run
(`status='suppressed'`, `action_result='alert_suppressed_cooldown'`) with
the evidence that triggered it.

Still deferred (the schema deliberately leaves room, but none of this is
built): multi-step sequencing (event A then B, time windows, waits),
multi-camera / account-wide workflows (camera_id is already nullable),
visual workflow builder, delivery channels (email/webhook/Slack), a
`workflow_run_events` step table, and review/acknowledge mutations
(`status='action_required'` is reserved but nothing produces it yet).

Migration: `20260613120000_workflow_runs.sql` — creates `workflow_runs`
(RLS member-select + grant) and adds `alerts.workflow_run_id` (FK,
on delete set null). The FK lives on alerts so the write path is
insert-run → insert-alert → finalize-run with no circular reference.

For M2 the existing `rules` table acts as the workflow definition layer
(`workflow_type = 'simple_rule'`); no separate workflows table. Run
creation happens inside the existing `_evaluate_rules_safe` savepoint on
`POST /v1/internal/processor/observations` — the M1 contract is unchanged:
a failing (or not-yet-migrated) workflow layer can NEVER fail observation
ingest.

Run lifecycle per matching rule: insert run (`status='active'`, evidence =
observation payload, payload = rule snapshot at match time) → cooldown-
guarded alert insert carrying `workflow_run_id` → finalize run
(`completed`/`alert_created` or `suppressed`/`alert_suppressed_cooldown`,
`ended_at` stamped). Non-matching and disabled rules produce no run.

Open vocabulary at the DB layer: `workflow_type`, `trigger_type`,
`action_result`. CHECK-constrained (dashboard switches on it): `status` ∈
{`active`, `completed`, `failed`, `action_required`, `suppressed`}.

### `GET /v1/workflow-runs?account_id=…&status=…&camera_id=…&rule_id=…&limit=…`

Auth: `current_user`, any member. All filters optional; `status` validated
against the known set (422 otherwise); `limit` 1–200 default 50. Newest
first. `alert` is the related alert (left join via
`alerts.workflow_run_id`) or null — suppressed runs have none.

Response 200:
```json
{
  "runs": [{
    "id": "uuid", "camera_id": "uuid", "rule_id": "uuid",
    "trigger_observation_id": "uuid",
    "workflow_type": "simple_rule", "status": "completed",
    "title": "Front door crossing",
    "summary": "Line crossing detected on Front door.",
    "trigger_type": "processor.line_crossing",
    "action_result": "alert_created",
    "evidence": {"line_id": "line-1", "count_delta": 1},
    "payload": {"rule": {"id": "uuid", "rule_type": "line_crossing",
                          "config": {}, "severity": "warning",
                          "cooldown_seconds": 300}},
    "alert": {"id": "uuid", "status": "open", "severity": "warning"},
    "started_at": "…", "ended_at": "…",
    "created_at": "…", "updated_at": "…"
  }]
}
```

`account_id` is never echoed (same posture as the observations read path).
`evidence` is the triggering observation's payload; `payload.rule` is the
rule definition snapshot at match time, so runs stay interpretable after
the rule is edited or deleted.

### `GET /v1/workflow-runs/{workflow_run_id}?account_id=…`

Auth: `current_user`, any member. Same shape as a list item (evidence,
action result, related alert included). 404 `workflow_run_not_found` for
unknown or cross-tenant ids (anti-enumeration).

### Alert responses gain `workflow_run_id`

`GET /v1/alerts` items (and the read/resolve responses) now include
`workflow_run_id` (uuid | null) — null for alerts written before the M2
migration. M2 is otherwise read-path only: no workflow-run mutation APIs.

## Workflow Engine M3 — Richer Rule Conditions

M1 added simple rules + in-app alerts; M2 made every match a workflow run
(internal history); M3 makes the one-step rule language more expressive.
Still deferred: multi-step sequencing, visual builder, delivery channels.

No migration and no new endpoints — M3 extends the per-type ``config``
vocabulary on the existing `/v1/rules` API and the pure matcher. Existing
M1/M2 rules keep working unchanged. Workflow runs keep snapshotting the
full (now richer) config in ``payload.rule``; evidence is unchanged.

### Condition availability follows the audited observation payloads

A condition is allowed ONLY on rule types whose production payloads can
prove the field it needs (422 ``invalid_rule_config`` otherwise) — a rule
that can never match is worse than an authoring-time rejection. Audited
against the DeepStream analytics worker:

| condition            | rule types                      | payload field        |
|----------------------|---------------------------------|----------------------|
| `object_classes`     | zone_entered, dwell_exceeded    | `class_label`        |
| `require_track_id`   | zone_entered, dwell_exceeded    | `track_id`           |
| `min_total_count`    | roi_occupancy (M1)              | `total_count`        |
| `max_total_count`    | roi_occupancy                   | `total_count`        |
| `schedule`           | all four types                  | observation time     |

**Deferred at M3 (processor follow-ups):**

> Update: the processor's payload-enrichment side task later unlocked
> `min_confidence` (zone/dwell) and per-class ROI counts — see the
> "Payload-enrichment unlock" section below. The rest stay deferred.

- `direction` — in the line_crossing payload contract but never produced:
  production line crossings come from nvdsanalytics CUMULATIVE line
  counters, which are direction-blind aggregates.
- `object_classes` / `require_track_id` / `min_confidence` on
  line_crossing — same root cause: cumulative counters have no per-object
  attribution. Per-object `lcStatus` labels exist in the processor's object
  metadata, so a per-object line-crossing producer is feasible, but it
  replaces the cumulative-counter state machine (idempotency/dedupe
  semantics) — a documented processor follow-up.
- "no detections for N minutes" — needs a periodic evaluator (still none).

### Match semantics

Unchanged posture, extended: a missing optional condition key means "match
any"; a payload missing the field a configured condition needs does NOT
match (no false matches). ``min_total_count`` and ``max_total_count`` are
both inclusive bounds.

### `schedule` — inline time-window gating

```json
{
  "schedule": {
    "days": ["mon", "tue", "wed", "thu", "fri"],
    "start_time": "08:00",
    "end_time": "18:00",
    "timezone": "America/New_York"
  }
}
```

Evaluated INLINE from the observation's ``observed_at`` (converted to the
schedule's IANA timezone) during ingest — there is no periodic evaluator.
``days`` (optional; default every day) names the LOCAL day the window
STARTS on. ``start_time < end_time`` is a same-day window matching
``start <= t < end``; ``start_time > end_time`` wraps midnight (22:00-06:00
on ["fri"] = Friday 22:00 through Saturday 05:59). ``start == end`` is
rejected at authoring. An observation outside the window is a NON-MATCH:
no workflow run, no alert. A malformed stored schedule degrades to
"no match" (the gate runs inside the ingest savepoint and never raises).

### Example richer configs

```json
{ "zone_id": "zone-1", "object_classes": ["person", "forklift"],
  "require_track_id": true }
```
```json
{ "roi_id": "roi-1", "min_total_count": 2, "max_total_count": 10,
  "schedule": {"start_time": "22:00", "end_time": "06:00", "timezone": "UTC"} }
```


## Workflow Engine — payload-enrichment unlock (post-M3)

The processor side task "Workflow Condition Payload Enrichment" enriched
observation payloads with provable fields, unlocking two previously-deferred
rule conditions. No migration, no new endpoints — `config` vocabulary and
matcher only. Missing fields still mean NO match (the processor omits
`confidence` when the track wasn't detected in the triggering frame, and
pre-enrichment events carry an empty `class_counts`).

| condition        | rule types                   | payload field            |
|------------------|------------------------------|--------------------------|
| `min_confidence` | zone_entered, dwell_exceeded | `confidence` (per-object detection confidence in the triggering frame; inclusive threshold, number in (0, 1]) |
| `object_class`   | roi_occupancy                | `class_counts` (object-attributed per-class ROI counts) |

`object_class` (single class name) scopes the existing
`min_total_count` / `max_total_count` bounds to that class's count instead
of the aggregate `total_count`. A class absent from the breakdown counts as
zero; a payload with no `class_counts` dict can't prove anything and never
matches. Because `min_total_count`'s floor is 1, pre-enrichment events
(empty breakdown) can never match a class-scoped rule.

Example: "alert when more than 3 cars are in the loading ROI, but only with
confident person detections in the pick zone after hours" is now two rules:

```json
{ "roi_id": "loading", "object_class": "car", "min_total_count": 4 }
```
```json
{ "zone_id": "pick", "object_classes": ["person"], "min_confidence": 0.75,
  "schedule": {"start_time": "18:00", "end_time": "08:00", "timezone": "America/New_York"} }
```

Still deferred (honestly): all line_crossing conditions beyond `line_id` +
`schedule`, and periodic-evaluation rule types.

## Workflow Engine — per-object line crossing unlock

The processor side task "Per-Object Line Crossing Producer" added a second
line-crossing observation type, `processor.line_crossing_object`: per-OBJECT
crossing evidence (track/class/confidence attribution, payload
`event_mode: "object_crossing"`), emitted ALONGSIDE the unchanged aggregate
`processor.line_crossing` count events.

**The partition rule (prevents double-fire).** One physical crossing reaches
this server as TWO observations. Both types map to the `line_crossing` rule
family, and the matcher partitions on the payload:

- a rule with ANY per-object condition (`object_classes`,
  `require_track_id`, `min_confidence`) matches ONLY per-object events;
- a plain rule (at most `line_id` + `schedule`) matches ONLY aggregate
  events — exactly its pre-existing behavior.

So each rule fires at most once per physical crossing, legacy rules are
untouched, and a per-object-conditioned rule never falls back to aggregate
events (an untracked crossing can't satisfy it — counted, not attributed).

**Unlocked `line_crossing` config keys:** `object_classes`,
`require_track_id`, `min_confidence` — same semantics and validation as on
zone/dwell rules; missing payload fields still mean no match.

**Still deferred:** `direction`. The per-object payload may carry
`analytics_direction_label` (a raw nvdsanalytics direction-rule name), but
that is NOT a geometry-relative a_to_b/b_to_a crossing direction, so
direction filters remain rejected at authoring time.

No migration; `ProcessorObservationType` widened in lock-step with the
processor. Workflow runs record `trigger_type:
"processor.line_crossing_object"` for per-object matches.

## Workflow Engine M4 — Time-Based Evaluator

M1 = simple rules + alerts; M2 = workflow runs/history; M3 = richer
conditions; the payload side tasks enriched processor evidence. M4 adds the
first PERIODIC evaluation path — rules that describe the ABSENCE of
activity, which no single incoming observation can prove. Still deferred:
multi-step sequencing, visual builder, delivery channels,
`occupancy_above_for` (needs continuous state tracking).

No migration. Two new rule types on the existing `/v1/rules` API:

### `camera_offline`

```json
{ "offline_for_seconds": 300,
  "schedule": {"days": ["mon","tue","wed","thu","fri"],
                "start_time": "08:00", "end_time": "18:00",
                "timezone": "America/Denver"} }
```

Matched when the camera has NO active streaming session AND no heartbeat
within `offline_for_seconds` (60–604800). A camera that has never
heartbeated matches; disabled cameras are never evaluated. Schedule is the
M3 shape (optional = always active).

### `no_detections_for`

```json
{ "quiet_for_seconds": 1800,
  "observation_types": ["processor.zone_entered", "processor.line_crossing_object"],
  "object_classes": ["person"] }
```

Evaluated ONLY while the camera has an active streaming session ("up but
quiet" is the anomaly; a down camera is camera_offline's job — gating
prevents double-alerting). The quiet clock anchors at max(last matching
observation, session started_at) so a fresh session never instant-fires.
`observation_types` (optional) must be known processor types; when omitted
it defaults to all types — or to the class-attributing subset when
`object_classes` is set (a class filter over class-blind types is rejected
at authoring).

### Firing semantics (differs from event rules — by design)

A timer condition is continuous, so the workflow-run INSERT itself carries
the re-fire guard: NOT EXISTS a run for the same rule within
`cooldown_seconds`. The cooldown is the RE-FIRE interval while the
condition persists; evaluator ticks inside the window create NOTHING (no
suppressed-run-per-tick flooding — the fired run is the episode marker).
Because of this, timer rules require `cooldown_seconds >= 60` (422
otherwise). Fired runs: `workflow_type = 'time_based_rule'`,
`trigger_type = 'timer.<rule_type>'`, evidence carrying
last_seen/threshold/quiet-window facts, alert linked as usual (or run
finalized suppressed if the alert guard races).

### `POST /v1/internal/workflow-evaluator/run`

Auth: processor shared secret (`X-Upstream-Webhook-Secret`) — internal
infrastructure surface. Safe to call repeatedly; returns
`{evaluated_rules, schedule_skipped, matched_rules, workflow_runs_created,
alerts_created, suppressed_by_cooldown, errors}`.

### Operational notes

The in-process loop is gated by
`UPSTREAM_WORKFLOW_EVALUATOR_INTERVAL_SECONDS` (default 60; 0 disables —
drive the endpoint from external cron instead). Single-instance
deployments only: with replicas, disable the loop on all but one. Overlap
safety is best-effort (the single-statement NOT EXISTS guard — same
posture as the M1 alert guard); no advisory locks. One rule's evaluation
failure is logged with scalar ids and never stops a tick; evaluator
failures never touch the observation-ingest path (separate code path
entirely).

## Workflow Engine M5 — Multi-Step Sequencing

The first real multi-step capability: **A → B within N seconds**, with
optional same-track correlation. Workflows are now a TOP-LEVEL product
object (`workflows` table); the `rules` table remains the M1-M4
simple/timer-rule layer, unchanged. Alerts remain optional outputs. Still
deferred: arbitrary graphs / >2 steps, branching, visual builder,
cross-camera sequencing, recovery/auto-resolve, delivery channels.

Migration: `20260614120000_workflows.sql` — `workflows` table (RLS +
grant) and three nullable `workflow_runs` columns: `workflow_id` (FK, set
null on delete — history survives), `deadline_at` (partial-indexed expiry
sweep), `dedupe_key` (duplicate-start guard).

### `/v1/workflows` CRUD (auth: `current_user`)

`POST /v1/workflows`, `GET /v1/workflows?camera_id=&type=`,
`GET|PATCH|DELETE /v1/workflows/{workflow_id}` — owner/admin mutations,
member reads, camera-ownership + anti-enumeration 404s,
422 `invalid_workflow_config` with `details.problems`. `workflow_type` is
immutable ('sequence' is the only accepted type today). Responses never
echo `account_id`.

### Sequence config

```json
{
  "start": {"event_type": "processor.zone_entered",
             "conditions": {"zone_id": "zone-a", "object_classes": ["person"],
                             "min_confidence": 0.7, "require_track_id": true}},
  "then": {"within_seconds": 30,
            "event_type": "processor.line_crossing_object",
            "conditions": {"line_id": "line-b"}},
  "correlation": {"same_track_id": true},
  "on_success": {"create_alert": false},
  "on_timeout": {"create_alert": true,
                  "alert_message": "Expected follow-up did not happen in time."},
  "schedule": {"days": ["mon","tue","wed","thu","fri"],
                "start_time": "08:00", "end_time": "18:00",
                "timezone": "America/Denver"}
}
```

Validation is strict: unknown keys rejected at every level; step
`event_type` must be a discrete event type (`zone_entered`, `zone_exited`,
`line_crossing`, `line_crossing_object`, `dwell_started`,
`dwell_exceeded` — snapshots like roi_occupancy excluded); per-step
conditions follow the M3 availability posture (aggregate `line_crossing`
allows only `line_id`); `within_seconds` 5–86400;
`correlation.same_track_id` requires BOTH event types to be
track-attributing. `on_success.create_alert` defaults false;
`on_timeout.create_alert` defaults true. Schedule is the M3 shape.

### Sequencer behavior (observation ingest)

Runs in its own savepoint after rule evaluation (one failing layer never
affects the other, and neither can fail ingest). Completions are processed
BEFORE starts. **Start**: schedule + start conditions match (and a track
exists when correlating) → guarded INSERT of an `active` run
(`workflow_type='sequence'`, `trigger_type=<start observation type>`,
`deadline_at = observed_at + within_seconds`, start evidence + definition
snapshot, correlation track captured). The guard: NOT EXISTS an active run
for the same workflow with the same `dedupe_key` (the start observation's
processor idempotency key, falling back to observation id) — duplicate
deliveries can't double-start; distinct physical starts (two people) DO run
in parallel. **Complete**: active runs on the camera whose `then` matches —
event type, deadline not passed, conditions, same-track — flip to
`completed` via a guarded UPDATE (`where status='active'`, single-shot),
follow-up evidence appended, `action_result='sequence_completed'` (alert
only when `on_success.create_alert`).

### Timeout (M4 evaluator loop)

Each evaluator tick sweeps `active` runs past `deadline_at`: guarded UPDATE
to `failed` with timeout evidence (single-shot — repeated ticks can't
re-fail), then the timeout alert (unless opted out), cooldown-guarded
ACROSS runs of the same workflow (the guard joins alerts →
workflow_runs.workflow_id, since alerts carry no workflow column). The
evaluator stats response gains `sequence_runs_expired`.

### Workflow run reads

`WorkflowRunSummary` gains `workflow_id` (null on rule runs) and
`deadline_at` (set while an active sequence run is waiting).

## Module 7 M1 — Usage + Billing Foundation

First production usage/billing surface: a usage summary read API and
enforcement of the (previously display-only) monthly streaming minutes
quota. Intentionally deferred: Stripe (M2), a plans table, monthly
rollups, and processor/compute minutes (not tracked anywhere — never
invented). **No migration required** — everything reads existing tables.

### `GET /v1/usage/summary?account_id=` (auth: `current_user`, any member)

```json
{
  "plan": {"key": "free", "label": "Free", "status": "active"},
  "period": {"start": "2026-06-01T00:00:00Z", "end": "2026-07-01T00:00:00Z"},
  "limits": {"max_cameras": 5, "max_concurrent_sessions": 5,
              "monthly_minutes_quota": 600,
              "allowed_stream_types": ["rtsp", "webcam"]},
  "usage": {"cameras": 2, "active_streams": 1, "stream_minutes": 430,
             "workflow_runs": 1204, "alerts": 86, "rules": 4, "workflows": 1},
  "states": {"cameras": "ok", "active_streams": "ok",
              "stream_minutes": "near_limit"}
}
```

- Period = current UTC calendar month (`date_trunc('month')` convention,
  same as agent bootstrap).
- Every counter reuses the predicate of the code that owns it: cameras
  `status <> 'disabled'` (max_cameras enforcement), active_streams
  `state in ('approved','streaming')` (session enforcement),
  stream_minutes = sum of usage_events **`session_ended` only** // 60
  (bootstrap convention; `stream_seconds` events report the SAME durations
  from the streaming-server side and must never be added). Completed
  sessions only — an active session counts when it ends.
- workflow_runs/alerts are period counts; rules/workflows are ALL current
  definitions (no soft delete exists).
- `plan.label` is a title-cased copy of `accounts.plan` (no plans table).
- States: `ok` < 80%, `near_limit` >= 80%, `at_limit` == 100%,
  `over_limit` > 100%; a null limit would read `ok` (unlimited), a
  zero/negative limit reads at/over.
- 404 `account_limits_missing` when the account or its limits row is gone.

### Monthly minutes quota enforcement

`check_session_start` now rejects session creation once COMPLETED stream
minutes this period reach `account_limits.monthly_minutes_quota`:

- 403 **`limit_exceeded.monthly_minutes_quota`** with details
  `{limit, used, period_start, period_end}` (minutes, ISO timestamps).
- Same minutes source as the summary (`session_ended` only). Documented
  softness: an account near the cap can start one final session and
  overshoot by its length — acceptable for a coarse cost-control cap.
- A quota of 0 blocks all session starts (no 0-means-unlimited convention
  in this codebase). All pre-existing checks (concurrent sessions, stream
  type, codec/fps/resolution/bitrate) are unchanged and run as before.

## Module 7 M2 — Stripe Checkout + Customer Portal

Stripe-HOSTED checkout and billing management — Upstream never renders a
card form and stores only customer/subscription/price identifiers.
Deliberately deferred: metered billing, usage invoices, coupons, invoice
history UI, plans table.

Migration `20260616120000_account_billing.sql`: `account_billing` (1:1
with accounts; nullable Stripe refs + subscription state; RLS member
select) and `stripe_events` (webhook idempotency ledger; id = Stripe
event id; RLS enabled with no policies — service-role only).

### Env (all optional — unset ⇒ 503 `billing_not_configured` and the
dashboard's "coming soon" fallback)

- `UPSTREAM_STRIPE_SECRET_KEY` — sk_test_/sk_live_
- `UPSTREAM_STRIPE_WEBHOOK_SECRET` — whsec_ for the endpoint below
- `UPSTREAM_STRIPE_PRICE_STARTER_MONTHLY` / `UPSTREAM_STRIPE_PRICE_PRO_MONTHLY`
  — price_ IDs from the Stripe dashboard
- `UPSTREAM_DASHBOARD_BASE_URL` — Checkout/Portal return base (no slash)

### `POST /v1/billing/checkout` (user auth, owner/admin)

Body `{"account_id": "...", "plan": "starter"|"pro"}` → `{"url": "https://checkout.stripe.com/..."}`.
Creates (or reuses) the account's Stripe customer, then a subscription-mode
Checkout Session with `account_id`/`plan_key` stamped in both session AND
subscription metadata. Plan keys map to price IDs via env; limits per plan
live in `services/plans.py` (PLACEHOLDER values — pricing is configurable,
not final). Errors: 403 role/tenancy, 422 unknown plan, 503 unconfigured.

### `POST /v1/billing/portal` (user auth, owner/admin)

Body `{"account_id": "..."}` → `{"url": "https://billing.stripe.com/..."}`.
409 `billing_no_customer` before any checkout has created a customer.

### `POST /v1/webhooks/stripe`

No user auth — authentication is the `Stripe-Signature` check against the
RAW request body (400 `invalid_webhook_signature` on failure/missing).
Every event id is recorded in `stripe_events` first; duplicates ack
`{"received": true, "duplicate": true}` without reprocessing. Handled:

- `checkout.session.completed` — links customer/subscription to
  metadata.account_id, then RETRIEVES the subscription from Stripe (the
  session payload's copy is incomplete) and syncs.
- `customer.subscription.created/updated/deleted` — sync status, price,
  period (reads item-level current_period_* with top-level fallback —
  both Stripe API shapes), cancel_at_period_end.
- `invoice.payment_failed` — records past_due immediately; plan/limits
  untouched.
- Anything else: recorded + acked, unprocessed.

### Subscription sync contract

- `active`/`trialing` + known price → `accounts.plan` and the WHOLE
  `account_limits` row are overwritten from the plan mapping (hand-tuned
  limits don't survive a plan change). Unknown price → state recorded,
  limits untouched (logged `stripe_price_unmapped`).
- `cancel_at_period_end` → flag recorded, nothing downgrades until Stripe
  deletes the subscription at period end.
- deleted/`canceled` → downgrade to `free` plan + free limits. Cameras /
  workflows / rules are never deleted; free limits just gate new activity.
- `past_due`/`unpaid`/`incomplete*` → status recorded, plan/limits kept
  (grace; Stripe dunning decides when it actually cancels).
- `accounts.status` is NEVER touched — it stays account lifecycle state.

### Usage summary `billing` block

`GET /v1/usage/summary` responses now include:

```json
"billing": {"provider": "stripe", "configured": true, "status": "active",
             "subscription_status": "active",
             "current_period_start": "...", "current_period_end": "...",
             "cancel_at_period_end": false, "management_available": true}
```

`status` is derived (`none|active|trialing|past_due|canceled`; every
payment-trouble Stripe status maps to `past_due`). Stripe IDs are never
exposed to the dashboard.
