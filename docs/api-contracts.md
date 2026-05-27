# API contracts

Stable contract between the API server and its callers. Paths are all under `/v1/` unless noted. Phase numbers refer to the delivery plan in `architecture.md`.

## Auth headers

| Caller | Header | Verified against |
|---|---|---|
| Agent | `Authorization: Bearer ups_<env>_<id>_<secret>` | `api_tokens` row, argon2id on the secret segment |
| Dashboard user | `Authorization: Bearer <Supabase access token>` | `UPSTREAM_SUPABASE_JWT_SECRET` (HS256, `aud=authenticated`) |
| Streaming server | `X-Upstream-Webhook-Secret: …` | `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` |
| Processor | `X-Upstream-Webhook-Secret: …` | `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` |

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
| 413 | `snapshot_too_large` | snapshot upload exceeds `UPSTREAM_SNAPSHOT_MAX_BYTES` |
| 415 | `invalid_content_type` | snapshot upload was not `image/jpeg` |
| 429 | `snapshot_quota_exceeded` | trailing-30-day snapshot bytes would exceed `account_limits.snapshot_storage_mb` |
| 422 | `validation_error` | pydantic request validation failed |
| 500 | `internal_error` | unhandled exception |

## Endpoints — Phase 1 (implemented)

### `GET /healthz`

No auth. Liveness only; doesn't touch the DB.

```json
{ "status": "ok" }
```

### `GET /readyz`

No auth. Pings the DB with a 500ms timeout.

200 → `{ "status": "ok" }`. 503 → `{ "status": "not_ready", "error": "…" }`.

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
| `UPSTREAM_PROMETHEUS_AUTH_TOKEN` | yes | Will gate `/admin/metrics` (Phase 6) |
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
| `UPSTREAM_SENTRY_DSN` | no | Sentry exception reporting |

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

### Intentionally deferred (not in M8.0)

- `upstream-processor` repo, DeepStream runtime, YOLO/person detection, Triton
- package-registry / model-registry tables, package marketplace
- dashboard zones/lines UI, custom line-crossing, rules engine, billing
- WebSocket event delivery, the stale-session sweeper
