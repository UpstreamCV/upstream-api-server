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
    "srt_url": "srt://ingest.upstream.tv:8890?streamid=publish:<jwt>&latency=200",
    "streamid": "publish:<jwt>",
    "expires_at": "2026-05-13T00:00:00Z",
    "started_at": null,
    "ended_at": null,
    "requested": { "width": 1280, "height": 720, "fps": 30, "codec": "h264", "bitrate_kbps": 3000 },
    "approved":  { "width": 1280, "height": 720, "fps": 30, "codec": "h264", "bitrate_kbps": 3000 }
  },
  "srt_url": "srt://ingest.upstream.tv:8890?streamid=publish:<jwt>&latency=200",
  "session_token": "<jwt>",
  "expires_at": "2026-05-13T00:00:00Z"
}
```

Errors: `camera_not_found`, `camera_already_streaming`, `limit_exceeded.*`.

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

The token embedded in SRT streamids:

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

Signed HS256 with `UPSTREAM_SESSION_JWT_SECRET`. TTL is `UPSTREAM_SESSION_TOKEN_TTL_SECONDS` (default 14400 = 4h). The streaming server enforces the `lim` claim as the contract for what the agent is allowed to stream.

## SRT streamid format

Ingest connections set `streamid=publish:<jwt>` (the literal prefix `publish:` followed by the JWT). The `publish:` prefix is reserved for ingest; subscribe-style connections will use a different prefix in Phase 4/5.

## Environment variables (consumed by this service)

| Variable | Required | Purpose |
|---|---|---|
| `UPSTREAM_DATABASE_URL` | yes | Direct Postgres connection (use port 5432 for long-running, 6543 for pooled) |
| `UPSTREAM_SUPABASE_URL` | yes | Supabase project URL (used when supabase-py client is wired up) |
| `UPSTREAM_SUPABASE_ANON_KEY` | yes | Supabase anon key (currently unused in hot path) |
| `UPSTREAM_SUPABASE_SERVICE_ROLE_KEY` | yes | Supabase service role key (for future storage uploads) |
| `UPSTREAM_SUPABASE_JWT_SECRET` | yes | Verifies dashboard user JWTs |
| `UPSTREAM_SESSION_JWT_SECRET` | yes | Signs session JWTs (independent of Supabase) |
| `UPSTREAM_STREAMID_HMAC_KEY` | yes | Reserved for the compact streamid form (currently unused; we embed the full JWT) |
| `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` | yes | Verifies `/v1/ingest/*` callers |
| `UPSTREAM_PROCESSOR_WEBHOOK_SECRET` | yes | Verifies `/v1/processor/*` callers |
| `UPSTREAM_PROMETHEUS_AUTH_TOKEN` | yes | Will gate `/admin/metrics` (Phase 6) |
| `UPSTREAM_PORT` | no | HTTP port (default 4000) |
| `UPSTREAM_ENVIRONMENT` | no | `development` \| `staging` \| `production` (default `development`) |
| `UPSTREAM_LOG_LEVEL` | no | `DEBUG`/`INFO`/`WARNING`/`ERROR` (default `INFO`) |
| `UPSTREAM_SESSION_TOKEN_TTL_SECONDS` | no | Default 14400 |
| `UPSTREAM_DB_POOL_MIN_SIZE` | no | Default 5 |
| `UPSTREAM_DB_POOL_MAX_SIZE` | no | Default 20 |
| `UPSTREAM_ALLOWED_ORIGINS` | no | CSV list of CORS origins (default `http://localhost:3000`) |
| `UPSTREAM_SENTRY_DSN` | no | Sentry exception reporting |

## Future endpoints (planned)

### Phase 2 — streaming server callbacks (auth: `current_ingest`)
- `POST /v1/ingest/validate-stream` — synchronous; returns whether to accept the streamid and what the expected stream params are.
- `POST /v1/ingest/stream-started` — transitions session to `streaming` after ffprobe match.
- `POST /v1/ingest/stream-ended` — transitions session to `ended`; writes duration.
- `POST /v1/ingest/validation-failed` — transitions session to `failed`.

### Phase 3 — agent telemetry (auth: `current_agent`)
- `POST /v1/heartbeats` — batched, up to 100 per call.
- `POST /v1/snapshots` — multipart upload of a JPEG frame; quota-checked.

### Phase 4 — dashboard reads (auth: `current_user`)
- `GET /v1/agents`, `/v1/cameras`, `/v1/stream-sessions`, `/v1/heartbeats` — cursor-paginated, filtered by `account_id`.

### Phase 5 — processor (auth: `current_processor`)
- `GET /v1/processor/sessions/active`
- `POST /v1/processor/sessions/{id}/status`
