# Agent integration quickstart

The minimum surface you need to register an agent, register a camera, and start a stream session. For the full contract (every endpoint, every error code, every header) see [`api-contracts.md`](./api-contracts.md).

## How auth works

The agent holds a long-lived **API token** in plaintext form:

```
ups_<env>_<8-char id>_<22-char secret>
```

Send it on every request as `Authorization: Bearer <token>`. The token is scoped to a single account; you don't have to pass `account_id` anywhere — the server derives it from the token.

Get a token:
- **Production**: a dashboard owner/admin issues one via `POST /v1/api-tokens` (or the dashboard UI). The plaintext is shown **once**, then only the prefix + last-four are stored.
- **Local dev**: see [Local test setup](#local-test-setup) below — `scripts/seed_dev_token.py` mints one for you.

Tokens are revoked via `DELETE /v1/api-tokens/{id}`; from that point any agent using it gets `401 token_revoked`.

The session JWT in the SRT streamid (returned by `/v1/stream-sessions/start`) is separate — short-lived (4 h), single-camera, validated by the streaming server. You never have to inspect it; just pass the returned `srt_url` to ffmpeg.

## Endpoints

Base URL: `https://api.upstreamcv.com` in prod, `http://localhost:4000` in local dev.

### Register the agent

Call once on startup. Idempotent on `(account_id, name)` — re-running with the same name updates `last_seen_at` and capabilities.

```
POST /v1/agents/register
Authorization: Bearer ups_test_…
Content-Type: application/json

{
  "name": "kitchen-mac",
  "hostname": "macbook.local",
  "version": "0.1.0",
  "os": "darwin",
  "python_version": "3.11.5",
  "capabilities": { "ffmpeg": "7.0.1", "srt": true }
}
```

Response `200`:

```json
{
  "agent": {
    "id": "0b91a1a8-7d3d-4c91-9c6e-ce5b9f0e6e3e",
    "account_id": "00000000-0000-0000-0000-000000000001",
    "name": "kitchen-mac",
    "hostname": "macbook.local",
    "version": "0.1.0",
    "os": "darwin",
    "python_version": "3.11.5",
    "capabilities": { "ffmpeg": "7.0.1", "srt": true },
    "status": "active",
    "last_seen_at": "2026-05-13T02:00:00Z",
    "created_at": "2026-05-12T20:00:00Z",
    "updated_at": "2026-05-13T02:00:00Z"
  },
  "server_time": "2026-05-13T02:00:00Z"
}
```

Store `agent.id` locally — you'll pass it to camera register and use it in heartbeats.

### Load the agent (look it up)

```
GET /v1/agents/{agent_id}
GET /v1/agents
```

Both accept the same Bearer token. `GET /v1/agents` returns `{ "agents": [...] }` for the calling account; supports `?status=`, `?limit=` (1–200, default 50).

### Register a camera

Idempotent on `(agent_id, external_id)` when `external_id` is provided — re-registering with the same `external_id` updates the row, doesn't create a duplicate. Without `external_id` each call creates a new row.

```
POST /v1/cameras/register
Authorization: Bearer ups_test_…
Content-Type: application/json

{
  "agent_id": "0b91a1a8-7d3d-4c91-9c6e-ce5b9f0e6e3e",
  "name": "kitchen-cam",
  "external_id": "kitchen-1",
  "device_path": "/dev/video0",
  "default_width": 1280,
  "default_height": 720,
  "default_fps": 30,
  "default_codec": "h264",
  "default_bitrate_kbps": 3000,
  "default_latency_mode": "balanced",
  "metadata": {}
}
```

`default_latency_mode` is one of `"low" | "balanced" | "stable"` (default `"balanced"`):
- `low` — 80 ms SRT buffer; best for glass-to-glass latency, requires a healthy link.
- `balanced` — 200 ms; typical dashboard viewing.
- `stable` — 1500 ms; tolerates jitter and packet loss on weak Wi-Fi / cellular.

Response `200`: a full `CameraResponse` with `id`, `status`, etc. Errors you'll actually see:

| HTTP | code | When |
|---|---|---|
| 404 | `agent_not_found` | bad `agent_id` |
| 409 | `agent_account_mismatch` | `agent_id` belongs to a different account |
| 403 | `limit_exceeded.max_cameras` | new camera would exceed `account_limits.max_cameras` |
| 403 | `limit_exceeded.codec_not_allowed` | codec not in `account_limits.allowed_codecs` |
| 403 | `limit_exceeded.{fps,resolution,bitrate}_too_high` | requested defaults exceed account limits |

### Load cameras

```
GET /v1/cameras                       # list for this account
GET /v1/cameras?agent_id=<uuid>       # filter to one agent
GET /v1/cameras/{camera_id}           # one camera by id
```

### Start a stream session

```
POST /v1/stream-sessions/start
Authorization: Bearer ups_test_…
Content-Type: application/json

{
  "camera_id": "9c8c7e3e-c1a6-4d6c-bdda-b8c0f5a3d211",
  "requested": {
    "width": 1280,
    "height": 720,
    "fps": 30,
    "codec": "h264",
    "bitrate_kbps": 3000,
    "latency_mode": "balanced"
  }
}
```

Response `200`:

```json
{
  "session": {
    "id": "2b6f0ad4-e7c4-4d61-aa1e-a8d4c47e9b22",
    "account_id": "00000000-0000-0000-0000-000000000001",
    "agent_id": "0b91a1a8-7d3d-4c91-9c6e-ce5b9f0e6e3e",
    "camera_id": "9c8c7e3e-c1a6-4d6c-bdda-b8c0f5a3d211",
    "state": "approved",
    "srt_url": "srt://73.203.70.44:18890?streamid=publish:sess_4f7a2b9c3e1d8a05&latency=200",
    "streamid": "publish:sess_4f7a2b9c3e1d8a05",
    "expires_at": "2026-05-13T06:00:00Z",
    "started_at": null,
    "ended_at": null,
    "requested": { "width": 1280, "height": 720, "fps": 30, "codec": "h264", "bitrate_kbps": 3000, "latency_mode": "balanced" },
    "approved":  { "width": 1280, "height": 720, "fps": 30, "codec": "h264", "bitrate_kbps": 3000, "latency_mode": "balanced" }
  },
  "srt_url": "srt://73.203.70.44:18890?streamid=publish:sess_4f7a2b9c3e1d8a05&latency=200",
  "session_token": "eyJhbGciOiJIUzI1NiI…",
  "expires_at": "2026-05-13T06:00:00Z"
}
```

Pipe `srt_url` straight into ffmpeg — the agent never has to parse the JWT.

Errors you'll see here:

| HTTP | code | When |
|---|---|---|
| 404 | `camera_not_found` | bad `camera_id`, or camera disabled, or not in your account |
| 409 | `camera_already_streaming` | another active session already exists for this camera (`approved` or `streaming` state) |
| 403 | `limit_exceeded.max_concurrent_sessions` | account already at its concurrent-session cap |
| 403 | `limit_exceeded.{fps,resolution,bitrate,codec_not_allowed}_*` | requested params exceed account limits |

Stop the session when you're done:

```
POST /v1/stream-sessions/{session_id}/stop
```

Idempotent — calling on an already-`ended` session returns `200` with the current state.

### Load a session

```
GET /v1/stream-sessions/{session_id}
GET /v1/stream-sessions?camera_id=<uuid>&state=streaming
```

## Error envelope

Every non-2xx uses this shape:

```json
{
  "error": {
    "code": "limit_exceeded.max_cameras",
    "message": "Camera slot limit reached: 3 of 3.",
    "details": { "in_use": 3, "max": 3 },
    "request_id": "abc123def456…"
  }
}
```

`code` is stable across versions — switch on it, never on `message`. `request_id` matches the `X-Request-ID` response header; quote it when reporting bugs.

Common auth-layer codes:

| HTTP | code |
|---|---|
| 401 | `missing_credentials` — no `Authorization` header |
| 401 | `invalid_auth_scheme` — not `Bearer <token>` |
| 401 | `invalid_token` — unknown prefix or argon2 mismatch |
| 401 | `token_revoked` — token was revoked via `DELETE /v1/api-tokens/{id}` |
| 401 | `token_expired` — token's `expires_at` has passed |

## Local test setup

Three commands to get a working local API + a fresh dev token.

### 1. Start the local stack

You need [`uv`](https://github.com/astral-sh/uv) and the [`supabase` CLI](https://supabase.com/docs/guides/cli).

```bash
# In the upstream-api-server checkout:
make install         # uv sync
make db-start        # supabase start — boots local Postgres on :54322
make db-push         # apply migrations
make dev             # uv run uvicorn ... --reload  → http://localhost:4000
```

`make db-start` prints the local Supabase API URL, anon key, service role key, and JWT secret. Copy them into a local `.env` at the repo root (or in `deploy/.env` if you're running via docker-compose):

```bash
UPSTREAM_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:54322/postgres
UPSTREAM_SUPABASE_URL=http://127.0.0.1:54321
UPSTREAM_SUPABASE_ANON_KEY=<from supabase start>
UPSTREAM_SUPABASE_SERVICE_ROLE_KEY=<from supabase start>
UPSTREAM_SUPABASE_JWT_SECRET=<from supabase start>
UPSTREAM_SESSION_JWT_SECRET=local-dev-session-secret-min-32-bytes
UPSTREAM_STREAMID_HMAC_KEY=local-dev-streamid-key-min-32-bytes
UPSTREAM_MEDIAMTX_WEBHOOK_SECRET=local-dev-mediamtx
UPSTREAM_PROCESSOR_WEBHOOK_SECRET=local-dev-processor
UPSTREAM_PROMETHEUS_AUTH_TOKEN=local-dev-prom
UPSTREAM_ENVIRONMENT=development
```

### 2. Mint a local dev token

`scripts/seed_dev_token.py` issues a fresh `ups_test_…` token and prints the matching SQL insert. It prints the plaintext **to stderr** (so you can capture it) and the SQL to stdout (so you can pipe it into psql):

```bash
uv run python scripts/seed_dev_token.py \
  | psql "$(supabase status --output env | grep DB_URL | cut -d= -f2- | tr -d '\"')"
```

stderr will show something like:

```
-- Save this plaintext token — it will not be retrievable again:
-- ups_test_AB12CDEF_FULLSECRET22CHARSWXYZ
-- (prefix: ups_test_AB12CDEF, last_four: WXYZ)
```

Grab that plaintext. The token belongs to the fixed dev account `00000000-0000-0000-0000-000000000001` (seeded with `max_cameras=1`, `max_concurrent_sessions=1`, `max_fps=30`, `max_width=1920`, `max_height=1080`, `max_bitrate_kbps=6000`, `allowed_codecs={h264}` by default — bump these in `account_limits` if you need more headroom for tests).

> The token is **not** committed to the repo because it's a real credential. Mint a fresh one whenever you reset your DB.

### 3. Smoke-test it

`scripts/smoke_local.py` walks the full happy path (register agent → register camera → start session → decode the JWT → stop session):

```bash
UPSTREAM_API_URL=http://localhost:4000 \
UPSTREAM_DEV_TOKEN=ups_test_AB12CDEF_FULLSECRET22CHARSWXYZ \
uv run python scripts/smoke_local.py
```

Expected output:

```
  ok  agent_id=0b91a1a8-7d3d-4c91-9c6e-ce5b9f0e6e3e
  ok  camera_id=9c8c7e3e-c1a6-4d6c-bdda-b8c0f5a3d211
  ok  srt_url=srt://73.203.70.44:18890?streamid=publish:sess_4f7a2b9c3e1d8a05&latency=200…
  ok  stream_key=sess_4f7a2b9c3e1d8a05
  ok  jwt claims session_id=2b6f0ad4-e7c4-4d61-aa1e-a8d4c47e9b22
  ok  session stopped
PASS
```

If it fails on `agent register: 401 …` your token isn't in the DB (re-run the seed); if it fails on `camera register: 403 limit_exceeded.*` bump the relevant column in `account_limits` for `00000000-0000-0000-0000-000000000001`.

### Hitting staging instead

If you'd rather test against the deployed staging API and skip the local stack: point at `https://api.upstreamcv.com` and use a token minted via the dashboard. The smoke script works unchanged:

```bash
UPSTREAM_API_URL=https://api.upstreamcv.com \
UPSTREAM_DEV_TOKEN=ups_live_… \
uv run python scripts/smoke_local.py
```

Note: against staging you can't decode the session JWT locally (the JWT secret isn't on your laptop), so the script's step 4 will fail at `verify_session_token`. Steps 1-3 and 5 still exercise the real network path.
