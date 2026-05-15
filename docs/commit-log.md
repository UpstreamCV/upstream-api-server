# Commit log

Reverse chronological. Newest at top. Every meaningful commit gets an entry following the template at the bottom of this file.

---

## 2026-05-15 - M5.1: add report-only media validation endpoints

### Summary
Two additive ingest endpoints for the streaming-server's M5.1
observe/report path. Neither mutates session state.

### What changed
- `GET /v1/ingest/expected/{session_id}` — state-agnostic per-session
  expected-config lookup. Returns the same `expected` block
  `validate-stream` returns on accept (`codec`, `width`, `height`,
  `fps`, `bitrate_kbps`, `latency_mode`), plus `account_id`,
  `agent_id`, `camera_id`, `state`, `expires_at`. Webhook-secret-gated
  via the existing `CurrentIngest` dep. Unlike `validate-stream` it
  does **not** state-gate: the streaming-server's `runOnReady` hook
  may fire after a reconnect into an already-`streaming` session and
  still needs the limits to run its comparator.
- `POST /v1/ingest/stream-validations` — report-only validation row
  insert. Accepts both `passed=true` and `passed=false`; in both
  cases it ONLY inserts a `stream_validations` row. It does NOT call
  `_sync_camera_status`, does NOT UPDATE `stream_sessions`, does NOT
  call `deprovision_path`. M5.2 will route mismatches to the existing
  `/validation-failed` endpoint for enforcement; until then this
  endpoint lets the dashboard surface mismatches while preview keeps
  playing.

### Files
- `src/upstream_api/schemas/ingest.py` — add `ExpectedConfigResponse`
  and `StreamValidationReportRequest`; reuse `ObservedStream`.
- `src/upstream_api/services/stream_validation.py` — add
  `insert_validation_report()` helper. Existence-checks the session
  and inserts a single `stream_validations` row.
- `src/upstream_api/routers/ingest.py` — add the two handlers and
  the `UUID` import (path-param needs it imported at module scope
  for `from __future__ import annotations` to resolve at FastAPI
  registration time).
- `tests/test_ingest_stream_validations.py` — 9 new tests:
  - GET: returns expected for `approved` (happy path)
  - GET: returns expected for `streaming` (state-agnostic — the M5.1
    -critical case)
  - GET: 404 on missing session
  - GET: 401 on missing/wrong webhook secret (x2)
  - POST: `passed=true` inserts a row, state untouched
  - POST: `passed=false` inserts a row with mismatch_fields, state
    STILL untouched
  - POST: 401 on missing webhook secret
  - POST: 404 on missing session

### Test runs
`uv run pytest` — 195/195 pass.
`uv run ruff check src tests` — clean.
`uv run ruff format --check src tests` — clean.
`uv run mypy src` — clean (52 source files).

### Strict non-goals
- No DB migration; reuses `stream_validations` from
  `20260512130300_stream_sessions.sql`.
- No change to existing routes (validate-stream, stream-started,
  stream-ended, validation-failed, lookup-session, mediamtx-auth).
- No dashboard changes (TS types + ValidationPanel were already
  wired since the M4-era validations endpoint).
- The streaming-server side ships in a paired commit in
  `upstream-streaming-server`.

---

## 2026-05-14 - Enable Supabase Realtime on stream_sessions and cameras

### Summary
New migration `20260514120000_realtime_publication.sql` adds the
`stream_sessions` and `cameras` tables to Supabase's built-in
`supabase_realtime` publication. With this in place, the dashboard can
subscribe to row changes via the browser's Supabase client instead of
polling `/v1/stream-sessions` every 5 s — cutting "agent connect → live
indicator" latency from up to 5 s down to ~50–200 ms (broadcast hop).

### Why
The api-server is the only writer to these tables (service-role pool;
RLS is `select`-only for end users), and RLS already restricts which
rows a member can read. Adding the tables to the publication means
Supabase Realtime tails the WAL and forwards inserts/updates to
subscribed clients, automatically filtering per-client against the
same RLS policies the api-server honors. No new auth surface, no
client-visible service-role key.

The companion dashboard commit (`upstream-dashboard`
`docs/commit-log.md` 2026-05-14) replaces `camera-detail-polling.tsx`
with `camera-detail-realtime.tsx`.

### Files touched
- `supabase/migrations/20260514120000_realtime_publication.sql` (NEW) —
  idempotent `alter publication supabase_realtime add table ...` for
  both tables, guarded by a `pg_publication_tables` existence check so
  rerunning the migration (or applying it to a project where the
  tables were already enabled by hand in the Supabase dashboard) is a
  no-op.

### Verification
`uv run pytest -q` → 145 passing (no new tests; the migration is
schema-only and doesn't touch any code path under test).

After the migration applies in production, sanity-check with:
```sh
psql "$UPSTREAM_DATABASE_URL" -c "
  select schemaname, tablename
    from pg_publication_tables
   where pubname = 'supabase_realtime'
     and tablename in ('stream_sessions', 'cameras')
   order by tablename;
"
# expect: two rows.
```

### Deploy
Auto-applies on push to `main` via Supabase's GitHub integration. No
api-server redeploy needed (no Python changes).

---

## 2026-05-14 - WHEP proxy: MediaMTX 1.9.3 uses /whep/<id>, not /whepsession/<id>

### Summary
The WHEP proxy was issuing `POST` to MediaMTX, getting a `201` back with
`Location: /<path>/whep/<uuid>`, and then failing to parse the resource id
because `_LOCATION_RE` required the literal substring `whepsession`.
Result: dashboard saw a `502 upstream_unavailable` on every WHEP attempt
even though MediaMTX was happy. The DELETE path had the same bug — it
built `/<path>/whepsession/<id>` URLs that would 404 against a real
MediaMTX 1.9.3.

### Fix
- `_LOCATION_RE` now matches `/whep(?:session)?/<id>` — both the
  MediaMTX 1.9.3+ form and the legacy `<= 1.8` form.
- `proxy_whep_delete` issues `DELETE /<path>/whep/<id>` to match the
  POST Location header MediaMTX returns.

Verified against a real `upstream-streaming-server` running
`bluenviron/mediamtx:1.9.3`:
```
Location: /sess_1d937c92aa231041/whep/3c4b69c1-ec6a-4f6e-b358-e89e86a16fc1
```

### Files touched
- `src/upstream_api/services/whep_proxy.py` — regex + DELETE URL.
- `tests/test_whep_proxy.py` — flipped 9 occurrences of `whepsession` to
  `whep` to match the new contract; added one new test asserting the
  legacy `/whepsession/` form is still accepted by the parser.

### Verification
`uv run pytest -q` → 145 passing (+1 for the legacy-form parser test).

### Deploy
```sh
cd ~/upstream-api-server && git pull --ff-only
cd deploy && docker compose up -d --build api
```

---

## 2026-05-15 - Add /start router integration tests (provision + failure paths)

### Summary
Phase 2 had unit tests for `provision_path` (the HTTP wrapper) and for
`start_session` (the DB layer), but the **router glue** that calls
`provision_path` AFTER the DB transaction commits — and translates a
`MediaMTXProvisioningFailure` into `mark_failed` + a 502 — was untested.
Added `tests/test_stream_sessions_router.py` with 4 cases:

- Happy path — `/start` returns 200; respx confirms POST `add` was
  called with the per-session passphrase + lifecycle hooks; PATCH was
  NOT called.
- MediaMTX 5xx — `/start` returns 502 `upstream_unavailable`; the
  session row gets `mark_failed`'d (verified via the captured
  `stream_validations` insert).
- MediaMTX transport error — same 502 path, no `httpx` exception
  leaks.
- POST→PATCH fallback — when MediaMTX returns 400 (path already
  exists), `/start` still returns 200; both POST and PATCH fire.

### Why
Closes the audit gap from `~/.claude/plans/luminous-coalescing-goblet.md`
A3. A future refactor could plausibly accidentally move the
`provision_path` call inside the DB transaction (rollback on MediaMTX
failure), or forget the `mark_failed` on failure, or change the error
code — without these tests, the only signal would be a production 5xx.

### Files touched
- `tests/test_stream_sessions_router.py` (NEW) — 4 tests, ~280 lines.

### Verification
`uv run pytest -q` → 186 passing (was 182, +4).

---

## 2026-05-15 - mediamtx_provisioning: POST→PATCH fallback (PATCH alone 404s)

### Fix
The Phase 2 commit assumed `PATCH /v3/config/paths/patch/{name}` was
idempotent against MediaMTX 1.9.3. It isn't — PATCH 404s on a missing
path; POST `add/{name}` 400s on an existing one. Verified live against
the deployed MediaMTX. Switched `provision_path` to POST first (the
common case: each `sess_<hex>` is unique), with a PATCH fallback on a
400 conflict (the retry case: orphan path from a prior failed deprovision,
or api-server retried provisioning). Test suite updated to assert both
paths exercise correctly and that PATCH is NOT called on first-time
success.

`uv run pytest -q` → 182 passing.

---

## 2026-05-14 - Phase 2 / FU-6: per-session SRT passphrase via MediaMTX path provisioning

### Summary
Each `stream_sessions` row now mints its own AES-256 passphrase at
`/v1/stream-sessions/start`, embeds it in the returned SRT URL
(`&passphrase=<X>&pbkeylen=32`), and PATCHes a matching MediaMTX path
via the streaming-server's HTTP API so the SRT handshake actually
negotiates encryption. On stop/end/failed the api-server DELETEs the
path. Closes FU-6.

### Why
The agent → MediaMTX hop was previously plaintext UDP. WebRTC playback
(MediaMTX → browser) is already DTLS-SRTP encrypted, so this work is
strictly about the publish hop. With per-session keys, a leaked
passphrase only exposes that one session.

### Files touched
- `supabase/migrations/20260514130000_session_passphrase.sql` (NEW) —
  adds nullable `srt_publish_passphrase text` column. Pre-existing rows
  stay NULL; new rows always populate. Cheap migration.
- `src/upstream_api/config.py` — new `mediamtx_api_base_url` setting
  (env `UPSTREAM_MEDIAMTX_API_BASE_URL`). Default `http://localhost:9997`
  for tests; production is `http://ingest.upstreamcv.com:19997` (the
  host-mapped MediaMTX admin port, firewall-allowed for the api-server's
  Elastic IP and `authHTTPExclude`'d for `action: api`).
- `src/upstream_api/services/mediamtx_provisioning.py` (NEW) — thin
  `httpx.AsyncClient` wrapper around `PATCH /v3/config/paths/patch/{name}`
  and `DELETE /v3/config/paths/delete/{name}`. PATCH is idempotent;
  DELETE treats 404 as success (MediaMTX restarted, path gone). Typed
  `MediaMTXProvisioningFailure` on transport/non-2xx so callers can
  distinguish failure modes.
- `src/upstream_api/services/session_lifecycle.py` — `_build_srt_url`
  signature gains `passphrase: str | None`; when set, appends
  `&passphrase=<url-encoded>&pbkeylen=32`. `start_session` mints via
  `secrets.token_urlsafe(24)` (~32 chars, inside libsrt's [10, 79]
  range), stores in DB, returns it on `StartSessionResult.srt_publish_passphrase`.
- `src/upstream_api/services/stream_validation.py` — `SessionStateRow`
  gains a `streamid` field so `mark_ended`/`mark_failed` callers can
  drive the per-session path teardown without a second DB query.
- `src/upstream_api/routers/stream_sessions.py` — `start` calls
  `provision_path` after the DB txn commits; on `MediaMTXProvisioningFailure`
  marks the session `failed` with `failure_code='mediamtx_provision_failed'`
  in a fresh txn and returns 502 (`upstream_unavailable`). `stop` calls
  `deprovision_path` after the txn (best-effort, log on failure).
- `src/upstream_api/routers/ingest.py` — `stream-ended` and
  `validation-failed` call `deprovision_path` after the txn (best-effort).
- `tests/test_mediamtx_provisioning.py` (NEW) — 9 `respx` cases:
  happy PATCH, trailing-slash base-url normalization, PATCH 4xx/5xx/transport
  → typed failure, happy DELETE, DELETE 404 (success), DELETE other-4xx
  → failure, DELETE transport-error → failure.
- `tests/test_session_lifecycle.py` — added passphrase + pbkeylen
  assertions to `test_start_session_srt_url_uses_settings_host_and_port`.
- `tests/test_ingest_mediamtx_auth.py`, `tests/test_ingest_validate.py`,
  `tests/test_stream_validation.py` — `_session_row` fixtures gain
  `streamid` to match the widened `SessionStateRow`.
- `tests/test_app_routes.py` — adds `/v1/ingest/mediamtx-auth/{secret}:POST`
  to the expected mounted routes.

### Verification
`uv run pytest -q` → 181 passing (was 172; +9 provisioning tests).

### Deploy
1. Push migration first (auto-applies via Supabase). Column is nullable
   so the api-server keeps working on existing rows during the gap
   between migration apply and api-server rebuild.
2. Push api-server. New sessions get passphrases + paths.
3. On crowdcounts2: bump mediamtx.yml to remove the catch-all
   `paths.all_others` block (every legitimate path is now explicitly
   provisioned by the api-server; unknown streamids fail authHTTP +
   no longer fall back to the permissive catch-all). `docker compose
   restart mediamtx`.

### Known limitation
MediaMTX's path config is in-memory. A streaming-server restart loses
all dynamic paths; in-flight sessions break until the agent calls
`/start` again (which re-provisions). Tracked as FU-7. Mitigation: a
periodic reconciler on the api-server. Deferred.

---

## 2026-05-14 - stop_session: mirror cameras.status to 'idle' (gap in prior commit)

### Summary
`stop_session` (used by `POST /v1/stream-sessions/{id}/stop`) does its own
inline UPDATE rather than calling `mark_ended`, so it skipped the
`cameras.status` mirror added in the previous commit. An agent-driven /stop
(or stale-session cleanup) would correctly transition the session row to
`ended` but leave `cameras.status` stuck on its prior value. Fixed by
adding the same `update cameras` statement inline in `stop_session` so the
agent path matches the streaming-server-hook path.

### Files touched
- `src/upstream_api/services/session_lifecycle.py` — added inline
  `update cameras set status='idle'` inside the existing `if state in
  ('approved','streaming'):` block.

### Verification
`uv run pytest -q` → 144 passing (no new tests; existing
`session_lifecycle` coverage doesn't include stop_session and adding a
FakeConn just for this would be larger than the fix).

### Deploy
```sh
cd ~/upstream-api-server && git pull --ff-only
cd deploy && docker compose up -d --build api
```

---

## 2026-05-13 - Sync cameras.status with stream lifecycle + add ingest lookup-session endpoint

### Summary
Two changes that together fix "camera shows Idle and stream cannot be viewed" when an agent is publishing:
1. `mark_streaming` / `mark_ended` / `mark_failed` now mirror their session-state transitions onto `cameras.status` (`'streaming'` / `'idle'` / `'error'`) inside the same transaction, so the dashboard's cameras list view (which reads `cameras.status` directly) tracks live streams instead of staying `'idle'` forever.
2. New `POST /v1/ingest/lookup-session` endpoint resolves a streamid (`publish:sess_<hex>`) to its session identifiers (`session_id`, `account_id`, `agent_id`, `camera_id`, `state`). Used by the streaming-server's `runOnReady` / `runOnNotReady` MediaMTX hooks to translate the streamid they receive into the `session_id` they need to call `stream-started` / `stream-ended`. Independent of `validate-stream` (which gates connection acceptance and only succeeds while the session is `'approved'`).

`mark_streaming` is now idempotent for the streaming→streaming reconnect case (SRT publisher reconnects re-fire `runOnReady`) — the `cameras.status` mirror still runs but no duplicate `stream_validations` row is written. `mark_ended` continues to compute duration server-side from the session row's `started_at`; the `stream-ended` router endpoint now uses that server-derived duration for usage billing instead of the streaming-server's request body, since hooks generally cannot track started_at across script invocations. `StreamEndedRequest.duration_seconds` is therefore now optional.

### Why
The streaming-server's M4 webhook integration is shipping in the sibling repo at the same time. Once those hooks land, sessions transition `approved` → `streaming` → `ended` correctly, but two pieces were still missing here:
- Nothing was syncing `cameras.status` with `stream_sessions.state`. The cameras list view at `/dashboard/cameras` reads `cameras.status` directly via `<StatusBadge>` (`cameras-table.tsx:49`), so even after sessions transitioned correctly, the list showed every camera as "Idle".
- The streaming-server hooks need to translate the streamid they see (`MTX_PATH = sess_<hex>`) into a `session_id` to call `/v1/ingest/stream-started`. `validate-stream` cannot serve this purpose — it returns `accept: false / stream_state_invalid` once the session leaves `'approved'`, so it would reject the lookup at `runOnNotReady` time.

### Files touched
- `src/upstream_api/services/stream_validation.py` — added private `_sync_camera_status` helper. `mark_streaming` rewritten to disambiguate "row missing" vs "already streaming reconnect" vs "wrong state" without the generic `_explain_unexpected_state` helper, so the streaming reconnect case becomes a no-op (cameras still nudged, no duplicate validation row). `mark_ended` and `mark_failed` call the helper after their UPDATE.
- `src/upstream_api/schemas/ingest.py` — added `LookupSessionRequest` / `LookupSessionResponse`. Made `StreamEndedRequest.duration_seconds` optional.
- `src/upstream_api/routers/ingest.py` — added `POST /v1/ingest/lookup-session`. `stream_ended` handler now prefers server-derived duration from `mark_ended` over the request body field for the `record_event` quantity.
- `tests/test_stream_validation.py` — `FakeConn.execute` now captures `update cameras` calls in a separate list. New `test_mark_streaming_idempotent_on_streaming_reconnect`. Existing transition tests assert the `cameras.status` mirror happens; rejection tests assert it does NOT.
- `tests/test_ingest_validate.py` — three new tests for `lookup-session` (happy path returns all four IDs + state, 404 on unknown, 401 without webhook secret).
- `tests/test_app_routes.py` — added `/v1/ingest/lookup-session:POST` to the expected mounted-routes set.

### Verification
`uv run pytest -q` → 144 passing (was 134; +3 lookup-session, +1 reconnect idempotency, +6 camera-status-mirror assertions on existing tests).

### Deploy
Code-only; no env changes. AWS deploy currently behind on `e8d08ba` ("fixing streaming session start time and heartbeat") — pulling this commit also brings that one. Confirm `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET` is set on the server (the streaming-server hooks will start using it):
```sh
cd ~/upstream-api-server && git pull --ff-only
cd deploy && docker compose up -d --build api
```

---

## 2026-05-13 - WHEP proxy endpoints for read-path auth

### Summary
Adds two endpoints (`POST /v1/stream-sessions/{id}/whep` + `DELETE /v1/stream-sessions/{id}/whep/{resource_id}`) that proxy WebRTC signaling between authenticated dashboard users and MediaMTX. The api-server enforces account-scoped auth via the existing `CurrentCaller` dependency; UDP/ICE media still flows browser ↔ MediaMTX directly. Closes FU-3 from `../upstream-streaming-server/docs/follow-ups.md`.

### Why
The dashboard's previous-commit WHEP player hit MediaMTX directly, which has no native read auth (`authInternalUsers` is permissive by design). Routing signaling through the api-server gives us auth for free — same pattern as every other read endpoint — without coordinating a new shared secret across three repos.

### Files touched
- `src/upstream_api/routers/stream_sessions.py` — added `whep_offer` and `whep_delete` handlers. Both look up the session row by id, verify `account_id` matches the caller's scope (returns 404 `session_not_found` on mismatch — never leaks existence), and on POST also verify state is `streaming` (409 `session_not_streaming` otherwise). Defensive regex `^[A-Za-z0-9_~-]{1,128}$` on `resource_id` to prevent path-traversal into other MediaMTX endpoints.
- `src/upstream_api/services/whep_proxy.py` (NEW) — pure I/O wrapper around `httpx.AsyncClient`: `proxy_whep_offer`, `proxy_whep_delete`, `parse_mediamtx_location`. Raises `UpstreamMediaMTXUnavailable` on transport failure for the router to translate to 502.
- `src/upstream_api/services/http_client.py` (NEW) — `build_http_client()` for lifespan, `get_http_client()` + `HttpClientDep` for the router. `httpx.AsyncClient(connect=2.0, read=10.0, ...)`.
- `src/upstream_api/app.py` — lifespan now creates and closes `app.state.http_client` alongside the DB pool.
- `src/upstream_api/config.py` — new setting `mediamtx_base_url: str = "http://localhost:8889"` (env `UPSTREAM_MEDIAMTX_BASE_URL`).
- `src/upstream_api/errors.py` — new subclasses `BadRequestError`, `UnsupportedMediaTypeError`, `BadGatewayError` (filled in the 400/415/502 gaps in the hierarchy).
- `tests/test_whep_proxy.py` (NEW) — 27 cases via `respx`: happy path, agent caller dual-auth, cross-account 404 (no info leak), unknown session, all four non-streaming states, wrong content type, empty body, MediaMTX 404/5xx/timeout/no-Location, plus DELETE happy path, invalid resource id, cross-account, MediaMTX 404 still returns 204. Also unit tests for `parse_mediamtx_location`.
- `scripts/smoke_whep_proxy.py` (NEW) — end-to-end smoke script that hits a running api-server with a real agent token; bootstraps a session, exercises POST/DELETE, and verifies the 400 invalid-resource-id negative path. Falls back to verifying the 409 not-streaming path when no publisher is running.
- `docs/api-contracts.md` — new "WHEP proxy" section under stream-sessions documenting both endpoints, all error codes, and the `UPSTREAM_MEDIAMTX_BASE_URL` config var. Replaced the older "follow-up: add a `playback` object" note since the proxy supersedes that approach.

### Verification
`pytest` → 134 passing (27 new in `test_whep_proxy.py`, 107 existing untouched).

### Deploy
Requires the new env var to be set on the deploy target:
```
UPSTREAM_MEDIAMTX_BASE_URL=http://upstream-streaming-server:8889  # private network DNS
```
**Production firewall posture:** MediaMTX's `8889/tcp`, `8554/tcp`, and `9997/tcp` MUST be reachable only from the api-server's egress IP. The UDP ICE port `8189/udp` stays public so browsers can negotiate ICE. See `../upstream-streaming-server/docs/stream-auth.md` (Read-path auth section).

---

## 2026-05-14 - Default SRT ingest host to ingest.upstreamcv.com

### Summary
DNS now points `ingest.upstreamcv.com` at `73.203.70.44` (the same Comcast box running MediaMTX). Switching the API's default `srt_ingest_host` from the raw IP to the hostname removes the IP from agent-visible URLs and means the next time the streaming server moves boxes we just update DNS — no code commit, no API redeploy. Env-var override (`UPSTREAM_SRT_INGEST_HOST`) still works for dev/staging that point at a local IP.

### Files touched
- `src/upstream_api/config.py` — default `srt_ingest_host` flipped to `"ingest.upstreamcv.com"`.
- `tests/test_session_lifecycle.py` — regression test now overrides to `127.0.0.1:9999` (visibly non-default) and asserts neither the new nor old hostname leaks into the URL.
- `deploy/.env.production.example` — commented-out SRT block updated to reference the new default.
- `docs/api-contracts.md`, `docs/agent-quickstart.md` — example `srt_url` strings and env-var table default now reference `ingest.upstreamcv.com:18890`.

### Deploy
Code-only change; no .env edit needed (server has no `UPSTREAM_SRT_INGEST_HOST` set, so the new default applies):
```sh
cd ~/upstream-api-server && git pull --ff-only
cd deploy && docker compose up -d --build api
docker exec upstream-api-api-1 python -c "from upstream_api.config import get_settings; print(get_settings().srt_ingest_host)"
# expect: ingest.upstreamcv.com
```

### How to verify
```sh
uv run pytest tests/test_session_lifecycle.py::test_start_session_srt_url_uses_settings_host_and_port -q
# After deploy, /v1/stream-sessions/start should return:
#   srt://ingest.upstreamcv.com:18890?streamid=publish:sess_<hex>&latency=...
```

### Note on the streaming server's IP
`73.203.70.44` is a residential Comcast range. If it isn't actually static, DNS will go stale at the next lease rotation — set up a DDNS updater on the streaming-server box or pin a static IP through Comcast Business. UDP `18890` on that IP must continue to be forwarded to whichever host runs MediaMTX.

---

## 2026-05-14 - Point SRT ingest at the real streaming server (73.203.70.44:18890)

### Summary
After the streamid fix shipped, `upstream run` was still failing because the API kept advertising `srt://ingest.upstream.tv:8890?...` — a hostname we don't own; it was a placeholder that survived from early scaffolding. The actual streaming server is at `73.203.70.44:18890`. Root cause: `_build_srt_url` had host/port hardcoded as default kwargs, the caller never overrode them, and there was no settings hook at all.

Fix is two parts: (1) add `srt_ingest_host` / `srt_ingest_port` to `Settings` so dev/staging/prod can each point at a different streaming server without code changes, and (2) set the *defaults* to the real server (`73.203.70.44:18890`) so the running box works out of the box — no operator env step needed on this deploy.

### Files touched
- `src/upstream_api/config.py` — added `srt_ingest_host: str = "73.203.70.44"` and `srt_ingest_port: int = 18890`.
- `src/upstream_api/services/session_lifecycle.py` — `_build_srt_url` now takes `ingest_host` / `port` as required kwargs; `start_session` plumbs them from `settings`.
- `tests/test_session_lifecycle.py` — new regression test `test_start_session_srt_url_uses_settings_host_and_port`: overrides both via `model_copy` and asserts the resulting `srt_url` starts with `srt://73.203.70.44:18890?streamid=publish:sess_` and contains no `ingest.upstream.tv`.
- `deploy/.env.production.example` — added a commented-out SRT section; defaults are correct so the keys stay optional.
- `docs/api-contracts.md` — env-var table now lists `UPSTREAM_SRT_INGEST_HOST` / `_PORT` with their real defaults, and the example `srt_url` strings reference `73.203.70.44:18890`.
- `docs/agent-quickstart.md` — example response and smoke-test output updated to show the real host:port plus the new `publish:sess_<hex>` streamid shape (the old examples still showed the JWT-bearing form from before the streamid fix).

### Deploy
No operator env step required — defaults match the live streaming server. On `18.225.104.21`:
```sh
cd ~/upstream-api-server
git pull --ff-only
cd deploy && docker compose up -d --build api
```

### How to verify
```sh
uv run pytest tests/test_session_lifecycle.py -q
# After deploy, call /v1/stream-sessions/start and confirm:
#   srt_url starts with `srt://73.203.70.44:18890?streamid=publish:sess_`
# Then re-run `upstream run`; ffmpeg should connect to the real ingest.
```

---

## 2026-05-13 - Apply ruff format to test_ingest_validate.py

### Summary
Follow-up to the streamid commit — CI's `ruff format --check` flagged `tests/test_ingest_validate.py` because the `_session_row` signature fit on one line after I rewrote the helper. Ran `uv run ruff format src tests` and committed the (single-line) diff. No behavioural change; only `ruff format --check` was failing in CI — `ruff check`, `mypy`, and `pytest` were all green locally and remain so.

### Files touched
- `tests/test_ingest_validate.py` — `_session_row(*, session_id, state, expires_at)` rejoined onto one line.

### How to verify
```sh
uv run ruff check src tests && \
  uv run ruff format --check src tests && \
  uv run mypy src tests && \
  uv run pytest -q
```

---

## 2026-05-13 - Use a short opaque key in SRT streamid instead of the session JWT

### Summary
`upstream run` reached the API-issued SRT URL but FFmpeg rejected it with `libsrt Query string option 'streamid' value is too long / Invalid argument`. Root cause: `POST /v1/stream-sessions/start` was packing the full session JWT into `streamid=publish:<jwt>...`. A signed JWT carrying the `lim` claim object (acc/sub/cam/sid/jti/iat/exp + nested limits) blows past libsrt's ~512-byte streamid cap. A dev override with `streamid=publish:agenttest` worked, which is what surfaced the size issue.

Switched ingest to a short opaque scheme: `streamid=publish:sess_<16 hex>`. The key is minted per session, stored in the existing unique-indexed `stream_sessions.streamid` column, and `/v1/ingest/validate-stream` now looks the session row up by the full streamid value instead of decoding/verifying a JWT. The session JWT is still minted and returned at the top level of the response (`session_token`) for any non-ingest caller, but it no longer participates in the SRT URL.

Net streamid size: `publish:sess_<16 hex>` = 29 bytes (was several hundred for a signed JWT).

### Files touched
- `src/upstream_api/security/streamid.py` — replaced JWT-wrapping helper with `mint_stream_key()` (returns `sess_<16 hex>`) and renamed `extract_session_token` → `extract_stream_key`. Updated module docstring to spell out the libsrt size constraint that motivates the design.
- `src/upstream_api/services/session_lifecycle.py` — `start_session` now generates a fresh stream_key via `mint_stream_key()` and puts that in `streamid`. JWT minting is unchanged; the token still rides in `StartSessionResult.session_token`.
- `src/upstream_api/services/stream_validation.py` — added `load_session_by_streamid(conn, streamid)` and factored the column list / row builder out of `load_session_state` so both lookups share one shape.
- `src/upstream_api/routers/ingest.py` — `validate_stream` no longer verifies a JWT signature or compares jti; it shape-checks the `publish:` prefix and looks the session up by streamid, then runs the same state/expiry gates. Reason codes `invalid_session_token` and `token_jti_mismatch` are gone (no JWT to verify against, no rotation surface from the URL).
- `scripts/smoke_local.py` — verifies the streamid carries a `sess_…` key under 64 bytes, then verifies the JWT separately from the top-level `session_token` field.
- `tests/test_streamid.py` — rewritten around `mint_stream_key()` / `extract_stream_key()`; added a size assertion (`< 64 bytes`) so a future regression that re-embeds a JWT trips the suite.
- `tests/test_ingest_validate.py` — FakeConn now matches `where streamid = $1`; happy path uses a fresh `mint_stream_key()`. Dropped `test_validate_stream_rejects_jti_mismatch` (no JWT in this path); added `test_validate_stream_rejects_unknown_streamid` to cover the `session_not_found` case for a well-formed-but-unknown key.
- `docs/architecture.md`, `docs/api-contracts.md` — updated streamid format, token-model section, reason-code list, and the `UPSTREAM_STREAMID_HMAC_KEY` note.

### System interaction notes
- The agent (upstream-agent) treats `streamid` and `session_token` as opaque (per `upstream_agent/diagnostics/api.py`), so a shorter streamid is a drop-in change on the client side.
- An attacker who learns a leaked streamid is no worse off than under the old design: the URL was already a bearer credential (anyone with the JWT-in-streamid could publish). The active-session and `expires_at` checks remain; once the session enters `streaming` or `ended` the streamid is unusable.
- No DB migration required — `streamid` is already `text not null unique` on `stream_sessions`. The change is purely in what we put in that column.
- `UPSTREAM_SESSION_JWT_SECRET` is still required: the JWT is still minted for `session_token`. No env var changes.

### How to verify
```sh
cd upstream-api-server
make test                                 # local suite, no DB needed
uv run python scripts/smoke_local.py      # against a running API + dev token

# Then end-to-end against the deployed stack:
ssh -i ~/.ssh/upstream-api.pem ec2-user@18.225.104.21
cd ~/upstream-api-server && git pull
cd deploy && docker compose up -d --build api
# On the agent host:
upstream run            # ffmpeg should no longer log "streamid too long"
```

---

## 2026-05-13 - Support Supabase's asymmetric JWT signing keys (ES256)

### Summary
Dashboard users on Supabase projects using the new asymmetric "JWT signing keys" (default for projects created since late 2024) hit `Invalid user token: The specified alg value is not allowed` on the first authenticated request — `verify_supabase_token` only accepted HS256. Updated to detect the algorithm from the JWT header and verify accordingly: HS256/HS384/HS512 against the static project secret (legacy projects), ES256/ES384/ES512/RS256/RS384/RS512 against the public key fetched from `<supabase_url>/auth/v1/.well-known/jwks.json` and matched by `kid`. Project `xhniroynuandklvgajoc` is on ES256, which is what triggered this.

### Files touched
- `src/upstream_api/security/supabase_jwt.py` — new dual-algorithm flow; module-level cached `PyJWKClient` per Supabase URL (lifespan 600 s). New optional `supabase_url` kwarg on `verify_supabase_token`, required when the token's alg is asymmetric.
- `src/upstream_api/security/deps.py` — `current_user` now passes `supabase_url=settings.supabase_url` into the verifier.

### System interaction notes
- `PyJWKClient` is sync (uses `urllib.request.urlopen` under the hood). With a 10-minute cache lifespan and ~1 request per dashboard view, the blocking fetch happens at most once every 10 min per worker — acceptable; can wrap in `asyncio.to_thread` later if request volume grows.
- HS256 still works for legacy projects whose `.env` has a valid `UPSTREAM_SUPABASE_JWT_SECRET`. We don't need to keep that env var populated for ES256-only projects, but leaving it in place doesn't break anything.
- The JWKS URL convention `<supabase_url>/auth/v1/.well-known/jwks.json` is stable Supabase Auth behavior; verified against the hosted project.
- No new dependencies — `pyjwt[crypto]>=2.9` was already declared in `pyproject.toml` and gives us `PyJWKClient` + EC verification.

### How to verify
```sh
make test
# 104 passed

# Then deploy + sign into the dashboard:
ssh -i ~/.ssh/upstream-api.pem ec2-user@18.225.104.21
cd ~/upstream-api-server && git pull
cd deploy && docker compose up -d --build api
# Sign in at http://localhost:3000 — error frame gone, /dashboard renders.
```

---

## 2026-05-13 - Phase 4.5 endpoint smoke tests

### Summary
Add `tests/test_phase45_endpoints.py` covering all nine new GET endpoints from the previous commit. Same unit-level no-DB pattern as the rest of `tests/` — verifies the auth dep is wired (missing header → 401 `missing_credentials`, non-Bearer scheme → 401 `invalid_auth_scheme`, garbage bearer → 401 `invalid_user_token`/`invalid_token`) plus one regression test that an agent token (`ups_*`) is rejected by `/v1/api-tokens` because it requires a Supabase user JWT specifically. 28 new tests; total suite is now 104, runs in ~2.6 s.

### Files touched
- `tests/test_phase45_endpoints.py` (new) — parameterized over the nine endpoints; overrides `get_pool` with a no-op stand-in so the lifespan isn't needed (auth dep rejects before any DB access).

### System interaction notes
- I considered also testing path-param UUID validation (malformed UUID → 422) but FastAPI processes the auth dep before pydantic path validation when an `Authorization` header is present, so the route always 401s before reaching UUID parsing. UUID parsing is FastAPI's framework guarantee, not our code, so the tests would have only verified framework behavior — left out to keep the suite focused.
- The full end-to-end happy path (real auth → real DB → real response shapes) lives in `scripts/smoke_local.py` and the manual `make dev-account` flow, not in `tests/`.

### How to verify
```sh
make test
# 104 passed in 2.6s
```

---

## 2026-05-13 - Phase 4.5 dashboard convenience endpoints + dev-account seeder

### Summary
Eight new GET endpoints close the gap between the dashboard's design-doc API contract and what the api-server actually exposed. Today the dashboard queries Supabase Postgres directly under RLS for token lists, latest frames, validations, and dashboard counts; once it migrates to these endpoints, authorization lives in one place. Also lands a `scripts/seed_dev_account.py` (run via `make dev-account`) that creates a stable Supabase Auth user (`dev@upstream.test`), lets the existing `handle_new_user` trigger provision the tenant, then populates the account with one agent, one camera, one active stream session, three heartbeats, one passing validation, an API token, and a placeholder snapshot — so a fresh dashboard isn't empty when a developer logs in for visual testing.

### Files touched
- `src/upstream_api/security/deps.py` — extracted `require_account_role(user, account_id, *allowed)` from the inline `_require_admin` in `routers/api_tokens.py`. Both POST/DELETE and the new GET reuse it. Same error envelope (`forbidden.role`).
- `src/upstream_api/routers/users.py` (new) — `GET /v1/me` returning `{ user_id, aal, amr, memberships[] }`. Auth: `current_user` only.
- `src/upstream_api/routers/accounts.py` (new) — `GET /v1/account`, `GET /v1/account/limits`. Auth: `current_caller`. Uses `resolve_account(account_id_hint)` so agents and dashboard users both work.
- `src/upstream_api/routers/dashboard.py` (new) — `GET /v1/dashboard/summary`. Single round-trip with a multi-subquery SELECT counting cameras by status, recent online agents, active sessions, and 24h validation failures.
- `src/upstream_api/routers/api_tokens.py` — added `GET /v1/api-tokens` (admin-only, mirrors POST gate). Replaced the inline admin check with `require_account_role`. Selects only safe columns (never `hash`).
- `src/upstream_api/routers/heartbeats.py` — added `GET /v1/agents/{agent_id}/heartbeats` and `GET /v1/cameras/{camera_id}/heartbeats` (mounted via the new `history_router` to match the design doc's nested path scheme). Ownership-checked. Filters: `limit` (1-500, default 50) and optional `before` ISO datetime cursor.
- `src/upstream_api/routers/snapshots.py` — added `GET /v1/cameras/{camera_id}/latest-frame` mounted via the new `camera_router`. Returns metadata + a short-lived (~60 s) Supabase Storage signed URL minted server-side via a new `services.storage.create_signed_url` helper.
- `src/upstream_api/routers/stream_sessions.py` — added `GET /v1/stream-sessions/{session_id}/validations`. Newest first.
- `src/upstream_api/schemas/{users,accounts,dashboard,stream_validations}.py` (new) — typed response models.
- `src/upstream_api/schemas/{api_tokens,heartbeats,snapshots}.py` — extended with list / latest-frame response envelopes.
- `src/upstream_api/services/storage.py` — added `create_signed_url(settings, bucket, storage_key, expires_in=60)`. Wraps supabase-py's `create_signed_url` in `asyncio.to_thread` to match the existing `upload_bytes` pattern.
- `src/upstream_api/app.py` — mounts the three new routers (`users`, `accounts`, `dashboard`) plus the two sub-routers (`heartbeats.history_router`, `snapshots.camera_router`).
- `tests/test_app_routes.py` — extended the expected route set with the 9 new entries.
- `scripts/seed_dev_account.py` (new) — `make dev-account` / `make dev-account-reset` / `python scripts/seed_dev_account.py --token-only`. Uses Supabase Admin API (`auth.admin.create_user(...)` with `email_confirm=true`) so the dev user can sign in without going through email confirmation. Idempotent at the user level; `--reset` wipes through the `account_members` cascade then re-creates. Snapshot upload tolerates a missing bucket (logs a warning, skips that one fixture row, continues).
- `scripts/seed_dev_token.py` — replaced with a thin deprecation shim that calls `seed_dev_account.main(['--token-only'])` so existing instructions still work.
- `Makefile` — added `dev-account` and `dev-account-reset` phony targets.
- `README.md` — Quickstart step 5 simplified from "`seed_dev_token.py | psql`" to `make dev-account`.
- `docs/api-contracts.md` — new "Phase 4.5" section documenting all 8 new endpoints with request/response shapes.
- `docs/commit-log.md` — this entry.

### System interaction notes
- All eight endpoints use `current_caller` (the dual-auth dep) where possible, so an agent token can also hit them when useful. `/v1/me` is the exception — uses `current_user` only because there's no agent equivalent. `/v1/api-tokens` GET adds a role gate on top.
- The `latest-frame` endpoint deliberately returns the signed URL in the response body (not a 302 redirect) so callers can cache the URL until ~`expires_at` without following the redirect on every page render.
- `dashboard.summary` counts come from a single SELECT with subqueries — one round-trip per request, suitable for the dashboard's 10-second poll cadence.
- The seeder embeds a 1×1 black JPEG (~870 bytes) as a Python `bytes` literal. Hosted Supabase needs the `snapshots` bucket created in Studio first; local Supabase CLI doesn't auto-create buckets either — that's why the seeder logs a warning and proceeds without the snapshot fixture when the bucket is missing. Cleaner than failing the whole seed.
- `scripts/seed_dev_account.py` uses `auth.admin.delete_user` on `--reset` to wipe the auth.users row; the cascade then takes out `account_members` → `accounts` → all dependent fixtures via FK chains.
- `_resolve_account_for_user` in `routers/api_tokens.py` mirrors `CallerAuth.resolve_account` for the user-only endpoint. Could fold this into a single helper later, but the duplication keeps the user-only path obvious.

### How to verify
```sh
make db-start && make db-reset
make dev-account                    # creates dev@upstream.test + fixtures + token
make dev                            # in another shell
curl -H "Authorization: Bearer <token-from-stdout>" \
  http://localhost:4000/v1/dashboard/summary
# returns { "total_cameras": 1, "active_streams": 1, ... }
```
Idempotency: `make dev-account` re-run is a no-op. `make dev-account-reset` wipes + re-creates with fresh fixture UUIDs.

### Not addressed in this commit
- Dashboard migration to use these endpoints (still queries Supabase directly for four of them) — separate change in `upstream-dashboard`.
- `GET /v1/stream-sessions/{id}/playback` — still blocked on the streaming-server WHEP URL convention.
- `snapshots` bucket auto-creation — out-of-band Studio step for hosted.

---

## 2026-05-13 - Grant authenticated SELECT on dashboard tables + snapshots storage policy

### Summary
Fix `permission denied for table <x>` (Postgres code 42501) when the dashboard queries `public.*` tables against hosted Supabase. Tables created via migration in hosted Supabase don't receive automatic `GRANT SELECT TO authenticated` the way the local Supabase CLI grants them, so the RLS `member_select` policies on `accounts`, `account_members`, `account_limits`, `api_tokens`, `agents`, `cameras`, `stream_sessions`, `stream_validations`, `heartbeats`, `snapshots`, and `usage_events` were unreachable — Postgres rejected the query before even consulting RLS. This migration adds explicit grants for all eleven, plus a `storage.objects` policy mirroring `snapshots_member_select` so `createSignedUrl()` calls from the dashboard succeed when the bucket eventually has objects.

### Files touched
- `supabase/migrations/20260513150000_dashboard_grants.sql` (new) — eleven `grant select … to authenticated` statements (idempotent), `grant usage on schema public to authenticated`, and a `snapshots_storage_member_select` policy on `storage.objects` keyed off the leading `<account_id>/` path segment.
- `docs/commit-log.md` — this entry.

### System interaction notes
- The pattern going forward: any new public table intended for dashboard SELECT must include `grant select on public.<table> to authenticated;` in the same migration that creates it. Local Supabase CLI applies a similar grant by default, which is why this gap survived to hosted. Documented as a followup in the dashboard plan; should land in `AGENTS.md` of this repo too.
- No writes are granted to `authenticated` — dashboard writes go through the api-server (service role). Read-only matches the policy intent.
- The storage policy uses `(storage.foldername(name))[1]::uuid` to read the first path segment as the account id, matching the `storage_key` shape produced by `src/upstream_api/routers/snapshots.py:29` (`{account_id}/{camera_id}/{yyyymmdd}/{uuid}.jpg`). It applies the same membership check as the `snapshots` table's RLS so signed-URL access is consistent with table-row access.
- The storage policy is a no-op if the `snapshots` bucket doesn't exist yet — bucket creation is out-of-band (Studio or first service-role upload), not in SQL migrations.

### How to verify on hosted
```sql
-- 11 rows expected
select grantee, table_name, privilege_type
  from information_schema.table_privileges
 where table_schema = 'public' and grantee = 'authenticated'
 order by table_name;

-- 1 row expected
select polname from pg_policies
 where schemaname = 'storage' and tablename = 'objects'
   and polname = 'snapshots_storage_member_select';
```
Then refresh the dashboard at `http://localhost:3000` — the `permission denied for table account_members` error should be gone.

---

## 2026-05-13 - Signup auto-provisioning trigger on auth.users

### Summary
Close the bootstrap gap where a fresh Supabase Auth signup leaves a user with no `accounts`, `account_members`, or `account_limits` row. New migration adds a `SECURITY DEFINER` `handle_new_user()` function + `on_auth_user_created` trigger on `auth.users` insert that auto-provisions all three. Account name comes from `raw_user_meta_data.workspace_name` (set by the dashboard signup form) and falls back to the email handle. Slug is `slugify(name) + '-' + <6-char uuid suffix>` so collisions don't need a retry loop. Limits match the existing permissive `seed.sql` defaults (5 cameras, 5 concurrent sessions, 60 fps, 1920×1080, 8000 kbps, `{h264,h265}`).

### Files touched
- `supabase/migrations/20260513140000_signup_trigger.sql` (new) — `handle_new_user()` function and `on_auth_user_created` trigger. `SECURITY DEFINER` with explicit `set search_path = public, pg_temp` per Supabase hardening guidance. Idempotent: short-circuits via `account_members` existence check so re-runs against the same user (test fixtures, manual re-inserts, `supabase db reset`) are safe.
- `docs/commit-log.md` — this entry.

### System interaction notes
- The trigger fires for **every** signup path: email/password, magic link, OAuth, and Studio's "Add user" — anything that lands a row in `auth.users`. The dashboard's `signUpAction` passes `workspace_name` via `options.data`, which Supabase Auth stores as `raw_user_meta_data->>'workspace_name'`. The trigger picks it up; without it, the trigger falls back to `initcap(split_part(email, '@', 1))` (`alice@…` → "Alice").
- `SECURITY DEFINER` bypasses the SELECT-only RLS policies on `accounts`, `account_members`, `account_limits` for the trigger's INSERTs. Function ownership is the Supabase-default `postgres` role.
- The fixed-suffix slug (`<base>-<6char>`) is collision-resistant without catching unique-violation errors. Probability of collision at any realistic scale is negligible (`16^6 ≈ 16.7M` per slug base).
- The trigger runs in the same DB transaction as the `auth.users` insert, so by the time `signUpAction` in the dashboard returns, `getActiveAccount()` will find the membership on the very next request. No async lag.
- Local-dev `seed.sql` is unaffected: it still creates the "Local Dev" account used by `scripts/seed_dev_token.py` for the API-token smoke path. That account has no `auth.users` row, which is fine — it's for the agent-side path, not the dashboard-side.
- Pre-existing orphan `auth.users` rows (created before this migration, no `account_members` row) are NOT backfilled here. If any exist, a one-time `select handle_new_user() ...` migration can sweep them; not needed for fresh projects.

### How to verify locally
1. `supabase db reset` — applies the new migration + seed (Local Dev account untouched).
2. Sign up in the dashboard at `/signup` with `alice@test.local`, workspace `Acme Vision`.
3. `select a.name, a.slug, m.role, l.max_cameras from accounts a join account_members m on m.account_id = a.id join account_limits l on l.account_id = a.id where m.user_id = (select id from auth.users where email = 'alice@test.local');` returns one row with `max_cameras=5`, slug like `acme-vision-a1b2c3`.

---

## 2026-05-12 - Phase 2 + Phase 3 + Phase 4 API surface: ingest webhooks, heartbeats, snapshots, dashboard reads

### Summary
Land the full set of control-plane endpoints needed by `upstream-agent` (pip lib) and `upstream-streaming-server` (MediaMTX) so both can be built next without coming back for API additions. Three new tables (`heartbeats`, `snapshots`, `usage_events`) and three new column adds (`cameras.default_latency_mode`, `stream_sessions.req_latency_mode`/`lim_latency_mode`). Six new routers/endpoints: the four MediaMTX webhooks (`validate-stream`, `stream-started`, `stream-ended`, `validation-failed`), batched heartbeats, JPEG snapshot upload to Supabase Storage, and three account-scoped list/get routes (`GET /v1/agents`, `/v1/cameras`, `/v1/stream-sessions`) that accept either an agent token or a dashboard user JWT via a new `CurrentCaller` dual-auth dep. SRT URL now selects `latency=` from the session's `lim_latency_mode` (low=80 ms, balanced=200 ms, stable=1500 ms) so the three-mode buffer tradeoff is honored end-to-end.

### Files touched
- `supabase/migrations/20260513120000_heartbeats_snapshots_usage.sql` (new) — `heartbeats`, `snapshots`, `usage_events` tables with RLS member-select policies and recency indexes; matches existing schema style. Unblocks `services/usage.py`'s table-existence probe.
- `supabase/migrations/20260513121000_latency_mode.sql` (new) — `cameras.default_latency_mode` + `stream_sessions.req_latency_mode`/`lim_latency_mode` with `low|balanced|stable` check constraints, default `balanced`.
- `src/upstream_api/security/deps.py` — added `CallerAuth` + `current_caller` dep; dispatches on token shape (`ups_*` => agent, otherwise Supabase JWT) and exposes `resolve_account(account_id_hint)` that enforces single-membership inference vs explicit `?account_id=` for multi-account users.
- `src/upstream_api/services/limits.py` — `RequestedStream` and `LimitsSnapshot` gained `latency_mode: str = "balanced"`; `check_session_start` mirrors it onto the snapshot so it lands in `lim_latency_mode`.
- `src/upstream_api/services/session_lifecycle.py` — INSERT/SELECT now carry the two latency_mode columns; `_build_srt_url` maps mode → ms (`{low: 80, balanced: 200, stable: 1500}`).
- `src/upstream_api/services/stream_validation.py` (new) — state transitions for ingest webhooks (`mark_streaming`, `mark_ended`, `mark_failed`) plus `load_session_state`; each transition is a single `UPDATE ... RETURNING` and distinguishes "row missing" from "wrong state" via a post-update probe (`ConflictError(code="stream_state_invalid")`).
- `src/upstream_api/services/heartbeats.py` (new) — `record_batch` verifies camera/session ownership in one round-trip per kind, then `executemany`s the inserts. Updates `agents.last_seen_at` once per batch (not per heartbeat) to avoid hot-row contention on chatty fleets.
- `src/upstream_api/services/storage.py` (new) — supabase-py wrapper for `upload_bytes`; sync client wrapped in `asyncio.to_thread`; client is `lru_cache`d per-settings.
- `src/upstream_api/routers/ingest.py` (new) — four endpoints under `/v1/ingest/*`, all behind `CurrentIngest`. `validate-stream` always returns 200 with `accept: true|false` (and a `reason_code` on rejection) so the streaming server treats it as a decision, not an HTTP error.
- `src/upstream_api/routers/heartbeats.py` (new) — `POST /v1/heartbeats?agent_id=<uuid>`; max-100 batch enforced in the schema.
- `src/upstream_api/routers/snapshots.py` (new) — `POST /v1/snapshots` multipart form. Quota = `account_limits.snapshot_storage_mb` summed across trailing 30 days; per-request cap `UPSTREAM_SNAPSHOT_MAX_BYTES` (default 5 MB). Upload-first, insert-second; on rare crash a janitor job sweeps Storage against the `snapshots` table.
- `src/upstream_api/routers/agents.py` — `GET /v1/agents` (list, filter by `status`) and `GET /v1/agents/{agent_id}`, both via `CurrentCaller`. Existing `POST /register` unchanged.
- `src/upstream_api/routers/cameras.py` — `GET /v1/cameras` (filter `agent_id`, `status`) and `GET /v1/cameras/{camera_id}` via `CurrentCaller`. `POST /register` now accepts `default_latency_mode`.
- `src/upstream_api/routers/stream_sessions.py` — `GET /v1/stream-sessions` (filter `agent_id`, `camera_id`, `state`) via `CurrentCaller`; `POST /start` threads `latency_mode` from `RequestedStreamParams`. `GET /v1/stream-sessions/{session_id}` migrated to `CurrentCaller`.
- `src/upstream_api/schemas/{cameras,agents,stream_sessions,heartbeats,ingest,snapshots}.py` — new list responses + new request/response shapes; `LatencyMode` literal carried in the camera + session schemas.
- `src/upstream_api/app.py` — registered `heartbeats`, `snapshots`, `ingest` routers.
- `src/upstream_api/config.py` — `supabase_storage_bucket` (default `snapshots`) and `snapshot_max_bytes` (default 5 MB).
- `docs/architecture.md` — Phase 2/3/4 marked done; added the MediaMTX ingest webhook flow diagram and the latency-mode table.
- `docs/api-contracts.md` — fleshed out the previously-deferred Phase 2/3/4 sections with concrete request/response shapes; added six new error codes (`stream_state_invalid`, `forbidden_resource`, `forbidden_account`, `account_id_required`, `snapshot_too_large`, `snapshot_quota_exceeded`, `invalid_content_type`).
- `tests/test_app_routes.py` — expected route set extended for the six new endpoints + the three new list routes.

### System interaction notes
- The dual-auth `CurrentCaller` discriminator (`token.startswith("ups_")`) is cheap and unambiguous because Supabase JWTs always start with `eyJ` (base64-encoded JSON header). If we ever change the agent token prefix this branch needs updating in one place.
- `validate-stream` returning `accept: false` with HTTP 200 is intentional — the streaming server already handles "reject and close" as a normal outcome; a 4xx would force it to special-case our API.
- `mark_streaming` and `mark_failed` use `UPDATE ... WHERE state IN (...) RETURNING` so the transition is atomic at the row level. When `RETURNING` is empty we re-fetch and raise `stream_state_invalid` with the current state, which is what the streaming server needs to decide whether to retry vs give up.
- Heartbeat ownership is verified up-front with one `SELECT ... WHERE id = ANY($1::uuid[])` per resource kind; the alternative (per-row FK checks at insert time) would have produced harder-to-read errors and one round-trip per heartbeat.
- Snapshot upload-then-insert can orphan a Storage object if the row insert fails after the upload succeeds; a janitor job (out of scope here) reconciles. The reverse order would orphan a row pointing at nothing — worse, because the dashboard would 404 on the snapshot fetch.
- `services/usage.py` keeps its table-existence probe for the deploy window so the new code doesn't crash on a host that hasn't yet had the migration applied; the probe caches `True` after first call and is effectively a no-op forever after.

### Testing/checks
- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest -q` clean against the existing 43 tests; the route-enumeration test now expects the six new routes.
- After `supabase db push`:
  - `\d heartbeats`, `\d snapshots`, `\d usage_events` show the three new tables.
  - `\d+ cameras` shows `default_latency_mode` with check constraint.
  - `\d+ stream_sessions` shows `req_latency_mode` / `lim_latency_mode`.
- Manual smoke (against staging):
  - Start a session, copy the JWT out of the streamid → `POST /v1/ingest/validate-stream` returns `accept: true` with the `lim_*` snapshot as `expected`.
  - `POST /v1/heartbeats?agent_id=<uuid>` with a 3-item batch returns `accepted: 3`; `select last_seen_at from agents` updates within ~50 ms.
  - `curl -F file=@frame.jpg -F camera_id=... -F agent_id=... -F captured_at=...` against `/v1/snapshots` returns 201 and a row appears in Storage at `<account>/<camera>/<yyyymmdd>/<uuid>.jpg`.

### Follow-up TODOs
- Bump the Caddy per-request body cap on `/v1/snapshots` to >= 5 MB (currently 1 MB globally). Track with a per-path matcher in `deploy/caddyfile.
- Wire `upstream-agent` (pip lib) against this surface; record real-world heartbeat cadence to tune Caddy rate-limit per-token if needed.
- Wire `upstream-streaming-server` (MediaMTX) `runOnPublish` hook to call `validate-stream` and `runOnUnpublish` to call `stream-ended`.
- Add integration tests with testcontainers (asyncpg + Supabase Storage emulator) to exercise the full ingest webhook flow against a real DB and storage backend.

---

## 2026-05-12 - Auth hardening: argon2 off the event loop, MFA claims, membership cache, no token-id oracle, last_used_at writes

### Summary
Five related auth fixes. (1) `argon2id` verify and hash now run via `asyncio.to_thread` so each ~50 ms hash doesn't block the worker's event loop — agent-authed throughput on `gunicorn --workers 2` is no longer capped at ~20 req/s/worker. (2) `current_agent` now writes `api_tokens.last_used_at` on each successful verify, throttled to once per 60 s per token to avoid hot-row contention; the column existed in the migration but was never populated. (3) `DELETE /v1/api-tokens/{token_id}` returns 404 in both "doesn't exist" and "exists but caller isn't an admin" cases — the JOIN against `account_members` makes the SELECT either match or not, removing the previous 404-vs-403 enumeration oracle. (4) Supabase JWT verifier now extracts and surfaces `aal` and `amr` claims through `SupabaseUserClaims` and `UserAuth` so handlers can gate sensitive ops on MFA when an account-level MFA policy lands. (5) `current_user` now reads memberships via a 60 s `cachetools.TTLCache` keyed on `user_id`, eliminating the per-request DB round-trip on dashboard endpoints.

### Files touched
- `src/upstream_api/security/api_tokens.py` — added `verify_secret_async` and `issue_token_async` wrappers around the CPU-bound argon2 calls.
- `src/upstream_api/security/deps.py` — `current_agent` now uses the async verify, includes `last_used_at` in the SELECT, and writes a throttled update inside the same connection acquire. `current_user` uses a module-level `TTLCache` and propagates `aal`/`amr`. `UserAuth` gained `aal: str` and `amr: tuple[str, ...]`. Added `clear_membership_cache()` for tests.
- `src/upstream_api/security/supabase_jwt.py` — `SupabaseUserClaims` gained `aal` (default `"aal1"`) and `amr` (default `()`); decoder handles both Supabase's list-of-strings and list-of-objects `amr` shapes.
- `src/upstream_api/routers/api_tokens.py` — `revoke_token` JOINs `api_tokens` with `account_members` filtered to `role in ('owner','admin')`; returns 404 on miss. `create_token` now `await`s `issue_token_async`.
- `pyproject.toml` — added `cachetools>=5.5` (runtime) and `types-cachetools>=5.5` (dev).
- `tests/test_api_tokens.py` — added async wrapper round-trip; two `_FakePool`/`_FakeConn` tests for the `last_used_at` throttle (writes when null, skips when recent); two TestClient tests for the revoke 404 oracle (unknown id and non-admin caller produce identical bodies).
- `tests/test_supabase_jwt.py` (new) — five tests covering `aal2` extraction, both `amr` shapes, the `aal1` default, expiry, and bad audience.

### System interaction notes
The argon2 wrapping is the highest-leverage piece: previously a single bursty agent could starve every other request handled by the same worker. The membership cache means a role change takes up to 60 s to propagate to the API — acceptable for the dashboard use cases today but worth revisiting if/when an admin yanks a user's access and expects it to take effect instantly. The 404-collapse on revoke removes a small token-id enumeration oracle; combined with the existing per-IP rate limit at Caddy this closes the obvious abuse path.

### Testing/checks
- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest -q` clean (43 tests, was 23).
- After deploy:
  - `select last_used_at from api_tokens where prefix = ...` populates within a second of the first authed call.
  - `DELETE /v1/api-tokens/<random uuid>` and `DELETE /v1/api-tokens/<existing token id from another account>` return identical JSON 404s.
  - Dashboard auth latency drops by one round-trip per call after the first within a 60 s window per user.

### Follow-up TODOs
- Once an account-level MFA policy table exists, gate `POST /v1/api-tokens` and `DELETE /v1/api-tokens/{id}` on `aal == "aal2"` for accounts that require it.
- Consider extending the membership cache key to `(user_id, jti)` if/when Supabase JWT rotation gets aggressive — currently a stale role survives for at most 60 s.

---

## 2026-05-12 - Close TOCTOU race on stream_sessions start

### Summary
Two concurrent `POST /v1/stream-sessions/start` calls on the same camera previously both passed the "is the camera streaming?" pre-check and both inserted, briefly minting two SRT URLs for one camera; the same race could let a single account exceed `max_concurrent_sessions`. Two changes belt-and-brace the fix. (1) New partial unique index `stream_sessions_one_active_per_camera_idx on stream_sessions(camera_id) where state in ('approved','streaming')` — Postgres rejects the second insert with a unique-violation, which `start_session` catches and re-raises as `ConflictError(code="camera_already_streaming")` (same shape as the pre-check error). (2) `check_session_start` now takes a transaction-scoped `pg_advisory_xact_lock(hashtextextended(account_id::text, 0))` before counting active sessions, serializing per-account decisions without contending across accounts.

### Files touched
- `supabase/migrations/20260512131000_session_unique_active_per_camera.sql` (new) — the partial unique index.
- `src/upstream_api/services/session_lifecycle.py` — wraps the `INSERT ... RETURNING` in `try/except asyncpg.UniqueViolationError` and re-raises as `ConflictError`.
- `src/upstream_api/services/limits.py` — adds the advisory lock at the top of `check_session_start` (after `load_account_limits`, before `count(*)`).
- `docs/architecture.md` — new "Concurrency safeguards on session start" section documenting both mechanisms.
- `tests/test_session_lifecycle.py` (new) — fake-conn tests for (a) `UniqueViolationError` -> `ConflictError(code="camera_already_streaming")` translation and (b) the advisory-lock SQL is issued.

### System interaction notes
The advisory lock is keyed on a 64-bit hash of `account_id::text` rather than the raw UUID — Postgres advisory locks are 64-bit integers, and `hashtextextended` is a stable in-DB hash that's transaction-safe. Collisions (two different accounts hashing to the same 64-bit key) would silently serialize them too, but the cost is one extra await per concurrent start, which is fine. A real concurrency test for this needs a live Postgres harness (testcontainers or `pytest-postgresql`) — same gap that's been outstanding for the integration test layer; deferred.

### Testing/checks
- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest -q` clean (43 tests).
- After deploy / `supabase db push`:
  - `\d stream_sessions` shows `stream_sessions_one_active_per_camera_idx` as a partial unique index with the `WHERE` clause.
  - `xargs -P 2 -I _ curl -X POST -H 'Authorization: Bearer …' …/v1/stream-sessions/start` (twice for the same camera) yields exactly one 200 and one 409 `camera_already_streaming`.

### Follow-up TODOs
- Add an integration test layer (testcontainers + asyncpg) that exercises the true concurrent path for both the per-camera unique index and the per-account advisory lock — would have caught this class of bug originally.

---

## 2026-05-12 - Scope usage.py error handling + tighten CORS allowlists

### Summary
Two unrelated hardening tweaks bundled because both shrink the surface of "wrong by default" config. (1) `services/usage.record_event` previously wrapped its INSERT in a blanket `except asyncpg.UndefinedTableError` to tolerate the Phase 1-4 absence of `usage_events`. That swallowed any future SQL typo that referenced a missing table too. Replaced with a one-shot `to_regclass('public.usage_events') is not null` probe cached in module state under an `asyncio.Lock`; the INSERT now runs without try/except, so unrelated errors propagate. (2) `CORSMiddleware` previously combined `allow_credentials=True` with `allow_methods=["*"]` and `allow_headers=["*"]`, which browsers silently downgrade. Replaced with explicit lists (`GET, POST, DELETE` and `Authorization, Content-Type, X-Request-ID`) so credentialed cross-origin actually works and any new method gets a deliberate review.

### Files touched
- `src/upstream_api/services/usage.py` — module-level `_table_exists` cache + `_probe_lock`; new `_probe_table_exists` and `reset_usage_table_cache` helpers; insert no longer guarded by `except`.
- `src/upstream_api/app.py` — CORS middleware switched to explicit method/header lists; comment notes why.
- `tests/test_usage.py` (new) — fake-conn tests for skip-when-missing, insert-when-present, and probe-result-caching.
- `tests/test_app_routes.py` — added `test_cors_middleware_uses_explicit_methods_and_headers` which inspects `app.user_middleware` directly.

### System interaction notes
The `to_regclass` probe runs once per process and the cache is process-local, so a deploy after the Phase 5 migration ships will pick up the new state without a config flip — the next process boot's first `record_event` call will discover the table. The CORS change adds `OPTIONS` implicitly via Starlette's CORSMiddleware preflight handling, so no explicit OPTIONS in the methods list is needed.

### Testing/checks
- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest -q` clean (43 tests).
- After deploy:
  - From the dashboard origin: a credentialed `fetch(..., {credentials: 'include'})` against `POST /v1/api-tokens` succeeds (previously failed silently in browsers).
  - `select count(*) from usage_events;` (once Phase 5 ships) shows new rows; before then, `_logger.debug("usage_events_table_missing", ...)` fires once per process.

### Follow-up TODOs
- Add `PATCH`/`PUT` to the CORS methods list when (and only when) the API grows them — matches the explicit-allow philosophy of this change.

---

## 2026-05-12 - Disable interactive docs in production + cap request body at 1 MB

### Summary
Two hardening tweaks. (1) `/docs` (Swagger UI) and `/redoc` are now 404 when `UPSTREAM_ENVIRONMENT=production`; both stay on in development. `/openapi.json` is kept available everywhere so tooling (Postman, codegen, the future agent SDK) can still introspect the schema. (2) Caddy now rejects any request body over 1 MiB with HTTP 413 before forwarding to FastAPI — a body-size DoS guard since pydantic doesn't enforce one and `metadata: dict[str, Any]` fields are unbounded.

### Files touched
- `src/upstream_api/app.py` — `FastAPI(...)` constructor now sets `docs_url=None` / `redoc_url=None` when `settings.is_production`, leaving `openapi_url="/openapi.json"` always on.
- `deploy/Caddyfile` — added `request_body { max_size 1MB }` inside the `api.upstreamcv.com` site block.

### System interaction notes
The interactive Swagger UI was the friendliest abuse surface — anyone could paste a stolen token into the "Authorize" button and start firing requests. Closing it doesn't add real protection (the token is still the actual gate) but cuts off lazy/scripted abuse. The 1 MB body cap is enforced at Caddy so a malicious 100 MB body is dropped at the edge before consuming any FastAPI process memory. When `/v1/snapshots` lands in Phase 3, that endpoint will need a higher cap via a Caddy matcher — noted as a TODO.

### Testing/checks
- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest -q` clean (26 tests).
- After deploy:
  - `GET /` → 200, `GET /openapi.json` → 200, `GET /healthz` → 200.
  - `GET /docs` → 404, `GET /redoc` → 404.
  - `POST /v1/agents/register` with a 2 MB body → HTTP 413 from Caddy (does not reach FastAPI).

### Follow-up TODOs
- Raise the body-size cap on `/v1/snapshots` (Phase 3) via a Caddy matcher — the global default is too small for a JPEG frame upload.
- Consider switching pydantic models from `extra="ignore"` (default) to `extra="forbid"` for a stricter contract; trade-off is forward compatibility for explicit-contract clarity.

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
