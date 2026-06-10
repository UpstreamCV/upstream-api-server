# Module map

What each file and directory owns. Read this before adding new code so you reuse what's already there.

## Top level

- `pyproject.toml` — dependencies (managed with `uv`), `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`. Python ≥ 3.11.
- `Makefile` — `install`, `dev`, `test`, `lint`, `typecheck`, `format`, `db-start`, `db-stop`, `db-reset`, `db-diff`, `db-push`, `db-types`, `dev-account`, `dev-account-reset`.
- `Dockerfile` — multi-stage build (uv-based builder, slim runtime, non-root user). Runs `gunicorn` with `uvicorn.workers.UvicornWorker`.
- `.env.example` — every `UPSTREAM_*` env var the service needs. `.env` is gitignored.
- `.github/workflows/ci.yml` — ruff + ruff format check + mypy strict + pytest on push/PR.
- `deploy/` — production deploy bundle: `docker-compose.yml` (FastAPI + Caddy), `Caddyfile` (`api.upstreamcv.com`, Let's Encrypt, 1 MB body cap with a wider matcher for `/v1/snapshots`), `.env.production.example`.
- `scripts/` — operator + dev-loop helpers. `seed_dev_account.py` (driven by `make dev-account` — creates the Supabase Auth user, lets the signup trigger seed the tenant, then mints an API token); `seed_dev_token.py` (deprecated shim around `seed_dev_account.py --token-only`); `smoke_local.py`, `smoke_whep_proxy.py`, `_smoke_reliability_inprocess.py` (smoke scripts); `run_detection_metrics_rollup.py` (manual M6 rollup maintenance runner).

## `supabase/`

Supabase CLI layout. `supabase start` boots the local stack; the Supabase GitHub integration auto-applies anything new in `migrations/` to the hosted DB on push to `main`.

- `config.toml` — Supabase CLI config (project id, ports, schemas).
- `migrations/<timestamp>_<name>.sql` — schema migrations, applied in lexical order.
- `seed.sql` — local-dev seed (Local Dev account + permissive limits). **Only runs on `supabase db reset`** — never applied to the hosted project.

### Migration files (current)

Applied in lexical order. Adding a migration means picking a timestamp strictly greater than the latest in this list.

- `20260512130000_init_extensions.sql` — `pgcrypto`, `citext`, `set_updated_at()` trigger fn.
- `20260512130100_accounts_and_auth.sql` — `accounts`, `account_members`, `account_limits`, `api_tokens` + RLS.
- `20260512130200_agents_and_cameras.sql` — `agents`, `cameras` + RLS.
- `20260512130300_stream_sessions.sql` — `stream_session_state` enum, `stream_sessions`, `stream_validations` + RLS.
- `20260512131000_session_unique_active_per_camera.sql` — partial unique index on `stream_sessions(camera_id) where state in ('approved','streaming')`; defence-in-depth against the start-session TOCTOU race.
- `20260513120000_heartbeats_snapshots_usage.sql` — Phase 3 telemetry tables: `heartbeats`, `snapshots`, `usage_events` + RLS.
- `20260513121000_latency_mode.sql` — adds `default_latency_mode` to cameras + `req_latency_mode` / `lim_latency_mode` to sessions (`low | balanced | stable`).
- `20260513140000_signup_trigger.sql` — `auth.users` trigger that auto-provisions a tenant (`accounts`, `account_members`, `account_limits`) for every new Supabase Auth user.
- `20260513150000_dashboard_grants.sql` — explicit `grant select … to authenticated` on every public table the dashboard SELECTs (hosted Supabase doesn't auto-grant; local does).
- `20260514120000_realtime_publication.sql` — adds `stream_sessions` and `cameras` to the `supabase_realtime` publication for dashboard live updates.
- `20260514130000_session_passphrase.sql` — adds nullable `srt_publish_passphrase` column on `stream_sessions` for the FU-6 per-session AES key.
- `20260518140000_processor_status_observations.sql` — M8.0 processor surface: `processor_statuses` (upsert by `(processor_id, stream_session_id)`) and append-only `observations` + RLS.
- `20260521120000_latest_perception_snapshots.sql` — P5-C `latest_perception_snapshots` (upsert one row per camera).
- `20260522120000_camera_geometry_configs.sql` — P6-A `camera_geometry_configs` (upsert one config per camera; `revision` bumps on every save).
- `20260528120000_reliability_findings.sql` — reliability M0/M1-A `reliability_findings` table (rule-driven verdicts per camera; partial unique index = one OPEN row per `(camera_id, dedupe_key)`) + RLS.
- `20260528170000_reliability_findings_detection_category.sql` — reliability M2: adds `detection` to the findings category CHECK constraint.
- `20260528180000_realtime_perception_snapshots.sql` — adds `latest_perception_snapshots` to the `supabase_realtime` publication (live detection overlay push).
- `20260529120000_replica_identity_full_realtime.sql` — `REPLICA IDENTITY FULL` on Realtime-published RLS tables (`latest_perception_snapshots`, `stream_sessions`) so UPDATE events deliver.
- `20260529130000_grant_authenticated_realtime_tables.sql` — backfills `grant select … to authenticated` for public tables created after the original dashboard-grants migration (the actual cause of "Realtime delivers no events").
- `20260529140000_snapshot_detection_metrics.sql` — reliability M5 `snapshot_detection_metrics` (compact per-snapshot confidence-summary time series).
- `20260529150000_detection_confidence_hourly_rollups.sql` — reliability M6 `detection_confidence_hourly_rollups` (hourly weighted summaries; raw rows retained ~72h, rollups ~90d).
- `20260601120000_camera_processing_configs.sql` — model-config M3/M4 `camera_processing_configs` (upsert one model/processing config per camera; `revision` bumps on every save).
- `20260609120000_processing_config_dimensions.sql` — model-config M9: applied-processing-config dimension columns on the M5/M6 metrics tables (extracted from the processor's lineage stamp at ingest).
- `20260610120000_processing_fps_dimensions.sql` — model-config M9.1: dedicated `requested_processing_fps` / `applied_processing_fps` dimension columns (M7E output-cadence FPS).

## `src/upstream_api/`

### `app.py` — FastAPI app factory

`create_app(settings)` is the only thing the rest of the system needs. It:
- Configures structlog (JSON in prod, console in dev).
- Builds the FastAPI app with the right lifespan.
- Adds middleware: `CORSMiddleware`, `AccessLogMiddleware`, `RequestIdMiddleware`.
- Registers exception handlers from `errors.py`.
- Mounts every router.

`lifespan()` opens the asyncpg pool on startup and closes it on shutdown.

### `main.py` — uvicorn entrypoint

A single `app = create_app()`. Imported by `uvicorn upstream_api.main:app …`.

### `config.py` — `Settings(BaseSettings)`

Reads `UPSTREAM_*` env vars. Uses pydantic-settings v2. `get_settings()` is `lru_cache`d.

Notable design choices:
- All secrets are `SecretStr` to avoid accidental logs.
- `allowed_origins` is a `list[str]` exposed as a `@property` over a CSV string field (`UPSTREAM_ALLOWED_ORIGINS=a,b,c`), because pydantic-settings tries to JSON-decode `list[str]` env values by default.
- `session_token_ttl_seconds` defaults to 14400 (4h).

### `errors.py` — error envelope and exception handlers

- `ApiError` (base) with `status_code`, `code`, `message`, `details` — these are the stable error codes in the contract.
- Concrete: `NotFoundError`, `UnauthorizedError`, `ForbiddenError`, `ConflictError`, `LimitExceeded`.
- `api_error_handler`, `http_exception_handler`, `validation_exception_handler`, `unhandled_exception_handler` — all emit the same envelope shape `{ "error": { "code", "message", "details", "request_id" } }`.

### `db/pool.py` — asyncpg pool

- `create_pool(settings)` — opens the pool with `application_name="upstream-api"`, lifespan-managed.
- `get_pool(request)` — FastAPI dep; reads from `request.app.state.db_pool`.

### `security/` — auth and token machinery

- `api_tokens.py` — `issue_token()`, `split_token()`, `verify_secret()`, `ALLOWED_SCOPES`. Plaintext shape is `ups_<env>_<8 base32 id>_<22 base32 secret>`; the `prefix` (everything except the last segment) is the unique DB lookup key and the last segment is argon2id-hashed. `ALLOWED_SCOPES = frozenset({"agent"})` — used by the api-tokens router to reject scope escalation.
- `session_jwt.py` — `mint_session_token()`, `verify_session_token()`, `SessionLimits`, `SessionTokenClaims`. Issues HS256 JWTs with `iss=upstream-api`, `aud=srt-ingest`, plus `acc`/`sub`/`cam`/`sid`/`jti`/`exp`/`iat`/`lim`. The `lim` claim mirrors the session row's `lim_*` columns and is the contract the streaming server enforces.
- `supabase_jwt.py` — `verify_supabase_token()`, returns `SupabaseUserClaims(user_id, email, role)`. Validates with `aud="authenticated"`.
- `shared_secret.py` — `verify_shared_secret(presented, expected)` using `secrets.compare_digest`.
- `streamid.py` — `build_streamid(jwt)` returns `"publish:<jwt>"`. `extract_session_token(streamid)` reverses it. Used by `start_session` and (later) `/v1/ingest/validate-stream`.
- `deps.py` — five FastAPI deps + their `Annotated` aliases, plus two small helpers:
  - `current_agent` → `AgentAuth(account_id, token_id, scopes)`. Looks the token up in `api_tokens` by prefix, argon2-verifies the secret, checks `revoked_at` / `expires_at`, refreshes `last_used_at` at most every 60 s. Binds `account_id` into the logging contextvar.
  - `current_user` → `UserAuth(user_id, memberships: dict[UUID, role], aal, amr)`. Verifies the Supabase JWT (`aud=authenticated`) and caches each user's `account_members` lookup in a 60 s `TTLCache`. The `aal` (MFA assurance) and `amr` (auth methods) claims are exposed so handlers can later gate sensitive ops on `aal == "aal2"` once a per-account MFA-policy table lands.
  - `current_ingest` → `IngestAuth()`. Constant-time-compares `X-Upstream-Webhook-Secret` against `UPSTREAM_MEDIAMTX_WEBHOOK_SECRET`. The dataclass is empty by design — a shared-secret webhook carries no per-caller state.
  - `current_processor` → `ProcessorAuth()`. Same shape as `current_ingest` but verifies against `UPSTREAM_PROCESSOR_WEBHOOK_SECRET`.
  - `current_caller` → `CallerAuth(agent: AgentAuth | None, user: UserAuth | None)`. Disambiguates on token prefix (`ups_` → agent, else → Supabase JWT). Handlers that accept either kind use this dep and call `resolve_account(account_id_hint)` to pick the right account; users with multiple memberships must pass `?account_id=`.
  - `require_account_role(user, account_id, *allowed)` — raise `ForbiddenError(forbidden.role)` unless the user's role on that account is in `allowed`. Used by routes that need finer-grained gating than `CallerAuth` provides (e.g. `POST /v1/api-tokens` is owner/admin only).
  - `bind_agent_context(auth, agent_id)` — bind the agent + account onto the logging contextvars for the rest of the request.

### `services/` — business logic, no HTTP

One file per resource / external system. Handlers consume these and never write SQL directly.

- `limits.py` — `load_account_limits()`, `check_session_start()`, `check_camera_defaults()`, `check_camera_slot_available()`. Raises `LimitExceeded(code="limit_exceeded.*")` with stable codes the agent SDK can switch on. `check_session_start` takes a `pg_advisory_xact_lock` keyed on the account id before counting active sessions, so two concurrent `/start` calls within one account serialize and observe the same count.
- `session_lifecycle.py` — `start_session()`, `stop_session()`. Owns the state machine. `start_session` fetches the camera once, derives `agent_id`, checks busy + limits, mints the session JWT and a per-session SRT passphrase + stream key, inserts the row, writes a `usage_events` row. `stop_session` mirrors `cameras.status` to `'idle'` in the same transaction.
- `stream_validation.py` — state-transition helpers for the four ingest webhooks: `mark_streaming`, `mark_ended`, `mark_failed`, `insert_validation_report` (M5.1 report-only), `load_session_state`, `check_session_admit`. Each transition is a single SQL statement so the DB is the source of truth. The three mutating helpers also mirror `cameras.status` (`'streaming'` / `'idle'` / `'error'`) inside the same transaction.
- `usage.py` — `record_event()`. The `usage_events` table now exists; a one-shot cached `to_regclass('public.usage_events') is not null` probe still keeps `record_event` a no-op on databases where the migration hasn't applied (logs `usage_events_table_missing` at debug).
- `heartbeats.py` — `ingest_heartbeats(...)` batch insert + ownership pre-check + bumping `agents.last_seen_at` / `cameras.last_seen_at` once per batch.
- `storage.py` — Supabase Storage upload helpers, wrapped in `asyncio.to_thread` because supabase-py is sync. The client is cached per `(supabase_url, service_role_key)` — `Settings` itself is unhashable, so caching on the `Settings` object would crash on first real upload.
- `mediamtx_provisioning.py` — `provision_path()` / `deprovision_path()` against MediaMTX's HTTP control API. POST-first with PATCH fallback on 400 (MediaMTX 1.9.3's PATCH alone 404s on a missing path; POST `add` 400s on an existing one). Typed `MediaMTXProvisioningFailure` on transport / non-2xx so callers can distinguish failure modes.
- `whep_proxy.py` — `proxy_whep_offer()` / `proxy_whep_delete()` / `parse_mediamtx_location()`. WHEP signaling proxy in front of MediaMTX. Raises `UpstreamMediaMTXUnavailable` on transport failure so the router can translate to 502.
- `http_client.py` — `build_http_client()` for lifespan and `get_http_client()` / `HttpClientDep` for routers. One pooled `httpx.AsyncClient` shared by `whep_proxy` and `mediamtx_provisioning` so outbound calls reuse connections.
- `geometry.py` (P6-A) — `default_geometry_config_record()` probe default, `fetch_geometry_config()`, `upsert_geometry_config()` (revision bump on conflict). All tenant args are passed in by the caller; never taken from the request body.
- `processor.py` (M8.0 + P5-C) — `fetch_eligible_sessions()` (the eligibility WHERE clause is the single source of truth; inner `join lateral` picks the latest validation), `upsert_processor_status()` (upsert by `(processor_id, stream_session_id)`), `insert_observation()`, `upsert_perception_snapshot()` / `fetch_latest_perception_snapshot()` (P5-C), `fetch_recent_observations()` (P5-A), plus `coerce_jsonb` / `parse_iso_datetime` coercion helpers.
- `detection_metrics.py` (reliability M5) — writes one compact confidence-summary row to `snapshot_detection_metrics` per accepted perception snapshot (`write_detection_metrics_for_perception_snapshot`) and reads rolling current-vs-baseline windows back for drift (`fetch_detection_confidence_windows`, `_WINDOW_SQL`). Numeric summaries only; never the raw snapshot.
- `detection_metrics_rollup.py` (reliability M6) — retention + downsampling maintenance for `snapshot_detection_metrics`. `run_detection_metrics_rollup()` rolls completed hours into `detection_confidence_hourly_rollups` (`_ROLLUP_HOURLY_SQL`, idempotent upsert), then prunes raw rows past a 72h hour-aligned cutoff that a bucket already covers (`_PRUNE_RAW_SQL`, coverage join), then ages out 90d-old rollups (`_PRUNE_ROLLUPS_SQL`) — all in one transaction with a single captured `now`. Named retention constants + a pure-Python reference (`compute_hourly_rollup_buckets`) for the weighted math. Does NOT touch the M5 drift path.
- `reliability_trends.py` (reliability M7) — read side of the M6 rollups for the Reliability tab's confidence-trend chart. `get_confidence_trend_for_camera()` runs `_CONFIDENCE_TREND_SQL` (one set-based query that aggregates the per-package rollup rows of each completed hour into one camera-level point, re-weighting per-package weighted means by `confidence_sample_count` per-field exactly as M6 does, over a both-edges-hour-aligned window) and `summarize_confidence_trend()` (pure headline stats). Renames internal weighted columns to clean API names; exposes no tenant/internal columns. `_RANGES` keys `24h`/`7d`. Does NOT run the evaluator or touch raw metrics/M5 drift. Surfaced by `routers/reliability.py` `GET /v1/cameras/{camera_id}/reliability/confidence-trend`.
- `reliability_evaluator.py` (reliability M0–M2) — rule-based evaluator, one entry point `evaluate_camera()`. Pulls the facts the api-server already persists (latest session, processor status, snapshot + image bytes, latest observation, confidence windows), runs the five shipped rules (`stream_valid_processor_silent`, `latest_frame_stale`, `processor_using_default_geometry`, `scene_too_dark_for_reliable_detection`, `unstable_detection_confidence`) and upserts/resolves findings via `reliability_repo`. `DETECTOR_VERSION` stamps every row.
- `reliability_repo.py` (reliability M0) — `reliability_findings` DB helpers: `upsert_open_finding()` (select-then-upsert under a row lock because the one-OPEN-row invariant is a *partial* unique index, which `ON CONFLICT` can't target), `resolve_open_findings()`, `list_open_findings_for_camera()`, `list_findings_for_camera()`.
- `reliability_service.py` (reliability M0) — request-shaping layer between the router and evaluator + repo: `get_summary_for_camera()` (runs the evaluator, reads open findings back, rolls them up into the categorical `healthy|info|needs_review|critical|unknown` verdict + top findings) and `get_findings_for_camera()` (status-filtered list).
- `reliability_config_comparison.py` (model-config M11) — `get_config_comparison_summary()`: descriptive report card comparing two applied-config groups from the M9/M9.1 by-config data. Deliberately NOT a recommender — factual metric directions/deltas only, standing "not causal proof" caveats, `DEFAULT_MIN_SAMPLES=30` inconclusive gate. Pure read over rollups.
- `detection_confidence.py` (reliability M2) — pure functions, no DB: `extract_detection_confidences()` (tolerant of top-level / `detections` / `objects` / `tracks` / `score`-aliased payload shapes; never mistakes `track_id`/`class_id` for a confidence), `count_detection_objects()`, `compute_detection_confidence_stats()`.
- `image_analysis.py` (reliability M1) — `compute_image_brightness(bytes)` via Pillow grayscale mean; never raises, bounds decode size with a `MAX_IMAGE_PIXELS` decompression-bomb guard.
- `snapshot_brightness.py` (reliability M1) — `get_brightness_for_snapshot()`: per-snapshot brightness with a process-local `TTLCache` keyed by `snapshot_id` (`maxsize=1024`, `ttl=300s`) so the summary read path doesn't re-decode the same JPEG on every dashboard poll. Failures are not cached.
- `models.py` (model-config M2) — static, in-process model registry (NO DB table): `list_models()`, `get_model(model_key)`. Source of truth the per-camera processing config validates against; imported by `schemas/processing_config_camera.py`, never the reverse.
- `processing_config.py` (model-config M3) — account-level processing master switch stored as a `processing_enabled` key in `accounts.metadata` JSONB: `get_processing_config()`, `update_processing_config()` (JSONB-concat merge that preserves other keys). Absent key = `true` so pre-M3 accounts are unaffected; the eligible-sessions WHERE clause enforces it.
- `processing_config_camera.py` (model-config M3/M4) — per-camera processing-config DB helpers, mirroring `geometry.py`: `default_camera_processing_config_record()` (`revision: 0` unsaved default), `fetch_camera_processing_config()`, `upsert_camera_processing_config()` (revision bump on conflict). Tenant columns always passed in by the caller.
- `processing_config_dimensions.py` (model-config M9) — `extract_processing_config_dimensions()` + `normalize_selected_classes_key()` / `has_processing_config_warning()`: pulls the compact applied-config dimension set out of the processor's `snapshot.source.applied_processing_config` lineage stamp at metric-ingest time, feeding the M9 dimension columns on the rollup tables.

### `routers/`

Each router owns its own path prefix. They consume `services.*` and never write SQL directly.

- `admin.py` — `GET /` (service-info landing), `GET /healthz`, `GET /readyz`.
- `accounts.py` — `/v1/account` prefix. Dashboard-user reads of the caller's account + members + limits, plus the M3 account-level processing master switch: `GET`/`PATCH /v1/account/processing-config` (`processing_enabled` toggle the eligible-sessions SQL enforces).
- `users.py` — `/v1/me` prefix. Dashboard-user introspection of their own user / membership state.
- `agents.py` — `POST /v1/agents/register` (upserts on `(account_id, name)`), `GET /v1/agents`, `GET /v1/agents/{agent_id}`. Reads accept agent token or user JWT (`CurrentCaller`).
- `cameras.py` — `POST /v1/cameras/register` (upserts on `(agent_id, external_id)` if provided), `GET /v1/cameras`, `GET /v1/cameras/{camera_id}`, plus the camera-scoped extensions: `GET /v1/cameras/{camera_id}/observations/recent` (P5-A), `GET /v1/cameras/{camera_id}/perception/latest` (P5-C), `GET /v1/cameras/{camera_id}/processor-status/latest` (P6-D), `GET`/`PUT /v1/cameras/{camera_id}/geometry-config` (P6-A — the repo's first `PUT`), `GET`/`PUT /v1/cameras/{camera_id}/processing-config` (model-config M3/M4, registry-validated).
- `heartbeats.py` — two routers: `router` (`POST /v1/heartbeats?agent_id=…`, agent token, batched up to 100); `history_router` (`GET /v1/agents/{agent_id}/heartbeats`, `GET /v1/cameras/{camera_id}/heartbeats`, dashboard reads).
- `stream_sessions.py` — `POST /v1/stream-sessions/start`, `POST /v1/stream-sessions/{id}/stop`, `GET /v1/stream-sessions`, `GET /v1/stream-sessions/{id}`. Plus the WHEP read-path proxy: `POST /v1/stream-sessions/{id}/whep`, `DELETE /v1/stream-sessions/{id}/whep/{resource_id}` (`CurrentCaller`-authed).
- `ingest.py` — `/v1/ingest/*`. Most webhook-authed: `validate-stream`, `stream-started`, `stream-ended`, `validation-failed`, `lookup-session`, `expected/{session_id}` (M5.1), `stream-validations` (M5.1). `mediamtx-auth/{secret}` is path-segment authed (M4.1 — MediaMTX 1.9.3 can't send custom headers).
- `dashboard.py` — `/v1/dashboard` prefix. Aggregate reads used by the dashboard home / overview pages.
- `api_tokens.py` — `POST /v1/api-tokens` (issue with one-time plaintext), `DELETE /v1/api-tokens/{id}` (soft-revoke). Owner/admin role required via `require_account_role`.
- `snapshots.py` — three routers in one file: `router` (`POST /v1/snapshots`, agent token), `camera_router` (`GET /v1/cameras/{camera_id}/latest-frame`, `current_caller`), `ingest_router` (`POST /v1/ingest/snapshots`, `current_ingest` webhook; identity derived from `session_id`, `streaming`-only). Shared file-validation / quota / upload-insert logic in module-private helpers (`_read_validated_jpeg`, `_enforce_snapshot_quota`, `_persist_snapshot`).
- `processor.py` (M8.0 + P5-C) — `/v1/internal/processor` prefix, all `CurrentProcessor`-authed: `GET /eligible-sessions` (SQL-only safety filter; builds `internal_rtsp_url`, never returns `srt_url`), `POST /status` (upsert), `POST /observations` (mock types only), `POST /perception-snapshots` (P5-C latest-state upsert). Tenant always derived from the session via `load_session_state`. `POST /perception-snapshots` also writes the reliability M5 detection-confidence summary row (best-effort, savepoint-isolated).
- `models.py` (model-config M2) — `GET /v1/models`: the static model catalog (`CurrentCaller`-authed, platform-global, not account-scoped). Thin wrapper over `services/models.list_models()`.
- `reliability.py` (reliability M0–M7 + model-config M9/M9.1/M11) — feature-namespaced reads under `/v1/cameras/{camera_id}/reliability/*`, all `CurrentCaller`-authed with camera-ownership → 404: `GET …/summary` and `GET …/findings?status=` (both evaluate-on-read: run the rule set, persist upserts/resolves, return state in one transaction), `GET …/confidence-trend?range=24h|7d` (M7), `GET …/confidence-trend/by-config` (M9, per-applied-config series + dimension filters), `GET …/config-comparison-summary` (M11 report card).

### `schemas/`

Pydantic v2 request/response models, one file per resource. All models default to `model_config = ConfigDict(extra="forbid")` so unknown fields 422 rather than silently ignore. Currently 17 files:

`accounts.py`, `agents.py`, `api_tokens.py`, `cameras.py`, `dashboard.py`, `geometry.py` (P6-A — `GeometryPoint`/`Frame`/`Roi`/`Line`/`Config` with a duplicate-id `model_validator`), `heartbeats.py`, `ingest.py` (every webhook request/response, including the M5.1 `ExpectedConfigResponse` / `StreamValidationReportRequest` and `LookupSessionRequest` / `LookupSessionResponse`), `models.py` (model-config M2 — `ModelRegistryEntry` + `ModelsListResponse` for the static catalog), `processing_config.py` (model-config M3 — account-level `ProcessingConfigResponse` / `ProcessingConfigUpdate` master-switch shapes), `processing_config_camera.py` (model-config M3/M4 — `CameraProcessingConfig` with structural + registry validation against `services/models.py`, `CameraProcessingConfigRecord`/`Response`), `processor.py` (M8.0 + P5-A + P5-C + M7B — `Literal` enums for processor status + the 3 mock observation types, `ObservationRecord` for the open-vocabulary read side, `PerceptionSnapshotIngestRequest` for the latest-state write side, `EligibleGeometryConfig` / `EligibleProcessingConfig` envelopes), `reliability.py` (reliability M0–M7 + M9/M11 — `ReliabilityFindingRecord`/`Findings`/`SummaryResponse` with `Literal` category/severity/status vocabularies, `ConfidenceTrend*` point/summary/by-config shapes, `ConfigComparison*`), `snapshots.py`, `stream_sessions.py`, `stream_validations.py`, `users.py`.

### `observability/`

- `context.py` — `request_id_var`, `account_id_var`, `agent_id_var`, `session_id_var`. Bound by middleware and the auth deps; auto-included in every log line.
- `logging.py` — `configure_logging(level, json_logs)`, `get_logger(name)`. Secret-key denylist scrubs `authorization`, `*token*`, `*secret*`, `*_key`, `password`.
- `middleware.py` — `RequestIdMiddleware` (reads/sets `X-Request-ID`), `AccessLogMiddleware` (one log line per request with duration).

## `tests/`

Organized one file per feature. `glob tests/test_*.py` for the full list (currently ~43 files, ~600 cases). Two patterns are reused widely and worth knowing before adding a new test:

- **Fake-asyncpg pool.** Most route-level tests use an in-process fake (`FakeConn` / `FakePool`) that records executed statements and returns canned rows. Avoids needing a real DB but means the fake's behavior has to be kept in sync with production SQL — handled by the SQL-pin pattern below.
- **Production-SQL pin.** A separate test imports the production SQL constant (`_ELIGIBLE_SESSIONS_SQL`, `_UPSERT_GEOMETRY_CONFIG_SQL`, etc.) and asserts its substring shape (WHERE clauses, table names, `on conflict` target) so a refactor that drifts the fake from real SQL fails loudly. See `tests/test_processor.py` and `tests/test_camera_geometry_config.py` for the canonical examples.

Anchoring tests worth knowing:

- `conftest.py` — populates dummy `UPSTREAM_*` env vars before settings load; clears the `get_settings` cache between tests.
- `test_health.py` — TestClient hits `/healthz` and `/readyz` without entering the lifespan (avoids needing a real DB).
- `test_app_routes.py` — enumerates the mounted route table and asserts every Phase 1+ endpoint is present. New routes MUST be added here.
- `test_api_tokens.py` — token format, uniqueness of prefixes, argon2 round-trip.
- `test_session_jwt.py` — JWT mint/verify, expiry, tampering, audience/issuer enforcement.
- `test_streamid.py` — `publish:` round-trip + rejection of unknown prefixes; size pin (`< 64 bytes`) so a future regression that re-embeds a JWT trips the suite.
- `test_shared_secret.py` — constant-time compare semantics.
- `test_access_log_redaction.py` — verifies the `mediamtx-auth/<secret>` path is redacted before logging.
- `test_processor.py`, `test_camera_geometry_config.py`, `test_perception_snapshots.py`, `test_observations.py` — the M8.0 + P5/P6 feature suites; together they exercise the eligibility matrix, geometry validation, and the open-vocabulary observations read path.
- `test_reliability_endpoints.py`, `test_reliability_evaluator.py`, `test_reliability_trends.py`, `test_reliability_config_comparison.py`, `test_detection_confidence.py`, `test_detection_metrics.py`, `test_detection_metrics_rollup.py`, `test_image_analysis.py`, `test_snapshot_brightness.py` — the reliability track (M0–M7 findings/evaluator/trends + M11 comparison, M5/M6 metrics, M1 brightness).
- `test_models.py`, `test_processing_config.py`, `test_camera_processing_config.py`, `test_processing_config_dimensions.py` — the model-config track (M2 catalog, M3 account master switch, M3/M4 per-camera config + registry validation, M9 dimension extraction).

## `scripts/`

- `seed_dev_account.py` — driven by `make dev-account`. Creates the Supabase Auth user `dev@upstream.test` (so the `handle_new_user` trigger provisions the tenant), then mints a fresh API token and prints the plaintext. Idempotent on re-run; `--reset` purges and re-creates; `--token-only` skips the seed work and just mints a token.
- `seed_dev_token.py` — deprecated shim: re-execs `seed_dev_account.py --token-only`. Kept for older docs and any external scripts that reference the old name.
- `smoke_local.py` — end-to-end smoke test: register agent → register camera → start session → decode JWT claims → stop session. Reads `UPSTREAM_API_URL` and `UPSTREAM_DEV_TOKEN` from env.
- `run_detection_metrics_rollup.py` (reliability M6) — manual maintenance runner. Connects with `UPSTREAM_DATABASE_URL` (service-role) and calls `run_detection_metrics_rollup()`, printing a JSON summary of counts (`rolled_up_buckets` / `raw_rows_pruned` / `hourly_rows_pruned`). Retention windows overridable via `--lookback-hours` / `--raw-retention-hours` / `--hourly-retention-days`. Not a public endpoint; no cron yet (run manually / future systemd timer).
- `smoke_whep_proxy.py` — exercises the WHEP proxy against a running api-server: bootstraps a session, POSTs an offer, DELETEs the resource, verifies the 400-on-invalid-resource-id negative path. Falls back to the 409-not-streaming path when no publisher is running.
- `_smoke_reliability_inprocess.py` (reliability M0/M1-A) — in-process smoke walk of the reliability slice: drives the assembled app via TestClient against a fake pool through `healthy → unknown → needs_review → critical → dedupe → resolve → 404 scope`. Deliberately not collected by pytest — a verification walk, not a regression suite.
