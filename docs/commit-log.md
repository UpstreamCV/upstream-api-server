# Commit log

Reverse chronological. Newest at top. Every meaningful commit gets an entry following the template at the bottom of this file.

---

## 2026-06-01 - Model config M7B: carry processing_config on eligible-sessions (api-server, stamp-only)

### Summary
The per-camera `camera_processing_configs` row (M3/M4) was saveable but
invisible to the processor — eligible-sessions only carried `geometry_config`.
M7B clones that envelope **additively** so the internal eligible-sessions
response now also carries `processing_config` `{revision, config}` (the
`CameraProcessingConfig.model_dump()` shape), `null` when the camera has no
row. This is STAMP-ONLY: nothing from the config is applied. The processor's
status payload gains an additive `applied_processing_config` block whose
`applied` is ALWAYS the current runtime defaults
(`trafficcamnet_v1` / `[]` all-classes / `iou` / `null` / `null`) — a
requested value is never echoed into `applied`, and `partially_applied` is
reserved for a later milestone. Live behavior is unchanged: the LEFT join
(never inner) does not touch eligibility, the WHERE clause,
`processing_enabled`, or `package_id`; no runtime/inference/analytics/geometry
path is edited. The only observable changes are one new nullable envelope
field and one additive status-payload key.

### Files touched
- `schemas/processor.py` — new `EligibleProcessingConfig(BaseModel)`
  (`revision: int`, `config: dict[str, Any]`), mirroring
  `EligibleGeometryConfig`; new nullable
  `EligibleProcessorSession.processing_config` field (default `None`).
- `services/processor.py` — `_ELIGIBLE_SESSIONS_SQL` gains a
  `left join camera_processing_configs pc on pc.camera_id = ss.camera_id`
  and `pc.revision as pc_revision, pc.config as pc_config`. WHERE clause /
  eligibility untouched.
- `routers/processor.py` — `EligibleProcessorSession(...)` construction adds
  the null-safe `processing_config` attach (mirrors `geometry_config`);
  imports `EligibleProcessingConfig`.
- `docs/model-configuration-progress.md`, `docs/api-contracts.md`,
  `docs/commit-log.md`.

### Functions/classes changed
- `EligibleProcessingConfig` — strongly-typed envelope around the opaque
  processing-config JSON; the nested shape is validated on the write path,
  not re-validated here.
- `EligibleProcessorSession` — gains `processing_config: EligibleProcessingConfig | None`.

### System interaction notes
`GET /v1/internal/processor/eligible-sessions → fetch_eligible_sessions →
_ELIGIBLE_SESSIONS_SQL (LEFT join camera_processing_configs) →
EligibleProcessorSession(processing_config=… if pc_revision is not None else
None) → ProcessorEligibleSessionsResponse`. The processor consumes this for
lineage only; the runtime continues to run its hard-coded defaults.

### Testing/checks
Deferred to a later step (ruff + mypy strict + pytest), not run in this commit.

### Follow-up TODOs
- Actually apply the per-camera processing config (model/class/tracker
  selection) in the runtime — deferred past M7B's stamp-only scope.
- `partially_applied` status emission once partial application lands.

---

## 2026-05-30 - Reliability M7: confidence-trend read API (backs the Reliability tab chart)

### Summary
M6 created and scheduled the hourly `detection_confidence_hourly_rollups`; that
history was invisible to users. M7 adds the read side: a camera-scoped endpoint
that serves hourly detection-confidence history for the dashboard's new
"Detection confidence trend" chart. The rollup table holds one row per
(camera, hour, package); the read **aggregates across packages to one
camera-level point per hour**, re-weighting the per-package weighted means by
`confidence_sample_count` per-field (byte-for-byte the M6 weighting), over a
both-edges hour-aligned window of completed hours. Internal weighted column
names are renamed to clean public names and no tenant/internal columns are
exposed. Pure read — the evaluator and M5 drift are untouched (drift still reads
raw `snapshot_detection_metrics`). No migration (the M6 table + its
`(account_id, camera_id, bucket_start desc)` index already exist).

### Files touched
- `services/reliability_trends.py` (new) — `_CONFIDENCE_TREND_SQL` (per-package
  weighted aggregation, hour-aligned `[now-range, now)` window), `_RANGES`
  (`24h`/`7d`), `get_confidence_trend_for_camera()`, pure `summarize_confidence_trend()`.
- `schemas/reliability.py` — `ConfidenceTrendRange`/`Point`/`Summary`/`Response`.
- `routers/reliability.py` — `GET /v1/cameras/{camera_id}/reliability/confidence-trend?range=`
  (reuses the exact ownership/scoping idiom; `Literal` range → 422).
- `tests/test_reliability_endpoints.py` (+`FakeReliabilityConn` trend branch and
  endpoint cases), `tests/test_reliability_trends.py` (new — SQL pins + summary math).
- `docs/api-contracts.md`, `docs/module-map.md`.

### System interaction notes
`GET …/reliability/confidence-trend → CurrentCaller.resolve_account → camera
ownership check (404 camera_not_found) → get_confidence_trend_for_camera →
_CONFIDENCE_TREND_SQL over detection_confidence_hourly_rollups → ConfidenceTrendResponse`.
The dashboard fetches it resiliently (any failure → "unavailable" card, never
breaking the Reliability tab).

### Testing/checks
- `uv run pytest -q` → 543 passed (19 new M7 + M5/M6 regression). ruff + `mypy src`
  strict clean. SQL sqlglot-validated (Postgres dialect, 6 FILTER clauses).
- Designed + reviewed via multi-agent workflows. The adversarial review caught a
  latent off-by-one: the read window had no upper bound, so `order by asc limit`
  could (in a clock-skew/backfill edge) drop the newest point. Fixed by adding an
  explicit hour-aligned upper bound `bucket_start < date_trunc('hour', now)` so
  the window admits exactly `lookback_hours` completed hours — self-bounding,
  independent of the M6 writer's current-hour exclusion.
- No live DB locally; SQL pinned by substring + sqlglot.

### Follow-up TODOs
- 30d range; optional daily buckets; a second plotted series (low-confidence
  ratio is tooltip-only in v1); per-package breakdown.

---

## 2026-05-29 - Reliability M6: detection metrics retention + hourly rollup

### Summary
`snapshot_detection_metrics` (M5) is append-only and grows ~1 row/sec/camera, so
it can't be kept at full resolution forever. M6 adds retention + downsampling:
completed hours of raw metrics are rolled up into a new compact
`detection_confidence_hourly_rollups` table (per account/camera/hour/package
weighted confidence summaries, kept ~90d), raw rows past a 72h hour-aligned
cutoff are pruned **only where an hourly bucket already covers them**, and very
old hourly rows are aged out — all in one atomic maintenance pass driven by a
manual script. No charts yet; this is the storage/retention foundation. The M5
drift evaluator is deliberately untouched — it keeps reading raw metrics (its
~30-min window is far inside the 72h raw-retention horizon); rollups are
long-term history for future Reliability-tab trend charts, not a drift input.

Safety design: both the rollup lookback lower bound and the prune cutoff are
HOUR-ALIGNED, so the rollup only ever scans whole hours and the prune deletes
whole buckets atomically — a bucket is never recomputed from, or pruned out of,
a partial slice of its hour. `ROLLUP_LOOKBACK_HOURS=96 > RAW_METRIC_RETENTION_HOURS=72`
on purpose: the 24h surplus is catch-up so a lapsed-then-resumed run still rolls
up prunable hours before deleting their raw rows. The coverage join (not the age
filter) is the load-bearing "never delete un-rolled data" guarantee.

### Files touched
- `supabase/migrations/20260529150000_detection_confidence_hourly_rollups.sql` —
  new rollup table: per (account, camera, hour, package) weighted summaries,
  unique EXPRESSION index on `(account_id, camera_id, bucket_start, coalesce(package_id, ''))`,
  three trend-read indexes, `set_updated_at` trigger, RLS member-select + grant
  to `authenticated`. Numeric only — no raw snapshot/JSON.
- `src/upstream_api/services/detection_metrics_rollup.py` — new service. SQL
  string constants `_ROLLUP_HOURLY_SQL` (idempotent `INSERT … ON CONFLICT DO
  UPDATE`, weighted aggregates per-field FILTERed, hour-aligned window),
  `_PRUNE_RAW_SQL` (DELETE…USING coverage join, hour-aligned cutoff),
  `_PRUNE_ROLLUPS_SQL`; named retention constants; `run_detection_metrics_rollup()`
  (one transaction, single captured `now`); `RollupRunResult` (+`to_dict`); a
  pure-Python reference `compute_hourly_rollup_buckets()` (executable spec for the
  weighted math).
- `scripts/run_detection_metrics_rollup.py` — manual runner (service-role DSN,
  argparse window overrides, prints JSON counts, no secrets logged).
- `tests/test_detection_metrics_rollup.py` — 23 unit tests (SQL/migration pins,
  service orchestration + transaction + error unwind, weighted math, hour-aligned
  lookback boundary, NULL/'' package fold, per-field FILTER, prune coverage).
- `docs/api-contracts.md`, `docs/module-map.md` — M6 notes.

### Functions/classes changed
- `run_detection_metrics_rollup(conn, *, now=None, lookback_hours, raw_retention_hours, rollup_retention_days)`
  — rollup → prune-raw → prune-rollups in one `conn.transaction()`; returns a
  `RollupRunResult` of counts parsed from the asyncpg command tags.
- `compute_hourly_rollup_buckets(rows, *, now, lookback_hours)` — pure reference
  mirroring `_ROLLUP_HOURLY_SQL` (hour-aligned windows, per-field weighted means,
  NULL/'' package fold), used by the math tests.

### System interaction notes
`scripts/run_detection_metrics_rollup.py → get_settings() → asyncpg.connect(service-role DSN)
→ run_detection_metrics_rollup → [rollup INSERT…ON CONFLICT, prune-raw DELETE…USING,
prune-rollups DELETE] in one transaction → RollupRunResult → JSON to stdout`.
No HTTP surface; reads/writes via the service role (RLS-exempt). M5 ingest +
drift path is unchanged.

### Testing/checks
- `uv run pytest -q` → 524 passed (23 new M6 + full M5/reliability regression).
- `ruff check` + `ruff format --check` clean; `mypy src` strict clean (69 files).
- SQL syntactically validated with sqlglot (postgres dialect): the two DELETEs,
  the rollup INSERT…SELECT body (FILTER / make_interval / weighted division), the
  table DDL and the expression unique index all parse; the `ON CONFLICT
  (coalesce(...))` expression target is valid Postgres (confirmed sqlglot-only
  parser gap on expression conflict targets).
- Designed + reviewed via multi-agent workflows; the adversarial review caught
  and fixed (a) a non-hour-aligned rollup lower bound that could corrupt the
  boundary bucket and delete never-rolled rows, and (b) a NULL-vs-empty-string
  `package_id` ON CONFLICT collision that would abort the whole run.
- No live DB available locally (no Docker/psql/supabase) — see manual verification.

### Follow-up TODOs
- Manual verify against hosted DB after migration applies (run the script, confirm
  buckets + weighted values + coverage-guarded prune).
- Future: schedule the script (systemd timer / cron) once a pattern exists;
  daily rollups; the actual 24h/7d Reliability-tab trend charts that consume
  `detection_confidence_hourly_rollups`.
- Optional defense-in-depth: normalize empty-string `package_id` → NULL at ingest
  (M5 writer / `PerceptionSnapshotIngestRequest`) so '' never persists at all
  (the rollup already folds NULL/'' safely, so this is belt-and-suspenders).

---

## 2026-05-29 - Reliability M5 fix: window-aggregate confidence drift (works at low object counts)

### Summary
Field finding: on real cameras (1-3 objects/frame) the M5 drift finding never
fired and `snapshot_detection_metrics.avg_confidence` was always null, because
both the metric writer and the drift comparison reused M2's per-frame
`MIN_SAMPLE_COUNT = 5` gate — meaningful for judging a single frame, wrong for
a trend over time. This decouples them: the writer now summarises a per-row
average for any frame with ≥1 detection, and drift is judged over rolling,
weighted TIME WINDOWS (current = last 10 min vs. prior = 10-30 min ago, adjacent
and non-overlapping) instead of the latest single frame. Robustness now comes
from total samples per window (`MIN_BASELINE_SAMPLE_COUNT`), so a quiet scene
still accumulates plenty of signal. The M2 per-frame static rule (avg / ratio /
stddev over the latest snapshot) keeps its ≥5 gate and is unchanged. No
migration change — the table is identical; rows simply start carrying
`avg_confidence` for low-count frames, and the windows self-populate from new
rows (no backfill).

### Files touched
- `services/detection_confidence.py` — `compute_detection_confidence_stats`
  gains a `min_sample_count` param (writer passes 1; default stays 5); dropped
  `BASELINE_EXCLUSION_SECONDS` (windows are adjacent now).
- `services/detection_metrics.py` — writer summarises for ≥1 sample
  (`_MIN_METRIC_SAMPLE_COUNT`); `fetch_detection_confidence_baseline` →
  `fetch_detection_confidence_windows` returning current + prior
  `DetectionConfidenceWindow`s (`_WINDOW_SQL`, no per-row min filter).
- `services/reliability_evaluator.py` — `_Facts.detection_confidence_windows`;
  rule restructured so the per-frame static signal (null at low count) and the
  windowed drift signal are independent — either can open or resolve; drift
  uses `current` vs `baseline` window averages. Evidence gains
  `current_avg_confidence` / `current_confidence_sample_count` /
  `current_metric_count`; `source` = `recent_window_history` when windowed.
- Tests updated across `test_detection_confidence.py`, `test_detection_metrics.py`,
  `test_reliability_evaluator.py` (window-routing fake by span; headline
  low-object-count drift test), `test_reliability_endpoints.py`, and the
  smoke `scripts/_smoke_reliability_inprocess.py` (Test 10 now low-count).
- `docs/api-contracts.md` — windowed drift + new evidence fields.

### Testing/checks
- `uv run pytest` — 501 passed. ruff + ruff format + mypy(strict) clean.
  Reliability smoke PASS. Dashboard: tsc / eslint / vitest clean (adds a
  "Recent avg confidence" row for `current_avg_confidence`).

### Follow-up TODOs
- Retention/downsampling of `snapshot_detection_metrics` (unchanged from M5).
- The current/prior window sizes + drop threshold are still fixed constants;
  per-camera or learned sensitivity is a later milestone.

---

## 2026-05-29 - Reliability M5: detection confidence history + drift

### Summary
`unstable_detection_confidence` used to judge only the latest perception
snapshot against static thresholds. M5 adds confidence **history** so it can
also say "this camera's model confidence has dropped versus its own recent
behaviour". A new append-only `snapshot_detection_metrics` table stores a
compact numeric summary per accepted snapshot (computed at ingest time, reusing
the M2 `detection_confidence` extractor/stats — never the raw snapshot JSON).
The reliability evaluator reads a weighted recent **baseline** from that table
during fact gathering and opens the finding when the current average confidence
has dropped by more than `CONFIDENCE_DROP_RATIO_WARNING_THRESHOLD` (0.30) — even
when the absolute numbers still clear the static thresholds. Static-threshold
behaviour is unchanged; a missing/too-thin baseline only disables the drop
comparison and never blocks resolution. Evidence now fills the previously-null
`previous_avg_confidence` / `drop_ratio` plus `baseline_*` and a
`trigger_reasons` list. The metric write is best-effort (savepoint-isolated) so
it can never break snapshot ingest. No processor changes (it already emits
per-object `confidence`).

### Files touched
- `supabase/migrations/20260529140000_snapshot_detection_metrics.sql` (new) —
  `snapshot_detection_metrics` (compact summary columns, `unique(camera_id,
  observed_at)` idempotency target — NOT `perception_snapshot_id`, which is
  per-camera-stable; account/camera/session/observed_at indexes; RLS
  member-select + `grant select ... to authenticated`; no jsonb, append-only).
- `src/upstream_api/services/detection_metrics.py` (new) —
  `write_detection_metrics_for_perception_snapshot` (idempotent
  `on conflict do nothing` INSERT; writes a row even with too-few samples,
  null stats then) + `fetch_detection_confidence_baseline` (weighted average
  over the previous window, excluding the most recent slice; `None` when
  history is too thin) + `DetectionMetricWriteResult` / `DetectionConfidenceBaseline`.
- `src/upstream_api/services/detection_confidence.py` — repurposed the
  reserved M3 window constants for M5 (`PREVIOUS_WINDOW_SECONDS` 600→1800),
  added `BASELINE_EXCLUSION_SECONDS` (120) + `MIN_BASELINE_SAMPLE_COUNT` (10),
  and `count_detection_objects`.
- `src/upstream_api/routers/processor.py` — `post_perception_snapshot` calls
  `_write_detection_metrics_safe` after the upsert, inside a savepoint +
  try/except (logs scalar ids only on failure).
- `src/upstream_api/services/reliability_evaluator.py` — `_gather_facts`
  fetches the baseline; `_Facts` gains `detection_confidence_baseline`; the
  rule folds a baseline-guarded drop term into the trigger/resolve predicates,
  fills the baseline evidence + `trigger_reasons`, and switches `source` to
  `latest_perception_snapshot_with_history` when a baseline exists.
  `DETECTOR_VERSION` v0.5 → v0.6.
- `tests/test_detection_metrics.py` (new, 16) — writer + baseline + SQL/migration
  pins. `tests/test_detection_confidence.py` — `count_detection_objects` +
  M5 constant pins. `tests/test_perception_snapshots.py` — ingest also writes a
  metric; metric failure doesn't break the POST. `tests/test_reliability_evaluator.py`
  — drop-opens / no-drop-resolves / thin-baseline / missing-baseline-still-resolves
  / both-reasons; DETECTOR_VERSION pin → v0.6. `tests/test_reliability_endpoints.py`
  — `FakeReliabilityConn` gains a `baseline_row`. `scripts/_smoke_reliability_inprocess.py`
  — Test 10 (drop opens → recovery resolves).
- `docs/api-contracts.md` — perception-snapshots M5 side-effect + evidence fields.

### Testing/checks
- `uv run pytest` — 500 passed. `ruff check` / `ruff format --check` clean.
  `mypy src` clean (strict). `_smoke_reliability_inprocess.py` — PASS.
- Dashboard: `tsc --noEmit` clean, `eslint` clean, vitest reliability suites pass.
- No live DB verification: fake-pool unit tests + production-SQL / migration pins,
  per repo convention.

### Follow-up TODOs
- `snapshot_detection_metrics` is append-only and unbounded (~1 row/sec/camera
  at the processor's snapshot rate). A retention / downsampling policy is a
  recommended next task before this runs long at scale.
- A camera-level confidence trend chart (Reliability tab) is intentionally
  deferred — M5 is text-only.

---

## 2026-05-29 - docs: Supabase Realtime requirements + "SUBSCRIBED but no events" troubleshooting

### Summary
Captured the reusable lessons from the multi-day Realtime debugging arc as
durable reference docs (the change history was only in commit-log entries).
`architecture.md` gains a "Supabase Realtime for dashboard push (RLS tables)"
section: the full requirement set (publication membership, `REPLICA IDENTITY
FULL`, `GRANT SELECT ... TO authenticated`, member-select RLS, client-side
`setAuth`-before-subscribe) and the rule that new realtime/dashboard tables must
include the grant (+ replica identity) in their creating migration — because the
API reads via the service role (RLS-exempt) so a missing grant is invisible to
the API yet makes Realtime silently deliver nothing. `troubleshooting.md` gains
a "Realtime channel SUBSCRIBED but no events" decision tree (401 → token;
no-events → missing grant / RLS false; UPDATEs dropped → replica identity) with
the SQL checks. Docs only.

## 2026-05-29 - GRANT SELECT to authenticated on realtime/dashboard tables created after the original backfill (the actual cause of "Realtime delivers no events")

### Summary
The dashboard live perception overlay's Supabase Realtime subscription never
delivered events: channel SUBSCRIBED, valid token (no 401), table in the
`supabase_realtime` publication and `REPLICA IDENTITY FULL`, processor upserting
~3/s — yet zero change events reached the client ("Fallback polling").

Root cause: **`latest_perception_snapshots` was missing `GRANT SELECT ... TO
authenticated`.** `20260513150000_dashboard_grants.sql` documents the trap —
hosted Supabase doesn't auto-grant table SELECT to `authenticated` for tables
created via migration, and "without these grants, RLS policies are unreachable:
Postgres rejects with 42501 before RLS runs." That backfill covered tables
existing on 2026-05-13; `latest_perception_snapshots` was created 2026-05-21 and
slipped through. The api-server reads via the **service role** (privileged,
RLS-exempt) so the gap was invisible to polling — but **Supabase Realtime
evaluates RLS AS the `authenticated` role**, hits 42501 before the
`member_select` policy runs, and silently delivers nothing.

`20260529130000_grant_authenticated_realtime_tables.sql` grants SELECT to
`authenticated` on `latest_perception_snapshots` (fixes the overlay) plus the
other RLS-protected dashboard tables created after the original backfill
(`camera_geometry_configs`, `reliability_findings`, `processor_statuses`,
`observations`) to match the documented per-table pattern and prevent the
identical bug. RLS still restricts rows, so the grant is safe. No app code
change. Confirmed fixed when the overlay chip reads "Realtime connected".

### Follow-up
New tables intended for dashboard/Realtime reads MUST include
`grant select on public.<table> to authenticated;` in their creating migration
(see the header of `20260513150000_dashboard_grants.sql`).

## 2026-05-29 - REPLICA IDENTITY FULL on Realtime-published RLS tables (perception snapshots + stream sessions)

### Summary
The dashboard live detection overlay's Supabase Realtime subscription was
`SUBSCRIBED` but received **zero** `postgres_changes` events ("Fallback polling"
forever), despite the table being on the `supabase_realtime` publication and the
processor actively upserting rows.

Root cause: **Supabase Realtime does not deliver `UPDATE`/`DELETE` events for an
RLS-protected table unless the table is `REPLICA IDENTITY FULL`.** It authorizes
each change by evaluating the table's RLS policy against the record, but with the
default replica identity the WAL only carries the PRIMARY KEY for the old tuple,
so a policy referencing other columns (`account_id`, in
`latest_perception_snapshots_member_select` / `stream_sessions_member_select`)
can't be evaluated → the event is dropped. `latest_perception_snapshots` is
UPSERTed (one row per camera) so after the first INSERT every write is an UPDATE
→ everything was dropped. `stream_sessions` has the same shape (state-transition
UPDATEs the camera-detail Realtime listener depends on).

Migration `20260529120000_replica_identity_full_realtime.sql` sets
`REPLICA IDENTITY FULL` on both tables. Idempotent; negligible extra WAL for
these small one-row-per-camera / per-session tables. No app code change here;
the dashboard already filters to the viewed camera client-side.

**Status:** the fix is confirmed only once the dashboard overlay chip reads
"Realtime connected" (temporary console diagnostics added on the dashboard side
to verify end-to-end this round).

## 2026-05-22 - P6-A: Geometry config API

### Summary
Adds backend support for dashboard-driven ROI/line geometry. Users will draw
zones/lines in the dashboard (P6-B) and the processor will use those saved
shapes (P6-C); P6-A is the storage + API foundation. A new
`camera_geometry_configs` table holds ONE upserted config row per camera. Two
new `current_caller` camera-scoped endpoints read and write it (`GET`/`PUT
/v1/cameras/{camera_id}/geometry-config`) — the repo's first `PUT`. The
processor eligible-sessions response carries the saved config in a new
`geometry_config` field (replacing the always-null `zones_lines`
placeholder). Geometry is **processing geometry only** — colors/CSS/styling
are rejected (422), staying frontend-owned. Coordinates are normalized
`[0, 1]` for resolution independence. `config.version` (schema version,
always 1) and the DB row `revision` (bumps on every save) are kept strictly
separate.

### Files touched
- `supabase/migrations/20260522120000_camera_geometry_configs.sql` (new) —
  `camera_geometry_configs` (unique `(camera_id)` upsert target, `revision`
  int, `config` jsonb, `set_updated_at` trigger, RLS member-select).
- `src/upstream_api/schemas/geometry.py` (new) — `GeometryPoint`,
  `GeometryFrame`, `GeometryRoi`, `GeometryLine`, `GeometryConfig` (all
  `extra="forbid"`; duplicate-id `model_validator`), `GeometryConfigRecord`,
  `GeometryConfigResponse`, `GeometryConfigUpdateRequest`.
- `src/upstream_api/services/geometry.py` (new) — `DEFAULT_GEOMETRY_CONFIG` +
  `default_geometry_config_record`, `_SELECT_GEOMETRY_CONFIG_SQL` /
  `fetch_geometry_config`, `_UPSERT_GEOMETRY_CONFIG_SQL` /
  `upsert_geometry_config` (revision bump on conflict).
- `src/upstream_api/routers/cameras.py` — `GET`/`PUT
  /v1/cameras/{camera_id}/geometry-config` (`current_caller`;
  camera-ownership 404; default fallback on GET).
- `src/upstream_api/schemas/processor.py` — new `EligibleGeometryConfig`
  envelope; `EligibleProcessorSession.zones_lines` → `geometry_config`.
- `src/upstream_api/services/processor.py` — `_ELIGIBLE_SESSIONS_SQL` left
  joins `camera_geometry_configs`.
- `src/upstream_api/routers/processor.py` — eligible-sessions row mapping
  builds the `geometry_config` envelope.
- `src/upstream_api/errors.py` — `validation_exception_handler` now
  `jsonable_encoder`s `exc.errors()` so a `model_validator` `ValueError`
  (its object lands in `ctx`) does not break JSON encoding.
- `tests/test_camera_geometry_config.py` (new) — 29 cases. `tests/test_processor.py`
  — `geometry_config` null/envelope + SQL pin. `tests/test_app_routes.py` —
  2 new routes.
- `docs/api-contracts.md` — both endpoints + the eligible-sessions field.

### Testing/checks
- `uv run pytest` — all pass (333 total; 31 new).
- `uv run ruff check src tests` / `ruff format --check` — clean.
- `uv run mypy src` — clean (strict).
- No live DB verification: route-level tests use a fake asyncpg pool; separate
  tests pin the production upsert/select SQL and the migration file structure.

### Strict non-goals
- No dashboard geometry editor (P6-B), no processor consumption of the config
  (P6-C) — saved geometry only takes effect on a future session.
- No live reload, no geometry history, no overlay style settings.
- No styling fields (colors/CSS/stroke/fill/opacity) stored — rejected at the
  schema layer.

---

## 2026-05-21 - P5-C: Latest perception snapshot API

### Summary
Adds the API foundation for visual overlays. A "perception snapshot" is the
processor's current visual state — detected objects (bbox, tracker id, class,
confidence, ROI labels) plus the configured ROI/line geometry. It is a
dedicated **latest-state** path, deliberately separate from `observations`
(which is an append-only event log): a new `latest_perception_snapshots`
table holds ONE upserted row per camera. A new internal processor write
endpoint upserts it (tenant derived from the session, never the body); a new
camera-scoped `current_caller` read endpoint serves it for a future dashboard
overlay (P5-D). Nothing was added to `ProcessorObservationType` / the
`observations` table — the two paths share nothing.

### Files touched
- `supabase/migrations/20260521120000_latest_perception_snapshots.sql` (new)
  — `latest_perception_snapshots` (unique `(camera_id)` upsert target,
  `snapshot` jsonb, `set_updated_at` trigger, RLS member-select).
- `src/upstream_api/schemas/processor.py` — `PerceptionSnapshotIngestRequest`,
  `PerceptionSnapshotRecord`, `LatestPerceptionSnapshotResponse`. `snapshot`
  is an opaque `dict` (envelope validated; nested shape not deep-validated).
- `src/upstream_api/services/processor.py` — `_UPSERT_PERCEPTION_SNAPSHOT_SQL`
  (`on conflict (camera_id)`), `_LATEST_PERCEPTION_SNAPSHOT_SQL`,
  `upsert_perception_snapshot`, `fetch_latest_perception_snapshot`.
- `src/upstream_api/routers/processor.py` — `POST /v1/internal/processor/perception-snapshots`
  (`current_processor`; tenant via `load_session_state`).
- `src/upstream_api/routers/cameras.py` — `GET /v1/cameras/{camera_id}/perception/latest`
  (`current_caller`; camera-ownership 404; 404 `perception_snapshot_not_found`
  when none yet, mirroring latest-frame).
- `tests/test_perception_snapshots.py` (new) — 19 cases. `tests/test_app_routes.py`
  — 2 new routes in the manifest.
- `docs/api-contracts.md` — both endpoints.

### Testing/checks
- `uv run pytest` — all pass (19 new).
- `uv run ruff check src tests` / `ruff format --check` — clean.
- `uv run mypy src` — clean (strict).
- No live DB verification: route-level tests use a fake asyncpg pool; a
  separate test pins the production upsert/read SQL.

### Strict non-goals
- No dashboard overlay (P5-D), no bounding-box drawing, no live WebRTC
  overlay, no geometry editor, no rules/events/webhooks.
- Latest-state only — no historical perception snapshots.

---

## 2026-05-21 - P5-A: Recent observations read endpoint

### Summary
Adds `GET /v1/cameras/{camera_id}/observations/recent` so operators can see
recent processor observations for a camera without direct DB access — closing
the blind-testing gap the P5-A0 probe identified (the processor writes seven
real observation types via P4-B..P4-E, but nothing could read them back).
Read-only, `current_caller`-authed (agent token OR dashboard user JWT),
double-scoped to the caller's account: an explicit camera-ownership 404 check
AND an `account_id`-scoped query. `type` is an optional exact-match filter
over the open-vocabulary `observations.type` column — an unknown value
returns an empty list, never a 422. The response models `type` as a plain
string (not `ProcessorObservationType`) so older or future observation types
never break a read. Newest-first by `observed_at`, `limit` 1-200 (default 50).
No migration — the `observations` table and its indexes already exist (M8.0).
API-server only; the dashboard panel is the separate P5-B task.

### Files touched
- `src/upstream_api/schemas/processor.py` — new `ObservationRecord` and
  `RecentObservationsResponse` models (`account_id` omitted from the read
  response; `type` is plain `str`, not the write-side `Literal`).
- `src/upstream_api/services/processor.py` — new `_RECENT_OBSERVATIONS_SQL`
  constant and `fetch_recent_observations` helper, double-scoped by
  `account_id` + `camera_id`, optional `type` filter, `observed_at desc`.
- `src/upstream_api/routers/cameras.py` — new `list_recent_observations`
  handler; lean `select account_id from cameras` ownership check reusing the
  existing `camera_not_found` 404 code (no cross-tenant existence oracle).
- `tests/test_observations.py` (new) — 23 cases (route happy path, ordering,
  payload passthrough, limit bounds, type filter, open-vocabulary unknown
  type, 404 unknown/cross-tenant camera, 401 unauthenticated, tenant
  scoping, response-shape, all 7 P4 types round-tripping, production-SQL pin).
- `tests/test_app_routes.py` — new route added to the mounted-routes manifest.
- `docs/api-contracts.md` — endpoint contract under Phase 4.5 reads.

### Testing/checks
- `uv run pytest` — all pass (23 new in `test_observations.py`).
- `uv run ruff check src tests` / `ruff format --check` — clean.
- `uv run mypy src` — clean (strict).
- No live DB verification: route-level tests use a fake asyncpg pool whose
  `fetch` re-implements the WHERE/ORDER/LIMIT; a separate test pins the
  production SQL so the simulation cannot drift.

### Strict non-goals
- No dashboard panel (P5-B), no session-scoped companion endpoint, no
  overlays, no rules/events/webhooks, no server-side idempotency.
- No DB migration; no new index (the `observations` table from M8.0 is
  reused — `camera_id` has no dedicated index, acceptable at debug volume).

---

## 2026-05-18 - M8.0: Processor Readiness Gate

### Summary
Adds the safe API contract a future `upstream-processor` uses to discover
only processable streams, report status, and post mock observations —
before any processor or DeepStream code exists. Wires the already-present
but unrouted `current_processor` shared-secret dependency to a new
`/v1/internal/processor` router. The eligibility filter lives entirely in
SQL: a session is returned only if `state='streaming'` AND `expires_at >
now()` AND its **latest** `stream_validations` row exists with
`passed=true` — so the documented gap (validation enforcement may be OFF
and a failed stream may still be readable) cannot leak an unsafe stream.
`account_id`/`camera_id` are always derived from the session row, never
trusted from the request. No `srt_url`, token, or secret is ever returned.
M8.0 does not decode video and does not run DeepStream.

### Files touched
- `supabase/migrations/20260518140000_processor_status_observations.sql` (new)
  — `processor_statuses` (upsert by `(processor_id, stream_session_id)`,
  6-value status CHECK, RLS member-select, `set_updated_at` trigger) and
  append-only `observations` (open `type`, API-enforced; RLS member-select).
- `src/upstream_api/config.py` — new `mediamtx_rtsp_base`
  (`UPSTREAM_MEDIAMTX_RTSP_BASE`); read surface, not the SRT publish URL.
- `src/upstream_api/schemas/processor.py` (new) — eligible/status/observation
  request+response models; `Literal` enums for status + the 3 mock types.
- `src/upstream_api/services/processor.py` (new) — eligibility SQL (inner
  `join lateral` picks the single latest validation), status upsert,
  observation insert, jsonb/datetime coercion helpers.
- `src/upstream_api/routers/processor.py` (new) — 3 endpoints, all
  `CurrentProcessor`-authed; tenant derived via `load_session_state`;
  malformed `streamid` rows skipped (logged) not 500.
- `src/upstream_api/app.py` — `include_router(processor.router)`.
- `tests/test_processor.py` (new) — 31 cases; `tests/test_app_routes.py`
  — 3 new routes added to the manifest.
- `docs/api-contracts.md` — M8.0 endpoint section, env-var rows.

### Functions/classes changed
- `services.processor.fetch_eligible_sessions / upsert_processor_status /
  insert_observation` — single-statement DB helpers, tenant passed in by
  the caller (derived from the session, never the body).
- `routers.processor.eligible_sessions / report_status / post_observation`
  — auth-gated handlers; status/observation reuse `load_session_state` for
  tenant + 404.

### System interaction notes
`current_processor (X-Upstream-Webhook-Secret) → routers.processor →
services.processor / services.stream_validation.load_session_state →
asyncpg pool → processor_statuses|observations|stream_sessions →
safe-field response`. The ingest webhook secret does **not** authenticate
these routes (separate `UPSTREAM_PROCESSOR_WEBHOOK_SECRET`).

### Testing/checks
- `uv run pytest` — 252/252 pass (31 new).
- `uv run ruff check src tests` — clean.
- `uv run ruff format --check src tests` — clean.
- `uv run mypy src` — clean (strict).
- No live DB verification: route-level tests use a fake asyncpg pool. The
  eligibility fake re-implements the documented WHERE clause and a
  separate test pins the production SQL so the two cannot drift.

### Follow-up TODOs
- M8.1: build `upstream-processor`, mock runtime consuming this contract.
- Deferred: package/model registry tables, DeepStream config generation,
  zones/lines, rules engine, stale-session sweeper.

---

## 2026-05-18 - M6.2-delta: webhook-authed POST /v1/ingest/snapshots

### Summary
Adds the streaming-server snapshot upload path so M6.1A's locally
extracted JPEG can reach Storage + the dashboard. Webhook-secret authed
(`X-Upstream-Webhook-Secret`), not agent-token authed. `account_id` /
`agent_id` / `camera_id` are derived from the session row — never trusted
from the request. Only `streaming` sessions are accepted. Reuses the
existing Storage/quota/`snapshots`-table pipeline; no DB migration.

### What changed
- New `POST /v1/ingest/snapshots` (third router `ingest_router` in
  `routers/snapshots.py`, full-path, mounted separately — same convention
  as `camera_router`). Resolves the session via `load_session_state`;
  404 `session_not_found` if missing, 409 `stream_state_invalid`
  (`details.current_state`) if not `streaming`.
- Extracted the shared pipeline into module-private helpers
  `_read_validated_jpeg` / `_enforce_snapshot_quota` / `_persist_snapshot`.
  `POST /v1/snapshots` refactored onto the same helpers — behavior and SQL
  byte-identical (its 7 tests pass unchanged).
- Tenant-spoof-safe: only `session_id`/`captured_at`/`file`/`width`/
  `height` are declared, so any `camera_id`/`agent_id`/`account_id`/
  `storage_key` form fields are ignored; `storage_key` is server-built
  from the session's tenant.

### Files
- `src/upstream_api/routers/snapshots.py` — 3 helpers + `ingest_router` +
  `ingest_upload_snapshot`; `upload_snapshot` refactored onto the helpers;
  new imports (`asyncpg`, `Settings`, `session_id_var`, `CurrentIngest`,
  `load_session_state`).
- `src/upstream_api/app.py` — `app.include_router(snapshots.ingest_router)`.
- `tests/test_ingest_snapshots.py` (new) — 17 cases: happy, 401
  (missing/wrong secret), 404 unknown session, 409 (approved/ended/
  failed/expired), 415/400/413 file, 429 quota, 404 account_limits, 500
  storage-failure (no insert), tenant-spoof ignored, plus 2 latest-frame
  regression tests.
- `docs/api-contracts.md` — new endpoint section + `empty_upload` row +
  widened `stream_state_invalid` row.
- `docs/module-map.md` — `routers/` + `tests/` bullets.

### Test runs
`uv run pytest` — 219/219 pass.
`uv run ruff check src tests` — clean.
`uv run ruff format --check src tests` — clean.
`uv run mypy src` — clean (52 source files).

### Strict non-goals
- No streaming-server / dashboard / agent change (the M6.1B/M6.2 wiring
  that POSTs the local JPEG is a separate, later task).
- No DB migration (reuses the `snapshots` table).
- No change to `POST /v1/snapshots` semantics, the `snapshots` bucket
  privacy, `load_session_state`, or M4/M5 auth/lifecycle/validation.
- No `expires_at` check — a `streaming` row past expiry is accepted;
  session expiry remains governed by the ingest lifecycle webhooks.
- M6 is not complete.

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
