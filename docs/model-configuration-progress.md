# Model Configuration — Progress Log

A milestone log for the per-camera model/processing configuration work across
the platform (`upstream-api-server`, `upstream-dashboard`, `upstream-processor`).
Each milestone notes which repo it landed in.

## Honest status

> Users can configure model, classes, tracker, confidence threshold, and
> processing FPS per camera in the dashboard. The saved config is carried to
> the processor, stamped as requested-vs-applied lineage on processor status,
> snapshots, and observations, and reliability metrics can now be grouped and
> filtered by applied processing config dimensions. Confidence threshold and
> selected classes are applied only where supported; model switching, tracker
> changes, processing FPS, and class-filtered line/ROI counts remain not
> live-applied.

## Milestones

### M1 — Probe complete

Investigated how the processor selects a detector today and where per-camera
model selection would slot in; confirmed there is no existing model registry
or per-camera processing config to build on, and that the processor runs a
hard-coded detector.

### M2 — Static model registry added

Added a read-only, in-process model catalog and exposed it at
`GET /v1/models` (returns `ModelsListResponse` — `{ "models": [...] }`).
There is no DB table behind it; the catalog is platform-global static data.
Two entries ship today, both `enabled: true`:

- `trafficcamnet_v1` — TrafficCamNet object detector (runtime `deepstream`;
  supported classes `car`, `bicycle`, `person`, `roadsign`; tracker `iou`;
  default confidence `0.4`).
- `mock_detect_v0` — synthetic development/testing detector (runtime `mock`;
  supported classes `person`, `car`; tracker `none`).

The registry is the source of truth the per-camera processing config
validates against.

### M3 — Per-camera `camera_processing_configs` table + GET/PUT API

Cloned from the geometry-config pattern: one row per camera (unique on
`camera_id`), UPSERT semantics where a save fully replaces the prior config.

- `GET /v1/cameras/{camera_id}/processing-config` — returns the saved
  config, or a **non-destructive default** (`is_default: true`,
  `revision: 0`, `id`/timestamps `null`, `model_key: "trafficcamnet_v1"`)
  when the camera has none yet. No write side effects.
- `PUT /v1/cameras/{camera_id}/processing-config` — full-replace upsert;
  `revision` starts at `1` and bumps by one on every subsequent PUT.
  `account_id`/`camera_id` are derived server-side, never trusted from the
  body.

`config.version` (JSON schema version, always `1`) and `revision` (DB row
revision) are kept distinct and never conflated.

### M4 — `selected_classes` + registry validation

Added registry-backed validation on write (PUT), surfaced as `422`
`validation_error` on violation:

- `model_key` must exist in the registry **and** be `enabled`; unknown or
  disabled models are rejected.
- `selected_classes` must be a subset of the model's `supported_classes`;
  an empty list (`[]`) means **all supported classes**.
- `tracker`, when set, must be in the model's `supported_trackers`.
- `confidence_threshold` in `[0, 1]`; `processing_fps` in `[1, 30]`;
  unknown/extra fields (including any styling/overlay field) rejected.

The base config payload re-validates structurally on read, so a stored row
stays readable even if the registry later changes — registry checks gate
writes only.

### M5 — Dashboard "Model & Detection" UI: model + class selection (`upstream-dashboard`)

New `/dashboard/cameras/{cameraId}/configure/detection` route, cloned from the
geometry-editor triad (server page + Server Action + client panel). It reads
`GET /v1/models` and GET/PUTs the per-camera processing config:

- Model dropdown sourced from the registry (disabled/unavailable models are
  shown but not selectable).
- "All supported classes" toggle + a per-class checklist; an empty selection
  persists as `[]` (= all supported, never "detect nothing").
- Save flows through a Server Action → `PUT .../processing-config` →
  `revalidatePath`; a status tile shows "Not configured yet" vs
  "Operator-saved r{revision}".

Tracker / confidence / FPS were deferred to M6 (sent as `null`). Saves operator
intent only — no runtime effect.

### M6 — Dashboard tracker / confidence / FPS controls (`upstream-dashboard`)

Completed the Model & Detection editor with the three remaining fields (the
api-server already validated them in M4):

- **Tracker** — `<select>` ("Runtime default" = `null`, else a value from the
  model's `supported_trackers`); changing the model resets an unsupported tracker.
- **Confidence threshold** — range slider + numeric input with a "Use default"
  (`null`) affordance; helper copy shows the model's `default_confidence_threshold`.
- **Processing FPS** — preset `<select>` (Default = `null`, else 1/2/5/10/15/30).

The client validator was flipped from "these must ride as `null`" to real
bounds checks matching the api-server (tracker ∈ `supported_trackers`,
confidence `[0, 1]`, fps integer `[1, 30]`; `null` always valid). The save
payload now sends real values (or `null`), and the saved-config summary shows
all five fields. Copy makes clear the config is saved but not live-applied.

### M7B — Processing config envelope + requested-vs-applied lineage

Two repos: `upstream-api-server` carries the saved config on eligible-sessions;
`upstream-processor` resolves it at worker startup and stamps lineage. STAMP-ONLY,
cloned additively from the geometry-config envelope (P6-A). The internal
`GET /v1/internal/processor/eligible-sessions` response now carries the camera's
saved processing config alongside `geometry_config`:

```json
"processing_config": { "revision": <int>, "config": <CameraProcessingConfig> } | null
```

- `processing_config` is `null` when the camera has no
  `camera_processing_configs` row (a LEFT join, never inner — eligibility,
  the WHERE clause, `processing_enabled`, and `package_id` resolution are
  untouched).
- `config` is the `CameraProcessingConfig.model_dump()` shape
  (`version`, `model_key`, `selected_classes`, `tracker`,
  `confidence_threshold`, `processing_fps`), kept opaque on the wire and
  validated only on the write path.

**Runtime behavior is unchanged.** M7B is stamp-only: nothing from the config
is applied. The processor status payload gains an additive
`applied_processing_config` block whose `applied` is ALWAYS the current
runtime defaults (`trafficcamnet_v1` / `[]` = all classes / `iou` /
`null` confidence / `null` fps) — a requested value is never echoed into
`applied`, and `partially_applied` is reserved for a later milestone and is
never emitted in M7B. The only observable changes are the new nullable
envelope field and the additive status-payload key.

### M7C — Apply per-camera `confidence_threshold` to live PGIE config (`upstream-processor`)

The first live-apply milestone: for supported DeepStream sessions, a saved
`confidence_threshold` is applied to the live PGIE (nvinfer) config at worker
startup. The lineage block reflects it (`applied.confidence_threshold` echoes
the requested value, `status` becomes `applied`/`partially_applied`); when the
runtime mode does not support it, the value stays default and a
`confidence_threshold_requested_not_applied` warning is stamped instead.

### M7D — Apply per-camera `selected_classes` (post-inference filter) (`upstream-processor`)

`selected_classes` is now applied through supported post-inference per-object
filtering paths (with class-label canonicalisation so registry labels match
model labels, e.g. `roadsign` ↔ `road_sign`). Caveat, stamped as a warning on
every config whose selected_classes is applied: **line/ROI aggregate counts
remain all-class** (`selected_classes_analytics_counts_not_filtered`) — only
per-object outputs are filtered. Model switching, tracker, and FPS remain
stamp-only.

### M8 — Stamp `applied_processing_config` onto snapshots + observations (`upstream-processor`)

The resolved lineage block (`source`, `revision`, `status`, `reason`,
`requested{...}`, `applied{...}`, `warnings[]`) is now stamped onto every
perception snapshot at `snapshot.source.applied_processing_config` and onto
every observation at `payload.source.applied_processing_config`, in addition
to the processor status payload (M7B). Downstream records are self-describing:
any consumer can tell which config a snapshot/observation ran under. The
api-server ingest paths preserve the block byte-for-byte (opaque
`dict[str, Any]` schemas into JSONB columns) — no api-server change was needed.

### M9 — Reliability grouping by applied config dimensions (`upstream-api-server`)

Reliability metrics are now config-aware: the M8 lineage on each snapshot is
reduced to a compact, queryable dimension set at metric-ingest time, preserved
through the hourly rollups, and exposed through a new grouped/filtered trend
read. **M9 adds no new runtime apply behavior** — it only records and serves
what was already applied.

- **Extraction** (`services/processing_config_dimensions.py`) — pulls from
  `snapshot.source.applied_processing_config`: `processing_config_source`,
  `processing_config_revision`, `processing_config_status`,
  `applied_model_key`, `applied_selected_classes_key` (canonical key:
  `[]`/null → `all`, `["person","car"]` → `car,person`, malformed →
  `unknown`), `applied_confidence_threshold`, `processing_config_warnings`
  (text[] of codes), and the headline boolean
  `selected_classes_analytics_counts_not_filtered`. Extraction never raises;
  pre-M8 or malformed snapshots yield all-NULL dimensions. Requested values
  are deliberately not columnised — the warnings codes record
  requested-but-not-applied facts and the full block stays in the snapshot /
  observation JSONB.
- **Migration** (`20260609120000_processing_config_dimensions.sql`) —
  additive nullable columns on `snapshot_detection_metrics` (all eight) and
  `detection_confidence_hourly_rollups` (seven; the boolean is derivable from
  warnings there), plus a re-key of the rollup unique index to include the
  dimension folds (`coalesce(dim, sentinel)`, the M6 package_id pattern). No
  backfill: old rows keep NULL dimensions and group as one "no metadata"
  bucket.
- **Rollups** (`services/detection_metrics_rollup.py`) — the hourly rollup
  GROUPs by the dimensions, so an hour spanning a config change yields one
  row per config instead of one blended point (mixed-config hours are split,
  not flagged). Old NULL-dimension raw rows still roll up; package_id
  splitting and prune safety are unchanged.
- **Read API** — the existing
  `GET /v1/cameras/{id}/reliability/confidence-trend` is byte-identical
  (its `GROUP BY bucket_start` re-weights across the extra rows exactly as it
  already did across packages). NEW:
  `GET /v1/cameras/{id}/reliability/confidence-trend/by-config` returns one
  series per applied-config group (dimensions + points + per-group summary),
  with optional filters `processing_config_revision`,
  `processing_config_status`, `applied_selected_classes_key`,
  `applied_confidence_threshold`, and `warning` (e.g.
  `selected_classes_analytics_counts_not_filtered`). Pre-M8 history appears
  as the all-null-dimension group, ordered first.
- **Dashboard** — deliberately untouched (M10 candidate: per-config series /
  filters on the Reliability tab, config labels in the trend tooltip).

This answers "did r4 perform better than r3", "did threshold 0.2 improve the
confidence trend", "did person-only reduce noisy detections", "which configs
were partially applied", and "which metrics carry the analytics-counts
caveat" — with the standing caveat that grouped trends show association, not
proven causation (scene conditions change too).

## Not yet done

- **Model switching at runtime** — the live processor still runs its
  hard-coded detector; `model_key` is stamped but never applied
  (`model_switch_requested_not_applied`).
- **Tracker + processing FPS apply** — stamped but not applied
  (`tracker_requested_not_applied`, `processing_fps_requested_not_applied`).
- **Class-filtered line/ROI counts** — DeepStream nvdsanalytics aggregate
  counts remain all-class even when `selected_classes` is applied to
  per-object outputs (`selected_classes_analytics_counts_not_filtered`).
- **Dashboard reliability config filters/comparison UI** — the M9 by-config
  API has no dashboard surface yet (M10).

## Decisions (final)

- Default config `model_key` is `trafficcamnet_v1`.
- `model_key` naming is **snake_case**.
- An empty `selected_classes` (`[]`) means **all supported classes**.
- The per-camera processing config is a **new table**
  (`camera_processing_configs`), separate from the account-level
  `processing_enabled` switch — they are distinct concerns and do not
  share schemas or routes.
