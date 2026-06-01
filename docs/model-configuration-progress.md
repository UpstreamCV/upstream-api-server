# Model Configuration — Progress Log

A milestone log for the per-camera model/processing configuration work across
the platform (`upstream-api-server`, `upstream-dashboard`, `upstream-processor`).
Each milestone notes which repo it landed in.

## Honest status

> Users can configure model, classes, tracker, confidence threshold, and
> processing FPS per camera in the dashboard, and the saved config is now
> carried to the processor and stamped as requested-vs-applied lineage — but
> live detector behavior is still unchanged (nothing from the config is
> applied at runtime yet).

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

## Not yet done

- **Processor consumption** — the live processor still runs its hard-coded
  detector and applies nothing from this config. M7B wire-carries the
  processing config on eligible-sessions (and stamps lineage), but actually
  applying model/class/tracker selection is deferred to a later milestone.
- **DeepStream config wiring** — the nvinfer/nvdsanalytics runtime config is
  untouched by the per-camera processing config.
- **Reliability grouping** — no reliability rollup dimension for model or
  processing config.
- **Applied-config metadata on observations** — observations do not yet
  record which model/processing config was actually applied.

## Decisions (final)

- Default config `model_key` is `trafficcamnet_v1`.
- `model_key` naming is **snake_case**.
- An empty `selected_classes` (`[]`) means **all supported classes**.
- The per-camera processing config is a **new table**
  (`camera_processing_configs`), separate from the account-level
  `processing_enabled` switch — they are distinct concerns and do not
  share schemas or routes.
