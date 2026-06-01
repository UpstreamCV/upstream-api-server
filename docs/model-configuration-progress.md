# Model Configuration — Progress Log

A milestone log for the per-camera model/processing configuration work in
`upstream-api-server`. This tracks the API-server surface only.

## Honest status

> Users/API can save intended per-camera processing configuration, but the
> live processor does not consume it yet.

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

## Not yet done

- **Processor consumption** — the eligible-sessions envelope and the
  DeepStream config are untouched; the live processor still runs its
  hard-coded detector and does not read this config.
- **Dashboard UI** — no model-picker / processing-config editor yet.
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
