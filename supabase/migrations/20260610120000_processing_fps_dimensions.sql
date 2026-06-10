-- Model-config M9.1: dedicated processing-FPS reliability dimensions.
--
-- M7E (upstream-processor) made the saved per-camera processing_fps genuinely
-- applied as the perception-snapshot OUTPUT CADENCE (1/fps emit floor) — and
-- the M8 lineage block on every snapshot now carries
-- requested.processing_fps / applied.processing_fps plus the honest
-- processing_fps_output_cadence_only warning. M9 stored the other config
-- dimensions but deliberately deferred FPS columns; M9.1 adds them so
-- reliability can cleanly group "default FPS vs 5 FPS vs 1 FPS" instead of
-- only inferring it from config revision/warnings.
--
-- Honest scope (mirrors the M7E caveat): processing_fps is output cadence
-- only — it does NOT reduce DeepStream inference/GPU work, and these columns
-- record what the processor stamped, nothing more. No runtime behavior
-- changes here; no backfill (old rows keep NULL FPS dimensions and group as
-- the existing NULL-dimension bucket, exactly like the M9 columns).
--
-- Requested AND applied are both stored for FPS (unlike the other M9
-- dimensions, which store applied only): "asked for 5, got default"
-- vs "asked for nothing" is precisely the comparison FPS grouping exists to
-- answer, and the warnings codes alone cannot distinguish the requested value.

alter table snapshot_detection_metrics
  add column requested_processing_fps int,
  add column applied_processing_fps   int;

alter table detection_confidence_hourly_rollups
  add column requested_processing_fps int,
  add column applied_processing_fps   int;

-- Re-key the bucket uniqueness to include the FPS dimensions, extending the
-- M9 expression unique index with the same coalesce-sentinel pattern
-- (-1 is unreachable: the write path bounds fps to 1..30 and the extractor
-- nulls anything else). Existing rows all carry NULL in both new columns, so
-- the wider key cannot collide where the old key was unique — safe on a
-- populated table, no backfill.
--
-- DEPLOY ORDER NOTE (same as M9): between applying this migration and
-- deploying the M9.1 code, a run of the OLD rollup script would fail to infer
-- its (narrower) ON CONFLICT arbiter and error out — harmless (single
-- transaction) but apply migration + deploy + rerun in that order. The rollup
-- job is the manual scripts/run_detection_metrics_rollup.py.
--
-- The rollup's `on conflict (...)` target MUST stay byte-identical to this
-- expression list or the upsert fails to infer the index (a test pins both).
drop index detection_confidence_hourly_rollups_bucket_idx;
create unique index detection_confidence_hourly_rollups_bucket_idx
  on detection_confidence_hourly_rollups
     (account_id, camera_id, bucket_start, coalesce(package_id, ''),
      coalesce(processing_config_source, ''),
      coalesce(processing_config_revision, -1),
      coalesce(processing_config_status, ''),
      coalesce(applied_model_key, ''),
      coalesce(applied_selected_classes_key, ''),
      coalesce(applied_confidence_threshold, -1.0),
      coalesce(requested_processing_fps, -1),
      coalesce(applied_processing_fps, -1),
      coalesce(processing_config_warnings, '{}'::text[]));

-- No new read indexes: the trend reads stay anchored on the existing
-- (account_id, camera_id, bucket_start) indexes; per-camera windows are small
-- and the FPS predicates only narrow them further (same call as M9).
-- No RLS/grant changes: new columns inherit the existing member-select policy.
