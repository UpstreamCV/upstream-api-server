-- Model-config M9: applied-processing-config dimensions on the reliability
-- metrics tables.
--
-- M8 (upstream-processor) started stamping the resolved processing-config
-- lineage onto every perception snapshot at
-- `snapshot.source.applied_processing_config` (and onto observation payloads).
-- M9 makes the reliability layer ABLE TO SEE that lineage: the M5 metric
-- writer extracts a compact dimension set from the snapshot at ingest, the M6
-- rollup preserves those dimensions instead of collapsing them, and the trend
-- read API can group/filter by them. This migration only adds the columns and
-- re-keys the rollup uniqueness; the extraction/rollup SQL lives in
-- `services/detection_metrics.py` / `services/detection_metrics_rollup.py`
-- and the new read SQL in `services/reliability_trends.py`.
--
-- What this migration deliberately does NOT do:
-- - No backfill. Old rows keep NULL dimensions ("no config metadata") and old
--   pre-M8 snapshots keep ingesting with NULL dimensions — both group under a
--   single NULL-dimension bucket exactly like a NULL package_id does today.
-- - No new runtime behavior. M9 is read/metadata only: model switching,
--   tracker changes, processing FPS, and class-filtered line/ROI counts remain
--   NOT live-applied; this migration records what WAS applied, nothing more.
-- - No requested-value columns. The warnings codes already record requested-
--   but-not-applied facts, and the full lineage block stays available in the
--   `latest_perception_snapshots.snapshot` / `observations.payload` JSON.
-- - No new read indexes. The trend reads stay anchored on the existing
--   (account_id, camera_id, bucket_start) indexes; the per-config dimensions
--   only narrow those small per-camera windows further. Revisit if a
--   cross-camera config-comparison read path ever appears.
--
-- Dimension columns (all nullable; NULL = "no config metadata", pre-M8):
--   processing_config_source    'operator_saved'|'default'|'fallback_default_on_error'
--   processing_config_revision  camera_processing_configs.revision (save counter)
--   processing_config_status    'applied'|'partially_applied'|'requested_not_applied'
--                               |'applied_defaults'|'fallback_default_on_error'
--   applied_model_key           e.g. 'trafficcamnet_v1'
--   applied_selected_classes_key  canonical key: ''/null -> 'all',
--                               ['car','person'] (any order) -> 'car,person',
--                               malformed -> 'unknown'
--   applied_confidence_threshold  the threshold actually applied (M7C), or NULL
--   processing_config_warnings  text[] of producer warning codes, e.g.
--                               'selected_classes_analytics_counts_not_filtered'
--   selected_classes_analytics_counts_not_filtered (raw table only)
--                               the headline M7D caveat as a boolean; on the
--                               rollup table it is derivable from
--                               processing_config_warnings (`? = any(...)`),
--                               so it is not duplicated there.
--
-- Open vocabularies on purpose (no CHECK constraints): the value sets are
-- produced by the processor's resolver and may grow; the columns are grouping
-- dimensions, not enums the dashboard switches on.

-- ---------------------------------------------------------------------------
-- Raw per-frame metrics (M5 table): the writer stamps the dimensions of the
-- snapshot that produced each row.
-- ---------------------------------------------------------------------------

alter table snapshot_detection_metrics
  add column processing_config_source       text,
  add column processing_config_revision     int,
  add column processing_config_status       text,
  add column applied_model_key              text,
  add column applied_selected_classes_key   text,
  add column applied_confidence_threshold   double precision,
  add column processing_config_warnings     text[],
  add column selected_classes_analytics_counts_not_filtered boolean;

-- ---------------------------------------------------------------------------
-- Hourly rollups (M6 table): the rollup GROUPs by these dimensions so an hour
-- in which the camera ran under two different applied configs produces two
-- distinguishable rows instead of one blended point.
-- ---------------------------------------------------------------------------

alter table detection_confidence_hourly_rollups
  add column processing_config_source       text,
  add column processing_config_revision     int,
  add column processing_config_status       text,
  add column applied_model_key              text,
  add column applied_selected_classes_key   text,
  add column applied_confidence_threshold   double precision,
  add column processing_config_warnings     text[];

-- Re-key the bucket uniqueness to include the config dimensions. Same plain
-- (NOT partial) EXPRESSION unique index pattern as before: every nullable
-- dimension is folded to a sentinel via coalesce so NULL ("no metadata") rows
-- get exactly one bucket per hour, while no sentinel value is ever STORED
-- (the rollup projects nullif(...) — see services/detection_metrics_rollup.py).
--
-- Existing rolled-up rows all carry NULL in every new dimension, so the new,
-- WIDER key cannot collide where the old key was unique — the drop/create is
-- safe on a populated table and needs no backfill.
--
-- DEPLOY ORDER NOTE: between applying this migration and deploying the M9
-- code, a run of the OLD rollup script would fail to infer its (narrower)
-- ON CONFLICT arbiter and error out — harmless (single transaction, nothing
-- committed) but apply migration + deploy + rerun in that order. The rollup
-- job is the manual `scripts/run_detection_metrics_rollup.py`, so just don't
-- run it mid-deploy.
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
      coalesce(processing_config_warnings, '{}'::text[]));

-- No RLS/grant changes: both tables already carry the member-select policy and
-- authenticated grant; new columns inherit them.
