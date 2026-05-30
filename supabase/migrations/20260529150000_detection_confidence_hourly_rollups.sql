-- Reliability M6: detection confidence hourly rollups (retention + downsampling).
--
-- `snapshot_detection_metrics` (M5) is an append-only, ~1-row-per-second-per-
-- camera time series. It is perfect for SHORT-TERM drift detection (the M5
-- evaluator reads the last ~30 min of raw rows) but must not grow forever.
-- M6 rolls older raw rows into compact HOURLY summaries here and then safely
-- prunes the raw rows that a rollup already covers.
--
-- Division of labour (keep these straight):
--   snapshot_detection_metrics        = raw, high-resolution, SHORT-TERM
--                                       (kept ~72h). M5 drift reads it.
--   detection_confidence_hourly_rollups = compact, hourly, LONG-TERM
--                                       (kept ~90d). For future Reliability-tab
--                                       trend charts / degradation history.
-- The M5 drift path is intentionally NOT changed by M6 — it still reads raw
-- metrics, which outlive its 30-min window by a wide margin.
--
-- DELIBERATELY compact, same posture as the raw table: ONLY numeric summaries
-- are stored, NEVER the raw snapshot / objects JSON, never secrets. One row per
-- (account, camera, hour-bucket, package).
--
-- The rollup/prune SQL itself lives in
-- `services/detection_metrics_rollup.py` (string constants executed over
-- asyncpg, mirroring the M5 writer/window queries) and is driven by the manual
-- maintenance script `scripts/run_detection_metrics_rollup.py`. This migration
-- only provides the table, its uniqueness/indexes, RLS, grant, and the
-- updated_at trigger. There is no pg_cron in this project and none is added.

create table detection_confidence_hourly_rollups (
  id                       uuid primary key default gen_random_uuid(),
  account_id               uuid not null references accounts(id) on delete cascade,
  camera_id                uuid not null references cameras(id) on delete cascade,
  -- A bucket aggregates an hour of one camera and can span multiple stream
  -- sessions, so there is no single correct session id. Kept nullable and
  -- written as NULL by the rollup (the column exists for a possible future
  -- session-scoped rollup); it is NOT part of the bucket key.
  stream_session_id        uuid references stream_sessions(id) on delete set null,
  -- Nullable, exactly like snapshot_detection_metrics.package_id. NULL means
  -- "no package reported". The unique index below folds NULL to '' via
  -- coalesce so all "no package" rows of an hour share one bucket — no
  -- sentinel string is ever stored in this column.
  package_id               text,

  -- Hour bucket: bucket_start = date_trunc('hour', observed_at),
  -- bucket_end   = bucket_start + interval '1 hour'. Half-open [start, end).
  bucket_start             timestamptz not null,
  bucket_end               timestamptz not null,

  -- Volume of raw rows folded into this bucket. metric_count counts EVERY raw
  -- frame in the hour (including quiet frames with zero confidence samples, so
  -- that every active hour produces a bucket and the prune can safely cover
  -- those rows). object_count_total / confidence_sample_count are plain sums.
  metric_count             int not null default 0,
  object_count_total       int not null default 0,
  confidence_sample_count  int not null default 0,

  -- Weighted summaries over the confidence-bearing raw rows in the bucket
  -- (each weighted by its confidence_sample_count). NULL when the hour carried
  -- no usable confidences at all (only quiet/empty frames). See the rollup SQL
  -- and its docstring for the exact formulae.
  --
  -- NOTE on avg_confidence_stddev_weighted: this is the sample-weighted MEAN of
  -- the per-frame population stddevs, NOT the true stddev of the hour's
  -- confidence distribution (that would need sum-of-squares, which M5 does not
  -- store). Good enough for a coarse long-term trend; do not read it as a real
  -- bucket stddev. The name signals "mean of stddevs".
  avg_confidence_weighted            double precision,
  min_confidence                     double precision,
  max_confidence                     double precision,
  avg_low_confidence_ratio_weighted  double precision,
  avg_confidence_stddev_weighted     double precision,

  source                   text not null default 'snapshot_detection_metrics_hourly',

  created_at               timestamptz not null default now(),
  updated_at               timestamptz not null default now()
);

-- One bucket per (account, camera, hour, package). package_id is nullable, so
-- a plain unique constraint cannot be used directly; an EXPRESSION unique index
-- folding NULL -> '' gives one bucket per "no package" hour. This is a plain
-- (NOT partial) expression unique index, so it IS a valid ON CONFLICT arbiter
-- (unlike the partial index footgun documented in services/reliability_repo.py).
-- The rollup's `on conflict (account_id, camera_id, bucket_start,
-- coalesce(package_id, ''))` target MUST stay byte-identical to this expression
-- or the upsert fails to infer the index (a test pins both).
create unique index detection_confidence_hourly_rollups_bucket_idx
  on detection_confidence_hourly_rollups
     (account_id, camera_id, bucket_start, coalesce(package_id, ''));

-- Future trend-chart read paths: per camera within an account, per camera, and
-- account-wide, each newest-first over the hourly buckets.
create index detection_confidence_hourly_rollups_account_camera_bucket_idx
  on detection_confidence_hourly_rollups(account_id, camera_id, bucket_start desc);
create index detection_confidence_hourly_rollups_camera_bucket_idx
  on detection_confidence_hourly_rollups(camera_id, bucket_start desc);
create index detection_confidence_hourly_rollups_account_bucket_idx
  on detection_confidence_hourly_rollups(account_id, bucket_start desc);

-- Unlike the append-only raw table, rollups are UPSERTed (a bucket is
-- recomputed and replaced while its source hour is still within raw retention),
-- so updated_at is meaningful and gets the standard trigger.
create trigger detection_confidence_hourly_rollups_set_updated_at
  before update on detection_confidence_hourly_rollups
  for each row execute function set_updated_at();

alter table detection_confidence_hourly_rollups enable row level security;

-- Read-only for account members (future Reliability-tab charts). Writes are by
-- the service role only (RLS-exempt), via the maintenance script. Same posture
-- and grant as snapshot_detection_metrics and the other dashboard tables.
create policy detection_confidence_hourly_rollups_member_select
  on detection_confidence_hourly_rollups
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.detection_confidence_hourly_rollups to authenticated;
