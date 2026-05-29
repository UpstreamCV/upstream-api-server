-- Reliability M5: detection confidence history.
--
-- A compact, append-only time series of detection-confidence SUMMARY metrics
-- per camera. Each accepted perception snapshot ingest writes ONE row here
-- (in addition to the one-row-per-camera UPSERT into
-- `latest_perception_snapshots`). This gives the reliability evaluator a
-- recent baseline to compare the current snapshot against, so it can detect
-- "this camera's model confidence is degrading vs. its own recent behavior"
-- (drop-vs-baseline) rather than only judging the latest frame in isolation.
--
-- DELIBERATELY compact: only numeric summaries are stored, NEVER the raw
-- snapshot / objects JSON. The source of truth for current visual state stays
-- `latest_perception_snapshots`; this table is history for drift detection.
--
-- Uniqueness is (camera_id, observed_at) — one metric per camera per
-- frame-time. We do NOT key on `perception_snapshot_id`: that id comes from
-- `latest_perception_snapshots`, which is UPSERTed on camera_id alone (one
-- row per camera, the id is preserved across overwrites), so it is per-camera
-- stable, NOT per-frame. `perception_snapshot_id` is kept here only as a soft
-- traceability reference (no FK, no uniqueness).
--
-- Written ONLY by the API on the processor-secret-authed snapshot ingest
-- path; tenant columns (account_id / camera_id) are derived from the
-- stream_sessions row, never trusted from the request body — same posture as
-- latest_perception_snapshots / observations / reliability_findings.

create table snapshot_detection_metrics (
  id                       uuid primary key default gen_random_uuid(),
  account_id               uuid not null references accounts(id) on delete cascade,
  camera_id                uuid not null references cameras(id) on delete cascade,
  stream_session_id        uuid references stream_sessions(id) on delete set null,
  -- Soft reference to the latest_perception_snapshots row that produced this
  -- metric. Per-camera stable (not per-frame) by design — for traceability
  -- only, hence no FK / uniqueness on it.
  perception_snapshot_id   uuid,
  processor_id             text,
  package_id               text,

  observed_at              timestamptz not null,

  -- Detection volume on the frame.
  object_count             int not null default 0,
  -- How many objects carried a usable confidence value (>= this is what the
  -- baseline aggregates over). May be < object_count when objects lack a
  -- confidence field.
  confidence_sample_count  int not null default 0,

  -- Summary stats. NULL when there were too few confidence samples to
  -- summarise (the row is still written to preserve "few/no detections"
  -- history; the evaluator's baseline query ignores null-stat rows).
  avg_confidence           double precision,
  median_confidence        double precision,
  min_confidence           double precision,
  max_confidence           double precision,
  low_confidence_count     int not null default 0,
  low_confidence_ratio     double precision,
  confidence_stddev        double precision,

  source                   text not null default 'latest_perception_snapshot',

  created_at               timestamptz not null default now()
);

-- One metric per camera per accepted frame-time. The snapshot ingest path
-- inserts with `on conflict (camera_id, observed_at) do nothing`, so a
-- processor retry of the same observed_at is idempotent.
create unique index snapshot_detection_metrics_camera_observed_idx
  on snapshot_detection_metrics(camera_id, observed_at desc);

-- The reliability evaluator's baseline query scopes by account_id + camera_id
-- over a recent observed_at window.
create index snapshot_detection_metrics_account_camera_observed_idx
  on snapshot_detection_metrics(account_id, camera_id, observed_at desc);

-- Session-scoped history reads (e.g. a future per-session drift view).
create index snapshot_detection_metrics_session_observed_idx
  on snapshot_detection_metrics(stream_session_id, observed_at desc);

-- This table is append-only (no UPDATEs), so it needs no set_updated_at
-- trigger and carries only created_at.

alter table snapshot_detection_metrics enable row level security;

-- Read-only for account members if ever exposed directly. The API server
-- reads/writes via the service role (RLS-exempt); this policy + the grant
-- below follow the documented safest pattern for new dashboard-eligible
-- tables (see 20260513150000_dashboard_grants.sql and
-- 20260529130000_grant_authenticated_realtime_tables.sql).
create policy snapshot_detection_metrics_member_select on snapshot_detection_metrics
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- Hosted Supabase does not auto-grant SELECT to `authenticated` for tables
-- created via migration; granting now keeps this table consistent with the
-- other RLS-protected dashboard tables (RLS still restricts which rows are
-- visible, so the grant is safe). Not added to the supabase_realtime
-- publication — nothing subscribes to it.
grant select on public.snapshot_detection_metrics to authenticated;
