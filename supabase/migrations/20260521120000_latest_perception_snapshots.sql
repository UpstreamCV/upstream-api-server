-- P5-C Latest Perception Snapshot.
--
-- ONE current "what the processor currently sees" row per camera: detected
-- objects (bbox, tracker id, class, confidence, roi labels) plus the
-- configured ROI/line geometry. UPSERT semantics (unique on camera_id
-- alone) — in deliberate contrast to the append-only `observations` table.
-- Observations are an event/history log; this is latest visual state for a
-- dashboard overlay (P5-D).
--
-- Written ONLY by the API on behalf of a processor-secret-authed caller;
-- tenant columns (account_id / camera_id) are derived from the
-- stream_sessions row, never trusted from the request body. Reads are
-- dashboard SELECT under RLS, same posture as processor_statuses.

create table latest_perception_snapshots (
  id                 uuid primary key default gen_random_uuid(),
  account_id         uuid not null references accounts(id) on delete cascade,
  camera_id          uuid not null references cameras(id) on delete cascade,
  stream_session_id  uuid not null references stream_sessions(id) on delete cascade,
  processor_id       text,
  package_id         text,
  snapshot           jsonb not null,
  observed_at        timestamptz not null,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

-- ONE current snapshot per camera — the upsert ON CONFLICT target. Keyed on
-- camera_id alone (not per-session): a dashboard overlay wants "the latest
-- for this camera" regardless of which session produced it.
create unique index latest_perception_snapshots_camera_idx
  on latest_perception_snapshots(camera_id);

create index latest_perception_snapshots_account_updated_idx
  on latest_perception_snapshots(account_id, updated_at desc);
create index latest_perception_snapshots_session_idx
  on latest_perception_snapshots(stream_session_id);

create trigger latest_perception_snapshots_set_updated_at
  before update on latest_perception_snapshots
  for each row execute function set_updated_at();

alter table latest_perception_snapshots enable row level security;

create policy latest_perception_snapshots_member_select on latest_perception_snapshots
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );
