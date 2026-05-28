-- Reliability M0/M1-A: persisted findings.
--
-- One table that backs the dashboard's Reliability card. A `finding` is a
-- single rule-driven verdict about a camera (e.g. "stream is live but the
-- processor is silent"). Rules live in services/reliability_evaluator.py;
-- this table just stores what they decided.
--
-- The same finding type can open, get resolved, and open again over the
-- lifetime of a camera — so it is NOT a one-row-per-camera UPSERT. Instead a
-- partial unique index pins "at most one OPEN row per (camera_id, dedupe_key)"
-- and resolved rows accumulate as history. Evaluators upsert by dedupe_key:
-- update last_seen_at + evidence on a re-open, or insert if there is no
-- currently-open row. Healthy state flips an existing open row to 'resolved'.
--
-- Written and read only by the API server. Tenant columns (account_id /
-- camera_id) are always derived from the camera row in the router, never
-- trusted from the request — same posture as observations / processor_statuses.

create table reliability_findings (
  id                   uuid primary key default gen_random_uuid(),
  account_id           uuid not null references accounts(id) on delete cascade,
  camera_id            uuid not null references cameras(id) on delete cascade,
  stream_session_id    uuid references stream_sessions(id) on delete set null,
  processor_status_id  uuid references processor_statuses(id) on delete set null,

  -- Open vocabulary at the DB layer (same posture as observations.type and
  -- usage_events.event_type) so new rules can ship without a migration. The
  -- categorical buckets / severities / statuses are CHECK-constrained because
  -- the dashboard switches on them.
  finding_type         text not null,
  category             text not null
                       check (category in (
                         'stream', 'processor', 'scene_quality', 'geometry_config'
                       )),
  severity             text not null
                       check (severity in ('info', 'warning', 'critical')),
  status               text not null
                       check (status in ('open', 'resolved', 'suppressed', 'ignored'))
                       default 'open',

  title                text not null,
  message              text not null,
  likely_cause         text,
  recommended_action   text,

  evidence             jsonb not null default '{}'::jsonb,
  dedupe_key           text not null,
  detector_version     text not null,

  first_seen_at        timestamptz not null default now(),
  last_seen_at         timestamptz not null default now(),
  resolved_at          timestamptz,
  resolved_reason      text,

  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now()
);

-- At most one OPEN row per (camera, dedupe_key). Resolved rows are history
-- and may stack up. A new open finding can be inserted after a previous one
-- under the same key is resolved.
create unique index reliability_findings_open_dedupe_idx
  on reliability_findings(camera_id, dedupe_key)
  where status = 'open';

-- Summary endpoint reads open findings for one camera ordered by severity
-- then recency; account_id is the RLS tenant column.
create index reliability_findings_account_camera_status_idx
  on reliability_findings(account_id, camera_id, status);
create index reliability_findings_camera_last_seen_idx
  on reliability_findings(camera_id, last_seen_at desc);

create trigger reliability_findings_set_updated_at
  before update on reliability_findings
  for each row execute function set_updated_at();

alter table reliability_findings enable row level security;

create policy reliability_findings_member_select on reliability_findings
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );
