-- M8.0 Processor Readiness Gate.
--
-- Two write targets for a future processor (upstream-processor, not built
-- yet). Both are written ONLY by the API on behalf of a processor-secret-
-- authed caller; tenant columns (account_id / camera_id) are always derived
-- from the stream_sessions row, never trusted from the request body. Reads
-- are dashboard SELECT via PostgREST under RLS, same posture as heartbeats /
-- usage_events.

-- ----------------------------------------------------------------------------
-- processor_statuses: the current lifecycle/health of one processor working
-- one session. Upsert semantics (one row per processor_id + stream_session_id)
-- rather than the insert-only heartbeats model: a supervisor polls "what is
-- the latest status of this worker", not a time series. `updated_at` is kept
-- honest by the shared trigger so staleness can be derived by readers.
-- ----------------------------------------------------------------------------
create table processor_statuses (
  id                   uuid primary key default gen_random_uuid(),
  account_id           uuid not null references accounts(id) on delete cascade,
  camera_id            uuid not null references cameras(id) on delete cascade,
  stream_session_id    uuid not null references stream_sessions(id) on delete cascade,
  processor_id         text not null,
  runtime              text not null,
  package_id           text not null,
  status               text not null
                       check (status in (
                         'starting', 'attached', 'processing',
                         'lagging', 'failed', 'stopped'
                       )),
  fps_input            numeric,
  fps_processed        numeric,
  last_frame_at        timestamptz,
  last_observation_at  timestamptz,
  error_code           text,
  error_message        text,
  payload              jsonb not null default '{}'::jsonb,
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now()
);

-- One current-status row per (processor, session). Backs the upsert
-- ON CONFLICT target in services/processor.py.
create unique index processor_statuses_processor_session_idx
  on processor_statuses(processor_id, stream_session_id);

create index processor_statuses_account_updated_idx
  on processor_statuses(account_id, updated_at desc);
create index processor_statuses_session_idx
  on processor_statuses(stream_session_id);

create trigger processor_statuses_set_updated_at
  before update on processor_statuses
  for each row execute function set_updated_at();

alter table processor_statuses enable row level security;

create policy processor_statuses_member_select on processor_statuses
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- ----------------------------------------------------------------------------
-- observations: append-only normalized output from a processor. M8.0 only
-- accepts the three mock event types (enforced at the API layer, not by a DB
-- CHECK, so M8.1+ can add real types without a migration — same open-`type`
-- posture as usage_events.event_type).
-- ----------------------------------------------------------------------------
create table observations (
  id                 uuid primary key default gen_random_uuid(),
  account_id         uuid not null references accounts(id) on delete cascade,
  camera_id          uuid not null references cameras(id) on delete cascade,
  stream_session_id  uuid not null references stream_sessions(id) on delete cascade,
  processor_id       text,
  package_id         text,
  type               text not null,
  payload            jsonb not null default '{}'::jsonb,
  observed_at        timestamptz not null,
  created_at         timestamptz not null default now()
);

create index observations_account_observed_idx
  on observations(account_id, observed_at desc);
create index observations_session_observed_idx
  on observations(stream_session_id, observed_at desc);
create index observations_type_observed_idx
  on observations(type, observed_at desc);

alter table observations enable row level security;

create policy observations_member_select on observations
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );
