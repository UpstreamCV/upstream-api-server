-- Phase 3 telemetry tables: heartbeats (agent liveness), snapshots (latest
-- frame uploads), and usage_events (billing telemetry). All three are write-
-- heavy from the API; reads are dashboard SELECT via PostgREST under RLS.

-- ----------------------------------------------------------------------------
-- heartbeats: agent posts in batches of <= 100 every few seconds.
-- Insert-only; oldest rows are pruned by a retention job out-of-band.
-- ----------------------------------------------------------------------------
create table heartbeats (
  id             uuid primary key default gen_random_uuid(),
  account_id     uuid not null references accounts(id) on delete cascade,
  agent_id       uuid not null references agents(id) on delete cascade,
  camera_id      uuid references cameras(id) on delete set null,
  session_id     uuid references stream_sessions(id) on delete set null,
  status         text not null
                 check (status in ('online', 'streaming', 'error', 'reconnecting')),
  fps            real,
  bitrate_kbps   int,
  width          int,
  height         int,
  codec          text,
  last_error     text,
  agent_version  text,
  reported_at    timestamptz not null,
  created_at     timestamptz not null default now()
);

create index heartbeats_agent_recent_idx  on heartbeats(agent_id, reported_at desc);
create index heartbeats_camera_recent_idx on heartbeats(camera_id, reported_at desc)
  where camera_id is not null;
create index heartbeats_session_idx       on heartbeats(session_id)
  where session_id is not null;

alter table heartbeats enable row level security;

create policy heartbeats_member_select on heartbeats
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- ----------------------------------------------------------------------------
-- snapshots: latest-frame JPEG uploads, stored in Supabase Storage. The DB row
-- holds metadata + a storage_key; the bytes never live in Postgres.
-- ----------------------------------------------------------------------------
create table snapshots (
  id            uuid primary key default gen_random_uuid(),
  account_id    uuid not null references accounts(id) on delete cascade,
  agent_id      uuid not null references agents(id) on delete cascade,
  camera_id     uuid not null references cameras(id) on delete cascade,
  session_id    uuid references stream_sessions(id) on delete set null,
  storage_key   text not null unique,
  content_type  text not null default 'image/jpeg',
  width         int,
  height        int,
  bytes         int not null check (bytes > 0),
  captured_at   timestamptz not null,
  created_at    timestamptz not null default now()
);

create index snapshots_camera_recent_idx  on snapshots(camera_id, captured_at desc);
create index snapshots_account_recent_idx on snapshots(account_id, created_at desc);

alter table snapshots enable row level security;

create policy snapshots_member_select on snapshots
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- ----------------------------------------------------------------------------
-- usage_events: append-only billing/quota ledger. services/usage.py already
-- writes to this table when present; this migration unblocks the path.
-- ----------------------------------------------------------------------------
create table usage_events (
  id           uuid primary key default gen_random_uuid(),
  account_id   uuid not null references accounts(id) on delete cascade,
  event_type   text not null,
  session_id   uuid references stream_sessions(id) on delete set null,
  quantity     bigint not null check (quantity >= 0),
  unit         text not null,
  occurred_at  timestamptz not null default now(),
  metadata     jsonb not null default '{}'::jsonb
);

create index usage_events_account_occurred_idx on usage_events(account_id, occurred_at desc);
create index usage_events_type_occurred_idx    on usage_events(event_type, occurred_at desc);

alter table usage_events enable row level security;

create policy usage_events_member_select on usage_events
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );
