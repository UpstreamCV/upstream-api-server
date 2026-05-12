-- Stream sessions and their post-connect validation records.

-- ----------------------------------------------------------------------------
-- Lifecycle enum. We collapse `pending` away: limit checks are synchronous, so
-- a row either lands as `approved` or never persists.
-- ----------------------------------------------------------------------------
create type stream_session_state as enum (
  'approved',
  'streaming',
  'ended',
  'expired',
  'failed'
);

-- ----------------------------------------------------------------------------
-- stream_sessions
-- ----------------------------------------------------------------------------
create table stream_sessions (
  id          uuid primary key default gen_random_uuid(),
  account_id  uuid not null references accounts(id) on delete cascade,
  agent_id    uuid not null references agents(id) on delete cascade,
  camera_id   uuid not null references cameras(id) on delete cascade,
  state       stream_session_state not null default 'approved',

  -- What the agent asked for.
  req_width         int not null,
  req_height        int not null,
  req_fps           int not null,
  req_codec         text not null,
  req_bitrate_kbps  int not null,

  -- What we approved (snapshot of account_limits at approval time so a
  -- mid-stream limit change can't break an active session's contract).
  lim_width         int not null,
  lim_height        int not null,
  lim_fps           int not null,
  lim_codec         text not null,
  lim_bitrate_kbps  int not null,

  -- What ffprobe actually observed on connect.
  validated_width         int,
  validated_height        int,
  validated_fps           int,
  validated_codec         text,
  validated_bitrate_kbps  int,

  streamid           text not null unique,
  srt_url            text not null,
  session_token_jti  uuid not null unique,
  expires_at         timestamptz not null,
  started_at         timestamptz,
  ended_at           timestamptz,
  end_reason         text,
  failure_code       text,
  failure_message    text,

  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

create index stream_sessions_account_state_idx on stream_sessions(account_id, state);
create index stream_sessions_camera_state_idx  on stream_sessions(camera_id, state);

-- Partial indexes for the hot "find active sessions" path and the
-- "find sessions to expire" sweeper.
create index stream_sessions_active_idx
  on stream_sessions(account_id)
  where state in ('approved', 'streaming');

create index stream_sessions_expiring_idx
  on stream_sessions(expires_at)
  where state in ('approved', 'streaming');

create trigger stream_sessions_set_updated_at
  before update on stream_sessions
  for each row execute function set_updated_at();

alter table stream_sessions enable row level security;

create policy stream_sessions_member_select on stream_sessions
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- ----------------------------------------------------------------------------
-- stream_validations: one row per ffprobe result delivered by the streaming server.
-- ----------------------------------------------------------------------------
create table stream_validations (
  id               uuid primary key default gen_random_uuid(),
  session_id       uuid not null references stream_sessions(id) on delete cascade,
  passed           bool not null,
  observed         jsonb not null,
  expected         jsonb not null,
  mismatch_fields  text[] not null default '{}',
  created_at       timestamptz not null default now()
);

create index stream_validations_session_idx on stream_validations(session_id);

alter table stream_validations enable row level security;

create policy stream_validations_member_select on stream_validations
  for select
  using (
    session_id in (
      select id from stream_sessions
      where account_id in (
        select account_id from account_members where user_id = auth.uid()
      )
    )
  );
