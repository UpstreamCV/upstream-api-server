-- Agents (installed processes) and cameras (declared by agents).

-- ----------------------------------------------------------------------------
-- agents
-- ----------------------------------------------------------------------------
create table agents (
  id              uuid primary key default gen_random_uuid(),
  account_id      uuid not null references accounts(id) on delete cascade,
  name            text not null,
  hostname        text,
  version         text,
  os              text,
  python_version  text,
  capabilities    jsonb not null default '{}'::jsonb,
  last_seen_at    timestamptz,
  status          text not null default 'active'
                  check (status in ('active', 'disabled')),
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now(),
  unique (account_id, name)
);

create trigger agents_set_updated_at
  before update on agents
  for each row execute function set_updated_at();

alter table agents enable row level security;

create policy agents_member_select on agents
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- ----------------------------------------------------------------------------
-- cameras
-- ----------------------------------------------------------------------------
create table cameras (
  id                    uuid primary key default gen_random_uuid(),
  account_id            uuid not null references accounts(id) on delete cascade,
  agent_id              uuid not null references agents(id) on delete cascade,
  name                  text not null,
  external_id           text,
  device_path           text,
  default_width         int,
  default_height        int,
  default_fps           int,
  default_codec         text,
  default_bitrate_kbps  int,
  metadata              jsonb not null default '{}'::jsonb,
  status                text not null default 'idle'
                        check (status in ('idle', 'streaming', 'error', 'disabled')),
  created_at            timestamptz not null default now(),
  updated_at            timestamptz not null default now()
);

-- Idempotency: agent can re-register the same physical camera using external_id.
create unique index cameras_external_id_idx
  on cameras(agent_id, external_id)
  where external_id is not null;

-- Listing active cameras per account.
create index cameras_account_active_idx
  on cameras(account_id)
  where status <> 'disabled';

create trigger cameras_set_updated_at
  before update on cameras
  for each row execute function set_updated_at();

alter table cameras enable row level security;

create policy cameras_member_select on cameras
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );
