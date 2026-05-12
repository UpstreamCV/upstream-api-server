-- Tenancy + authentication tables.
-- API writes go through service role (which bypasses RLS). RLS policies here
-- only gate PostgREST reads for dashboard users.

-- ----------------------------------------------------------------------------
-- accounts: tenant root
-- ----------------------------------------------------------------------------
create table accounts (
  id          uuid primary key default gen_random_uuid(),
  name        text not null,
  slug        citext not null unique,
  plan        text not null default 'free',
  status      text not null default 'active'
              check (status in ('active', 'suspended', 'deleted')),
  metadata    jsonb not null default '{}'::jsonb,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

create trigger accounts_set_updated_at
  before update on accounts
  for each row execute function set_updated_at();

alter table accounts enable row level security;

-- ----------------------------------------------------------------------------
-- account_members: links Supabase auth.users to accounts (multi-user from day 1)
-- ----------------------------------------------------------------------------
create table account_members (
  account_id  uuid not null references accounts(id) on delete cascade,
  user_id     uuid not null,
  role        text not null check (role in ('owner', 'admin', 'viewer')),
  created_at  timestamptz not null default now(),
  primary key (account_id, user_id)
);

create index account_members_user_idx on account_members(user_id);

alter table account_members enable row level security;

-- ----------------------------------------------------------------------------
-- account_limits: 1:1 with accounts; snapshot of caps that gate session creation
-- ----------------------------------------------------------------------------
create table account_limits (
  account_id              uuid primary key
                          references accounts(id) on delete cascade,
  max_cameras             int not null default 1,
  max_concurrent_sessions int not null default 1,
  max_fps                 int not null default 30,
  max_width               int not null default 1920,
  max_height              int not null default 1080,
  max_bitrate_kbps        int not null default 6000,
  allowed_codecs          text[] not null default '{h264}',
  monthly_minutes_quota   int not null default 600,
  snapshot_storage_mb     int not null default 1024,
  created_at              timestamptz not null default now(),
  updated_at              timestamptz not null default now()
);

create trigger account_limits_set_updated_at
  before update on account_limits
  for each row execute function set_updated_at();

alter table account_limits enable row level security;

-- ----------------------------------------------------------------------------
-- api_tokens: agent-facing credentials. Argon2id hashed; plaintext never stored.
-- ----------------------------------------------------------------------------
create table api_tokens (
  id          uuid primary key default gen_random_uuid(),
  account_id  uuid not null references accounts(id) on delete cascade,
  name        text not null,
  prefix      text not null unique,           -- first segment of plaintext, used as index lookup key
  hash        text not null,                  -- argon2id encoded
  last_four   text not null,                  -- display: "...a3f1"
  scopes      text[] not null default '{agent}',
  last_used_at  timestamptz,
  expires_at    timestamptz,
  revoked_at    timestamptz,
  created_by    uuid,                         -- auth.users.id of issuer
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

create index api_tokens_account_active_idx
  on api_tokens(account_id)
  where revoked_at is null;

create trigger api_tokens_set_updated_at
  before update on api_tokens
  for each row execute function set_updated_at();

alter table api_tokens enable row level security;

-- ----------------------------------------------------------------------------
-- RLS policies (dashboard SELECT only; all writes are service-role from the API)
-- ----------------------------------------------------------------------------
create policy accounts_member_select on accounts
  for select
  using (
    id in (select account_id from account_members where user_id = auth.uid())
  );

-- Users can see only their own membership rows via PostgREST. Listing all
-- members of an account is a dashboard concern served by the API through the
-- service role — that path avoids self-referential RLS subqueries entirely.
create policy account_members_self_select on account_members
  for select
  using (user_id = auth.uid());

create policy account_limits_member_select on account_limits
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- api_tokens visible only to owners/admins; hash column is never displayed via API anyway.
create policy api_tokens_admin_select on api_tokens
  for select
  using (
    account_id in (
      select account_id from account_members
      where user_id = auth.uid() and role in ('owner', 'admin')
    )
  );
