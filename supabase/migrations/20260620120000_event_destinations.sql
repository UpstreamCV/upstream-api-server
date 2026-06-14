-- Module 6 M6.3 — webhook destination configuration.
--
-- Account-scoped config for WHERE canonical events (the M6.1 ledger) will
-- eventually be delivered. This milestone is config only — no outbound
-- delivery, no delivery-attempt log, no worker (that is M6.4).
--
-- Secret posture: each destination carries a RECOVERABLE signing secret used
-- by the future delivery worker to HMAC-sign outbound payloads. Unlike
-- api_tokens (one-way argon2id), this must be decryptable — so it is stored
-- pgcrypto-encrypted (pgp_sym_encrypt with an app-held key,
-- UPSTREAM_WEBHOOK_SECRET_ENC_KEY) and NEVER returned by the API except once
-- at creation. A masked preview is stored separately for display.
--
-- Write posture: all writes are service-role from the API server, gated to
-- account owner/admin (or an API token with destinations:write). Members may
-- READ via PostgREST (preview only — the API projects out the ciphertext).

create table event_destinations (
  id                        uuid primary key default gen_random_uuid(),
  account_id                uuid not null references accounts(id) on delete cascade,

  -- Open vocabulary (no CHECK) so future types (slack/email) ship without a
  -- migration; the API schema layer restricts the mintable set to 'webhook'.
  destination_type          text not null default 'webhook',
  name                      text not null,
  url                       text not null,
  enabled                   boolean not null default true,

  -- Match filters for M6.4 delivery. Empty array means "all" (no filter).
  event_type_filters        text[] not null default '{}',
  camera_id_filters         uuid[] not null default '{}',
  workflow_id_filters       uuid[] not null default '{}',

  -- pgp_sym_encrypt(secret, key) ciphertext — recoverable by the delivery
  -- worker to sign payloads; never returned by the API. NOT one-way hashed.
  signing_secret_encrypted  bytea not null,
  -- Masked, safe-to-display hint, e.g. "whsec_live_...AB12".
  signing_secret_preview    text not null,

  -- auth.users.id of the issuer (no FK — mirrors api_tokens.created_by).
  created_by                uuid,

  created_at                timestamptz not null default now(),
  updated_at                timestamptz not null default now(),

  -- Delivery health bookkeeping, populated by the M6.4 worker.
  last_success_at           timestamptz,
  last_failure_at           timestamptz,
  consecutive_failures      integer not null default 0,
  disabled_at               timestamptz
);

create index event_destinations_account_created_idx
  on event_destinations(account_id, created_at desc);
create index event_destinations_account_enabled_idx
  on event_destinations(account_id, enabled)
  where enabled = true;
create index event_destinations_account_type_idx
  on event_destinations(account_id, destination_type);

create trigger event_destinations_set_updated_at
  before update on event_destinations
  for each row execute function set_updated_at();

alter table event_destinations enable row level security;

-- Members may READ their own account's destinations via PostgREST (preview
-- only — the encrypted secret is never exposed to clients). There is no
-- insert/update/delete policy: clients can never write — all writes are
-- service-role from the API, gated to owner/admin.
create policy event_destinations_member_select on event_destinations
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.event_destinations to authenticated;
