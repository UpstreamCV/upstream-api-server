-- Module 7 M2 — Stripe Checkout + Customer Portal.
--
-- 1. `account_billing`: 1:1 with accounts (same shape as account_limits),
--    holding ONLY Stripe references and subscription state. No card data,
--    no payment details — Stripe hosts all of that. `accounts.plan` stays
--    the product-plan label and `accounts.status` stays account LIFECYCLE
--    status (active/suspended/deleted); subscription state lives here.
--
-- 2. `stripe_events`: webhook idempotency ledger. Stripe retries deliveries
--    and can deliver duplicates; handlers insert the event id first
--    (on conflict do nothing) and skip processing when the row already
--    exists. Service-role writes only; no dashboard read access.

-- ----------------------------------------------------------------------------
-- account_billing
-- ----------------------------------------------------------------------------
create table account_billing (
  account_id                 uuid primary key
                             references accounts(id) on delete cascade,
  stripe_customer_id         text unique,
  stripe_subscription_id     text unique,
  stripe_price_id            text,
  -- Raw Stripe subscription status (active, trialing, past_due, canceled,
  -- unpaid, incomplete, ...). Open vocabulary — Stripe owns this set.
  stripe_subscription_status text,
  cancel_at_period_end       boolean not null default false,
  current_period_start       timestamptz,
  current_period_end         timestamptz,
  created_at                 timestamptz not null default now(),
  updated_at                 timestamptz not null default now()
);

create trigger account_billing_set_updated_at
  before update on account_billing
  for each row execute function set_updated_at();

alter table account_billing enable row level security;

-- Dashboard members may read their account's billing state via PostgREST
-- (same posture as account_limits). All writes are service-role from the API.
create policy account_billing_member_select on account_billing
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.account_billing to authenticated;

-- ----------------------------------------------------------------------------
-- stripe_events: webhook delivery dedupe (append-only)
-- ----------------------------------------------------------------------------
create table stripe_events (
  id           text primary key,   -- Stripe event id (evt_...)
  event_type   text not null,
  received_at  timestamptz not null default now()
);

-- RLS on with no policies: PostgREST can read nothing; only the service
-- role (which bypasses RLS) touches this table.
alter table stripe_events enable row level security;
