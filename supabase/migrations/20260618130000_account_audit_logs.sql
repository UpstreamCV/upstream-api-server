-- Module 7 M3 — tenant audit logs.
--
-- Account-facing audit history: important actions taken INSIDE an account
-- (cameras, tokens, rules, workflows, billing, limit rejections), readable
-- by that account's members. This is deliberately separate from
-- `admin_audit_log` (platform-admin actions, internal-only): different
-- audience, different RLS, different retention pressure.
--
-- Never stored here (enforced by services/account_audit.py sanitization):
-- raw API tokens, auth headers, Stripe secrets, card data, raw webhook
-- payloads, passwords, recovery links, service-role keys.

create table account_audit_logs (
  id             uuid primary key default gen_random_uuid(),
  account_id     uuid not null references accounts(id) on delete cascade,
  -- Null for non-user actors (agent-initiated, Stripe webhooks, system).
  actor_user_id  uuid,
  actor_type     text not null default 'user'
                 check (actor_type in ('user', 'system', 'stripe', 'internal')),
  -- Open vocabulary of dotted verbs (camera.created, billing.plan_changed)
  -- — same posture as usage_events.event_type / admin_audit_log.action.
  event_type     text not null,
  resource_type  text,
  resource_id    text,
  summary        text not null,
  metadata       jsonb not null default '{}'::jsonb,
  request_id     text,
  ip_address     inet,
  user_agent     text,
  created_at     timestamptz not null default now()
);

create index account_audit_logs_recent_idx
  on account_audit_logs(account_id, created_at desc);
create index account_audit_logs_event_idx
  on account_audit_logs(account_id, event_type, created_at desc);
create index account_audit_logs_resource_idx
  on account_audit_logs(account_id, resource_type, resource_id, created_at desc);

alter table account_audit_logs enable row level security;

-- Members may READ their own account's logs via PostgREST (same posture as
-- alerts/usage_events). There is no insert/update/delete policy: clients can
-- never write audit rows — all writes are service-role from the API.
create policy account_audit_logs_member_select on account_audit_logs
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.account_audit_logs to authenticated;
