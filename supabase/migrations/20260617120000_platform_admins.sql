-- Platform admin panel foundations.
--
-- `platform_admins` is the allowlist behind the fifth identity class
-- (CurrentPlatformAdmin): a Supabase-authenticated user whose user_id appears
-- here may call /v1/admin/* across ALL tenants. Rows are managed manually
-- (seed script locally, SQL editor in prod) — there is deliberately no
-- self-service path to platform admin.
--
-- `admin_audit_log` records every mutating /v1/admin/* call (who, what,
-- before/after) so cross-tenant changes are reviewable from the panel.
--
-- Both tables are written and read ONLY by the API through the service role.
-- RLS is enabled with zero policies so PostgREST exposes nothing — same
-- posture would leak membership info if these ever got member-select policies.

-- ----------------------------------------------------------------------------
-- platform_admins: user_id allowlist. No FK to auth.users (consistent with
-- account_members.user_id) so local seeds and prod user imports don't fight
-- the auth schema.
-- ----------------------------------------------------------------------------
create table platform_admins (
  user_id     uuid primary key,
  note        text,
  created_by  uuid,
  created_at  timestamptz not null default now()
);

alter table platform_admins enable row level security;

-- ----------------------------------------------------------------------------
-- admin_audit_log: append-only. `action` is an open vocabulary of dotted verbs
-- (account.plan.update, account.status.update, account.limits.update) — same
-- posture as usage_events.event_type. `payload` holds a {before, after} diff;
-- `request_id` ties the row back to API access logs.
-- ----------------------------------------------------------------------------
create table admin_audit_log (
  id             uuid primary key default gen_random_uuid(),
  admin_user_id  uuid not null,
  action         text not null,
  target_type    text not null,
  target_id      uuid,
  payload        jsonb not null default '{}'::jsonb,
  request_id     text,
  created_at     timestamptz not null default now()
);

create index admin_audit_log_recent_idx
  on admin_audit_log(created_at desc);
create index admin_audit_log_admin_recent_idx
  on admin_audit_log(admin_user_id, created_at desc);
create index admin_audit_log_target_recent_idx
  on admin_audit_log(target_type, target_id, created_at desc);

alter table admin_audit_log enable row level security;

-- ----------------------------------------------------------------------------
-- Cross-account aggregate indexes for the admin panel's global queries.
-- Existing usage_events indexes lead with account_id or event_type; the
-- panel's time-bucketed reports scan a global time window.
-- ----------------------------------------------------------------------------
create index usage_events_occurred_idx on usage_events(occurred_at desc);

-- Latest-heartbeat-per-active-session lateral join needs (session_id,
-- reported_at desc); the original heartbeats_session_idx was unordered.
drop index heartbeats_session_idx;
create index heartbeats_session_recent_idx
  on heartbeats(session_id, reported_at desc)
  where session_id is not null;

-- Global "recent sessions across all accounts" listing. The existing partial
-- active index leads with account_id and doesn't help an un-scoped scan.
create index stream_sessions_created_idx on stream_sessions(created_at desc);
