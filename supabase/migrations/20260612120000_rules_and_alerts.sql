-- Rules/Triggers M1 (first milestone of the Rules/Triggers + Alerts product
-- track — numbered separately from the reliability milestone sequence):
-- user-authored rules over processor observations, and the alerts those
-- rules raise.
--
-- A `rule` is owned by an account, scoped to one camera, and matches one
-- observation type family (line crossing, ROI occupancy, zone entry, dwell).
-- Evaluation happens inline on the observation ingest path
-- (routers/processor.py), savepoint-isolated like detection metrics so it can
-- never fail the ingest. There is no background worker — rule types that need
-- one (camera_offline, no_detections_for_n_minutes) are deferred and ship
-- without a migration because rule_type is open vocabulary at the DB layer
-- (same posture as observations.type and reliability_findings.finding_type).
-- The CHECK-constrained columns (severity, alert status) are the ones the
-- dashboard switches on.
--
-- An `alert` is the user-facing record a matched rule produces. Distinct from
-- reliability_findings (system-health verdicts owned by the evaluator):
-- alerts are raised by user-authored rules and have a read/resolve lifecycle
-- driven from the dashboard inbox. Tenant columns are always derived
-- server-side from the rule/camera rows, never trusted from a request.

create table rules (
  id                uuid primary key default gen_random_uuid(),
  account_id        uuid not null references accounts(id) on delete cascade,
  camera_id         uuid not null references cameras(id) on delete cascade,

  name              text not null check (char_length(name) between 1 and 120),
  -- Open vocabulary at the DB layer; the API schema layer enforces the
  -- currently-supported set so new types ship without a migration.
  rule_type         text not null,
  enabled           boolean not null default true,
  -- Per-type match parameters (e.g. {"line_id": ..., "min_total_count": 3}).
  -- Validated per rule_type at the API schema layer.
  config            jsonb not null default '{}'::jsonb,

  -- Severity stamped onto alerts this rule raises.
  severity          text not null default 'warning'
                    check (severity in ('info', 'warning', 'critical')),
  -- Minimum seconds between two alerts from the same rule. Suppresses the
  -- flood a per-event observation stream would otherwise produce.
  cooldown_seconds  integer not null default 300
                    check (cooldown_seconds between 0 and 86400),

  created_by        uuid,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

create index rules_account_idx on rules(account_id);
-- Evaluation hot path: enabled rules for one camera.
create index rules_camera_enabled_idx on rules(camera_id) where enabled;

create trigger rules_set_updated_at
  before update on rules
  for each row execute function set_updated_at();

alter table rules enable row level security;

create policy rules_member_select on rules
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

create table alerts (
  id              uuid primary key default gen_random_uuid(),
  account_id      uuid not null references accounts(id) on delete cascade,
  camera_id       uuid not null references cameras(id) on delete cascade,
  -- Keep alert history when a rule is deleted.
  rule_id         uuid references rules(id) on delete set null,
  -- Soft reference (no FK): observations is an append-only log that may be
  -- pruned out-of-band.
  observation_id  uuid,

  severity        text not null
                  check (severity in ('info', 'warning', 'critical')),
  status          text not null default 'open'
                  check (status in ('open', 'resolved')),

  title           text not null,
  message         text not null,
  -- Evidence snapshot from the matched observation payload.
  payload         jsonb not null default '{}'::jsonb,

  created_at      timestamptz not null default now(),
  read_at         timestamptz,
  resolved_at     timestamptz,
  updated_at      timestamptz not null default now()
);

-- Inbox reads: newest first, optionally filtered by status / camera.
create index alerts_account_created_idx on alerts(account_id, created_at desc);
create index alerts_account_status_idx on alerts(account_id, status);
create index alerts_camera_created_idx on alerts(camera_id, created_at desc);
-- Cooldown check: latest alert per rule.
create index alerts_rule_created_idx on alerts(rule_id, created_at desc);

create trigger alerts_set_updated_at
  before update on alerts
  for each row execute function set_updated_at();

alter table alerts enable row level security;

create policy alerts_member_select on alerts
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- Hosted Supabase does not auto-grant table-level SELECT to `authenticated`
-- (see 20260513150000_dashboard_grants.sql for the pattern).
grant select on public.rules  to authenticated;
grant select on public.alerts to authenticated;
