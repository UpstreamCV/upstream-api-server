-- Workflow Engine M2: workflow runs + internal event history.
--
-- M1 (20260612120000_rules_and_alerts.sql) shipped simple rules + in-app
-- alerts. M2 reframes that work for the product's long-term shape: Upstream
-- is a camera workflow engine, not an alerts product. A `workflow_run` is
-- one execution instance of a workflow — for now always a simple rule match
-- (workflow_type = 'simple_rule', with the rules table acting as the
-- workflow definition layer) — recorded WHETHER OR NOT an alert was sent.
-- Cooldown-suppressed matches still produce a run, so the system of record
-- is the run history, and alerts are just one possible action/output.
--
-- Future shape this leaves room for (deliberately NOT built here): runs
-- with no camera (account-wide workflows → camera_id is already nullable),
-- multi-step sequencing (workflow_type is open vocab; a step/event table
-- can hang off workflow_run_id), more action types (action_result is open
-- vocab), and review flows (status CHECK already includes
-- 'action_required'; reviewed_at/reviewed_by columns can be added when
-- something produces that status).
--
-- Written only by the API server's rule evaluator (same savepoint-isolated
-- ingest path as alerts). Tenant columns are derived from the session row,
-- never trusted from a request.

create table workflow_runs (
  id                       uuid primary key default gen_random_uuid(),
  account_id               uuid not null references accounts(id) on delete cascade,
  -- Nullable so future account-wide workflows need no migration; every
  -- simple_rule run sets it.
  camera_id                uuid references cameras(id) on delete cascade,
  -- Keep run history when the rule is deleted.
  rule_id                  uuid references rules(id) on delete set null,
  -- Soft reference (no FK): observations is an append-only log that may be
  -- pruned out-of-band.
  trigger_observation_id   uuid,

  -- Open vocabulary: 'simple_rule' today; sequenced/multi-camera workflow
  -- types ship without a migration.
  workflow_type            text not null default 'simple_rule',
  -- CHECK-constrained because the dashboard switches on it. 'active' is the
  -- in-flight state; M2 runs finish within the ingest request, so they end
  -- 'completed' (alert created) or 'suppressed' (cooldown). 'failed' and
  -- 'action_required' are reserved for future producers.
  status                   text not null default 'active'
                           check (status in (
                             'active', 'completed', 'failed',
                             'action_required', 'suppressed'
                           )),

  title                    text not null,
  summary                  text not null,
  -- The observation type that triggered the run (e.g.
  -- 'processor.line_crossing'). Open vocabulary, same posture as
  -- observations.type.
  trigger_type             text not null,
  -- What the run's action step did: 'alert_created',
  -- 'alert_suppressed_cooldown' today; open vocabulary so future actions
  -- (webhook_sent, no_action, error, …) need no migration. Null while the
  -- run is active.
  action_result            text,

  -- What triggered the run: the matched observation's payload.
  evidence                 jsonb not null default '{}'::jsonb,
  -- Run metadata: a snapshot of the rule definition at match time, so the
  -- run stays interpretable after the rule is edited or deleted.
  payload                  jsonb not null default '{}'::jsonb,

  started_at               timestamptz not null default now(),
  ended_at                 timestamptz,
  created_at               timestamptz not null default now(),
  updated_at               timestamptz not null default now()
);

-- History reads: newest first, optionally filtered by status/camera/rule.
create index workflow_runs_account_created_idx
  on workflow_runs(account_id, created_at desc);
create index workflow_runs_account_status_idx
  on workflow_runs(account_id, status);
create index workflow_runs_camera_created_idx
  on workflow_runs(camera_id, created_at desc);
create index workflow_runs_rule_created_idx
  on workflow_runs(rule_id, created_at desc);

create trigger workflow_runs_set_updated_at
  before update on workflow_runs
  for each row execute function set_updated_at();

alter table workflow_runs enable row level security;

create policy workflow_runs_member_select on workflow_runs
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

-- Link alerts to the run that produced them. The FK lives on alerts (not a
-- workflow_runs.alert_id back-pointer) so the write path is insert-run →
-- insert-alert → finalize-run with no circular reference, and the inbox can
-- link straight to its run.
alter table alerts
  add column workflow_run_id uuid references workflow_runs(id) on delete set null;

create index alerts_workflow_run_idx
  on alerts(workflow_run_id)
  where workflow_run_id is not null;

-- Hosted Supabase does not auto-grant table-level SELECT to `authenticated`
-- (see 20260513150000_dashboard_grants.sql for the pattern).
grant select on public.workflow_runs to authenticated;
