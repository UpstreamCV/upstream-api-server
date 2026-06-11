-- Workflow Engine M5: multi-step sequencing.
--
-- `workflows` is the first TOP-LEVEL workflow definition table — the point
-- where workflows stop being synonymous with single rules. M5 ships one
-- workflow_type, 'sequence' (A → B within N seconds, optional same-track
-- correlation); the column is open vocabulary so richer types ship without
-- a migration. The existing `rules` table remains the definition layer for
-- the M1-M4 simple/timer rule path — deliberately NOT migrated or renamed.
--
-- Sequence execution state lives in the existing `workflow_runs` table
-- (M2), which gains three nullable columns:
--   - workflow_id: which definition an instance belongs to (set null on
--     definition delete — history survives).
--   - deadline_at: when an 'active' sequence run times out. Indexed
--     partially so the M4 evaluator's expiry sweep stays cheap.
--   - dedupe_key: the start observation's processor idempotency key (or
--     observation id) — the start-insert NOT EXISTS guard checks it so a
--     retried/duplicated start observation cannot spawn a second active
--     run for the same workflow.
--
-- Two-step evidence fits in `workflow_runs.evidence` jsonb
-- ({"start": …, "follow_up": …} or {"start": …, "timeout": …}); a
-- `workflow_run_steps` table is deferred until something needs >2 steps.

create table workflows (
  id                uuid primary key default gen_random_uuid(),
  account_id        uuid not null references accounts(id) on delete cascade,
  -- M5 sequences are single-camera (both steps on one camera). Cross-camera
  -- sequencing is deferred; relaxing to nullable later is trivial.
  camera_id         uuid not null references cameras(id) on delete cascade,

  name              text not null check (char_length(name) between 1 and 120),
  description       text,
  -- Open vocabulary; 'sequence' is the only type the API accepts today.
  workflow_type     text not null default 'sequence',
  enabled           boolean not null default true,
  -- The sequence definition (start/then/correlation/on_success/on_timeout/
  -- schedule). Validated per workflow_type at the API schema layer.
  config            jsonb not null default '{}'::jsonb,

  -- Severity stamped onto alerts this workflow raises.
  severity          text not null default 'warning'
                    check (severity in ('info', 'warning', 'critical')),
  -- Minimum seconds between two alerts from the same workflow (failure
  -- alerts are guarded across runs so a busy scene can't flood the inbox).
  cooldown_seconds  integer not null default 300
                    check (cooldown_seconds between 0 and 86400),

  created_by        uuid,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

create index workflows_account_idx on workflows(account_id);
-- Sequencer hot path: enabled workflows for one camera.
create index workflows_camera_enabled_idx on workflows(camera_id) where enabled;

create trigger workflows_set_updated_at
  before update on workflows
  for each row execute function set_updated_at();

alter table workflows enable row level security;

create policy workflows_member_select on workflows
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.workflows to authenticated;

-- Sequence instance state on the existing runs table.
alter table workflow_runs
  add column workflow_id uuid references workflows(id) on delete set null,
  add column deadline_at timestamptz,
  add column dedupe_key  text;

-- Expiry sweep: active runs past their deadline.
create index workflow_runs_active_deadline_idx
  on workflow_runs(deadline_at)
  where status = 'active' and deadline_at is not null;

-- Follow-up matching: active runs per workflow.
create index workflow_runs_workflow_active_idx
  on workflow_runs(workflow_id)
  where status = 'active';
