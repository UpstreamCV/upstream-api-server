-- Module 6 M6.1 — canonical event ledger.
--
-- The durable, append-only record of meaningful product events. M6.1 writes
-- only workflow-run-derived events (every rule/timer match finalizes a run,
-- including cooldown-suppressed ones — see services/events.py). The envelope
-- is shaped to also carry processor / reliability / observation-derived events
-- in later milestones with no further migration.
--
-- Write posture: rows are written ONLY by the api-server service role, in the
-- SAME transaction as the producing workflow_run finalization (transactional
-- outbox) — an event can never commit without its source committing, and a
-- rollback drops both. Tenant columns are derived server-side from the source
-- row, never trusted from a request (same posture as observations/alerts).
--
-- This is the foundation the future Event REST API, webhook destinations,
-- delivery attempts, replay, and WebSocket event stream will all read from.

create table events (
  id                     uuid primary key default gen_random_uuid(),
  -- Monotonic per-account cursor for keyset pagination + replay. A stable,
  -- gap-tolerant ordering that created_at (non-unique, clock-skew-prone)
  -- cannot give. Treat as ordering-only, not gap-free (concurrent inserts
  -- across workers may leave holes).
  seq                    bigserial not null,

  account_id             uuid not null references accounts(id) on delete cascade,
  -- Nullable: account-wide events have no camera (matches workflow_runs).
  camera_id              uuid references cameras(id) on delete set null,
  -- Soft reference (no FK): stream_sessions/observations are prunable.
  session_id             uuid,

  -- Open vocabulary at the DB layer (no CHECK) — same posture as
  -- observations.type / workflow_runs.action_result. The schema layer
  -- (schemas/events.py) enforces the supported set, so new event types ship
  -- without a migration.
  event_type             text not null,
  -- Bumped per event_type when its payload contract changes in a breaking
  -- way; consumers pin behavior off (event_type, schema_version).
  schema_version         integer not null default 1,

  -- Producer family: 'rule', 'timer', 'workflow', 'processor', 'reliability'.
  source                 text not null,

  -- Soft references (no FK) to the producing rows. observations/alerts may be
  -- pruned out-of-band; keep the event even if the source is gone.
  source_observation_id  uuid,
  workflow_run_id        uuid,
  source_alert_id        uuid,
  source_finding_id      uuid,

  rule_id                uuid,
  workflow_id            uuid,

  -- Exactly-once guard, stable per account. Repeated finalization (a
  -- documented double-fire across two API workers, or a retry) is absorbed by
  -- the unique index + INSERT ... ON CONFLICT DO NOTHING.
  dedupe_key             text not null,

  -- When the event happened in the world vs when the ledger row was written.
  occurred_at            timestamptz not null,
  created_at             timestamptz not null default now(),

  -- The normalized canonical body + the raw triggering evidence, both
  -- sanitized (secret-stripped) at emit time so the event is self-contained
  -- after the source mutates/is pruned.
  payload                jsonb not null default '{}'::jsonb,
  evidence               jsonb not null default '{}'::jsonb,
  -- Optional drift/health context; null for workflow-run-derived events.
  reliability_context    jsonb
);

-- Developer feed + replay cursor: keyset on the monotonic per-account seq.
create unique index events_account_seq_idx
  on events(account_id, seq);
-- Exactly-once emission: one event per (account, dedupe_key).
create unique index events_account_dedupe_idx
  on events(account_id, dedupe_key);
-- Type-filtered reads, newest-first.
create index events_account_type_seq_idx
  on events(account_id, event_type, seq desc);
-- Per-camera reads, newest-first.
create index events_camera_seq_idx
  on events(camera_id, seq desc)
  where camera_id is not null;
-- Lookup events emitted by a given workflow run.
create index events_workflow_run_idx
  on events(workflow_run_id)
  where workflow_run_id is not null;

alter table events enable row level security;

-- Members may READ their own account's events via PostgREST (same posture as
-- alerts / workflow_runs / account_audit_logs). There is no insert/update/
-- delete policy: clients can never write — all writes are service-role from
-- the API server, inside the producing transaction.
create policy events_member_select on events
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.events to authenticated;
