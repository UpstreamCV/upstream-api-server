-- Module 6 M6.4 — webhook delivery attempts (the durable delivery queue).
--
-- One row per (event, destination): the durable record of an outbound webhook
-- delivery, enqueued in the same transaction as the canonical event insert
-- (transactional outbox). The in-process delivery worker claims due rows with
-- FOR UPDATE SKIP LOCKED, so the row — not an in-memory queue — is the source
-- of truth and the worker is safe under multiple API processes.
--
-- Write posture: all writes are service-role from the API/worker. Members may
-- READ their own account's attempts via PostgREST (debug/observability); the
-- row carries no secret (signing secrets live encrypted on event_destinations).

create table event_delivery_attempts (
  id                    uuid primary key default gen_random_uuid(),

  account_id            uuid not null references accounts(id) on delete cascade,
  event_id              uuid not null references events(id) on delete cascade,
  destination_id        uuid not null references event_destinations(id) on delete cascade,

  -- pending: waiting | delivering: claimed by a worker | delivered: 2xx
  -- failed: retryable failure, will retry after next_attempt_at
  -- dead: permanent failure or attempts exhausted
  status                text not null default 'pending'
                        check (status in ('pending', 'delivering', 'delivered', 'failed', 'dead')),

  attempt_count         integer not null default 0,
  max_attempts          integer not null default 5,

  next_attempt_at       timestamptz not null default now(),

  started_at            timestamptz,
  completed_at          timestamptz,

  response_status_code  integer,
  response_body_preview text,
  error_message         text,

  last_attempt_at       timestamptz,

  created_at            timestamptz not null default now(),
  updated_at            timestamptz not null default now(),

  -- Exactly-once enqueue: one attempt per (event, destination). The enqueue
  -- uses ON CONFLICT DO NOTHING so a re-emitted event never double-delivers.
  unique (event_id, destination_id)
);

-- The worker's claim hot path: due, retryable rows oldest-first.
create index event_delivery_attempts_due_idx
  on event_delivery_attempts(next_attempt_at, created_at)
  where status in ('pending', 'failed');
-- Account-scoped debug listing.
create index event_delivery_attempts_account_created_idx
  on event_delivery_attempts(account_id, created_at desc);
-- Per-event and per-destination history.
create index event_delivery_attempts_event_idx
  on event_delivery_attempts(event_id);
create index event_delivery_attempts_destination_created_idx
  on event_delivery_attempts(destination_id, created_at desc);

create trigger event_delivery_attempts_set_updated_at
  before update on event_delivery_attempts
  for each row execute function set_updated_at();

alter table event_delivery_attempts enable row level security;

-- Members may READ their own account's delivery attempts via PostgREST. There
-- is no insert/update/delete policy: clients can never write — all writes are
-- service-role from the API server and the delivery worker.
create policy event_delivery_attempts_member_select on event_delivery_attempts
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.event_delivery_attempts to authenticated;
