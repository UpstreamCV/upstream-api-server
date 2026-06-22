-- Upstream Reliability M1a — observation ingest + issues foundation.
--
-- The cloud-side receiving end for the Upstream Collector: a customer's CV
-- system (or, later, the local collector) pushes batches of observations
-- (events, detection summaries, pipeline heartbeats, config changes) to
-- POST /v1/reliability/observations. We store the batch + its observations,
-- run simple reliability detectors over recent history, and open/refresh/
-- resolve deduped issues — the same partial-unique-open lifecycle as
-- reliability_findings, but account-keyed (the camera/source is encoded in the
-- dedupe_key value) and produced from collector observations rather than the
-- processor's own perception pipeline.
--
-- Write posture: rows are written ONLY by the api-server service role. Tenant
-- columns (account_id) are always derived from the auth credential in the
-- router, never trusted from the request body — same posture as
-- observations / reliability_findings / events. Members may only SELECT their
-- own account's rows via RLS; there is no insert/update/delete policy.
--
-- Three tables prefixed cv_ (the unprefixed `observations` table already exists
-- for processor output):
--   cv_observation_batches — one row per accepted upload; (account_id,
--                            dedupe_key=client_batch_id) is the idempotency key.
--   cv_observations        — append-only detail rows, one per observation.
--   cv_issues              — deduped reliability issues opened by detectors.

-- ---------------------------------------------------------------------------
-- cv_observation_batches — upload header + idempotency anchor.
-- ---------------------------------------------------------------------------
create table cv_observation_batches (
  id                  uuid primary key default gen_random_uuid(),
  account_id          uuid not null references accounts(id) on delete cascade,
  -- Optional resolved camera (set when the batch references a single owned
  -- camera); collector identity / external keys stay soft text for M1a.
  camera_id           uuid references cameras(id) on delete set null,
  external_camera_key text,
  -- The producing collector/pipeline identity, soft (no FK) — a real
  -- collectors registry is a later milestone.
  source              text not null,
  status              text not null
                      check (status in ('pending', 'processing', 'completed', 'failed'))
                      default 'completed',
  observation_count   integer not null default 0,
  -- = client_batch_id. The exactly-once guard: a replayed upload is absorbed by
  -- INSERT ... ON CONFLICT (account_id, dedupe_key) DO NOTHING.
  dedupe_key          text not null,
  received_at         timestamptz not null default now(),
  processed_at        timestamptz,
  error_message       text,
  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

-- Idempotent retry guard: at most one batch per (account, client_batch_id).
create unique index cv_observation_batches_account_dedupe_idx
  on cv_observation_batches(account_id, dedupe_key);
-- Account feed, newest-first (collector-health "last ingest" read).
create index cv_observation_batches_account_received_idx
  on cv_observation_batches(account_id, received_at desc);

create trigger cv_observation_batches_set_updated_at
  before update on cv_observation_batches
  for each row execute function set_updated_at();

alter table cv_observation_batches enable row level security;

create policy cv_observation_batches_member_select on cv_observation_batches
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.cv_observation_batches to authenticated;

-- ---------------------------------------------------------------------------
-- cv_observations — append-only detail (no updated_at / trigger).
-- ---------------------------------------------------------------------------
create table cv_observations (
  id                  uuid primary key default gen_random_uuid(),
  account_id          uuid not null references accounts(id) on delete cascade,
  -- Nullable: an observation may be keyed solely by external_camera_key (or be
  -- a source-level pipeline_heartbeat with no camera at all).
  camera_id           uuid references cameras(id) on delete set null,
  batch_id            uuid not null references cv_observation_batches(id) on delete cascade,
  external_camera_key text,
  -- Open vocabulary at the DB layer (same posture as observations.type) so new
  -- observation types ship without a migration. M1a: event_observed,
  -- detection_summary, pipeline_heartbeat, config_changed (+ optional
  -- detection_observed, feedback_observed).
  type                text not null,
  payload             jsonb not null default '{}'::jsonb,
  observed_at         timestamptz not null,
  created_at          timestamptz not null default now()
);

-- Detector history windows scan by (account, entity, observed_at). One index
-- per entity key; both are partial so the unkeyed rows don't bloat them.
create index cv_observations_account_observed_idx
  on cv_observations(account_id, observed_at desc);
create index cv_observations_batch_idx
  on cv_observations(batch_id);
create index cv_observations_account_camera_observed_idx
  on cv_observations(account_id, camera_id, observed_at desc)
  where camera_id is not null;
create index cv_observations_account_external_key_observed_idx
  on cv_observations(account_id, external_camera_key, observed_at desc)
  where external_camera_key is not null;
create index cv_observations_type_observed_idx
  on cv_observations(type, observed_at desc);

alter table cv_observations enable row level security;

create policy cv_observations_member_select on cv_observations
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.cv_observations to authenticated;

-- ---------------------------------------------------------------------------
-- cv_issues — deduped reliability issues (mirrors reliability_findings).
-- ---------------------------------------------------------------------------
create table cv_issues (
  id                  uuid primary key default gen_random_uuid(),
  account_id          uuid not null references accounts(id) on delete cascade,
  -- Nullable: an issue may pertain to an external-keyed source with no owned
  -- camera. The camera/source identity is encoded in dedupe_key; camera_id is
  -- a denormalized convenience for the per-camera read filter.
  camera_id           uuid references cameras(id) on delete set null,
  source_batch_id     uuid references cv_observation_batches(id) on delete set null,
  external_camera_key text,

  -- Open vocabulary; the dashboard switches on the CHECK-constrained
  -- category / severity / status buckets.
  issue_type          text not null,
  category            text not null
                      check (category in ('count', 'confidence', 'liveness', 'tracker')),
  severity            text not null
                      check (severity in ('info', 'warning', 'critical')),
  status              text not null
                      check (status in ('open', 'resolved', 'suppressed', 'ignored'))
                      default 'open',

  title               text not null,
  message             text not null,
  likely_cause        text,
  recommended_action  text,

  evidence            jsonb not null default '{}'::jsonb,
  dedupe_key          text not null,
  detector_version    text not null,
  source_observation_ids jsonb,

  first_seen_at       timestamptz not null default now(),
  last_seen_at        timestamptz not null default now(),
  resolved_at         timestamptz,
  resolved_reason     text,

  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

-- At most one OPEN row per (account, dedupe_key). Resolved rows are history and
-- may stack up; the same condition after resolution opens a fresh row. Camera/
-- source is encoded in the dedupe_key value (e.g. camera:<id>:event_count_drop,
-- external:<key>:confidence_drop), so this account-level key still separates
-- distinct entities/conditions. A partial unique index can't be an ON CONFLICT
-- arbiter — the service locks the open row then updates or inserts it.
create unique index cv_issues_open_dedupe_idx
  on cv_issues(account_id, dedupe_key)
  where status = 'open';

-- Account-wide inbox list, filtered by status, newest-first.
create index cv_issues_account_status_last_seen_idx
  on cv_issues(account_id, status, last_seen_at desc);
-- Per-camera issues, newest-first.
create index cv_issues_camera_last_seen_idx
  on cv_issues(camera_id, last_seen_at desc)
  where camera_id is not null;

create trigger cv_issues_set_updated_at
  before update on cv_issues
  for each row execute function set_updated_at();

alter table cv_issues enable row level security;

create policy cv_issues_member_select on cv_issues
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.cv_issues to authenticated;
