-- Upstream Reliability M2a — source registry / source identity.
--
-- M1a stores raw observation batches + detail rows (cv_observation_batches /
-- cv_observations) and deduped issues (cv_issues). Until now the collector
-- workflow tracked "sources" only as loose strings re-aggregated from the raw
-- observation rows over a rolling 24h window — a transient view, not a durable
-- registry. cv_sources promotes (source, external_camera_key) to a first-class,
-- account-scoped registry row so the product can durably answer: which sources
-- exist, when did each last send data, what data types, which collector/source
-- produced them, and which have open issues.
--
-- This is a MATERIALIZED PROJECTION of the observations (the system of record):
-- the ingest path upserts these rows after observations are accepted, inside a
-- savepoint so a pathological payload can never reject already-stored
-- observations. The registry can always be rebuilt from cv_observations.
--
-- Write posture matches M1a: rows are written ONLY by the api-server service
-- role; account_id is derived from the auth credential, never the request body.
-- Members may only SELECT their own account's rows via RLS; there is no
-- insert/update/delete policy.
--
-- Scope note: M2a is registry + read only. No camera mapping (cv_sources has no
-- camera_id column), no scheduled liveness sweep, no silent source issues — that
-- is M2b. The last_collector_heartbeat_at / last_seen_at indexes are laid down
-- now to serve the future liveness sweep.

create table cv_sources (
  id                  uuid primary key default gen_random_uuid(),
  account_id          uuid not null references accounts(id) on delete cascade,

  -- The producing collector/pipeline identity (soft text, no FK — same posture
  -- as cv_observation_batches.source) + the optional external camera/device key
  -- it forwards data for. An external CV system that does not use Upstream
  -- cameras is a first-class citizen here.
  source              text not null check (length(btrim(source)) > 0),
  external_camera_key text check (external_camera_key is null
                                  or length(btrim(external_camera_key)) > 0),

  -- Auto display label (external_camera_key, else source) set on insert; left
  -- untouched on update so a future manual rename can survive. No rename UI yet.
  display_name        text,
  source_kind         text not null
                      check (source_kind in ('collector', 'external_camera',
                                             'observation_source', 'unknown'))
                      default 'observation_source',

  -- first_seen_at = earliest observed_at the registry has recorded for this
  -- source (observed, not ingest, semantics); last_seen_at = most recent.
  first_seen_at       timestamptz not null,
  last_seen_at        timestamptz not null,

  -- Most-recent observed_at per observation type (null until first seen).
  last_observation_at        timestamptz,
  last_event_at              timestamptz,
  last_detection_summary_at  timestamptz,
  last_collector_heartbeat_at timestamptz,
  last_config_changed_at     timestamptz,

  -- Latest non-empty model identity seen in a detection_summary payload.
  last_model_name     text,
  last_model_version  text,

  observation_count_total        bigint not null default 0,
  event_count_total              bigint not null default 0,
  detection_summary_count_total  bigint not null default 0,
  collector_heartbeat_count_total bigint not null default 0,
  config_changed_count_total     bigint not null default 0,

  metadata            jsonb not null default '{}'::jsonb
                      check (jsonb_typeof(metadata) = 'object'),

  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

-- One registry row per (account, source, external_camera_key). external_camera_key
-- is nullable, so the unique key coalesces null to '' — a real key can never be
-- '' (the CHECK above forbids blank), so the sentinel is collision-free. This
-- expression MUST stay byte-identical to the ingest upsert's ON CONFLICT target
-- or the upsert fails to infer the index. (Precedent: detection_metrics_rollup.)
create unique index cv_sources_account_source_external_key_uidx
  on cv_sources (account_id, source, coalesce(external_camera_key, ''));

-- Default registry listing: account feed, most-recently-seen first.
create index cv_sources_account_last_seen_idx
  on cv_sources(account_id, last_seen_at desc);
-- source_kind filter + last-seen order in one shot.
create index cv_sources_account_kind_last_seen_idx
  on cv_sources(account_id, source_kind, last_seen_at desc);
-- Collector liveness lookups (M2b foundation).
create index cv_sources_account_heartbeat_idx
  on cv_sources(account_id, last_collector_heartbeat_at desc);

create trigger cv_sources_set_updated_at
  before update on cv_sources
  for each row execute function set_updated_at();

alter table cv_sources enable row level security;

create policy cv_sources_member_select on cv_sources
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.cv_sources to authenticated;
