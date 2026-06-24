-- Upstream Reliability M2b — source health + liveness monitoring.
--
-- Extends the M2a cv_sources registry (20260624120000) with liveness settings +
-- health state so a scheduled sweep can detect "data stopped flowing": external
-- sources that go silent and collectors that stop heartbeating. The sweep
-- (services/reliability_source_liveness.py) reads/updates these columns and
-- opens/resolves cv_issues (category 'liveness'); the dashboard reads the health
-- state via GET /v1/reliability/sources. No new table — health is per-source
-- state on the registry row. These columns are written ONLY by the api-server
-- service role; the existing member-select RLS / grant are unchanged.
--
-- Intervals are nullable and mean "use the central per-kind default" (resolved
-- in code: reliability_source_liveness.default_intervals) so a default change
-- applies everywhere and a reset is just NULL. Existing rows adopt
-- liveness_enabled=true / health_status='unknown' via the column defaults; the
-- migration opens no issues.

alter table cv_sources
  add column liveness_enabled boolean not null default true,
  add column expected_interval_seconds integer
    check (expected_interval_seconds is null or expected_interval_seconds > 0),
  add column liveness_grace_seconds integer
    check (liveness_grace_seconds is null or liveness_grace_seconds >= 0),
  add column health_status text not null default 'unknown'
    check (health_status in ('healthy', 'delayed', 'silent',
                             'collector_offline', 'unknown', 'disabled')),
  add column health_reason text,
  add column health_message text,
  add column last_liveness_checked_at timestamptz,
  add column last_health_status_changed_at timestamptz,
  add column liveness_silence_started_at timestamptz,
  -- Soft pointer to the open liveness cv_issues row (no FK — same posture as
  -- cv_sources.source; avoids delete/circular coupling with cv_issues).
  add column last_liveness_issue_id uuid;

-- The scheduled sweep claims the least-recently-checked enabled rows first
-- (ordered by last_liveness_checked_at ascending, nulls first, skip-locked);
-- partial on liveness_enabled so disabled rows never bloat the claim index.
create index cv_sources_liveness_claim_idx
  on cv_sources(last_liveness_checked_at)
  where liveness_enabled;

-- Health-state reads: the dashboard health filter + the collector-status health
-- counts, account-scoped, most-recently-seen first.
create index cv_sources_account_health_idx
  on cv_sources(account_id, health_status, last_seen_at desc);
