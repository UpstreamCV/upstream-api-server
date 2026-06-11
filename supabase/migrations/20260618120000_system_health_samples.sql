-- Server-side health history for the admin panel's System graphs.
--
-- The API samples its own component health (DB latency, MediaMTX
-- reachability, processor rollup, active sessions) on an in-process loop
-- (services/health_sampler.py, gated by
-- UPSTREAM_HEALTH_SAMPLE_INTERVAL_SECONDS — same posture as the workflow
-- evaluator loop) and appends here. The panel reads history instead of
-- collecting it client-side, so graphs exist even when nobody is watching.
--
-- Written and read only by the API through the service role. RLS enabled
-- with zero policies (no PostgREST exposure). Pruned by the sampler itself
-- (7-day retention) — no external retention job needed.

create table system_health_samples (
  id                     bigint generated always as identity primary key,
  sampled_at             timestamptz not null default now(),
  db_ok                  boolean not null,
  db_latency_ms          real,
  mediamtx_status        text not null
                         check (mediamtx_status in ('ok', 'unreachable', 'error')),
  mediamtx_active_paths  int,
  processors_active      int not null default 0,
  processors_stale       int not null default 0,
  processors_failed      int not null default 0,
  active_sessions        int not null default 0
);

create index system_health_samples_sampled_idx
  on system_health_samples(sampled_at desc);

alter table system_health_samples enable row level security;
