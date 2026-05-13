-- Three-mode SRT latency tradeoff (per spec): low | balanced | stable.
-- Stored on cameras as a default and snapshotted onto each session so the
-- ingest URL builder can pick the right SRT `latency=` value.

alter table cameras
  add column default_latency_mode text not null default 'balanced'
    check (default_latency_mode in ('low', 'balanced', 'stable'));

alter table stream_sessions
  add column req_latency_mode text not null default 'balanced'
    check (req_latency_mode in ('low', 'balanced', 'stable')),
  add column lim_latency_mode text not null default 'balanced'
    check (lim_latency_mode in ('low', 'balanced', 'stable'));
