-- Appliance-class sessions: per-account session TTL override.
--
-- 24/7 appliance fleets (e.g. the CrowdCount counters) cannot live with the
-- platform's default 4h session TTL: the edge client is "start once,
-- heartbeat forever", so the TTL boundary would kill processing mid-stream
-- ~6x/day. Rather than a separate session class enum, the appliance gate is
-- a per-account TTL override: NULL means "use the platform default"
-- (UPSTREAM_SESSION_TOKEN_TTL_SECONDS, 4h); a large value (e.g. 30 days)
-- makes every session the account starts an appliance-class session.
--
-- Snapshot semantics match the rest of account_limits: the effective TTL is
-- read at session start and baked into the row's expires_at + JWT exp, so
-- changing it never affects in-flight sessions.

alter table account_limits
  add column session_ttl_seconds int
  check (session_ttl_seconds is null or session_ttl_seconds > 0);

comment on column account_limits.session_ttl_seconds is
  'Per-account stream-session TTL override in seconds. NULL = platform '
  'default (UPSTREAM_SESSION_TOKEN_TTL_SECONDS). Set large (e.g. 2592000 = '
  '30 days) for appliance-class 24/7 fleets whose edge clients start one '
  'session and hold it open.';
