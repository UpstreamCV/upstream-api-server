-- Fleet health trend (GET /v1/dashboard/fleet-trend) aggregates heartbeats
-- ACCOUNT-wide over a trailing 24h/7d window. Every existing index on this
-- table is per-device -- heartbeats_agent_recent_idx (agent_id, reported_at
-- desc) and heartbeats_camera_recent_idx (camera_id, reported_at desc) -- so an
-- account-scoped range scan had no supporting index and would seq-scan a
-- write-heavy, insert-only table ("batches of <= 100 every few seconds").
--
-- camera_id is nullable (agent-level heartbeats carry none) and the trend
-- buckets cameras, so this is partial on camera_id is not null, matching the
-- shape of heartbeats_camera_recent_idx. reported_at desc mirrors the other two
-- so the ordering matches how every reader walks this table.

create index if not exists heartbeats_account_recent_idx
  on heartbeats(account_id, reported_at desc)
  where camera_id is not null;
