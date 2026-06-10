-- Agent smart-flow groundwork: server-controlled stream types + web-created
-- cameras.
--
-- 1. `account_limits.allowed_stream_types` — per-account allow-list of camera
--    source types ("webcam", "rtsp", ...). Enforced at camera registration
--    and stream-session start in the application layer, not by a DB check:
--    the valid set is account data, not a global enum, so a DB constraint
--    would need a migration every time a stream type is introduced.
--
-- 2. `cameras.source_type` — what kind of source this camera is. Nullable:
--    cameras registered by pre-smart-flow agents have no declared type and
--    are default-allowed by the application checks.
--
-- 3. `cameras.agent_id` becomes nullable — cameras can now be created from
--    the dashboard before any agent exists. An agent claims an unassigned
--    camera (POST /v1/cameras/{id}/claim) before it can stream; session
--    start rejects unclaimed cameras with `camera_unassigned`.

alter table account_limits
  add column allowed_stream_types text[] not null default array['webcam', 'rtsp'];

alter table cameras
  add column source_type text
    check (source_type is null or char_length(source_type) between 1 and 32);

alter table cameras
  alter column agent_id drop not null;
