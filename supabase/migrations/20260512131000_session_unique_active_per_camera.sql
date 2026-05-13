-- Defence-in-depth against the start_session TOCTOU race.
--
-- The application checks "is the camera already streaming?" before inserting,
-- but two concurrent /start calls can both pass that check and both insert.
-- This partial unique index forces Postgres to reject the second insert with
-- a unique-violation, which the service layer translates back into the same
-- ConflictError("camera_already_streaming") the pre-check raises.

create unique index stream_sessions_one_active_per_camera_idx
  on stream_sessions(camera_id)
  where state in ('approved', 'streaming');
