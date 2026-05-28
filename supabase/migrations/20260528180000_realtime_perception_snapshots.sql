-- Add latest_perception_snapshots to the Supabase Realtime publication.
--
-- The live detection overlay on the camera Monitor page polled this table's
-- /perception/latest endpoint at 1 Hz. Polling adds up to a full second of
-- staleness on top of the processor's ~1/s publish cadence, so detections
-- visibly lagged the WebRTC video. Putting the table on the realtime
-- publication lets the dashboard subscribe to row changes and refetch the
-- instant the processor upserts a new snapshot -- the same push pattern the
-- camera detail page already uses for stream_sessions (see
-- 20260514120000_realtime_publication.sql).
--
-- Realtime respects the existing RLS policy
-- (`latest_perception_snapshots_member_select`, from
-- 20260521120000_latest_perception_snapshots.sql) -- broadcasts arrive at the
-- browser only for rows the authenticated user can already read via select.
-- No new policy is introduced. The api-server still pushes nothing; Supabase's
-- Realtime server tails the WAL and forwards row changes to subscribers.
--
-- Idempotent: ``if not exists`` so re-running, or running on a project where
-- the table was enabled by hand in the Supabase dashboard UI, is a no-op.

do $$
begin
  if not exists (
    select 1
      from pg_publication_tables
     where pubname = 'supabase_realtime'
       and schemaname = 'public'
       and tablename = 'latest_perception_snapshots'
  ) then
    alter publication supabase_realtime add table latest_perception_snapshots;
  end if;
end $$;
