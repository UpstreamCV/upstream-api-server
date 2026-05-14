-- Enable Supabase Realtime broadcasts on the two tables the dashboard
-- needs to observe in near real-time:
--
--   - stream_sessions: drives the camera detail page's "Waiting for stream"
--     ↔ "live" transition and the resulting WHEP connect.
--   - cameras: drives the cameras list view's "Idle" / "Streaming" /
--     "Error" badge so the list reflects live state without polling.
--
-- Realtime respects the existing RLS policies on each table
-- (`stream_sessions_member_select`, `cameras_member_select`) — broadcasts
-- arrive at the browser only for rows the authenticated user is already
-- allowed to read via `select`. No new policies are introduced here.
--
-- The `supabase_realtime` publication is created automatically by Supabase
-- on every project. Adding tables to it via `alter publication` is the
-- recommended path. We use `if not exists` semantics so re-running the
-- migration (or running it on a project where these tables were already
-- enabled by hand in the dashboard UI) is a no-op.
--
-- After this migration applies, the dashboard's polling-based
-- CameraDetailPolling component is replaced with a Realtime subscription;
-- the api-server itself does not need to push anything — Supabase's
-- Realtime server tails the WAL and forwards row changes to subscribed
-- clients directly.

do $$
begin
  if not exists (
    select 1
      from pg_publication_tables
     where pubname = 'supabase_realtime'
       and schemaname = 'public'
       and tablename = 'stream_sessions'
  ) then
    alter publication supabase_realtime add table stream_sessions;
  end if;

  if not exists (
    select 1
      from pg_publication_tables
     where pubname = 'supabase_realtime'
       and schemaname = 'public'
       and tablename = 'cameras'
  ) then
    alter publication supabase_realtime add table cameras;
  end if;
end $$;
