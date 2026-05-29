-- Backfill the GRANT SELECT -> authenticated pattern documented in
-- 20260513150000_dashboard_grants.sql for public tables created AFTER it.
--
-- Hosted Supabase does not auto-grant table-level SELECT to the `authenticated`
-- role for tables created in `public` via migration; without it Postgres
-- rejects with `permission denied for table` (42501) BEFORE RLS runs. This is
-- invisible to the api-server (it reads via the service role, which is
-- privileged + RLS-exempt) but it breaks Supabase Realtime, which evaluates the
-- RLS check AS the `authenticated` role: the live perception overlay's channel
-- reported SUBSCRIBED with a valid token yet received ZERO change events
-- ("Fallback polling" forever), even though the table is in the
-- supabase_realtime publication, is REPLICA IDENTITY FULL, and has a
-- member-select policy. The missing table grant is the cause.
--
-- `latest_perception_snapshots` fixes the live overlay. The rest are the other
-- RLS-protected dashboard tables created after the original backfill; granting
-- them now matches the documented "every dashboard table gets the grant"
-- pattern and prevents the identical bug if/when they're subscribed to. RLS
-- still restricts which rows each user sees, so granting SELECT is safe.

grant select on public.latest_perception_snapshots to authenticated;
grant select on public.camera_geometry_configs     to authenticated;
grant select on public.reliability_findings        to authenticated;
grant select on public.processor_statuses          to authenticated;
grant select on public.observations                to authenticated;
