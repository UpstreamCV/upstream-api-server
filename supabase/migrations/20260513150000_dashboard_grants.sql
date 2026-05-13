-- Hosted Supabase does not auto-grant table-level SELECT to the
-- `authenticated` role for tables created in `public` via migration. The
-- local Supabase CLI does, which masked the problem in dev. Without these
-- grants, RLS policies are unreachable: Postgres rejects with
-- `permission denied for table <x>` at code 42501 before RLS runs.
--
-- Pattern: every public table intended for dashboard reads should receive
-- an explicit GRANT SELECT TO authenticated in the same migration that
-- creates it. This file backfills that pattern for the existing tables.

grant usage on schema public to authenticated;

grant select on public.accounts            to authenticated;
grant select on public.account_members     to authenticated;
grant select on public.account_limits      to authenticated;
grant select on public.api_tokens          to authenticated;
grant select on public.agents              to authenticated;
grant select on public.cameras             to authenticated;
grant select on public.stream_sessions     to authenticated;
grant select on public.stream_validations  to authenticated;
grant select on public.heartbeats          to authenticated;
grant select on public.snapshots           to authenticated;
grant select on public.usage_events        to authenticated;

-- Storage: dashboard's getLatestFrame() mints a signed URL with the user's
-- JWT (createSignedUrl on the snapshots bucket). Without a storage policy
-- the call returns 403. Mirror the same membership check the
-- snapshots.snapshots_member_select policy uses, this time against
-- storage.objects keyed on the leading <account_id>/ prefix.
--
-- The bucket itself is provisioned out-of-band (Studio or service-role
-- upload). This policy is harmless if the bucket doesn't exist yet.

drop policy if exists snapshots_storage_member_select on storage.objects;
create policy snapshots_storage_member_select
  on storage.objects
  for select
  to authenticated
  using (
    bucket_id = 'snapshots'
    and (
      -- storage_key shape: <account_id>/<camera_id>/<yyyymmdd>/<uuid>.jpg
      (storage.foldername(name))[1]::uuid
        in (select account_id from public.account_members where user_id = auth.uid())
    )
  );
