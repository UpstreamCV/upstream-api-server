-- Auto-provision a tenant on Supabase Auth signup.
--
-- When a row lands in auth.users (via email/password signup, magic link,
-- OAuth, or Studio "Add user"), we create the matching tenant rows so the
-- new user can immediately use the dashboard without manual SQL seeding:
--   accounts        — name from raw_user_meta_data.workspace_name or email handle
--   account_members — role='owner' linking the user to the new account
--   account_limits  — permissive defaults (5 cameras, 60 fps, 1920x1080, h264/h265)
--
-- Runs as SECURITY DEFINER so RLS doesn't block the inserts. The explicit
-- search_path is the Supabase-recommended hardening to prevent path tampering
-- via the auth.users row.

create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  new_account_id  uuid := gen_random_uuid();
  base_slug       text;
  workspace_name  text;
  account_slug    text;
begin
  -- Idempotency: a previous trigger run (or manual seed) already linked
  -- this user. Don't double-provision.
  if exists (select 1 from public.account_members where user_id = new.id) then
    return new;
  end if;

  workspace_name := coalesce(
    nullif(trim(new.raw_user_meta_data->>'workspace_name'), ''),
    initcap(split_part(coalesce(new.email, ''), '@', 1)),
    'Workspace'
  );

  -- Slug: lowercased, ASCII-ish base from the workspace name or email handle,
  -- then a 6-char suffix from the new account uuid. Suffix prevents
  -- collisions without a retry loop.
  base_slug := lower(coalesce(
    nullif(trim(new.raw_user_meta_data->>'workspace_name'), ''),
    split_part(coalesce(new.email, ''), '@', 1),
    ''
  ));
  base_slug := regexp_replace(base_slug, '[^a-z0-9]+', '-', 'g');
  base_slug := trim(both '-' from base_slug);
  if base_slug = '' then
    base_slug := 'workspace';
  end if;
  account_slug := base_slug || '-' || left(replace(new_account_id::text, '-', ''), 6);

  insert into public.accounts (id, name, slug, plan, status)
    values (new_account_id, workspace_name, account_slug, 'free', 'active');

  insert into public.account_members (account_id, user_id, role)
    values (new_account_id, new.id, 'owner');

  -- Match the permissive defaults the local-dev seed uses so first-run
  -- testing doesn't immediately hit a cap.
  insert into public.account_limits (
    account_id,
    max_cameras,
    max_concurrent_sessions,
    max_fps,
    max_width,
    max_height,
    max_bitrate_kbps,
    allowed_codecs
  ) values (
    new_account_id,
    5, 5, 60,
    1920, 1080, 8000,
    array['h264','h265']
  );

  return new;
end;
$$;

-- Re-runnable for local-dev `supabase db reset`.
drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();
