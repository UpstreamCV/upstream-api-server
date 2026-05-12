-- Local-dev seed. Only runs on `supabase db reset`. NEVER applied to hosted projects.
--
-- After `supabase db reset`, run `uv run python scripts/seed_dev_token.py`
-- to mint a real API token and insert it for this account.

-- Stable dev account so docs can reference fixed IDs.
insert into accounts (id, name, slug, plan)
  values ('00000000-0000-0000-0000-000000000001', 'Local Dev', 'local-dev', 'free')
on conflict (id) do nothing;

insert into account_limits (
  account_id, max_cameras, max_concurrent_sessions, max_fps,
  max_width, max_height, max_bitrate_kbps, allowed_codecs
)
values (
  '00000000-0000-0000-0000-000000000001', 5, 5, 60,
  1920, 1080, 8000, '{h264,h265}'
)
on conflict (account_id) do nothing;
