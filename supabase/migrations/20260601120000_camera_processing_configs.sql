-- Per-camera Processing Config.
--
-- ONE dashboard-authored model/processing config per camera. UPSERT
-- semantics (unique on camera_id) — a save overwrites the prior config in
-- place, in deliberate contrast to the append-only `observations` table.
--
-- `revision` is the DB ROW revision: it starts at 1 and bumps on every
-- save, giving a cheap "did processing config change since last session"
-- signal. It is NOT the config schema version — that lives inside the
-- `config` JSONB as `config->>'version'` (always 1 today).
--
-- Written by dashboard users via PUT /v1/cameras/{id}/processing-config;
-- account_id/camera_id are derived server-side from the authenticated
-- caller, never trusted from the request body. Reads are dashboard SELECT
-- under RLS, same posture as camera_geometry_configs.
--
-- The processor does NOT consume this config yet — it runs its hard-coded
-- detector. This table exists so the dashboard can author per-camera model
-- selection ahead of processor integration.

create table camera_processing_configs (
  id          uuid primary key default gen_random_uuid(),
  account_id  uuid not null references accounts(id) on delete cascade,
  camera_id   uuid not null references cameras(id) on delete cascade,
  revision    integer not null default 1,
  config      jsonb not null,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

-- ONE config per camera — the upsert ON CONFLICT target.
create unique index camera_processing_configs_camera_idx
  on camera_processing_configs(camera_id);

create index camera_processing_configs_account_camera_idx
  on camera_processing_configs(account_id, camera_id);

create trigger camera_processing_configs_set_updated_at
  before update on camera_processing_configs
  for each row execute function set_updated_at();

alter table camera_processing_configs enable row level security;

create policy camera_processing_configs_member_select on camera_processing_configs
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );

grant select on public.camera_processing_configs to authenticated;
