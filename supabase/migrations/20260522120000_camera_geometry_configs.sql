-- P6-A Camera Geometry Config.
--
-- ONE dashboard-authored ROI/line geometry config per camera. UPSERT
-- semantics (unique on camera_id) — a save overwrites the prior config in
-- place, in deliberate contrast to the append-only `observations` table.
--
-- `revision` is the DB ROW revision: it starts at 1 and bumps on every
-- save, giving the processor a cheap "did geometry change since last
-- session" signal. It is NOT the config schema version — that lives inside
-- the `config` JSONB as `config->>'version'` (always 1 in P6-A).
--
-- Written by dashboard users via PUT /v1/cameras/{id}/geometry-config;
-- account_id/camera_id are derived server-side from the authenticated
-- caller, never trusted from the request body. Reads are dashboard SELECT
-- under RLS, same posture as latest_perception_snapshots; the row is also
-- joined into the processor eligible-sessions response.

create table camera_geometry_configs (
  id          uuid primary key default gen_random_uuid(),
  account_id  uuid not null references accounts(id) on delete cascade,
  camera_id   uuid not null references cameras(id) on delete cascade,
  revision    integer not null default 1,
  config      jsonb not null,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

-- ONE config per camera — the upsert ON CONFLICT target.
create unique index camera_geometry_configs_camera_idx
  on camera_geometry_configs(camera_id);

create index camera_geometry_configs_account_camera_idx
  on camera_geometry_configs(account_id, camera_id);

create trigger camera_geometry_configs_set_updated_at
  before update on camera_geometry_configs
  for each row execute function set_updated_at();

alter table camera_geometry_configs enable row level security;

create policy camera_geometry_configs_member_select on camera_geometry_configs
  for select
  using (
    account_id in (select account_id from account_members where user_id = auth.uid())
  );
