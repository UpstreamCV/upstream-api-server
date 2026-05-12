-- Extensions and shared helpers used across every subsequent migration.

create extension if not exists pgcrypto;
create extension if not exists citext;

-- Generic trigger to keep updated_at honest on any table that has one.
create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;
