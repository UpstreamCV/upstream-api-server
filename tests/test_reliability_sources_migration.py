"""Upstream Reliability M2a — cv_sources migration shape pin.

Pins the table/index/CHECK/RLS/grant shape the source-registry service relies
on. The unique-index expression is pinned byte-for-byte because the ingest
upsert's ON CONFLICT target must match it exactly to infer the index. Also
guards the hard rule that the retired "blackbox" codename appears nowhere, and
that there is no client-writable policy (the `"for update" not in sql` assert
doubles as the no-`select ... for update` guard the M1a migration uses)."""

from __future__ import annotations

from pathlib import Path

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260624120000_reliability_sources.sql"
)


def _sql() -> str:
    return _MIGRATION.read_text().lower()


def test_table_exists() -> None:
    assert "create table cv_sources" in _sql()


def test_unique_index_expression_is_byte_identical_to_upsert() -> None:
    sql = _sql()
    assert "cv_sources_account_source_external_key_uidx" in sql
    # MUST match services/reliability_sources.py::_UPSERT_SQL's ON CONFLICT target.
    assert "on cv_sources (account_id, source, coalesce(external_camera_key, ''))" in sql


def test_check_constraints() -> None:
    sql = _sql()
    assert "check (length(btrim(source)) > 0)" in sql
    assert "length(btrim(external_camera_key)) > 0" in sql
    assert "source_kind in ('collector', 'external_camera'," in sql
    assert "'observation_source', 'unknown')" in sql
    assert "check (jsonb_typeof(metadata) = 'object')" in sql


def test_secondary_indexes_exist() -> None:
    sql = _sql()
    assert "cv_sources_account_last_seen_idx" in sql
    assert "cv_sources_account_kind_last_seen_idx" in sql
    assert "cv_sources_account_heartbeat_idx" in sql


def test_rls_member_select_and_grant() -> None:
    sql = _sql()
    assert "enable row level security" in sql
    assert "cv_sources_member_select" in sql
    assert "grant select on public.cv_sources to authenticated" in sql
    # Reads only — service-role writes, no client insert/update/delete policy.
    assert "for select" in sql
    assert "for insert" not in sql
    assert "for update" not in sql
    assert "for delete" not in sql


def test_updated_at_trigger() -> None:
    sql = _sql()
    assert "cv_sources_set_updated_at" in sql
    assert "execute function set_updated_at()" in sql


def test_no_blackbox_codename() -> None:
    assert "blackbox" not in _sql()
