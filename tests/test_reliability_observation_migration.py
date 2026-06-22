"""Upstream Reliability M1a — migration shape pin.

Pins the table/index/RLS/grant shape the ingest + issues services rely on, and
guards the hard requirement that the retired "blackbox" codename appears nowhere
in the migration.
"""

from __future__ import annotations

from pathlib import Path

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260622120000_reliability_observations.sql"
)


def _sql() -> str:
    return _MIGRATION.read_text().lower()


def test_tables_exist() -> None:
    sql = _sql()
    assert "create table cv_observation_batches" in sql
    assert "create table cv_observations" in sql
    assert "create table cv_issues" in sql


def test_partial_unique_open_issue_index() -> None:
    sql = _sql()
    assert "cv_issues_open_dedupe_idx" in sql
    assert "on cv_issues(account_id, dedupe_key)" in sql
    assert "where status = 'open'" in sql


def test_batch_idempotency_unique_index() -> None:
    sql = _sql()
    assert "cv_observation_batches_account_dedupe_idx" in sql
    assert "on cv_observation_batches(account_id, dedupe_key)" in sql


def test_issue_category_check() -> None:
    assert "check (category in ('count', 'confidence', 'liveness', 'tracker'))" in _sql()


def test_observed_at_history_indexes_exist() -> None:
    sql = _sql()
    assert "cv_observations_account_camera_observed_idx" in sql
    assert "cv_observations_account_external_key_observed_idx" in sql


def test_rls_member_select_and_grants() -> None:
    sql = _sql()
    # Three tables, each with RLS enabled and exactly a member-select policy.
    assert sql.count("enable row level security") == 3
    assert sql.count("create policy") == 3
    for table in ("cv_observation_batches", "cv_observations", "cv_issues"):
        assert f"{table}_member_select" in sql
        assert f"grant select on public.{table} to authenticated" in sql
    # Reads only — service-role writes, no client insert/update/delete policy.
    assert "for select" in sql
    assert "for insert" not in sql
    assert "for update" not in sql
    assert "for delete" not in sql


def test_no_blackbox_codename() -> None:
    assert "blackbox" not in _sql()
