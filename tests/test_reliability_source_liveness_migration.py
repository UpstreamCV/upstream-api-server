"""Upstream Reliability M2b — cv_sources liveness migration shape pin.

Pins the liveness/health columns + CHECKs + indexes the sweep and reads rely on,
and guards the no-"blackbox" rule + the no-`select ... for update` posture (the
sweep's FOR UPDATE SKIP LOCKED lives in the service, never in a migration)."""

from __future__ import annotations

from pathlib import Path

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260625120000_reliability_source_liveness.sql"
)


def _sql() -> str:
    return _MIGRATION.read_text().lower()


def test_alters_cv_sources() -> None:
    assert "alter table cv_sources" in _sql()


def test_new_columns_present() -> None:
    sql = _sql()
    for col in (
        "liveness_enabled",
        "expected_interval_seconds",
        "liveness_grace_seconds",
        "health_status",
        "health_reason",
        "health_message",
        "last_liveness_checked_at",
        "last_health_status_changed_at",
        "liveness_silence_started_at",
        "last_liveness_issue_id",
    ):
        assert f"add column {col}" in sql, col


def test_health_status_check_and_default() -> None:
    sql = _sql()
    assert "health_status text not null default 'unknown'" in sql
    assert "'healthy', 'delayed', 'silent'," in sql
    assert "'collector_offline', 'unknown', 'disabled'" in sql


def test_interval_checks() -> None:
    sql = _sql()
    assert "expected_interval_seconds is null or expected_interval_seconds > 0" in sql
    assert "liveness_grace_seconds is null or liveness_grace_seconds >= 0" in sql


def test_liveness_enabled_default_true() -> None:
    assert "liveness_enabled boolean not null default true" in _sql()


def test_indexes_present() -> None:
    sql = _sql()
    assert "cv_sources_liveness_claim_idx" in sql
    assert "where liveness_enabled" in sql  # partial claim index
    assert "cv_sources_account_health_idx" in sql
    assert "on cv_sources(account_id, health_status, last_seen_at desc)" in sql


def test_no_for_update_in_migration() -> None:
    assert "for update" not in _sql()


def test_no_blackbox_codename() -> None:
    assert "blackbox" not in _sql()
