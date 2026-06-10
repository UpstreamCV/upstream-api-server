"""Detection-metrics retention + hourly rollup (reliability M6).

Pure unit tests against a fake asyncpg connection, matching the project's
no-real-DB convention (mirrors tests/test_detection_metrics.py):

- the production rollup/prune SQL is pinned by lowercase substring so the
  migration and the code cannot drift apart on clause/column PRESENCE (these
  pins do not execute the SQL, so they guard structure, not numeric equality),
- the maintenance service is driven end-to-end through a fake conn that records
  each statement, its order, its bound args, and that it ran inside a
  transaction,
- the weighted rollup math, bucketing, retention windows and null/zero handling
  are asserted numerically against the pure-Python reference
  (``compute_hourly_rollup_buckets``), a hand-maintained spec for
  ``_ROLLUP_HOURLY_SQL`` (kept in lockstep with the SQL by review, not proven
  equal by a SQL engine — see the per-field-FILTER test below).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from upstream_api.services.detection_metrics_rollup import (
    _PRUNE_RAW_SQL,
    _PRUNE_ROLLUPS_SQL,
    _ROLLUP_HOURLY_SQL,
    HOURLY_ROLLUP_RETENTION_DAYS,
    RAW_METRIC_RETENTION_HOURS,
    ROLLUP_LOOKBACK_HOURS,
    _affected_count,
    compute_hourly_rollup_buckets,
    run_detection_metrics_rollup,
)

# Stable expression list shared by the (M9-re-keyed) unique index and the
# rollup's ON CONFLICT target. If these two ever differ byte-for-byte, the
# upsert fails to infer the arbiter index at runtime — so the tests pin BOTH
# against this. M9 extended the M6 package fold with the applied-processing-
# config dimension folds, same coalesce-sentinel pattern per dimension.
_CONFLICT_EXPR = (
    "coalesce(package_id, ''), "
    "coalesce(processing_config_source, ''), "
    "coalesce(processing_config_revision, -1), "
    "coalesce(processing_config_status, ''), "
    "coalesce(applied_model_key, ''), "
    "coalesce(applied_selected_classes_key, ''), "
    "coalesce(applied_confidence_threshold, -1.0), "
    "coalesce(requested_processing_fps, -1), "
    "coalesce(applied_processing_fps, -1), "
    "coalesce(processing_config_warnings, '{}'::text[])"
)


def _norm(sql: str) -> str:
    """Lowercase + collapse all runs of whitespace to single spaces, so the
    substring pins below are robust to indentation/wrapping rather than matching
    exact column alignment."""
    return " ".join(sql.lower().split())


# ---------------------------------------------------------------------------
# Retention policy constants.
# ---------------------------------------------------------------------------


def test_retention_constants_are_sane() -> None:
    assert RAW_METRIC_RETENTION_HOURS == 72
    assert HOURLY_ROLLUP_RETENTION_DAYS == 90
    assert ROLLUP_LOOKBACK_HOURS == 96
    # The catch-up margin: lookback MUST exceed raw retention so a lapsed-then-
    # resumed job can roll up still-prunable hours before their raw rows are
    # deleted. This is safe because the raw prune is hour-aligned (whole buckets
    # only) — do NOT "fix" this to lookback <= retention.
    assert ROLLUP_LOOKBACK_HOURS > RAW_METRIC_RETENTION_HOURS
    for value in (RAW_METRIC_RETENTION_HOURS, HOURLY_ROLLUP_RETENTION_DAYS, ROLLUP_LOOKBACK_HOURS):
        assert isinstance(value, int) and value > 0


# ---------------------------------------------------------------------------
# SQL pins.
# ---------------------------------------------------------------------------


def test_rollup_sql_pins_weighted_math_and_idempotent_upsert() -> None:
    sql = _norm(_ROLLUP_HOURLY_SQL)
    assert "insert into detection_confidence_hourly_rollups" in sql
    # Hour bucketing.
    assert "date_trunc('hour', m.observed_at)" in sql
    assert "date_trunc('hour', m.observed_at) + interval '1 hour'" in sql
    # Each weighted aggregate filters numerator AND denominator on ITS OWN field
    # (per-field FILTER), with a null-safe nullif divisor.
    assert (
        "sum(m.avg_confidence * m.confidence_sample_count) "
        "filter (where m.avg_confidence is not null)" in sql
    )
    assert (
        "nullif(sum(m.confidence_sample_count) filter (where m.avg_confidence is not null), 0)"
        in sql
    )
    assert (
        "sum(m.low_confidence_ratio * m.confidence_sample_count) "
        "filter (where m.low_confidence_ratio is not null)" in sql
    )
    assert (
        "nullif(sum(m.confidence_sample_count) "
        "filter (where m.low_confidence_ratio is not null), 0)" in sql
    )
    assert (
        "sum(m.confidence_stddev * m.confidence_sample_count) "
        "filter (where m.confidence_stddev is not null)" in sql
    )
    assert (
        "nullif(sum(m.confidence_sample_count) filter (where m.confidence_stddev is not null), 0)"
        in sql
    )
    assert "min(m.min_confidence)" in sql
    assert "max(m.max_confidence)" in sql
    # Group on coalesce(package_id,'') so a NULL and an empty-string package_id
    # fold into one group (matching the conflict arbiter); store nullif(...,'')
    # so no sentinel string is persisted. M9: the applied-processing-config
    # dimensions follow the exact same fold/store pattern as extra GROUP BY keys.
    assert (
        "group by m.account_id, m.camera_id, coalesce(m.package_id, ''), "
        "coalesce(m.processing_config_source, ''), "
        "coalesce(m.processing_config_revision, -1), "
        "coalesce(m.processing_config_status, ''), "
        "coalesce(m.applied_model_key, ''), "
        "coalesce(m.applied_selected_classes_key, ''), "
        "coalesce(m.applied_confidence_threshold, -1.0), "
        "coalesce(m.requested_processing_fps, -1), "
        "coalesce(m.applied_processing_fps, -1), "
        "coalesce(m.processing_config_warnings, '{}'::text[]), "
        "date_trunc('hour', m.observed_at)" in sql
    )
    assert "nullif(coalesce(m.package_id, ''), '') as package_id" in sql
    assert "nullif(coalesce(m.processing_config_source, ''), '') as processing_config_source" in sql
    assert (
        "nullif(coalesce(m.processing_config_revision, -1), -1) as processing_config_revision"
        in sql
    )
    assert "nullif(coalesce(m.processing_config_status, ''), '') as processing_config_status" in sql
    assert "nullif(coalesce(m.applied_model_key, ''), '') as applied_model_key" in sql
    assert (
        "nullif(coalesce(m.applied_selected_classes_key, ''), '') as applied_selected_classes_key"
        in sql
    )
    assert (
        "nullif(coalesce(m.applied_confidence_threshold, -1.0), -1.0) "
        "as applied_confidence_threshold" in sql
    )
    assert "nullif(coalesce(m.requested_processing_fps, -1), -1) as requested_processing_fps" in sql
    assert "nullif(coalesce(m.applied_processing_fps, -1), -1) as applied_processing_fps" in sql
    assert (
        "nullif(coalesce(m.processing_config_warnings, '{}'::text[]), '{}'::text[]) "
        "as processing_config_warnings" in sql
    )
    # Exclude the current incomplete hour; the lookback lower bound is ALSO
    # hour-aligned so the rollup only ever scans whole hours.
    assert "m.observed_at < date_trunc('hour', $1::timestamptz)" in sql
    assert (
        "m.observed_at >= date_trunc('hour', $1::timestamptz - make_interval(hours => $2::int))"
        in sql
    )
    # Idempotent upsert: recompute/replace every aggregate column.
    assert f"on conflict (account_id, camera_id, bucket_start, {_CONFLICT_EXPR}) do update" in sql
    for col in (
        "metric_count",
        "object_count_total",
        "confidence_sample_count",
        "avg_confidence_weighted",
        "min_confidence",
        "max_confidence",
        "avg_low_confidence_ratio_weighted",
        "avg_confidence_stddev_weighted",
        "bucket_end",
    ):
        assert f"= excluded.{col}" in sql


def test_rollup_sql_stores_no_raw_json_and_null_session() -> None:
    sql = _norm(_ROLLUP_HOURLY_SQL)
    # Compact summary only — never the raw snapshot blob / objects array. (The
    # source table name snapshot_detection_metrics legitimately contains
    # "snapshot"; what must never appear is a jsonb cast or an objects column.)
    assert "jsonb" not in sql
    assert "objects" not in sql
    # A bucket spans sessions: stream_session_id is stored NULL, never aggregated.
    assert "null::uuid as stream_session_id" in sql


def test_prune_raw_sql_is_coverage_guarded_and_hour_aligned() -> None:
    sql = _norm(_PRUNE_RAW_SQL)
    assert "delete from snapshot_detection_metrics m" in sql
    # The load-bearing safety guarantee: only delete a raw row whose hour is
    # already covered by an hourly bucket (join on all three keys).
    assert "using detection_confidence_hourly_rollups r" in sql
    assert "r.account_id = m.account_id" in sql
    assert "r.camera_id = m.camera_id" in sql
    assert "r.bucket_start = date_trunc('hour', m.observed_at)" in sql
    # Hour-aligned cutoff so whole buckets are deleted atomically (never half a
    # bucket — that is what makes the wider rollup lookback safe).
    assert (
        "m.observed_at < date_trunc('hour', $1::timestamptz - make_interval(hours => $2::int))"
        in sql
    )


def test_prune_rollups_sql_ages_out_old_buckets() -> None:
    sql = _norm(_PRUNE_ROLLUPS_SQL)
    assert "delete from detection_confidence_hourly_rollups" in sql
    assert "bucket_start < $1::timestamptz - make_interval(days => $2::int)" in sql


# ---------------------------------------------------------------------------
# Migration shape pin.
# ---------------------------------------------------------------------------


def test_migration_file_pins_rollup_table_shape() -> None:
    root = Path(__file__).resolve().parents[1]
    migration = (
        root / "supabase" / "migrations" / "20260529150000_detection_confidence_hourly_rollups.sql"
    )
    sql = _norm(migration.read_text())
    assert "create table detection_confidence_hourly_rollups" in sql
    # The ORIGINAL (M6) unique expression index. The M9 migration DROPs and
    # re-creates it with the config-dimension folds — the live index expression
    # is pinned against the M9 migration file in
    # test_m9_migration_file_pins_dimension_rekey below.
    assert "unique index detection_confidence_hourly_rollups_bucket_idx" in sql
    assert "(account_id, camera_id, bucket_start, coalesce(package_id, ''))" in sql
    # The three trend-chart read indexes.
    assert "detection_confidence_hourly_rollups_account_camera_bucket_idx" in sql
    assert "detection_confidence_hourly_rollups_camera_bucket_idx" in sql
    assert "detection_confidence_hourly_rollups_account_bucket_idx" in sql
    # UPSERTed table -> updated_at trigger (unlike the append-only raw table).
    assert "detection_confidence_hourly_rollups_set_updated_at" in sql
    assert "execute function set_updated_at()" in sql
    # RLS + member-select + grant, per the dashboard-table convention.
    assert "enable row level security" in sql
    assert "detection_confidence_hourly_rollups_member_select" in sql
    assert "grant select on public.detection_confidence_hourly_rollups to authenticated" in sql
    # Compact summary table — never store the raw snapshot.
    assert "jsonb" not in sql
    # Required aggregate columns exist.
    for col in (
        "metric_count",
        "object_count_total",
        "confidence_sample_count",
        "avg_confidence_weighted",
        "min_confidence",
        "max_confidence",
        "avg_low_confidence_ratio_weighted",
        "avg_confidence_stddev_weighted",
        "bucket_start",
        "bucket_end",
    ):
        assert col in sql


def test_m9_migration_file_pins_dimension_rekey() -> None:
    """The M9 migration adds the dimension columns to BOTH metrics tables and
    re-keys the rollup unique index with the dimension folds, byte-identical to
    the rollup's ON CONFLICT target (so the upsert can infer the arbiter)."""
    root = Path(__file__).resolve().parents[1]
    migration = root / "supabase" / "migrations" / "20260609120000_processing_config_dimensions.sql"
    sql = _norm(migration.read_text())
    assert "alter table snapshot_detection_metrics" in sql
    assert "alter table detection_confidence_hourly_rollups" in sql
    for col in (
        "processing_config_source",
        "processing_config_revision",
        "processing_config_status",
        "applied_model_key",
        "applied_selected_classes_key",
        "applied_confidence_threshold",
        "processing_config_warnings",
    ):
        assert f"add column {col}" in sql
    # The boolean caveat flag lives on the raw table only (derivable from
    # warnings on rollups).
    assert "add column selected_classes_analytics_counts_not_filtered boolean" in sql
    # Old index dropped, new one created with the dimension folds — and the
    # expression list is byte-identical to the service's ON CONFLICT target.
    assert "drop index detection_confidence_hourly_rollups_bucket_idx" in sql
    assert "create unique index detection_confidence_hourly_rollups_bucket_idx" in sql
    # The HISTORICAL M9 expression (superseded by the M9.1 re-key, which is
    # pinned against the live conflict target in
    # test_m9_1_migration_file_pins_fps_rekey below).
    assert (
        "(account_id, camera_id, bucket_start, coalesce(package_id, ''), "
        "coalesce(processing_config_source, ''), "
        "coalesce(processing_config_revision, -1), "
        "coalesce(processing_config_status, ''), "
        "coalesce(applied_model_key, ''), "
        "coalesce(applied_selected_classes_key, ''), "
        "coalesce(applied_confidence_threshold, -1.0), "
        "coalesce(processing_config_warnings, '{}'::text[]))" in sql
    )
    # Compact dimensions only — never the raw snapshot, never requested-value
    # columns (warnings codes already record requested-but-not-applied facts;
    # M9.1 later adds the deliberate exception: requested_processing_fps).
    assert "jsonb" not in sql
    assert "add column requested_" not in sql


def test_m9_1_migration_file_pins_fps_rekey() -> None:
    """The M9.1 migration adds the FPS dimension columns to BOTH metrics
    tables and re-keys the rollup unique index with the FPS folds, byte-
    identical to the rollup's live ON CONFLICT target."""
    root = Path(__file__).resolve().parents[1]
    migration = root / "supabase" / "migrations" / "20260610120000_processing_fps_dimensions.sql"
    sql = _norm(migration.read_text())
    assert "alter table snapshot_detection_metrics" in sql
    assert "alter table detection_confidence_hourly_rollups" in sql
    assert sql.count("add column requested_processing_fps int") == 2
    assert sql.count("add column applied_processing_fps int") == 2
    assert "drop index detection_confidence_hourly_rollups_bucket_idx" in sql
    assert "create unique index detection_confidence_hourly_rollups_bucket_idx" in sql
    assert f"(account_id, camera_id, bucket_start, {_CONFLICT_EXPR})" in sql
    assert _CONFLICT_EXPR in _norm(_ROLLUP_HOURLY_SQL)  # cross-pin: both sides match
    assert "jsonb" not in sql


# ---------------------------------------------------------------------------
# Service orchestration (fake conn).
# ---------------------------------------------------------------------------


class FakeRollupConn:
    """Records each statement, its bound args, and whether it ran inside a
    transaction. Returns canned command-status tags so the service's count
    parsing is exercised. ``raise_on_substr`` makes a chosen statement raise."""

    def __init__(
        self,
        *,
        statuses: dict[str, str] | None = None,
        raise_on_substr: str | None = None,
    ) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.tx_active_during: list[bool] = []
        self.tx_entered = 0
        self.tx_exited = 0
        self._tx_depth = 0
        self._statuses = statuses or {}
        self._raise_on_substr = raise_on_substr

    def transaction(self) -> Any:
        outer = self

        class _Tx:
            async def __aenter__(self: Any) -> Any:
                outer._tx_depth += 1
                outer.tx_entered += 1
                return self

            async def __aexit__(self: Any, *exc: Any) -> bool:
                outer._tx_depth -= 1
                outer.tx_exited += 1
                return False

        return _Tx()

    async def execute(self, sql: str, *args: Any) -> str:
        if self._raise_on_substr and self._raise_on_substr in sql:
            raise RuntimeError("simulated db failure")
        self.calls.append((sql, args))
        self.tx_active_during.append(self._tx_depth > 0)
        low = sql.lower()
        if "insert into detection_confidence_hourly_rollups" in low:
            return self._statuses.get("rollup", "INSERT 0 12")
        if "delete from snapshot_detection_metrics" in low:
            return self._statuses.get("prune_raw", "DELETE 345")
        if "delete from detection_confidence_hourly_rollups" in low:
            return self._statuses.get("prune_rollups", "DELETE 7")
        return "OK"


async def test_run_executes_rollup_then_prunes_in_one_transaction() -> None:
    conn = FakeRollupConn()
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
    await run_detection_metrics_rollup(conn, now=now)

    # Exactly three statements, in order: rollup, prune raw, prune rollups.
    assert len(conn.calls) == 3
    assert "insert into detection_confidence_hourly_rollups" in conn.calls[0][0].lower()
    assert "delete from snapshot_detection_metrics" in conn.calls[1][0].lower()
    assert "delete from detection_confidence_hourly_rollups" in conn.calls[2][0].lower()
    # All inside the transaction.
    assert conn.tx_active_during == [True, True, True]
    # The SAME captured now anchors every statement; the constants are bound.
    assert conn.calls[0][1] == (now, ROLLUP_LOOKBACK_HOURS)
    assert conn.calls[1][1] == (now, RAW_METRIC_RETENTION_HOURS)
    assert conn.calls[2][1] == (now, HOURLY_ROLLUP_RETENTION_DAYS)


async def test_run_returns_parsed_counts() -> None:
    conn = FakeRollupConn(
        statuses={
            "rollup": "INSERT 0 12",
            "prune_raw": "DELETE 12345",
            "prune_rollups": "DELETE 20",
        }
    )
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
    result = await run_detection_metrics_rollup(conn, now=now)
    assert result.rolled_up_buckets == 12
    assert result.raw_rows_pruned == 12345
    assert result.hourly_rows_pruned == 20
    assert result.started_at == now
    assert result.finished_at >= now
    # JSON-friendly, ISO timestamps, counts only (nothing sensitive).
    d = result.to_dict()
    assert d["rolled_up_buckets"] == 12
    assert d["raw_rows_pruned"] == 12345
    assert d["hourly_rows_pruned"] == 20
    assert d["started_at"] == now.isoformat()
    assert isinstance(d["finished_at"], str)


async def test_run_honours_overridden_windows() -> None:
    conn = FakeRollupConn()
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
    await run_detection_metrics_rollup(
        conn, now=now, lookback_hours=48, raw_retention_hours=36, rollup_retention_days=30
    )
    assert conn.calls[0][1] == (now, 48)
    assert conn.calls[1][1] == (now, 36)
    assert conn.calls[2][1] == (now, 30)


async def test_run_surfaces_db_errors() -> None:
    conn = FakeRollupConn(raise_on_substr="insert into detection_confidence_hourly_rollups")
    with pytest.raises(RuntimeError, match="simulated db failure"):
        await run_detection_metrics_rollup(conn, now=datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC))
    # The failure happened INSIDE the transaction, which then unwound (rolled
    # back). This pins the crash-safety property: a failed run never leaves a
    # half-applied rollup/prune outside a transaction.
    assert conn.tx_entered == 1
    assert conn.tx_exited == 1


async def test_run_unwinds_transaction_on_mid_run_failure() -> None:
    """A failure on the SECOND statement (prune raw) — after the rollup INSERT
    already ran — must still unwind the transaction, so the rollup is rolled back
    rather than committed without its prune."""
    conn = FakeRollupConn(raise_on_substr="delete from snapshot_detection_metrics")
    with pytest.raises(RuntimeError, match="simulated db failure"):
        await run_detection_metrics_rollup(conn, now=datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC))
    # The rollup INSERT ran (and was inside the tx) before the prune raised.
    assert len(conn.calls) == 1
    assert "insert into detection_confidence_hourly_rollups" in conn.calls[0][0].lower()
    assert conn.tx_active_during == [True]
    assert conn.tx_entered == 1
    assert conn.tx_exited == 1


def test_affected_count_parses_command_tags() -> None:
    assert _affected_count("INSERT 0 12") == 12
    assert _affected_count("DELETE 5") == 5
    assert _affected_count("DELETE 0") == 0
    # Defensive: unexpected shapes degrade to 0, never raise.
    assert _affected_count("") == 0
    assert _affected_count("WEIRD") == 0


# ---------------------------------------------------------------------------
# Rollup math (pure reference = executable spec for _ROLLUP_HOURLY_SQL).
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
_ACCOUNT = uuid4()
_CAMERA = uuid4()


def _raw(
    observed_at: datetime,
    *,
    avg: float | None = None,
    samples: int = 0,
    objects: int = 0,
    low_ratio: float | None = None,
    stddev: float | None = None,
    mn: float | None = None,
    mx: float | None = None,
    account: UUID = _ACCOUNT,
    camera: UUID = _CAMERA,
    package: str | None = "pkg-1",
    config_source: str | None = None,
    config_revision: int | None = None,
    config_status: str | None = None,
    model_key: str | None = None,
    classes_key: str | None = None,
    threshold: float | None = None,
    requested_fps: int | None = None,
    applied_fps: int | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "account_id": account,
        "camera_id": camera,
        "package_id": package,
        "observed_at": observed_at,
        "object_count": objects,
        "confidence_sample_count": samples,
        "avg_confidence": avg,
        "min_confidence": mn,
        "max_confidence": mx,
        "low_confidence_ratio": low_ratio,
        "confidence_stddev": stddev,
        "processing_config_source": config_source,
        "processing_config_revision": config_revision,
        "processing_config_status": config_status,
        "applied_model_key": model_key,
        "applied_selected_classes_key": classes_key,
        "applied_confidence_threshold": threshold,
        "requested_processing_fps": requested_fps,
        "applied_processing_fps": applied_fps,
        "processing_config_warnings": warnings,
    }


def test_weighted_aggregates_and_counts() -> None:
    """Two confidence-bearing rows in one completed hour (10:00). Weighted by
    confidence_sample_count: avg=(0.8*30+0.6*20)/50=0.72; ratio=(0.1*30+0.5*20)/50=0.26;
    stddev=(0.05*30+0.15*20)/50=0.09; min=0.4; max=0.95; objects=8; samples=50."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 15, tzinfo=UTC),
            avg=0.8,
            samples=30,
            objects=5,
            low_ratio=0.1,
            stddev=0.05,
            mn=0.5,
            mx=0.95,
        ),
        _raw(
            datetime(2026, 5, 29, 10, 45, tzinfo=UTC),
            avg=0.6,
            samples=20,
            objects=3,
            low_ratio=0.5,
            stddev=0.15,
            mn=0.4,
            mx=0.9,
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.account_id == _ACCOUNT
    assert b.camera_id == _CAMERA
    assert b.package_id == "pkg-1"
    assert b.bucket_start == datetime(2026, 5, 29, 10, 0, tzinfo=UTC)
    assert b.bucket_end == datetime(2026, 5, 29, 11, 0, tzinfo=UTC)
    assert b.metric_count == 2
    assert b.object_count_total == 8
    assert b.confidence_sample_count == 50
    assert b.avg_confidence_weighted == pytest.approx(0.72)
    assert b.avg_low_confidence_ratio_weighted == pytest.approx(0.26)
    assert b.avg_confidence_stddev_weighted == pytest.approx(0.09)
    assert b.min_confidence == pytest.approx(0.4)
    assert b.max_confidence == pytest.approx(0.95)


def test_zero_sample_rows_are_counted_but_excluded_from_weighted_math() -> None:
    """A confidence-less frame (samples=0, null stats) still increments
    metric_count so the hour produces a bucket the prune can cover, AND its
    objects still flow into object_count_total (object_count can exceed the
    confidence-sample count — confidence-less detections are real objects) — but
    it contributes nothing to the weighted confidence aggregates or min/max."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=30,
            objects=5,
            low_ratio=0.1,
            stddev=0.05,
            mn=0.5,
            mx=0.95,
        ),
        # samples=0 but objects=4: a frame of confidence-less detections.
        _raw(datetime(2026, 5, 29, 10, 30, tzinfo=UTC), avg=None, samples=0, objects=4),
        _raw(
            datetime(2026, 5, 29, 10, 50, tzinfo=UTC),
            avg=0.6,
            samples=20,
            objects=3,
            low_ratio=0.5,
            stddev=0.15,
            mn=0.4,
            mx=0.9,
        ),
    ]
    b = compute_hourly_rollup_buckets(rows, now=_NOW)[0]
    assert b.metric_count == 3  # confidence-less frame counted
    assert b.object_count_total == 12  # 5 + 4 + 3: its objects flow in unconditionally
    assert b.confidence_sample_count == 50  # but its (zero) samples do not move this
    # Weighted math + min/max unchanged by the null row.
    assert b.avg_confidence_weighted == pytest.approx(0.72)
    assert b.min_confidence == pytest.approx(0.4)
    assert b.max_confidence == pytest.approx(0.95)


def test_all_empty_hour_still_yields_a_bucket_with_null_confidence() -> None:
    """An hour of only empty frames produces a bucket (so the prune covers those
    rows) with confidence_sample_count=0 and NULL weighted confidence."""
    rows = [
        _raw(datetime(2026, 5, 29, 9, 5, tzinfo=UTC), avg=None, samples=0, objects=0),
        _raw(datetime(2026, 5, 29, 9, 55, tzinfo=UTC), avg=None, samples=0, objects=0),
    ]
    b = compute_hourly_rollup_buckets(rows, now=_NOW)[0]
    assert b.metric_count == 2
    assert b.confidence_sample_count == 0
    assert b.avg_confidence_weighted is None
    assert b.avg_low_confidence_ratio_weighted is None
    assert b.avg_confidence_stddev_weighted is None
    assert b.min_confidence is None
    assert b.max_confidence is None


def test_excludes_current_incomplete_hour() -> None:
    """Rows in the current (still-filling) hour must not be rolled up."""
    now = datetime(2026, 5, 29, 12, 30, tzinfo=UTC)
    rows = [
        _raw(datetime(2026, 5, 29, 11, 50, tzinfo=UTC), avg=0.7, samples=10, mn=0.6, mx=0.8),
        _raw(datetime(2026, 5, 29, 12, 10, tzinfo=UTC), avg=0.9, samples=10, mn=0.9, mx=0.9),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=now)
    assert len(buckets) == 1
    assert buckets[0].bucket_start == datetime(2026, 5, 29, 11, 0, tzinfo=UTC)


def test_lookback_lower_bound_is_hour_aligned_and_inclusive() -> None:
    """The lookback lower bound is hour-aligned (floor(now - lookback)) and
    inclusive: a row exactly at the boundary hour is rolled up, one microsecond
    earlier is excluded. _NOW is hour-aligned so floor(now-96h) == now-96h. This
    pins the hour-alignment that keeps the rollup from ever building a bucket
    from a partial slice of its boundary hour."""
    boundary = _NOW - timedelta(hours=ROLLUP_LOOKBACK_HOURS)  # hour-aligned
    rows = [
        _raw(boundary, avg=0.7, samples=10, mn=0.7, mx=0.7),
        _raw(boundary - timedelta(microseconds=1), avg=0.2, samples=10, mn=0.2, mx=0.2),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 1
    assert buckets[0].bucket_start == boundary
    assert buckets[0].avg_confidence_weighted == pytest.approx(0.7)


def test_lookback_hours_parameter_is_honoured() -> None:
    """A non-default lookback_hours actually narrows the reference window (not
    just the SQL bound args)."""
    rows = [
        _raw(_NOW - timedelta(hours=10), avg=0.8, samples=10, mn=0.8, mx=0.8),  # inside 24h
        _raw(_NOW - timedelta(hours=50), avg=0.2, samples=10, mn=0.2, mx=0.2),  # outside 24h
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW, lookback_hours=24)
    assert len(buckets) == 1
    assert buckets[0].avg_confidence_weighted == pytest.approx(0.8)


def test_weighted_aggregates_filter_each_field_independently() -> None:
    """Per-field FILTER: a row carrying avg_confidence but a NULL
    low_confidence_ratio / confidence_stddev is excluded from the ratio/stddev
    weighted means (numerator AND denominator) while still counting toward the
    avg. Mirrors the SQL's per-field `filter (where <field> is not null)` — the
    malformed/non-writer-row defense. (Writer rows never split like this; without
    per-field filtering the ratio would be diluted toward 0.)"""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=10,
            low_ratio=0.1,
            stddev=0.05,
            mn=0.8,
            mx=0.8,
        ),
        _raw(
            datetime(2026, 5, 29, 10, 40, tzinfo=UTC),
            avg=0.6,
            samples=10,
            low_ratio=None,
            stddev=None,
            mn=0.6,
            mx=0.6,
        ),
    ]
    b = compute_hourly_rollup_buckets(rows, now=_NOW)[0]
    # avg uses both rows: (0.8*10 + 0.6*10)/20 = 0.7.
    assert b.avg_confidence_weighted == pytest.approx(0.7)
    # ratio/stddev use ONLY the row that carries them — the NULL-field row's
    # weight is excluded from the divisor, so the value is NOT diluted toward 0.
    assert b.avg_low_confidence_ratio_weighted == pytest.approx(0.1)
    assert b.avg_confidence_stddev_weighted == pytest.approx(0.05)


def test_separate_buckets_per_package_including_null() -> None:
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=10,
            mn=0.8,
            mx=0.8,
            package="a",
        ),
        _raw(
            datetime(2026, 5, 29, 10, 20, tzinfo=UTC),
            avg=0.6,
            samples=10,
            mn=0.6,
            mx=0.6,
            package="b",
        ),
        _raw(
            datetime(2026, 5, 29, 10, 30, tzinfo=UTC),
            avg=0.4,
            samples=10,
            mn=0.4,
            mx=0.4,
            package=None,
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    by_pkg = {b.package_id: b for b in buckets}
    assert set(by_pkg) == {"a", "b", None}
    assert by_pkg["a"].avg_confidence_weighted == pytest.approx(0.8)
    assert by_pkg[None].avg_confidence_weighted == pytest.approx(0.4)
    # NULL package_id is preserved as None (no sentinel string).
    assert by_pkg[None].package_id is None


def test_null_and_empty_package_fold_into_one_bucket() -> None:
    """A NULL and an empty-string package_id in the same hour MUST collapse into
    ONE bucket (stored package_id None). Otherwise the SQL's GROUP BY would emit
    two rows that both map to the coalesce(package_id,'')='' unique key, raising
    'ON CONFLICT DO UPDATE cannot affect row a second time' and aborting the
    whole maintenance transaction. The empty string is reachable input."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=10,
            mn=0.8,
            mx=0.8,
            package=None,
        ),
        _raw(
            datetime(2026, 5, 29, 10, 40, tzinfo=UTC),
            avg=0.6,
            samples=10,
            mn=0.6,
            mx=0.6,
            package="",
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 1
    assert buckets[0].package_id is None  # '' and NULL both stored as None, no sentinel
    assert buckets[0].metric_count == 2
    assert buckets[0].avg_confidence_weighted == pytest.approx(0.7)  # (0.8*10 + 0.6*10)/20


def test_buckets_isolated_per_account_and_camera() -> None:
    other_account, other_camera = uuid4(), uuid4()
    rows = [
        _raw(datetime(2026, 5, 29, 10, 10, tzinfo=UTC), avg=0.8, samples=10, mn=0.8, mx=0.8),
        _raw(
            datetime(2026, 5, 29, 10, 20, tzinfo=UTC),
            avg=0.5,
            samples=10,
            mn=0.5,
            mx=0.5,
            account=other_account,
            camera=other_camera,
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 2
    keys = {(b.account_id, b.camera_id) for b in buckets}
    assert keys == {(_ACCOUNT, _CAMERA), (other_account, other_camera)}


# ---------------------------------------------------------------------------
# M9: applied-processing-config dimensions split buckets, never blend.
# ---------------------------------------------------------------------------


def test_two_confidence_thresholds_in_one_hour_yield_two_buckets() -> None:
    """A camera whose applied confidence_threshold changed mid-hour must NOT
    blend both configs into one indistinguishable point."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.5,
            samples=10,
            mn=0.5,
            mx=0.5,
            config_source="operator_saved",
            config_revision=3,
            config_status="applied",
            model_key="trafficcamnet_v1",
            classes_key="all",
            threshold=None,
        ),
        _raw(
            datetime(2026, 5, 29, 10, 40, tzinfo=UTC),
            avg=0.9,
            samples=10,
            mn=0.9,
            mx=0.9,
            config_source="operator_saved",
            config_revision=4,
            config_status="applied",
            model_key="trafficcamnet_v1",
            classes_key="all",
            threshold=0.2,
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 2
    by_threshold = {b.applied_confidence_threshold: b for b in buckets}
    assert set(by_threshold) == {None, 0.2}
    assert by_threshold[None].processing_config_revision == 3
    assert by_threshold[None].avg_confidence_weighted == pytest.approx(0.5)
    assert by_threshold[0.2].processing_config_revision == 4
    assert by_threshold[0.2].avg_confidence_weighted == pytest.approx(0.9)
    # Both buckets share the same hour — they split on config, not time.
    assert by_threshold[None].bucket_start == by_threshold[0.2].bucket_start


def test_person_only_and_all_classes_in_one_hour_yield_two_buckets() -> None:
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.6,
            samples=10,
            mn=0.6,
            mx=0.6,
            config_source="default",
            config_status="applied_defaults",
            model_key="trafficcamnet_v1",
            classes_key="all",
        ),
        _raw(
            datetime(2026, 5, 29, 10, 40, tzinfo=UTC),
            avg=0.8,
            samples=10,
            mn=0.8,
            mx=0.8,
            config_source="operator_saved",
            config_revision=2,
            config_status="partially_applied",
            model_key="trafficcamnet_v1",
            classes_key="person",
            warnings=["selected_classes_analytics_counts_not_filtered"],
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 2
    by_classes = {b.applied_selected_classes_key: b for b in buckets}
    assert set(by_classes) == {"all", "person"}
    assert by_classes["person"].processing_config_warnings == (
        "selected_classes_analytics_counts_not_filtered",
    )
    assert by_classes["all"].processing_config_warnings is None
    assert by_classes["person"].avg_confidence_weighted == pytest.approx(0.8)


def test_old_null_dimension_rows_still_roll_up_together() -> None:
    """Pre-M9 raw rows (no dimension keys at all) and explicit-None rows fold
    into ONE null-dimension bucket — backward compatible, no split, no crash."""
    legacy_row = {
        "account_id": _ACCOUNT,
        "camera_id": _CAMERA,
        "package_id": "pkg-1",
        "observed_at": datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
        "object_count": 5,
        "confidence_sample_count": 10,
        "avg_confidence": 0.8,
        "min_confidence": 0.8,
        "max_confidence": 0.8,
        "low_confidence_ratio": None,
        "confidence_stddev": None,
        # No M9 keys at all — the pre-migration row shape.
    }
    rows = [
        legacy_row,
        _raw(datetime(2026, 5, 29, 10, 40, tzinfo=UTC), avg=0.6, samples=10, mn=0.6, mx=0.6),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.metric_count == 2
    assert b.avg_confidence_weighted == pytest.approx(0.7)
    assert b.processing_config_source is None
    assert b.processing_config_revision is None
    assert b.applied_selected_classes_key is None
    assert b.processing_config_warnings is None


def test_empty_string_and_null_dimensions_fold_like_package_id() -> None:
    """'' and NULL text dimensions (and [] vs NULL warnings) fold into one
    group, mirroring the SQL's coalesce sentinels — otherwise the upsert would
    hit 'cannot affect row a second time' exactly like the package_id case."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=10,
            mn=0.8,
            mx=0.8,
            config_source="",
            classes_key="",
            warnings=[],
        ),
        _raw(datetime(2026, 5, 29, 10, 40, tzinfo=UTC), avg=0.6, samples=10, mn=0.6, mx=0.6),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 1
    assert buckets[0].processing_config_source is None  # no sentinel stored
    assert buckets[0].applied_selected_classes_key is None
    assert buckets[0].processing_config_warnings is None
    assert buckets[0].metric_count == 2


def test_two_applied_fps_in_one_hour_yield_two_buckets() -> None:
    """M9.1: a camera whose applied processing_fps changed mid-hour (5 -> 1)
    must split into two rollup rows, not blend."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=10,
            mn=0.8,
            mx=0.8,
            config_revision=5,
            requested_fps=5,
            applied_fps=5,
            warnings=["processing_fps_output_cadence_only"],
        ),
        _raw(
            datetime(2026, 5, 29, 10, 40, tzinfo=UTC),
            avg=0.6,
            samples=10,
            mn=0.6,
            mx=0.6,
            config_revision=6,
            requested_fps=1,
            applied_fps=1,
            warnings=["processing_fps_output_cadence_only"],
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 2
    by_fps = {b.applied_processing_fps: b for b in buckets}
    assert set(by_fps) == {5, 1}
    assert by_fps[5].requested_processing_fps == 5
    assert by_fps[5].avg_confidence_weighted == pytest.approx(0.8)
    assert by_fps[1].avg_confidence_weighted == pytest.approx(0.6)
    assert by_fps[5].bucket_start == by_fps[1].bucket_start


def test_null_fps_and_applied_fps_in_one_hour_yield_two_buckets() -> None:
    """Default-FPS history (null) and an applied 5 FPS in the same hour stay
    distinguishable; the null rows still roll up safely as their own group."""
    rows = [
        _raw(datetime(2026, 5, 29, 10, 10, tzinfo=UTC), avg=0.7, samples=10, mn=0.7, mx=0.7),
        _raw(
            datetime(2026, 5, 29, 10, 40, tzinfo=UTC),
            avg=0.9,
            samples=10,
            mn=0.9,
            mx=0.9,
            requested_fps=5,
            applied_fps=5,
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 2
    by_fps = {b.applied_processing_fps: b for b in buckets}
    assert set(by_fps) == {None, 5}
    assert by_fps[None].requested_processing_fps is None
    assert by_fps[None].avg_confidence_weighted == pytest.approx(0.7)


def test_package_split_is_preserved_alongside_config_dimensions() -> None:
    """M6's per-package split keeps working under M9: same config dimensions
    but different packages still produce separate buckets."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 10, tzinfo=UTC),
            avg=0.8,
            samples=10,
            mn=0.8,
            mx=0.8,
            package="a",
            config_revision=3,
            classes_key="all",
        ),
        _raw(
            datetime(2026, 5, 29, 10, 20, tzinfo=UTC),
            avg=0.6,
            samples=10,
            mn=0.6,
            mx=0.6,
            package="b",
            config_revision=3,
            classes_key="all",
        ),
    ]
    buckets = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert len(buckets) == 2
    assert {b.package_id for b in buckets} == {"a", "b"}
    assert all(b.processing_config_revision == 3 for b in buckets)


def test_recompute_is_idempotent() -> None:
    """The reference is pure: rolling the same rows again yields identical
    buckets — mirroring the SQL's ON CONFLICT DO UPDATE recompute/replace."""
    rows = [
        _raw(
            datetime(2026, 5, 29, 10, 15, tzinfo=UTC),
            avg=0.8,
            samples=30,
            objects=5,
            low_ratio=0.1,
            stddev=0.05,
            mn=0.5,
            mx=0.95,
        ),
        _raw(
            datetime(2026, 5, 29, 10, 45, tzinfo=UTC),
            avg=0.6,
            samples=20,
            objects=3,
            low_ratio=0.5,
            stddev=0.15,
            mn=0.4,
            mx=0.9,
        ),
    ]
    first = compute_hourly_rollup_buckets(rows, now=_NOW)
    second = compute_hourly_rollup_buckets(rows, now=_NOW)
    assert first == second
