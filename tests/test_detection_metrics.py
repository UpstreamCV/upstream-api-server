"""Detection-metrics writer + recent-baseline query (reliability M5).

Pure unit tests against a fake asyncpg connection, matching the project's
no-real-DB convention. The writer's INSERT and the baseline aggregate query are
exercised end-to-end through the real confidence extractor/stats, and the
production SQL is pinned by substring so the migration and the code can't drift
apart silently.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from upstream_api.services.detection_confidence import (
    BASELINE_EXCLUSION_SECONDS,
    MIN_BASELINE_SAMPLE_COUNT,
    PREVIOUS_WINDOW_SECONDS,
)
from upstream_api.services.detection_metrics import (
    _BASELINE_SQL,
    _INSERT_DETECTION_METRIC_SQL,
    fetch_detection_confidence_baseline,
    write_detection_metrics_for_perception_snapshot,
)

# ---------------------------------------------------------------------------
# Fakes.
# ---------------------------------------------------------------------------


class FakeMetricWriteConn:
    """Captures the metric INSERT args. ``insert_returns_row`` toggles the
    INSERT...ON CONFLICT DO NOTHING RETURNING outcome (a row = inserted, None =
    idempotent conflict skip)."""

    def __init__(self, *, insert_returns_row: bool = True) -> None:
        self.insert_returns_row = insert_returns_row
        self.inserts: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "insert into snapshot_detection_metrics" in query:
            self.inserts.append(args)
            return {"id": uuid4()} if self.insert_returns_row else None
        return None


class FakeBaselineConn:
    """Returns one canned aggregate row for the baseline query and records the
    args it was called with (so the window bounds can be asserted)."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row
        self.calls: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from snapshot_detection_metrics" in query:
            self.calls.append(args)
            return self.row
        return None


async def _write(conn: Any, **overrides: Any) -> Any:
    defaults: dict[str, Any] = {
        "account_id": uuid4(),
        "camera_id": uuid4(),
        "stream_session_id": uuid4(),
        "perception_snapshot_id": uuid4(),
        "processor_id": "proc-1",
        "package_id": "pkg-1",
        "observed_at": datetime.now(tz=UTC),
        "snapshot": {"objects": [{"confidence": c} for c in [0.8, 0.7, 0.6, 0.9, 0.5]]},
    }
    defaults.update(overrides)
    return await write_detection_metrics_for_perception_snapshot(conn, **defaults)


# Column order of _INSERT_DETECTION_METRIC_SQL ($1..$17).
_ACCOUNT_ID, _CAMERA_ID, _SESSION_ID, _SNAPSHOT_ID = 0, 1, 2, 3
_PROCESSOR_ID, _PACKAGE_ID, _OBSERVED_AT = 4, 5, 6
_OBJECT_COUNT, _SAMPLE_COUNT = 7, 8
_AVG, _MEDIAN, _MIN, _MAX = 9, 10, 11, 12
_LOW_COUNT, _LOW_RATIO, _STDDEV = 13, 14, 15
_SOURCE = 16


# ---------------------------------------------------------------------------
# Writer.
# ---------------------------------------------------------------------------


async def test_writes_metric_for_normal_snapshot() -> None:
    conn = FakeMetricWriteConn()
    result = await _write(conn)
    assert result.written is True
    assert len(conn.inserts) == 1
    args = conn.inserts[0]
    assert args[_OBJECT_COUNT] == 5
    assert args[_SAMPLE_COUNT] == 5
    assert args[_AVG] is not None
    assert 0.0 <= args[_AVG] <= 1.0
    assert args[_MIN] == 0.5
    assert args[_MAX] == 0.9
    assert args[_SOURCE] == "latest_perception_snapshot"
    assert result.avg_confidence == args[_AVG]


async def test_writes_row_with_null_stats_when_too_few_samples() -> None:
    """Below MIN_SAMPLE_COUNT (5) the row is still written to preserve history,
    but the summary stats are null and avg comes back None."""
    conn = FakeMetricWriteConn()
    result = await _write(conn, snapshot={"objects": [{"confidence": 0.8}, {"confidence": 0.7}]})
    assert result.written is True
    assert result.confidence_sample_count == 2
    assert result.avg_confidence is None
    args = conn.inserts[0]
    assert args[_OBJECT_COUNT] == 2
    assert args[_SAMPLE_COUNT] == 2
    assert args[_AVG] is None
    assert args[_MEDIAN] is None
    assert args[_MIN] is None
    assert args[_MAX] is None
    assert args[_LOW_RATIO] is None
    assert args[_STDDEV] is None
    # NOT-NULL columns still get a concrete value.
    assert args[_LOW_COUNT] == 0


async def test_skips_invalid_confidence_values() -> None:
    """Strings, out-of-range, and booleans are not counted as confidences; only
    the five valid floats survive into the sample count and stats."""
    conn = FakeMetricWriteConn()
    snapshot = {
        "objects": [
            {"confidence": 0.8},
            {"confidence": "0.5"},  # string -> rejected
            {"confidence": 1.7},  # > 1.0 -> rejected
            {"confidence": True},  # bool -> rejected
            {"confidence": 0.6},
            {"confidence": 0.7},
            {"confidence": 0.9},
            {"confidence": 0.55},
            {"track_id": "t-1"},  # no confidence key
        ]
    }
    result = await _write(conn, snapshot=snapshot)
    args = conn.inserts[0]
    # 9 object entries, but only 5 carry a usable confidence.
    assert args[_OBJECT_COUNT] == 9
    assert args[_SAMPLE_COUNT] == 5
    assert result.confidence_sample_count == 5


async def test_handles_missing_and_empty_objects() -> None:
    for snapshot in ({}, {"objects": []}, {"frame": {"width": 100}}):
        conn = FakeMetricWriteConn()
        result = await _write(conn, snapshot=snapshot)
        assert result.written is True
        assert result.object_count == 0
        assert result.confidence_sample_count == 0
        args = conn.inserts[0]
        assert args[_OBJECT_COUNT] == 0
        assert args[_SAMPLE_COUNT] == 0
        assert args[_AVG] is None


async def test_passes_through_tenant_and_provenance_columns() -> None:
    conn = FakeMetricWriteConn()
    account_id, camera_id = uuid4(), uuid4()
    session_id, snap_id = uuid4(), uuid4()
    await _write(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        stream_session_id=session_id,
        perception_snapshot_id=snap_id,
        processor_id="proc-x",
        package_id="pkg-y",
    )
    args = conn.inserts[0]
    assert args[_ACCOUNT_ID] == account_id
    assert args[_CAMERA_ID] == camera_id
    assert args[_SESSION_ID] == session_id
    assert args[_SNAPSHOT_ID] == snap_id
    assert args[_PROCESSOR_ID] == "proc-x"
    assert args[_PACKAGE_ID] == "pkg-y"


async def test_handles_null_session_and_snapshot_ids() -> None:
    conn = FakeMetricWriteConn()
    result = await _write(conn, stream_session_id=None, perception_snapshot_id=None)
    assert result.written is True
    args = conn.inserts[0]
    assert args[_SESSION_ID] is None
    assert args[_SNAPSHOT_ID] is None


async def test_does_not_store_raw_snapshot_json() -> None:
    """The metric row holds ONLY compact scalars — never the snapshot dict or
    its objects array."""
    conn = FakeMetricWriteConn()
    await _write(
        conn,
        snapshot={
            "objects": [{"confidence": c, "bbox": {"x": 1}} for c in [0.8, 0.7, 0.6, 0.9, 0.5]],
            "geometry": {"rois": [{"id": "secret-roi"}]},
        },
    )
    args = conn.inserts[0]
    # No arg is a container (dict/list) — so neither the snapshot nor objects
    # nor geometry can be persisted.
    for value in args:
        assert not isinstance(value, (dict, list)), value


async def test_idempotent_conflict_skip_reports_not_written() -> None:
    conn = FakeMetricWriteConn(insert_returns_row=False)
    result = await _write(conn)
    assert result.written is False
    # The compute still ran and the counts are reported.
    assert result.confidence_sample_count == 5


def test_insert_sql_pins_idempotency_target() -> None:
    sql = _INSERT_DETECTION_METRIC_SQL.lower()
    assert "insert into snapshot_detection_metrics" in sql
    assert "on conflict (camera_id, observed_at) do nothing" in sql
    assert "returning id" in sql
    # The raw snapshot/objects must never be a bound column (only the
    # perception_snapshot_id reference and numeric summaries are stored).
    assert "objects" not in sql
    assert "jsonb" not in sql


# ---------------------------------------------------------------------------
# Recent baseline query.
# ---------------------------------------------------------------------------


async def test_baseline_weighted_average() -> None:
    """Two windows worth of rows: (0.8 * 30) + (0.6 * 20) over 50 samples =
    0.72 weighted average."""
    conn = FakeBaselineConn(
        {"weighted_sum": 0.8 * 30 + 0.6 * 20, "total_samples": 50, "metric_count": 9}
    )
    now = datetime.now(tz=UTC)
    baseline = await fetch_detection_confidence_baseline(
        conn, account_id=uuid4(), camera_id=uuid4(), now=now
    )
    assert baseline is not None
    assert abs(baseline.previous_avg_confidence - 0.72) < 1e-9
    assert baseline.confidence_sample_count == 50
    assert baseline.metric_count == 9
    assert baseline.window_seconds == PREVIOUS_WINDOW_SECONDS


async def test_baseline_none_when_too_few_samples() -> None:
    conn = FakeBaselineConn(
        {
            "weighted_sum": 0.8 * (MIN_BASELINE_SAMPLE_COUNT - 1),
            "total_samples": MIN_BASELINE_SAMPLE_COUNT - 1,
            "metric_count": 2,
        }
    )
    baseline = await fetch_detection_confidence_baseline(
        conn, account_id=uuid4(), camera_id=uuid4(), now=datetime.now(tz=UTC)
    )
    assert baseline is None


async def test_baseline_none_when_no_rows_match() -> None:
    # Postgres returns one row of nulls when nothing matches the WHERE.
    conn = FakeBaselineConn({"weighted_sum": None, "total_samples": None, "metric_count": 0})
    baseline = await fetch_detection_confidence_baseline(
        conn, account_id=uuid4(), camera_id=uuid4(), now=datetime.now(tz=UTC)
    )
    assert baseline is None


async def test_baseline_none_when_row_missing() -> None:
    conn = FakeBaselineConn(None)
    baseline = await fetch_detection_confidence_baseline(
        conn, account_id=uuid4(), camera_id=uuid4(), now=datetime.now(tz=UTC)
    )
    assert baseline is None


async def test_baseline_window_bounds_exclude_recent_slice() -> None:
    """The query asks for [now - PREVIOUS_WINDOW, now - EXCLUSION) so the
    current snapshot is never part of its own baseline."""
    conn = FakeBaselineConn({"weighted_sum": 0.8 * 40, "total_samples": 40, "metric_count": 8})
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
    await fetch_detection_confidence_baseline(conn, account_id=uuid4(), camera_id=uuid4(), now=now)
    args = conn.calls[0]
    window_start, window_end = args[2], args[3]
    assert window_start == now - timedelta(seconds=PREVIOUS_WINDOW_SECONDS)
    assert window_end == now - timedelta(seconds=BASELINE_EXCLUSION_SECONDS)
    # Per-row minimum sample filter is bound too.
    assert isinstance(args[4], int)


def test_baseline_sql_pins_predicates() -> None:
    sql = _BASELINE_SQL.lower()
    assert "from snapshot_detection_metrics" in sql
    assert "avg_confidence is not null" in sql
    assert "observed_at >=" in sql
    assert "observed_at  <" in sql or "observed_at <" in sql
    assert "sum(avg_confidence * confidence_sample_count)" in sql


# ---------------------------------------------------------------------------
# Migration shape pin.
# ---------------------------------------------------------------------------


def test_migration_file_pins_table_shape() -> None:
    root = Path(__file__).resolve().parents[1]
    migration = root / "supabase" / "migrations" / "20260529140000_snapshot_detection_metrics.sql"
    sql = migration.read_text().lower()
    assert "create table snapshot_detection_metrics" in sql
    # Uniqueness is (camera_id, observed_at) — NOT perception_snapshot_id.
    assert "unique index snapshot_detection_metrics_camera_observed_idx" in sql
    assert "on snapshot_detection_metrics(camera_id, observed_at desc)" in sql
    # RLS + member-select + grant follow the dashboard-table convention.
    assert "enable row level security" in sql
    assert "snapshot_detection_metrics_member_select" in sql
    assert "grant select on public.snapshot_detection_metrics to authenticated" in sql
    # Append-only summary table — never store the raw snapshot.
    assert "jsonb" not in sql
