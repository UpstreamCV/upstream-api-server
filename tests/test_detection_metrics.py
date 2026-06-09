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
    MIN_BASELINE_SAMPLE_COUNT,
    PREVIOUS_WINDOW_SECONDS,
    RECENT_WINDOW_SECONDS,
)
from upstream_api.services.detection_metrics import (
    _INSERT_DETECTION_METRIC_SQL,
    _WINDOW_SQL,
    fetch_detection_confidence_windows,
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


class FakeWindowConn:
    """Serves the rolling-window query. The current and baseline windows use
    the same SQL with different [start, end) bounds, so we route by span:
    the current window spans RECENT_WINDOW_SECONDS, the baseline window spans
    (PREVIOUS_WINDOW_SECONDS - RECENT_WINDOW_SECONDS). Records every call so
    the bounds can be asserted."""

    _CURRENT_SPAN = RECENT_WINDOW_SECONDS
    _BASELINE_SPAN = PREVIOUS_WINDOW_SECONDS - RECENT_WINDOW_SECONDS

    def __init__(
        self,
        *,
        current: dict[str, Any] | None = None,
        baseline: dict[str, Any] | None = None,
    ) -> None:
        self.current = current
        self.baseline = baseline
        self.calls: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from snapshot_detection_metrics" in query:
            self.calls.append(args)
            span = round((args[3] - args[2]).total_seconds())
            if span == self._CURRENT_SPAN:
                return self.current
            if span == self._BASELINE_SPAN:
                return self.baseline
            raise AssertionError(f"unexpected window span {span}")
        return None


def _window_row(*, avg: float, sample_count: int, metric_count: int = 6) -> dict[str, Any]:
    """An aggregate window row whose weighted average equals ``avg``."""
    return {
        "weighted_sum": avg * sample_count,
        "total_samples": sample_count,
        "metric_count": metric_count,
    }


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


# Column order of _INSERT_DETECTION_METRIC_SQL ($1..$25).
_ACCOUNT_ID, _CAMERA_ID, _SESSION_ID, _SNAPSHOT_ID = 0, 1, 2, 3
_PROCESSOR_ID, _PACKAGE_ID, _OBSERVED_AT = 4, 5, 6
_OBJECT_COUNT, _SAMPLE_COUNT = 7, 8
_AVG, _MEDIAN, _MIN, _MAX = 9, 10, 11, 12
_LOW_COUNT, _LOW_RATIO, _STDDEV = 13, 14, 15
_SOURCE = 16
# M9 applied-processing-config dimensions.
_PC_SOURCE, _PC_REVISION, _PC_STATUS = 17, 18, 19
_PC_MODEL_KEY, _PC_CLASSES_KEY, _PC_THRESHOLD = 20, 21, 22
_PC_WARNINGS, _PC_CLASSES_WARNING_FLAG = 23, 24


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
    # No source.applied_processing_config on this snapshot (pre-M8 shape) ->
    # every M9 dimension is NULL, the backward-compatible "no metadata" row.
    for index in (
        _PC_SOURCE,
        _PC_REVISION,
        _PC_STATUS,
        _PC_MODEL_KEY,
        _PC_CLASSES_KEY,
        _PC_THRESHOLD,
        _PC_WARNINGS,
        _PC_CLASSES_WARNING_FLAG,
    ):
        assert args[index] is None


async def test_writes_stats_for_low_sample_count() -> None:
    """M5 fix: the writer summarises a row from as few as one confidence (the
    per-frame 5-gate is the static rule's, not the history's) so quiet 1-3
    object scenes still accumulate usable history for the rolling windows."""
    conn = FakeMetricWriteConn()
    result = await _write(conn, snapshot={"objects": [{"confidence": 0.8}, {"confidence": 0.6}]})
    assert result.written is True
    assert result.confidence_sample_count == 2
    assert result.avg_confidence is not None
    assert abs(result.avg_confidence - 0.7) < 1e-9
    args = conn.inserts[0]
    assert args[_OBJECT_COUNT] == 2
    assert args[_SAMPLE_COUNT] == 2
    assert args[_AVG] is not None
    assert args[_MIN] == 0.6
    assert args[_MAX] == 0.8


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


# ---------------------------------------------------------------------------
# M9: applied-processing-config dimensions ride along with the metric row.
# ---------------------------------------------------------------------------


def _m8_source_block(**config_overrides: Any) -> dict[str, Any]:
    """A realistic M8 snapshot.source block with applied_processing_config."""
    config: dict[str, Any] = {
        "source": "operator_saved",
        "revision": 4,
        "status": "partially_applied",
        "reason": None,
        "requested": {
            "model_key": "trafficcamnet_v1",
            "selected_classes": ["person", "car"],
            "tracker": None,
            "confidence_threshold": 0.2,
            "processing_fps": None,
        },
        "applied": {
            "model_key": "trafficcamnet_v1",
            "selected_classes": ["person", "car"],
            "tracker": "iou",
            "confidence_threshold": 0.2,
            "processing_fps": None,
        },
        "warnings": ["selected_classes_analytics_counts_not_filtered"],
    }
    config.update(config_overrides)
    return {
        "runtime": "deepstream",
        "metadata_source": "pyds_pad_probe",
        "analytics_plugin": "nvdsanalytics",
        "tracker_algorithm": "iou",
        "applied_processing_config": config,
    }


async def test_writes_processing_config_dimensions_from_m8_snapshot() -> None:
    conn = FakeMetricWriteConn()
    snapshot = {
        "objects": [{"confidence": c} for c in [0.8, 0.7, 0.6, 0.9, 0.5]],
        "source": _m8_source_block(),
    }
    result = await _write(conn, snapshot=snapshot)
    assert result.written is True
    args = conn.inserts[0]
    assert args[_PC_SOURCE] == "operator_saved"
    assert args[_PC_REVISION] == 4
    assert args[_PC_STATUS] == "partially_applied"
    assert args[_PC_MODEL_KEY] == "trafficcamnet_v1"
    assert args[_PC_CLASSES_KEY] == "car,person"  # canonical sort
    assert args[_PC_THRESHOLD] == 0.2
    assert args[_PC_WARNINGS] == ["selected_classes_analytics_counts_not_filtered"]
    assert args[_PC_CLASSES_WARNING_FLAG] is True


async def test_dimensions_do_not_change_numeric_confidence_metrics() -> None:
    """The same objects with and without the M8 source block produce identical
    numeric metric args — the dimensions are additive, never behavioral."""
    objects = [{"confidence": c} for c in [0.8, 0.7, 0.6, 0.9, 0.5]]
    plain_conn = FakeMetricWriteConn()
    stamped_conn = FakeMetricWriteConn()
    shared = {"account_id": uuid4(), "camera_id": uuid4()}
    await _write(plain_conn, snapshot={"objects": objects}, **shared)
    await _write(
        stamped_conn, snapshot={"objects": objects, "source": _m8_source_block()}, **shared
    )
    plain_args = plain_conn.inserts[0]
    stamped_args = stamped_conn.inserts[0]
    for index in (
        _OBJECT_COUNT,
        _SAMPLE_COUNT,
        _AVG,
        _MEDIAN,
        _MIN,
        _MAX,
        _LOW_COUNT,
        _LOW_RATIO,
        _STDDEV,
        _SOURCE,
    ):
        assert plain_args[index] == stamped_args[index]


async def test_malformed_applied_processing_config_still_writes_row() -> None:
    """A garbage block must not crash the writer — the row writes with NULL
    dimensions (the savepoint in the router is the LAST line of defense, not
    the first)."""
    conn = FakeMetricWriteConn()
    snapshot = {
        "objects": [{"confidence": 0.8}],
        "source": {"applied_processing_config": ["not", "a", "mapping"]},
    }
    result = await _write(conn, snapshot=snapshot)
    assert result.written is True
    args = conn.inserts[0]
    assert args[_PC_SOURCE] is None
    assert args[_PC_WARNINGS] is None
    assert args[_PC_CLASSES_WARNING_FLAG] is None


async def test_does_not_store_raw_snapshot_json() -> None:
    """The metric row holds ONLY compact scalars — never the snapshot dict or
    its objects array. The single allowed container is the M9
    processing_config_warnings text[] (a list of short extracted code strings,
    never payload)."""
    conn = FakeMetricWriteConn()
    await _write(
        conn,
        snapshot={
            "objects": [{"confidence": c, "bbox": {"x": 1}} for c in [0.8, 0.7, 0.6, 0.9, 0.5]],
            "geometry": {"rois": [{"id": "secret-roi"}]},
            "source": _m8_source_block(),
        },
    )
    args = conn.inserts[0]
    # No arg is ever a dict, and the ONLY list is the warnings codes column —
    # so neither the snapshot nor objects nor geometry can be persisted.
    for index, value in enumerate(args):
        assert not isinstance(value, dict), value
        if isinstance(value, list):
            assert index == _PC_WARNINGS, value
            assert all(isinstance(entry, str) for entry in value), value


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
    # perception_snapshot_id reference, numeric summaries, and the compact M9
    # dimension columns are stored — warnings is text[], never jsonb).
    assert "objects" not in sql
    assert "jsonb" not in sql
    # M9 dimension columns are bound.
    for column in (
        "processing_config_source",
        "processing_config_revision",
        "processing_config_status",
        "applied_model_key",
        "applied_selected_classes_key",
        "applied_confidence_threshold",
        "processing_config_warnings",
        "selected_classes_analytics_counts_not_filtered",
    ):
        assert column in sql
    assert "$25" in sql


# ---------------------------------------------------------------------------
# Rolling-window query (drift).
# ---------------------------------------------------------------------------


async def test_windows_weighted_average() -> None:
    """Current window weighted: (0.8*30)+(0.6*20) over 50 = 0.72; baseline 0.90."""
    conn = FakeWindowConn(
        current=_window_row(avg=0.72, sample_count=50, metric_count=9),
        baseline=_window_row(avg=0.90, sample_count=120, metric_count=20),
    )
    now = datetime.now(tz=UTC)
    windows = await fetch_detection_confidence_windows(
        conn, account_id=uuid4(), camera_id=uuid4(), now=now
    )
    assert windows.current is not None
    assert abs(windows.current.avg_confidence - 0.72) < 1e-9
    assert windows.current.confidence_sample_count == 50
    assert windows.current.metric_count == 9
    assert windows.current.window_seconds == RECENT_WINDOW_SECONDS
    assert windows.baseline is not None
    assert abs(windows.baseline.avg_confidence - 0.90) < 1e-9
    assert windows.baseline.window_seconds == PREVIOUS_WINDOW_SECONDS


async def test_window_none_when_too_few_samples() -> None:
    """A window below MIN_BASELINE_SAMPLE_COUNT total samples is untrusted."""
    conn = FakeWindowConn(
        current=_window_row(avg=0.8, sample_count=MIN_BASELINE_SAMPLE_COUNT - 1),
        baseline=_window_row(avg=0.9, sample_count=200),
    )
    windows = await fetch_detection_confidence_windows(
        conn, account_id=uuid4(), camera_id=uuid4(), now=datetime.now(tz=UTC)
    )
    assert windows.current is None
    assert windows.baseline is not None


async def test_window_none_when_no_rows_match() -> None:
    # Postgres returns one row of nulls when nothing matches the WHERE.
    conn = FakeWindowConn(
        current={"weighted_sum": None, "total_samples": None, "metric_count": 0},
        baseline=None,
    )
    windows = await fetch_detection_confidence_windows(
        conn, account_id=uuid4(), camera_id=uuid4(), now=datetime.now(tz=UTC)
    )
    assert windows.current is None
    assert windows.baseline is None


async def test_window_bounds_are_adjacent_and_non_overlapping() -> None:
    """current = [now-RECENT, now); baseline = [now-PREVIOUS, now-RECENT). The
    current window's start equals the baseline window's end — adjacent, so the
    current data is never counted in its own baseline."""
    conn = FakeWindowConn(
        current=_window_row(avg=0.6, sample_count=40),
        baseline=_window_row(avg=0.9, sample_count=80),
    )
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
    await fetch_detection_confidence_windows(conn, account_id=uuid4(), camera_id=uuid4(), now=now)
    # Two calls: current then baseline.
    cur_start, cur_end = conn.calls[0][2], conn.calls[0][3]
    base_start, base_end = conn.calls[1][2], conn.calls[1][3]
    assert cur_end == now
    assert cur_start == now - timedelta(seconds=RECENT_WINDOW_SECONDS)
    assert base_end == now - timedelta(seconds=RECENT_WINDOW_SECONDS)
    assert base_start == now - timedelta(seconds=PREVIOUS_WINDOW_SECONDS)
    assert base_end == cur_start  # adjacent, non-overlapping


def test_window_sql_pins_predicates() -> None:
    sql = _WINDOW_SQL.lower()
    assert "from snapshot_detection_metrics" in sql
    assert "avg_confidence is not null" in sql
    assert "observed_at >=" in sql
    assert "observed_at <" in sql
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
