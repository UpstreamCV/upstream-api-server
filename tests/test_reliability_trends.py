"""Confidence-trend read query + summary (reliability M7).

Two layers, mirroring tests/test_detection_metrics_rollup.py:
- the aggregation SQL is pinned by lowercase whitespace-normalised substring, so
  the per-package weighted re-aggregation (which must mirror M6) and the DB->API
  name mapping cannot drift,
- ``summarize_confidence_trend`` is exercised numerically against built points.
The full route (auth/scoping/422/JSON) is covered in test_reliability_endpoints.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from upstream_api.schemas.reliability import ConfidenceTrendPoint
from upstream_api.services.reliability_trends import (
    _CONFIDENCE_TREND_BY_CONFIG_SQL,
    _CONFIDENCE_TREND_SQL,
    _MAX_CONFIG_GROUPS_PER_HOUR,
    _RANGES,
    _split_rows_into_config_groups,
    summarize_confidence_trend,
)


def _norm(sql: str) -> str:
    return " ".join(sql.lower().split())


# ---------------------------------------------------------------------------
# Range config.
# ---------------------------------------------------------------------------


def test_ranges_are_closed_and_capped() -> None:
    assert set(_RANGES) == {"24h", "7d"}
    assert _RANGES["24h"].lookback_hours == 24
    assert _RANGES["24h"].point_cap == 24
    assert _RANGES["7d"].lookback_hours == 24 * 7 == 168
    assert _RANGES["7d"].point_cap == 168


# ---------------------------------------------------------------------------
# SQL pins.
# ---------------------------------------------------------------------------


def test_trend_sql_reads_rollups_scoped_and_ordered() -> None:
    sql = _norm(_CONFIDENCE_TREND_SQL)
    assert "from detection_confidence_hourly_rollups" in sql
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
    assert "group by bucket_start" in sql
    assert "order by bucket_start asc" in sql
    assert "limit $5" in sql
    # Window is hour-aligned on BOTH edges: lower via make_interval, upper
    # excludes the current incomplete hour so the count is exactly lookback_hours
    # and the limit can never drop the newest point (self-bounding).
    assert (
        "bucket_start >= date_trunc('hour', $3::timestamptz) "
        "- make_interval(hours => $4::int)" in sql
    )
    assert "bucket_start < date_trunc('hour', $3::timestamptz)" in sql
    assert "bucket_start + interval '1 hour' as bucket_end" in sql


def test_trend_sql_weighting_mirrors_m6_per_field() -> None:
    """Each weighted re-aggregate FILTERs numerator AND denominator on its OWN
    source field, with a nullif divisor — byte-for-byte the M6 posture."""
    sql = _norm(_CONFIDENCE_TREND_SQL)
    # avg_confidence
    assert (
        "sum(avg_confidence_weighted * confidence_sample_count) "
        "filter (where avg_confidence_weighted is not null)" in sql
    )
    assert (
        "nullif(sum(confidence_sample_count) filter (where avg_confidence_weighted is not null), 0)"
        in sql
    )
    assert "as avg_confidence" in sql
    # low_confidence_ratio
    assert (
        "sum(avg_low_confidence_ratio_weighted * confidence_sample_count) "
        "filter (where avg_low_confidence_ratio_weighted is not null)" in sql
    )
    assert (
        "nullif(sum(confidence_sample_count) "
        "filter (where avg_low_confidence_ratio_weighted is not null), 0)" in sql
    )
    assert "as low_confidence_ratio" in sql
    # confidence_stddev
    assert (
        "sum(avg_confidence_stddev_weighted * confidence_sample_count) "
        "filter (where avg_confidence_stddev_weighted is not null)" in sql
    )
    assert (
        "nullif(sum(confidence_sample_count) "
        "filter (where avg_confidence_stddev_weighted is not null), 0)" in sql
    )
    assert "as confidence_stddev" in sql
    # min/max + plain-sum volumes.
    assert "min(min_confidence) as min_confidence" in sql
    assert "max(max_confidence) as max_confidence" in sql
    assert "sum(metric_count) as metric_count" in sql
    assert "sum(object_count_total) as object_count_total" in sql
    assert "sum(confidence_sample_count) as confidence_sample_count" in sql


def test_trend_sql_does_not_split_or_leak_internal_columns() -> None:
    sql = _norm(_CONFIDENCE_TREND_SQL)
    # Aggregated across packages -> package_id is neither selected nor grouped.
    assert "package_id" not in sql
    # No tenant/internal columns projected (account_id appears only in the WHERE).
    assert "stream_session_id" not in sql
    assert "source" not in sql
    # M9 must NOT touch the camera-level query: the dimension columns belong to
    # the by-config query only, so the default endpoint keeps blending across
    # config groups exactly as it blends across packages.
    assert "processing_config" not in sql
    assert "applied_" not in sql


# ---------------------------------------------------------------------------
# M9: by-config SQL pins.
# ---------------------------------------------------------------------------


def test_by_config_sql_groups_and_projects_dimension_columns() -> None:
    sql = _norm(_CONFIDENCE_TREND_BY_CONFIG_SQL)
    assert "from detection_confidence_hourly_rollups" in sql
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
    # Same hour-aligned half-open window as the camera-level query.
    assert (
        "bucket_start >= date_trunc('hour', $3::timestamptz) "
        "- make_interval(hours => $4::int)" in sql
    )
    assert "bucket_start < date_trunc('hour', $3::timestamptz)" in sql
    assert "order by bucket_start asc" in sql
    assert "limit $5" in sql
    # All seven dimension columns are projected AND grouped — never blended.
    for col in (
        "processing_config_source",
        "processing_config_revision",
        "processing_config_status",
        "applied_model_key",
        "applied_selected_classes_key",
        "applied_confidence_threshold",
        "requested_processing_fps",
        "applied_processing_fps",
        "processing_config_warnings",
    ):
        assert col in sql
    assert (
        "group by processing_config_source, processing_config_revision, "
        "processing_config_status, applied_model_key, applied_selected_classes_key, "
        "applied_confidence_threshold, requested_processing_fps, applied_processing_fps, "
        "processing_config_warnings, bucket_start" in sql
    )
    # package_id stays blended (a config group can span packages) and tenant
    # columns are never projected.
    assert "package_id" not in sql
    assert "stream_session_id" not in sql


def test_by_config_sql_pins_null_or_match_filters() -> None:
    sql = _norm(_CONFIDENCE_TREND_BY_CONFIG_SQL)
    assert "and ($6::int is null or processing_config_revision = $6::int)" in sql
    assert "and ($7::text is null or processing_config_status = $7::text)" in sql
    assert "and ($8::text is null or applied_selected_classes_key = $8::text)" in sql
    assert (
        "and ($9::double precision is null "
        "or applied_confidence_threshold = $9::double precision)" in sql
    )
    assert "and ($10::text is null or $10::text = any(processing_config_warnings))" in sql
    assert "and ($11::int is null or applied_processing_fps = $11::int)" in sql


def test_by_config_sql_weighting_mirrors_camera_level_query() -> None:
    """The by-config query re-weights per-package rows within each config group
    with the same per-field FILTER posture as the camera-level query."""
    sql = _norm(_CONFIDENCE_TREND_BY_CONFIG_SQL)
    assert (
        "sum(avg_confidence_weighted * confidence_sample_count) "
        "filter (where avg_confidence_weighted is not null)" in sql
    )
    assert (
        "nullif(sum(confidence_sample_count) filter (where avg_confidence_weighted is not null), 0)"
        in sql
    )
    assert "as avg_confidence" in sql
    assert "as low_confidence_ratio" in sql
    assert "as confidence_stddev" in sql
    assert "min(min_confidence) as min_confidence" in sql
    assert "max(max_confidence) as max_confidence" in sql
    assert "sum(metric_count) as metric_count" in sql
    assert "sum(confidence_sample_count) as confidence_sample_count" in sql


def test_by_config_row_cap_scales_the_point_cap() -> None:
    """Rows = hours x config groups, so the cap must exceed the plain point cap
    by the defensive per-hour group ceiling."""
    assert _MAX_CONFIG_GROUPS_PER_HOUR >= 2
    for spec in _RANGES.values():
        assert spec.point_cap * _MAX_CONFIG_GROUPS_PER_HOUR > spec.point_cap


# ---------------------------------------------------------------------------
# M9: _split_rows_into_config_groups (pure).
# ---------------------------------------------------------------------------

_T0_SPLIT = datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC)


def _by_config_row(
    hour: int,
    *,
    avg: float | None,
    samples: int,
    revision: int | None = None,
    source: str | None = None,
    status: str | None = None,
    model_key: str | None = None,
    classes_key: str | None = None,
    threshold: float | None = None,
    requested_fps: int | None = None,
    applied_fps: int | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    start = _T0_SPLIT + timedelta(hours=hour)
    return {
        "processing_config_source": source,
        "processing_config_revision": revision,
        "processing_config_status": status,
        "applied_model_key": model_key,
        "applied_selected_classes_key": classes_key,
        "applied_confidence_threshold": threshold,
        "requested_processing_fps": requested_fps,
        "applied_processing_fps": applied_fps,
        "processing_config_warnings": warnings,
        "bucket_start": start,
        "bucket_end": start + timedelta(hours=1),
        "avg_confidence": avg,
        "min_confidence": None,
        "max_confidence": None,
        "low_confidence_ratio": None,
        "confidence_stddev": None,
        "metric_count": samples,
        "object_count_total": samples,
        "confidence_sample_count": samples,
    }


def test_split_separates_config_groups_and_keeps_point_order() -> None:
    rows = [
        _by_config_row(
            0, avg=0.5, samples=10, revision=3, source="operator_saved", status="applied"
        ),
        _by_config_row(
            0,
            avg=0.9,
            samples=20,
            revision=4,
            source="operator_saved",
            status="applied",
            threshold=0.2,
        ),
        _by_config_row(
            1, avg=0.6, samples=10, revision=3, source="operator_saved", status="applied"
        ),
    ]
    groups = _split_rows_into_config_groups(rows)
    assert len(groups) == 2
    r3 = next(g for g in groups if g.processing_config_revision == 3)
    r4 = next(g for g in groups if g.processing_config_revision == 4)
    assert [p.avg_confidence for p in r3.points] == [0.5, 0.6]
    assert r3.points[0].bucket_start < r3.points[1].bucket_start
    assert r3.summary.point_count == 2
    assert r4.applied_confidence_threshold == 0.2
    assert [p.avg_confidence for p in r4.points] == [0.9]
    assert r4.summary.latest_avg_confidence == 0.9
    # Deterministic ordering: ascending revision.
    assert [g.processing_config_revision for g in groups] == [3, 4]


def test_split_null_dimension_rows_form_their_own_group_first() -> None:
    """Pre-M8 history (all-None dimensions) is its own visible group, ordered
    before revisioned groups — never blended into them, never dropped."""
    rows = [
        _by_config_row(0, avg=0.7, samples=10),  # all-None dims (old data)
        _by_config_row(0, avg=0.9, samples=10, revision=1, source="operator_saved"),
    ]
    groups = _split_rows_into_config_groups(rows)
    assert len(groups) == 2
    assert groups[0].processing_config_revision is None
    assert groups[0].processing_config_source is None
    assert groups[0].processing_config_warnings is None
    assert groups[1].processing_config_revision == 1


def test_split_warnings_identity_distinguishes_groups() -> None:
    """Same scalar dims but different warning sets are different groups (the
    rollup table keys on warnings too), and the warnings list round-trips."""
    code = "selected_classes_analytics_counts_not_filtered"
    rows = [
        _by_config_row(0, avg=0.7, samples=10, revision=2, classes_key="person", warnings=[code]),
        _by_config_row(0, avg=0.8, samples=10, revision=2, classes_key="person", warnings=None),
    ]
    groups = _split_rows_into_config_groups(rows)
    assert len(groups) == 2
    warned = next(g for g in groups if g.processing_config_warnings is not None)
    assert warned.processing_config_warnings == [code]


def test_split_empty_rows_yield_no_groups() -> None:
    assert _split_rows_into_config_groups([]) == []


# ---------------------------------------------------------------------------
# summarize_confidence_trend (pure).
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 5, 30, 0, 0, 0, tzinfo=UTC)


def _pt(hour: int, *, avg: float | None, samples: int) -> ConfidenceTrendPoint:
    start = _T0 + timedelta(hours=hour)
    return ConfidenceTrendPoint(
        bucket_start=start,
        bucket_end=start + timedelta(hours=1),
        avg_confidence=avg,
        min_confidence=None,
        max_confidence=None,
        low_confidence_ratio=None,
        confidence_stddev=None,
        metric_count=samples,
        object_count_total=samples,
        confidence_sample_count=samples,
    )


def test_summary_empty() -> None:
    s = summarize_confidence_trend([])
    assert s.point_count == 0
    assert s.latest_avg_confidence is None
    assert s.lowest_avg_confidence is None
    assert s.highest_avg_confidence is None
    assert s.total_confidence_samples == 0
    assert s.latest_bucket_start is None


def test_summary_all_null_avg_keeps_time_and_samples() -> None:
    pts = [_pt(0, avg=None, samples=10), _pt(1, avg=None, samples=5)]
    s = summarize_confidence_trend(pts)
    assert s.point_count == 2
    assert s.latest_avg_confidence is None
    assert s.lowest_avg_confidence is None
    assert s.highest_avg_confidence is None
    assert s.total_confidence_samples == 15  # empty hours still counted
    assert s.latest_bucket_start == _T0 + timedelta(hours=1)


def test_summary_latest_skips_trailing_empty_hour() -> None:
    pts = [
        _pt(0, avg=0.8, samples=100),
        _pt(1, avg=0.6, samples=80),
        _pt(2, avg=None, samples=20),  # trailing empty
    ]
    s = summarize_confidence_trend(pts)
    assert s.point_count == 3
    assert s.latest_avg_confidence == 0.6  # last RATED point, not the null one
    assert s.lowest_avg_confidence == 0.6
    assert s.highest_avg_confidence == 0.8
    assert s.total_confidence_samples == 200
    assert s.latest_bucket_start == _T0 + timedelta(hours=2)  # last point regardless


def test_summary_single_rated_point() -> None:
    s = summarize_confidence_trend([_pt(0, avg=0.73, samples=42)])
    assert s.point_count == 1
    assert s.latest_avg_confidence == 0.73
    assert s.lowest_avg_confidence == 0.73
    assert s.highest_avg_confidence == 0.73
    assert s.total_confidence_samples == 42


def test_split_fps_identity_distinguishes_groups() -> None:
    """M9.1: same scalar dims but different applied FPS are different groups,
    and both FPS fields round-trip into the group model."""
    rows = [
        _by_config_row(0, avg=0.7, samples=10, revision=5, requested_fps=5, applied_fps=5),
        _by_config_row(0, avg=0.9, samples=10, revision=6, requested_fps=1, applied_fps=1),
        _by_config_row(0, avg=0.5, samples=10),  # default-FPS (null) history
    ]
    groups = _split_rows_into_config_groups(rows)
    assert len(groups) == 3
    by_fps = {g.applied_processing_fps: g for g in groups}
    assert set(by_fps) == {5, 1, None}
    assert by_fps[5].requested_processing_fps == 5
    assert by_fps[None].requested_processing_fps is None
