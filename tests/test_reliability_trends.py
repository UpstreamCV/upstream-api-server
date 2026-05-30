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

from upstream_api.schemas.reliability import ConfidenceTrendPoint
from upstream_api.services.reliability_trends import (
    _CONFIDENCE_TREND_SQL,
    _RANGES,
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
