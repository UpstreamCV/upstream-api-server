"""Confidence trend read query for the camera Reliability tab (reliability M7).

Serves hourly detection-confidence history from the M6
``detection_confidence_hourly_rollups`` table for a single camera, aggregated
across packages to ONE camera-level point per hour. This is the read side of the
M6 rollups — it does NOT touch raw ``snapshot_detection_metrics`` and does NOT
change the M5 drift evaluator (which still reads recent raw metrics).

Two pieces, mirroring the style of ``detection_metrics_rollup.py``:

- ``_CONFIDENCE_TREND_SQL`` — one set-based query that GROUPs the per-package
  rollup rows of each hour into a single point, re-weighting the already-weighted
  per-package means by ``confidence_sample_count`` exactly as M6 does. Because
  ``Σ(meanₚ·nₚ)/Σnₚ`` over packages equals the true camera-level weighted mean,
  this recovers the camera-level number with no information loss.
- ``summarize_confidence_trend`` — a pure function that rolls the points up into
  the headline ``ConfidenceTrendSummary`` (unit-testable without a DB).

Internal column names are renamed to clean public names in the SQL aliases
(``avg_confidence_weighted`` -> ``avg_confidence`` etc.), and no tenant/internal
columns (account_id, stream_session_id, package_id, source) are projected.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from upstream_api.schemas.reliability import (
    ConfidenceTrendPoint,
    ConfidenceTrendRange,
    ConfidenceTrendResponse,
    ConfidenceTrendSummary,
)


@dataclass(frozen=True)
class _RangeSpec:
    """How far back to look, and a defensive ceiling on returned points. The
    SQL bounds the window to exactly ``lookback_hours`` whole, completed hours
    (``[now-lookback_hours, now)`` hour-aligned), so ``point_cap`` (==
    ``lookback_hours``) can never truncate an in-window bucket — it's a pure
    safety ceiling against a pathological table, never a data-dropping limit."""

    lookback_hours: int
    point_cap: int


# Closed for M7 (the router's Literal mirrors these keys). 30d / daily buckets
# are a future milestone — keep the map and the schema Literal in sync.
_RANGES: dict[str, _RangeSpec] = {
    "24h": _RangeSpec(lookback_hours=24, point_cap=24),
    "7d": _RangeSpec(lookback_hours=24 * 7, point_cap=168),
}


# Aggregate the per-package rollup rows of each hour into one camera-level point.
# Each weighted re-aggregate FILTERs numerator AND denominator on its own source
# field (so a package row missing that field is excluded from both), with a
# nullif() divisor that yields NULL — not a divide-by-zero — when no package row
# in the hour carried it. Volumes (metric/object/sample counts) are plain sums;
# min/max ignore NULLs natively. This is byte-for-byte the M6 weighting posture.
#
# bucket_start is already hour-aligned by M6, so GROUP BY bucket_start is exact.
# The window is half-open and HOUR-ALIGNED on BOTH edges:
# [date_trunc('hour', now) - lookback_hours, date_trunc('hour', now)) — i.e. the
# last `lookback_hours` WHOLE, completed hours, excluding the current incomplete
# hour (mirroring the M6 writer's own upper edge). That makes the in-window
# bucket count exactly `lookback_hours`, so the `limit $5` ceiling (== lookback)
# is a pure safety guard and can never drop the newest point — the read is
# self-bounding and does not depend on the M6 writer's current-hour exclusion.
# $1=account_id, $2=camera_id, $3=now, $4=lookback_hours, $5=point_cap.
_CONFIDENCE_TREND_SQL = """
select
    bucket_start,
    bucket_start + interval '1 hour' as bucket_end,
    sum(avg_confidence_weighted * confidence_sample_count)
      filter (where avg_confidence_weighted is not null)
    / nullif(sum(confidence_sample_count)
             filter (where avg_confidence_weighted is not null), 0) as avg_confidence,
    min(min_confidence) as min_confidence,
    max(max_confidence) as max_confidence,
    sum(avg_low_confidence_ratio_weighted * confidence_sample_count)
      filter (where avg_low_confidence_ratio_weighted is not null)
    / nullif(sum(confidence_sample_count)
             filter (where avg_low_confidence_ratio_weighted is not null), 0)
      as low_confidence_ratio,
    sum(avg_confidence_stddev_weighted * confidence_sample_count)
      filter (where avg_confidence_stddev_weighted is not null)
    / nullif(sum(confidence_sample_count)
             filter (where avg_confidence_stddev_weighted is not null), 0) as confidence_stddev,
    sum(metric_count) as metric_count,
    sum(object_count_total) as object_count_total,
    sum(confidence_sample_count) as confidence_sample_count
  from detection_confidence_hourly_rollups
 where account_id = $1
   and camera_id = $2
   and bucket_start >= date_trunc('hour', $3::timestamptz) - make_interval(hours => $4::int)
   and bucket_start <  date_trunc('hour', $3::timestamptz)
 group by bucket_start
 order by bucket_start asc
 limit $5
"""


async def get_confidence_trend_for_camera(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    range_: ConfidenceTrendRange,
    now: datetime | None = None,
) -> ConfidenceTrendResponse:
    """Hourly confidence-trend points (oldest-first) + a headline summary.

    ``range_`` is one of ``_RANGES`` (the router's ``Literal`` guarantees it).
    The row keys already match the API field names via the SQL aliases, so each
    point is built straight from the row.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    spec = _RANGES[range_]
    rows = await conn.fetch(
        _CONFIDENCE_TREND_SQL,
        account_id,
        camera_id,
        now,
        spec.lookback_hours,
        spec.point_cap,
    )
    points = [ConfidenceTrendPoint(**dict(row)) for row in rows]
    return ConfidenceTrendResponse(
        camera_id=camera_id,
        range=range_,
        bucket_size="1h",
        points=points,
        summary=summarize_confidence_trend(points),
    )


def summarize_confidence_trend(points: list[ConfidenceTrendPoint]) -> ConfidenceTrendSummary:
    """Roll points (assumed oldest-first, as the SQL returns them) into headline
    stats. ``latest_*`` use the most recent point that actually has a confidence
    value, so a trailing empty hour never blanks the headline; ``latest_bucket_start``
    is the most recent point regardless (a time is always meaningful); sample
    total counts every point (empty hours contribute 0)."""
    rated = [p for p in points if p.avg_confidence is not None]
    rated_avgs = [p.avg_confidence for p in rated if p.avg_confidence is not None]
    return ConfidenceTrendSummary(
        point_count=len(points),
        latest_avg_confidence=rated[-1].avg_confidence if rated else None,
        lowest_avg_confidence=min(rated_avgs) if rated_avgs else None,
        highest_avg_confidence=max(rated_avgs) if rated_avgs else None,
        total_confidence_samples=sum(p.confidence_sample_count for p in points),
        latest_bucket_start=points[-1].bucket_start if points else None,
    )
