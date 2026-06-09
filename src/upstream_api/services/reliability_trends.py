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

Model-config M9 adds a SECOND, separate read: the by-config trend
(``_CONFIDENCE_TREND_BY_CONFIG_SQL`` + ``get_confidence_trend_by_config_for_camera``),
which keeps the M9 applied-processing-config dimension columns as GROUP BY keys
(one series per applied config) and supports optional dimension filters. The
camera-level query above is deliberately untouched: it keeps blending across
ALL rollup rows of an hour — packages and config dimensions alike — so the
existing dashboard chart contract is byte-identical with or without M9 data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.reliability import (
    ConfidenceTrendByConfigResponse,
    ConfidenceTrendConfigGroup,
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


# M9: the by-config variant. Same window/weighting posture as
# _CONFIDENCE_TREND_SQL, but the applied-processing-config dimension columns
# stay in GROUP BY (one output row per config group per hour) and are projected
# so the rows can be assembled into per-config series. package_id remains
# blended (a config group can span packages, re-weighted exactly like the
# camera-level query re-weights per-package rows).
#
# Optional dimension filters use the static null-or-match pattern (`$n is null
# or col = $n`) so the SQL stays one pinned constant — an unset filter binds
# NULL and the predicate collapses to true. The warning filter matches a code
# inside the text[] column; rows with NULL warnings (no metadata, or no
# warnings) never match it.
#
# Output rows = hours x config-groups, so the cap is point_cap x
# _MAX_CONFIG_GROUPS_PER_HOUR (a defensive ceiling, not an expected shape: more
# than a handful of distinct applied configs per camera per window is
# pathological). Ordered by bucket_start so each group's points come out
# oldest-first after the Python group-split.
#
# $1=account_id, $2=camera_id, $3=now, $4=lookback_hours, $5=row_cap,
# $6=processing_config_revision, $7=processing_config_status,
# $8=applied_selected_classes_key, $9=applied_confidence_threshold,
# $10=warning code.
_MAX_CONFIG_GROUPS_PER_HOUR = 8

_CONFIDENCE_TREND_BY_CONFIG_SQL = """
select
    processing_config_source,
    processing_config_revision,
    processing_config_status,
    applied_model_key,
    applied_selected_classes_key,
    applied_confidence_threshold,
    processing_config_warnings,
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
   and ($6::int is null or processing_config_revision = $6::int)
   and ($7::text is null or processing_config_status = $7::text)
   and ($8::text is null or applied_selected_classes_key = $8::text)
   and ($9::double precision is null or applied_confidence_threshold = $9::double precision)
   and ($10::text is null or $10::text = any(processing_config_warnings))
 group by processing_config_source, processing_config_revision, processing_config_status,
          applied_model_key, applied_selected_classes_key, applied_confidence_threshold,
          processing_config_warnings, bucket_start
 order by bucket_start asc
 limit $5
"""

# The dimension columns a by-config row carries, in group-identity order.
_CONFIG_DIMENSION_COLUMNS = (
    "processing_config_source",
    "processing_config_revision",
    "processing_config_status",
    "applied_model_key",
    "applied_selected_classes_key",
    "applied_confidence_threshold",
    "processing_config_warnings",
)

# The point columns (the SQL aliases == the API field names, as in the
# camera-level query).
_POINT_COLUMNS = (
    "bucket_start",
    "bucket_end",
    "avg_confidence",
    "min_confidence",
    "max_confidence",
    "low_confidence_ratio",
    "confidence_stddev",
    "metric_count",
    "object_count_total",
    "confidence_sample_count",
)


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


async def get_confidence_trend_by_config_for_camera(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    range_: ConfidenceTrendRange,
    now: datetime | None = None,
    processing_config_revision: int | None = None,
    processing_config_status: str | None = None,
    applied_selected_classes_key: str | None = None,
    applied_confidence_threshold: float | None = None,
    warning: str | None = None,
) -> ConfidenceTrendByConfigResponse:
    """Hourly confidence-trend points split per applied-config group (M9).

    Each filter kwarg is optional; ``None`` means "don't filter on this
    dimension" (the SQL's null-or-match predicates). Rows recorded before the
    processor stamped applied_processing_config carry all-``None`` dimensions
    and form their own group — so old and new history are both visible, never
    blended into each other.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    spec = _RANGES[range_]
    rows = await conn.fetch(
        _CONFIDENCE_TREND_BY_CONFIG_SQL,
        account_id,
        camera_id,
        now,
        spec.lookback_hours,
        spec.point_cap * _MAX_CONFIG_GROUPS_PER_HOUR,
        processing_config_revision,
        processing_config_status,
        applied_selected_classes_key,
        applied_confidence_threshold,
        warning,
    )
    groups = _split_rows_into_config_groups(rows)
    return ConfidenceTrendByConfigResponse(
        camera_id=camera_id,
        range=range_,
        bucket_size="1h",
        group_count=len(groups),
        groups=groups,
    )


def _split_rows_into_config_groups(
    rows: list[Any],
) -> list[ConfidenceTrendConfigGroup]:
    """Assemble by-config rows (ordered bucket_start asc) into one group per
    distinct dimension tuple, each with oldest-first points and its own
    summary. Pure — unit-testable without a DB.

    Groups come out in a deterministic order: ascending config revision with
    the no-revision groups (defaults / pre-M8 metadata) first, then by the
    remaining dimension values — so "r3 vs r4" comparisons read top-to-bottom.
    """
    grouped: dict[tuple[Any, ...], list[ConfidenceTrendPoint]] = {}
    dims_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        record = dict(row)
        dims = {column: record.get(column) for column in _CONFIG_DIMENSION_COLUMNS}
        # asyncpg returns text[] as a list — normalise to a tuple for the key.
        warnings = dims["processing_config_warnings"]
        key_warnings = tuple(warnings) if warnings is not None else None
        key = tuple(
            key_warnings if column == "processing_config_warnings" else dims[column]
            for column in _CONFIG_DIMENSION_COLUMNS
        )
        point = ConfidenceTrendPoint(**{column: record[column] for column in _POINT_COLUMNS})
        grouped.setdefault(key, []).append(point)
        dims_by_key.setdefault(key, dims)

    def _sort_key(key: tuple[Any, ...]) -> tuple[Any, ...]:
        revision = key[1]
        return (
            revision is not None,  # no-revision groups (defaults / pre-M8) first
            revision if revision is not None else -1,
            tuple("" if part is None else str(part) for part in key),
        )

    groups: list[ConfidenceTrendConfigGroup] = []
    for key in sorted(grouped, key=_sort_key):
        dims = dims_by_key[key]
        points = grouped[key]
        warnings = dims["processing_config_warnings"]
        groups.append(
            ConfidenceTrendConfigGroup(
                processing_config_source=dims["processing_config_source"],
                processing_config_revision=dims["processing_config_revision"],
                processing_config_status=dims["processing_config_status"],
                applied_model_key=dims["applied_model_key"],
                applied_selected_classes_key=dims["applied_selected_classes_key"],
                applied_confidence_threshold=dims["applied_confidence_threshold"],
                processing_config_warnings=list(warnings) if warnings is not None else None,
                points=points,
                summary=summarize_confidence_trend(points),
            )
        )
    return groups


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
