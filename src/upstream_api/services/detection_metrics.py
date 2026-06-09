"""Detection-confidence history: metric writer + recent-baseline query (M5).

This module turns each accepted perception snapshot into ONE compact numeric
summary row in ``snapshot_detection_metrics`` (the writer), and reads recent
history back as a weighted baseline so the reliability evaluator can detect a
camera whose model confidence is degrading versus its OWN recent behaviour
(the baseline query).

Two hard rules:

- It stores ONLY numeric summaries — never the raw snapshot / objects JSON.
  (M9 adds the compact applied-processing-config DIMENSIONS — scalar lineage
  columns plus a text[] of warning codes — which are extracted identifiers,
  never the snapshot/objects payload.)
- It must never break perception snapshot ingest. The writer raises on a DB
  error like any repo helper; the ingest router wraps the call in a savepoint
  + try/except so a failure (including the table not existing yet, before the
  M5 migration applies) is logged and swallowed without rolling back the
  snapshot upsert. Dimension extraction itself never raises — an old (pre-M8)
  or malformed snapshot yields all-NULL dimensions and the row still writes.

Confidence parsing and the summary statistics are reused wholesale from
``detection_confidence``; the M9 dimension extraction comes wholesale from
``processing_config_dimensions`` — this module re-implements neither.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.services.detection_confidence import (
    MIN_BASELINE_SAMPLE_COUNT,
    PREVIOUS_WINDOW_SECONDS,
    RECENT_WINDOW_SECONDS,
    compute_detection_confidence_stats,
    count_detection_objects,
    extract_detection_confidences,
)
from upstream_api.services.processing_config_dimensions import (
    extract_processing_config_dimensions,
)

# The metric writer summarises a row whenever a frame has at least one usable
# confidence (not the per-frame static rule's MIN_SAMPLE_COUNT of 5): drift
# aggregates these rows over a time window, so a quiet 1-3 object frame is
# still worth recording. Frames with zero confidences keep null stats.
_MIN_METRIC_SAMPLE_COUNT = 1

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

# One metric per camera per frame-time. ``on conflict (camera_id, observed_at)
# do nothing`` makes a processor retry of the same observed_at idempotent. We
# do NOT use perception_snapshot_id as the conflict key — it is per-camera
# stable (latest_perception_snapshots is UPSERTed on camera_id), not per-frame.
_INSERT_DETECTION_METRIC_SQL = """
insert into snapshot_detection_metrics (
    account_id, camera_id, stream_session_id, perception_snapshot_id,
    processor_id, package_id, observed_at,
    object_count, confidence_sample_count,
    avg_confidence, median_confidence, min_confidence, max_confidence,
    low_confidence_count, low_confidence_ratio, confidence_stddev,
    source,
    processing_config_source, processing_config_revision, processing_config_status,
    applied_model_key, applied_selected_classes_key, applied_confidence_threshold,
    processing_config_warnings, selected_classes_analytics_counts_not_filtered
)
values (
    $1, $2, $3, $4,
    $5, $6, $7,
    $8, $9,
    $10, $11, $12, $13,
    $14, $15, $16,
    $17,
    $18, $19, $20,
    $21, $22, $23,
    $24, $25
)
on conflict (camera_id, observed_at) do nothing
returning id
"""


@dataclass(frozen=True)
class DetectionMetricWriteResult:
    """Outcome of one metric write.

    ``written`` is ``False`` when the row already existed (idempotent conflict
    skip). ``avg_confidence`` is ``None`` when there were too few confidence
    samples to summarise — the row is still written to preserve history.
    """

    written: bool
    object_count: int
    confidence_sample_count: int
    avg_confidence: float | None


async def write_detection_metrics_for_perception_snapshot(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    stream_session_id: UUID | None,
    perception_snapshot_id: UUID | None,
    processor_id: str | None,
    package_id: str | None,
    observed_at: datetime,
    snapshot: Mapping[str, Any],
    source: str = "latest_perception_snapshot",
) -> DetectionMetricWriteResult:
    """Compute and persist a compact confidence summary for one snapshot.

    Always writes a row (even with zero detections) so the history records that
    the camera had little to detect — the window query ignores rows whose stats
    are null. Stats are summarised whenever there is at least one usable
    confidence value (``_MIN_METRIC_SAMPLE_COUNT``); a frame with zero
    confidences carries counts but null stats.

    M9: the row also carries the applied-processing-config dimensions extracted
    from ``snapshot.source.applied_processing_config`` (all NULL for pre-M8 or
    malformed snapshots — extraction never raises, and the numeric confidence
    metrics are computed exactly as before, untouched by the dimensions).
    """
    values = extract_detection_confidences(snapshot)
    object_count = count_detection_objects(snapshot)
    confidence_sample_count = len(values)
    stats = compute_detection_confidence_stats(values, min_sample_count=_MIN_METRIC_SAMPLE_COUNT)
    dimensions = extract_processing_config_dimensions(snapshot)

    if stats is None:
        avg = median = min_c = max_c = low_ratio = stddev = None
        low_count = 0
    else:
        avg = stats.avg_confidence
        median = stats.median_confidence
        min_c = stats.min_confidence
        max_c = stats.max_confidence
        low_ratio = stats.low_confidence_ratio
        stddev = stats.confidence_stddev
        low_count = stats.low_confidence_count

    row = await conn.fetchrow(
        _INSERT_DETECTION_METRIC_SQL,
        account_id,
        camera_id,
        stream_session_id,
        perception_snapshot_id,
        processor_id,
        package_id,
        observed_at,
        object_count,
        confidence_sample_count,
        avg,
        median,
        min_c,
        max_c,
        low_count,
        low_ratio,
        stddev,
        source,
        dimensions.processing_config_source,
        dimensions.processing_config_revision,
        dimensions.processing_config_status,
        dimensions.applied_model_key,
        dimensions.applied_selected_classes_key,
        dimensions.applied_confidence_threshold,
        dimensions.warnings_list(),
        dimensions.selected_classes_analytics_counts_not_filtered,
    )
    return DetectionMetricWriteResult(
        written=row is not None,
        object_count=object_count,
        confidence_sample_count=confidence_sample_count,
        avg_confidence=avg,
    )


# ---------------------------------------------------------------------------
# Rolling-window confidence query (drift)
# ---------------------------------------------------------------------------

# Weighted average confidence over a time window, weighting each metric row by
# its confidence_sample_count. Rows with null avg_confidence (no usable
# confidences that frame) are excluded; everything else aggregates, so a quiet
# 1-3 object/frame scene still contributes plenty of samples over the window.
_WINDOW_SQL = """
select
    sum(avg_confidence * confidence_sample_count) as weighted_sum,
    sum(confidence_sample_count)                  as total_samples,
    count(*)                                      as metric_count
  from snapshot_detection_metrics
 where account_id = $1
   and camera_id  = $2
   and observed_at >= $3
   and observed_at <  $4
   and avg_confidence is not null
"""


@dataclass(frozen=True)
class DetectionConfidenceWindow:
    """Weighted confidence average over one time window."""

    avg_confidence: float
    confidence_sample_count: int
    metric_count: int
    window_seconds: int


@dataclass(frozen=True)
class DetectionConfidenceWindows:
    """The current and prior windows used for drift comparison. Either may be
    ``None`` when that window holds too little history to trust."""

    current: DetectionConfidenceWindow | None
    baseline: DetectionConfidenceWindow | None


async def _fetch_window(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    start: datetime,
    end: datetime,
    window_seconds: int,
) -> DetectionConfidenceWindow | None:
    """Weighted average over [start, end), or ``None`` when the window holds
    fewer than ``MIN_BASELINE_SAMPLE_COUNT`` total confidence samples (too
    little to trust) — the "no signal" marker the evaluator skips on."""
    row = await conn.fetchrow(_WINDOW_SQL, account_id, camera_id, start, end)
    if row is None:
        return None
    total_samples = row["total_samples"]
    weighted_sum = row["weighted_sum"]
    if total_samples is None or weighted_sum is None:
        return None
    if total_samples < MIN_BASELINE_SAMPLE_COUNT:
        return None
    avg = weighted_sum / total_samples
    if avg <= 0:
        return None
    return DetectionConfidenceWindow(
        avg_confidence=avg,
        confidence_sample_count=int(total_samples),
        metric_count=int(row["metric_count"]),
        window_seconds=window_seconds,
    )


async def fetch_detection_confidence_windows(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    now: datetime,
) -> DetectionConfidenceWindows:
    """The current vs. prior confidence windows for drift detection.

    - current  = [now - RECENT_WINDOW_SECONDS, now)            (the last 10 min)
    - baseline = [now - PREVIOUS_WINDOW_SECONDS, now - RECENT_WINDOW_SECONDS)
      (10-30 min ago)

    The windows are adjacent and non-overlapping, so the current window is never
    compared against itself. Each is ``None`` when it lacks enough samples — a
    missing window must never force-open a finding or block resolution.
    """
    recent = timedelta(seconds=RECENT_WINDOW_SECONDS)
    current = await _fetch_window(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        start=now - recent,
        end=now,
        window_seconds=RECENT_WINDOW_SECONDS,
    )
    baseline = await _fetch_window(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        start=now - timedelta(seconds=PREVIOUS_WINDOW_SECONDS),
        end=now - recent,
        window_seconds=PREVIOUS_WINDOW_SECONDS,
    )
    return DetectionConfidenceWindows(current=current, baseline=baseline)
