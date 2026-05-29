"""Detection-confidence history: metric writer + recent-baseline query (M5).

This module turns each accepted perception snapshot into ONE compact numeric
summary row in ``snapshot_detection_metrics`` (the writer), and reads recent
history back as a weighted baseline so the reliability evaluator can detect a
camera whose model confidence is degrading versus its OWN recent behaviour
(the baseline query).

Two hard rules:

- It stores ONLY numeric summaries — never the raw snapshot / objects JSON.
- It must never break perception snapshot ingest. The writer raises on a DB
  error like any repo helper; the ingest router wraps the call in a savepoint
  + try/except so a failure (including the table not existing yet, before the
  M5 migration applies) is logged and swallowed without rolling back the
  snapshot upsert.

Confidence parsing and the summary statistics are reused wholesale from
``detection_confidence`` — this module does not re-implement them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.services.detection_confidence import (
    BASELINE_EXCLUSION_SECONDS,
    MIN_BASELINE_SAMPLE_COUNT,
    MIN_SAMPLE_COUNT,
    PREVIOUS_WINDOW_SECONDS,
    compute_detection_confidence_stats,
    count_detection_objects,
    extract_detection_confidences,
)

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
    source
)
values (
    $1, $2, $3, $4,
    $5, $6, $7,
    $8, $9,
    $10, $11, $12, $13,
    $14, $15, $16,
    $17
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

    Always writes a row (even with zero/few detections) so the history records
    that the camera had little to detect — the baseline query ignores rows
    whose stats are null. Stats are summarised only when there are at least
    ``MIN_SAMPLE_COUNT`` usable confidence values; below that the row carries
    counts but null stats.
    """
    values = extract_detection_confidences(snapshot)
    object_count = count_detection_objects(snapshot)
    confidence_sample_count = len(values)
    stats = compute_detection_confidence_stats(values)

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
    )
    return DetectionMetricWriteResult(
        written=row is not None,
        object_count=object_count,
        confidence_sample_count=confidence_sample_count,
        avg_confidence=avg,
    )


# ---------------------------------------------------------------------------
# Recent baseline query
# ---------------------------------------------------------------------------

# Weighted average confidence over the baseline window, weighting each metric
# row by its confidence_sample_count. Excludes rows with null avg_confidence
# and rows summarising fewer than MIN_SAMPLE_COUNT samples. The window reaches
# back PREVIOUS_WINDOW_SECONDS and excludes the most recent
# BASELINE_EXCLUSION_SECONDS so the current snapshot is never compared against
# itself.
_BASELINE_SQL = """
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
   and confidence_sample_count >= $5
"""


@dataclass(frozen=True)
class DetectionConfidenceBaseline:
    """A camera's recent confidence baseline, for drop-vs-history comparison."""

    previous_avg_confidence: float
    confidence_sample_count: int
    metric_count: int
    window_seconds: int


async def fetch_detection_confidence_baseline(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    now: datetime,
) -> DetectionConfidenceBaseline | None:
    """Weighted recent baseline for a camera, or ``None`` when there is too
    little history to trust (fewer than ``MIN_BASELINE_SAMPLE_COUNT`` total
    confidence samples across the window). Returning ``None`` is the
    "no baseline" signal — the evaluator must treat it as "skip the drop
    comparison", never as "resolve" or "open".
    """
    window_start = now - timedelta(seconds=PREVIOUS_WINDOW_SECONDS)
    window_end = now - timedelta(seconds=BASELINE_EXCLUSION_SECONDS)
    row = await conn.fetchrow(
        _BASELINE_SQL,
        account_id,
        camera_id,
        window_start,
        window_end,
        MIN_SAMPLE_COUNT,
    )
    if row is None:
        return None
    total_samples = row["total_samples"]
    weighted_sum = row["weighted_sum"]
    if total_samples is None or weighted_sum is None:
        return None
    if total_samples < MIN_BASELINE_SAMPLE_COUNT:
        return None
    previous_avg = weighted_sum / total_samples
    if previous_avg <= 0:
        return None
    return DetectionConfidenceBaseline(
        previous_avg_confidence=previous_avg,
        confidence_sample_count=int(total_samples),
        metric_count=int(row["metric_count"]),
        window_seconds=PREVIOUS_WINDOW_SECONDS,
    )
