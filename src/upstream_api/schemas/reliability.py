"""Reliability findings — read schemas.

Shapes the dashboard's Reliability card consumes. Write paths run inside the
API server (services/reliability_evaluator.py + services/reliability_repo.py)
and never accept findings from a client, so there is no request schema here.

``account_id`` is intentionally omitted from ``ReliabilityFindingRecord`` — the
caller is already account-scoped, same posture as ``ProcessorStatusRecord`` /
``ObservationRecord``.

Vocabularies:

- ``ReliabilityCategory`` / ``ReliabilitySeverity`` / ``ReliabilityStatus`` are
  ``Literal`` types because every value is CHECK-constrained at the DB layer.
- ``finding_type`` is a plain ``str`` (open vocabulary at the DB layer, matching
  ``observations.type``) so a new rule can ship without a migration or a schema
  change. The dashboard switches on ``category`` + ``severity``; ``finding_type``
  is identifier-grade copy for logs and dedup.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

ReliabilityCategory = Literal[
    "stream", "processor", "scene_quality", "geometry_config", "detection"
]
ReliabilitySeverity = Literal["info", "warning", "critical"]
ReliabilityStatus = Literal["open", "resolved", "suppressed", "ignored"]

# The summary endpoint rolls many findings into one categorical verdict; it has
# its own vocabulary so "no open findings" and "not enough data" stay distinct.
ReliabilitySummaryStatus = Literal["healthy", "info", "needs_review", "critical", "unknown"]


class ReliabilityFindingRecord(BaseModel):
    """One ``reliability_findings`` row, shaped for a read response."""

    id: UUID
    camera_id: UUID
    stream_session_id: UUID | None
    processor_status_id: UUID | None

    finding_type: str
    category: ReliabilityCategory
    severity: ReliabilitySeverity
    status: ReliabilityStatus

    title: str
    message: str
    likely_cause: str | None
    recommended_action: str | None

    evidence: dict[str, Any]
    dedupe_key: str
    detector_version: str

    first_seen_at: datetime
    last_seen_at: datetime
    resolved_at: datetime | None
    resolved_reason: str | None

    created_at: datetime
    updated_at: datetime


class ReliabilityFindingsResponse(BaseModel):
    findings: list[ReliabilityFindingRecord]
    last_evaluated_at: datetime


class ReliabilitySummaryResponse(BaseModel):
    camera_id: UUID
    status: ReliabilitySummaryStatus
    highest_severity: ReliabilitySeverity | None
    open_findings_count: int
    top_findings: list[ReliabilityFindingRecord]
    recommended_next_action: str | None
    last_evaluated_at: datetime


# ---------------------------------------------------------------------------
# Confidence trend (reliability M7) — hourly detection-confidence history for
# the camera Reliability tab's trend chart. Backed by the M6
# ``detection_confidence_hourly_rollups`` table; the read API aggregates across
# packages to ONE camera-level point per hour and renames the internal weighted
# columns to clean public names (avg_confidence_weighted -> avg_confidence,
# etc.). NO internal/tenant columns (account_id, stream_session_id, package_id,
# source) are exposed — same account-scoped posture as ReliabilityFindingRecord.
# ---------------------------------------------------------------------------

# Closed for M7. 30d (and finer/daily buckets) are a future milestone.
ConfidenceTrendRange = Literal["24h", "7d"]


class ConfidenceTrendPoint(BaseModel):
    """One hourly point, aggregated across packages. Confidence stats are
    ``None`` for an hour whose rollups carried no usable confidence (e.g.
    empty-frame hours) — the dashboard renders those as gaps in the line."""

    bucket_start: datetime
    bucket_end: datetime
    avg_confidence: float | None
    min_confidence: float | None
    max_confidence: float | None
    low_confidence_ratio: float | None
    confidence_stddev: float | None
    metric_count: int
    object_count_total: int
    confidence_sample_count: int


class ConfidenceTrendSummary(BaseModel):
    """Headline stats over the returned points. ``latest_*`` read the most
    recent point that actually has a confidence value, so a trailing empty hour
    never blanks the headline while data exists."""

    point_count: int
    latest_avg_confidence: float | None
    lowest_avg_confidence: float | None
    highest_avg_confidence: float | None
    total_confidence_samples: int
    latest_bucket_start: datetime | None


class ConfidenceTrendResponse(BaseModel):
    camera_id: UUID
    range: ConfidenceTrendRange
    bucket_size: Literal["1h"]
    points: list[ConfidenceTrendPoint]
    summary: ConfidenceTrendSummary


# ---------------------------------------------------------------------------
# Confidence trend by applied processing config (model-config M9) — the same
# hourly history, split into one series per applied-processing-config dimension
# group instead of blended to a single camera-level line. Backed by the M9
# dimension columns on ``detection_confidence_hourly_rollups``. A separate
# endpoint/response (not new fields on ConfidenceTrendResponse) so the existing
# dashboard chart contract stays byte-identical.
#
# Dimension fields are ``None`` for data recorded before the processor stamped
# applied_processing_config (pre-M8) — those rows form their own "no config
# metadata" group rather than being dropped or mislabeled.
# ---------------------------------------------------------------------------

# The processor resolver's status vocabulary (closed today; extend in lockstep
# with upstream-processor's ResolvedProcessingConfig if it grows).
ProcessingConfigStatus = Literal[
    "applied",
    "partially_applied",
    "requested_not_applied",
    "applied_defaults",
    "fallback_default_on_error",
]


class ConfidenceTrendConfigGroup(BaseModel):
    """One applied-config dimension group: the dimensions that identify it,
    its hourly points (oldest-first), and a per-group headline summary. All
    dimension fields ``None`` == the "no config metadata" (pre-M8) group."""

    processing_config_source: str | None
    processing_config_revision: int | None
    processing_config_status: str | None
    applied_model_key: str | None
    applied_selected_classes_key: str | None
    applied_confidence_threshold: float | None
    # M9.1: dedicated FPS dimensions (M7E output-cadence apply). Honest scope:
    # processing_fps is output cadence only — it does not reduce inference/GPU
    # work, and these fields record what the processor stamped.
    requested_processing_fps: int | None
    applied_processing_fps: int | None
    processing_config_warnings: list[str] | None
    points: list[ConfidenceTrendPoint]
    summary: ConfidenceTrendSummary


class ConfidenceTrendByConfigResponse(BaseModel):
    camera_id: UUID
    range: ConfidenceTrendRange
    bucket_size: Literal["1h"]
    group_count: int
    groups: list[ConfidenceTrendConfigGroup]


# ---------------------------------------------------------------------------
# Config comparison report card (model-config M11) — a small, read-side,
# DESCRIPTIVE synthesis over the M9/M9.1 by-config groups: pick a baseline and
# a candidate config group in the window and report what changed between them.
# This is a report card, NOT a recommender: no verdicts, no "better/worse", no
# "recommended config", no causal claims — only factual directions ("lower",
# "higher", "unchanged"), comparability notes (a changed threshold or class
# set changes the measured population), and standing caveats.
# ---------------------------------------------------------------------------

ConfigComparisonStatus = Literal["comparable", "mixed_metrics", "inconclusive", "no_data"]
ConfigComparisonMode = Literal["explicit", "latest_vs_previous", "recent_vs_prior", "none"]


class ConfigComparisonGroup(BaseModel):
    """One side of the comparison: a config group's identity plus its window
    aggregates (sample-count-weighted over the group's hourly points)."""

    group_key: str
    label: str
    processing_config_source: str | None
    processing_config_revision: int | None
    processing_config_status: str | None
    applied_model_key: str | None
    applied_selected_classes_key: str | None
    applied_confidence_threshold: float | None
    requested_processing_fps: int | None
    applied_processing_fps: int | None
    warnings: list[str]
    sample_count: int
    avg_confidence: float | None
    low_confidence_ratio: float | None
    latest_bucket_start: datetime | None


class ConfigComparisonDeltas(BaseModel):
    """Candidate minus baseline. ``None`` when either side lacks the metric.
    Descriptive only — a delta's sign is a direction, not a judgment."""

    avg_confidence_delta: float | None
    avg_confidence_percent_delta: float | None
    low_confidence_ratio_delta: float | None
    sample_count_delta: int
    applied_processing_fps_delta: int | None


class ConfigComparisonSummaryResponse(BaseModel):
    """The report card. ``comparison_status`` semantics:

    - ``comparable`` — both groups have enough samples AND the population-
      defining settings (confidence threshold, selected classes) match, so the
      metric deltas describe the same measured population.
    - ``mixed_metrics`` — both groups have enough samples but the threshold or
      class set changed, so metrics moved AND the measured population moved;
      ``comparability_notes`` explains why numbers aren't directly comparable.
    - ``inconclusive`` — fewer than two groups, missing aggregates, or a side
      below ``min_samples``.
    - ``no_data`` — no config-aware data in the window at all.

    ``metric_directions`` is a flat, factual vocabulary for the dashboard:
    ``avg_confidence_higher|lower|unchanged``,
    ``low_confidence_ratio_higher|lower|unchanged``,
    ``samples_higher|lower|unchanged``.
    """

    camera_id: UUID
    range: ConfidenceTrendRange
    generated_at: datetime
    comparison_mode: ConfigComparisonMode
    min_samples: int
    baseline: ConfigComparisonGroup | None
    candidate: ConfigComparisonGroup | None
    metric_deltas: ConfigComparisonDeltas | None
    comparison_status: ConfigComparisonStatus
    metric_directions: list[str]
    summary: str
    bullets: list[str]
    comparability_notes: list[str]
    caveats: list[str]
