"""Upstream Reliability M1a — observation ingest + issue read schemas.

The request side (``ReliabilityObservationBatchRequest``) is what the Upstream
Collector (or any client) pushes to ``POST /v1/reliability/observations``. The
issue side (``ReliabilityIssueRecord``) shapes the read responses for
``GET /v1/reliability/issues`` — same posture as ``ReliabilityFindingRecord``:
``account_id`` is intentionally omitted because the caller is already
account-scoped.

Vocabularies:

- ``type`` (observation) and ``issue_type`` are plain ``str`` — open vocabulary
  at the DB layer, matching ``observations.type`` / ``reliability_findings``,
  so new observation/detector kinds ship without a migration.
- ``ReliabilityIssueCategory`` / severity / status are ``Literal`` because every
  value is CHECK-constrained at the DB layer.

The observation ``payload`` is an opaque ``dict`` — it is NOT deeply validated
here (it carries collector-computed numeric summaries the detectors read), only
sanitized when promoted into the event ledger.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from upstream_api.schemas.reliability import ReliabilitySeverity, ReliabilityStatus

# M1a observation types the collector emits. Open at the DB layer; this Literal
# documents the supported set without rejecting forward-compatible types — the
# request model keeps ``type`` a plain ``str`` so a newer collector can ship a
# new type before this build knows it.
ReliabilityObservationType = Literal[
    "event_observed",
    "detection_summary",
    "pipeline_heartbeat",
    "config_changed",
    "detection_observed",
    "feedback_observed",
]

# Issue categories (CHECK-constrained at the DB layer). Distinct from the
# processor-side ``ReliabilityCategory`` — observation issues bucket differently.
ReliabilityIssueCategory = Literal["count", "confidence", "liveness", "tracker"]


class ReliabilityObservationItem(BaseModel):
    """One observation in an ingest batch.

    At least one of ``camera_id`` / ``external_camera_key`` identifies the
    entity for per-camera detectors; a source-level observation (e.g. a
    ``pipeline_heartbeat``) may carry neither and is keyed by the batch
    ``source`` instead.
    """

    type: str = Field(min_length=1, max_length=64)
    camera_id: UUID | None = None
    external_camera_key: str | None = Field(default=None, max_length=256)
    observed_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class ReliabilityObservationBatchRequest(BaseModel):
    """A batch upload. ``account_id`` is NOT a field — the tenant always comes
    from the auth credential, never the body (a spoofed ``account_id`` is
    silently ignored as an unknown field)."""

    client_batch_id: str = Field(min_length=1, max_length=256)
    source: str = Field(min_length=1, max_length=256)
    observations: list[ReliabilityObservationItem] = Field(min_length=1, max_length=500)


class ReliabilityObservationBatchResponse(BaseModel):
    accepted: int
    batch_id: UUID
    server_time: datetime


class ReliabilityIssueRecord(BaseModel):
    """One ``cv_issues`` row, shaped for a read response (``account_id`` omitted
    — the caller is already account-scoped)."""

    id: UUID
    camera_id: UUID | None
    source_batch_id: UUID | None
    external_camera_key: str | None

    issue_type: str
    category: ReliabilityIssueCategory
    severity: ReliabilitySeverity
    status: ReliabilityStatus

    title: str
    message: str
    likely_cause: str | None
    recommended_action: str | None

    evidence: dict[str, Any]
    dedupe_key: str
    detector_version: str
    source_observation_ids: list[UUID] | None

    first_seen_at: datetime
    last_seen_at: datetime
    resolved_at: datetime | None
    resolved_reason: str | None

    created_at: datetime
    updated_at: datetime


class ReliabilityIssuesResponse(BaseModel):
    issues: list[ReliabilityIssueRecord]


class ReliabilityIssueResolveRequest(BaseModel):
    resolved_reason: str | None = Field(default=None, max_length=512)
