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

ReliabilityCategory = Literal["stream", "processor", "scene_quality", "geometry_config"]
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
