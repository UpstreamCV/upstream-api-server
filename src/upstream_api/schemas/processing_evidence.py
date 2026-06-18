"""P5-J — processing runtime evidence (read-only visibility).

A camera-level summary of what the processing package is doing over a recent
window: package + lineage, latest processor status, windowed observation counts,
lifecycle signals (inferred zone exits), session-level tracking metrics, and the
recent package-change history.

VISIBILITY, NOT recommendations — no verdicts, no "this package is wrong", no
auto-apply. Later milestones can build recommendations on top of these signals.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from upstream_api.schemas.processing_config_camera import CameraPackageState

ProcessingEvidenceRange = Literal["1h", "24h"]


class EvidenceWindow(BaseModel):
    range: ProcessingEvidenceRange
    started_at: datetime
    ended_at: datetime


class EvidencePackage(BaseModel):
    """Requested vs applied package + derived state (reuses the P5-G view)."""

    requested_package_id: str | None = None
    applied_package_id: str | None = None
    state: CameraPackageState
    requested_revision: int | None = None


class EvidenceLineage(BaseModel):
    """What actually ran, read from the latest processor status payload. All
    nullable — a camera with no status row has no lineage yet."""

    model_id: str | None = None
    tracker_profile_id: str | None = None
    tracker_algorithm: str | None = None
    analytics_profile_id: str | None = None


class EvidenceStatus(BaseModel):
    status: str | None = None
    last_seen_at: datetime | None = None
    last_error_code: str | None = None
    updated_at: datetime | None = None


class EvidenceObservationCount(BaseModel):
    """One observation type + its count over the window. `type` is a plain str
    (open vocabulary), matching the observations read path."""

    type: str
    count: int


class EvidenceLifecycle(BaseModel):
    """Lifecycle-policy signals counted from observation payloads over the
    window (P5-Gap inferred exits). `track_missing_timeout_exits` is the subset
    of inferred exits whose `exit_reason` is `track_missing_timeout`."""

    inferred_zone_exits: int = 0
    track_missing_timeout_exits: int = 0


class EvidenceTracking(BaseModel):
    """Tracker metrics from the latest status payload. IMPORTANT: these are
    SESSION-LEVEL (cumulative for the current processing session), NOT windowed
    — `is_session_level` is always true so the UI labels them honestly."""

    unique_tracker_ids_count: int | None = None
    tracker_ids_sample: list[str] = Field(default_factory=list)
    is_session_level: bool = True


class EvidencePackageHistoryItem(BaseModel):
    """One package request/apply audit event (from `account_audit_logs`)."""

    id: UUID
    event_type: str
    package_id: str | None = None
    actor_type: str
    actor_user_id: UUID | None = None
    summary: str
    created_at: datetime


class ProcessingEvidenceResponse(BaseModel):
    """GET /v1/cameras/{camera_id}/processing/evidence. Nested objects are
    always populated (with null fields) even when there is no status / no
    observations, so the dashboard renders a stable shape."""

    camera_id: UUID
    window: EvidenceWindow
    package: EvidencePackage
    lineage: EvidenceLineage
    status: EvidenceStatus
    observation_counts: list[EvidenceObservationCount]
    lifecycle: EvidenceLifecycle
    tracking: EvidenceTracking
    package_history: list[EvidencePackageHistoryItem]
    warnings: list[str]
