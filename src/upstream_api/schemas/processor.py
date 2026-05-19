"""M8.0 Processor Readiness Gate — request/response models.

These back the three `/v1/internal/processor/*` endpoints. They deliberately
expose only fields a processor needs to attach to a validated internal RTSP
stream and report back. No `srt_url`, no API/agent token, no webhook secret,
no signed URL is ever modelled here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

# Allowed processor lifecycle states (M8.0 fixed set).
ProcessorStatusValue = Literal[
    "starting",
    "attached",
    "processing",
    "lagging",
    "failed",
    "stopped",
]

# M8.0 only accepts mock observation types. M8.1+ widens this.
ProcessorObservationType = Literal[
    "processor.mock_frame_seen",
    "processor.mock_heartbeat",
    "processor.mock_detection",
]


class ApprovedStreamSummary(BaseModel):
    """The codec/geometry the API approved for this session (the `lim_*`
    snapshot taken at session start, not what ffprobe observed)."""

    codec: str
    width: int
    height: int
    fps: int
    bitrate_kbps: int


class ProcessorValidationSummary(BaseModel):
    """The latest stream_validations row for the session. Always present in
    an eligible-sessions item (a session with no validation row is excluded
    upstream), and always `passed == True`."""

    passed: bool
    observed: dict[str, Any]
    expected: dict[str, Any]
    mismatch_fields: list[str]
    created_at: datetime


class EligibleProcessorSession(BaseModel):
    session_id: UUID
    # The MediaMTX path / read identifier (`sess_<16hex>`). Not a secret; it
    # is only resolvable on the firewalled internal RTSP surface.
    stream_key: str
    camera_id: UUID
    account_id: UUID
    internal_rtsp_url: str
    package_id: str
    runtime: str
    approved: ApprovedStreamSummary
    validation: ProcessorValidationSummary
    # Zone/line geometry is deferred (no dashboard drawing UI yet). Always
    # null in M8.0; the field exists so M8.3+ does not break the contract.
    zones_lines: None = None
    started_at: datetime | None
    processor_eligible: bool = True


class ProcessorEligibleSessionsResponse(BaseModel):
    sessions: list[EligibleProcessorSession]


class ProcessorStatusRequest(BaseModel):
    processor_id: str = Field(min_length=1, max_length=128)
    stream_session_id: UUID
    runtime: str = Field(min_length=1, max_length=64)
    package_id: str = Field(min_length=1, max_length=128)
    status: ProcessorStatusValue
    fps_input: float | None = Field(default=None, ge=0, le=1000)
    fps_processed: float | None = Field(default=None, ge=0, le=1000)
    last_frame_at: datetime | None = None
    last_observation_at: datetime | None = None
    error_code: str | None = Field(default=None, max_length=64)
    error_message: str | None = Field(default=None, max_length=512)
    payload: dict[str, Any] = Field(default_factory=dict)


class ProcessorStatusResponse(BaseModel):
    id: UUID
    # account_id / camera_id are echoed from the session row, NOT from the
    # request, so a processor cannot mislabel another tenant's status.
    account_id: UUID
    camera_id: UUID
    stream_session_id: UUID
    processor_id: str
    runtime: str
    package_id: str
    status: ProcessorStatusValue
    created_at: datetime
    updated_at: datetime


class ProcessorObservationRequest(BaseModel):
    stream_session_id: UUID
    type: ProcessorObservationType
    payload: dict[str, Any] = Field(default_factory=dict)
    observed_at: datetime
    processor_id: str | None = Field(default=None, max_length=128)
    package_id: str | None = Field(default=None, max_length=128)


class ProcessorObservationResponse(BaseModel):
    id: UUID
    count: int
