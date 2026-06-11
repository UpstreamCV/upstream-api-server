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

# M8.0 only accepted mock observation types. P4-B (processing layer)
# adds ``processor.line_crossing`` — emitted by the DeepStream analytics
# probe when nvdsanalytics' ``objLCCumCnt`` counter increments. The DB
# column ``observations.type`` is plain text with no CHECK constraint
# (see migration ``20260518140000_processor_status_observations.sql``),
# so widening this whitelist does not require a DDL change. Future
# observation types (ROI occupancy, zone enter/exit, dwell, etc.) are
# added one-at-a-time per the processing-layer taxonomy.
ProcessorObservationType = Literal[
    "processor.mock_frame_seen",
    "processor.mock_heartbeat",
    "processor.mock_detection",
    "processor.line_crossing",
    # Side task (per-object line crossing): per-OBJECT crossing evidence
    # with track/class/confidence attribution and event_mode
    # "object_crossing" in the payload. Flows ALONGSIDE the aggregate
    # cumulative-counter type above — rule evaluation partitions on
    # event_mode so one physical crossing never double-fires a rule.
    "processor.line_crossing_object",
    # P4-C: second real DeepStream-backed type. The analytics worker
    # emits ROI occupancy snapshots gated by an on-change-OR-interval
    # policy (never per-frame). Same DB column / route / auth as
    # processor.line_crossing — only the Literal widens.
    "processor.roi_occupancy",
    # P4-D: per-track zone transitions. The analytics worker pairs the
    # stable tracker_id (P3) with per-object roi_labels (P3.5) to emit
    # one zone_entered when a tracked object stably enters a configured
    # ROI and one zone_exited when it stably leaves. Stability is
    # measured in consecutive observed frames (default 5) so detection
    # / tracker / analytics jitter does not flood. Disappearance does
    # NOT emit zone_exited — only an observed-with-no-label trajectory
    # does. Same DB column / route / auth; only the Literal widens.
    "processor.zone_entered",
    "processor.zone_exited",
    # P4-E: dwell observations layered on top of P4-D's stable zone
    # membership. ``dwell_started`` fires once per zone enter event;
    # ``dwell_updated`` emits periodically while the tracked object
    # is still inside (default 30s); ``dwell_exceeded`` fires once
    # when the dwell duration crosses the configured threshold
    # (default 60s). No DB migration; processor closes the dwell
    # session on the matching zone_exited (or never, if the track
    # vanishes — matching P4-D's "disappearance does NOT emit
    # zone_exited" invariant).
    "processor.dwell_started",
    "processor.dwell_updated",
    "processor.dwell_exceeded",
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


class EligibleGeometryConfig(BaseModel):
    """The camera's saved geometry config, carried in eligible-sessions (P6-A).

    ``revision`` is the DB row revision (bumps on every save) — the
    processor's signal that geometry changed between sessions. ``config`` is
    the geometry config JSON, kept as an opaque ``dict``: the envelope is
    strongly typed, but the nested shape is validated on the write path
    (``PUT /v1/cameras/{id}/geometry-config``), not re-validated here. The
    processor consumes this in P6-C; in P6-A it is wire-carried only.
    """

    revision: int
    config: dict[str, Any]


class EligibleProcessingConfig(BaseModel):
    """The camera's saved processing config, carried in eligible-sessions (M7B).

    ``revision`` is the DB row revision (bumps on every save) — the
    processor's signal that the per-camera model/processing selection changed
    between sessions. ``config`` is the ``CameraProcessingConfig.model_dump()``
    JSON, kept as an opaque ``dict``: the envelope is strongly typed, but the
    nested shape is validated on the write path
    (``PUT /v1/cameras/{id}/processing-config``), not re-validated here. M7B is
    STAMP-ONLY — this is wire-carried for lineage; the runtime still runs its
    hard-coded defaults and consumes nothing from this config.
    """

    revision: int
    config: dict[str, Any]


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
    # P6-A: the camera's dashboard-authored ROI/line geometry config, or
    # null when the camera has no saved config. Wire-carried only — the
    # processor begins consuming it in P6-C.
    geometry_config: EligibleGeometryConfig | None = None
    # M7B: the camera's dashboard-authored per-camera processing config, or
    # null when the camera has no saved config. Wire-carried; consumed for
    # lineage only — runtime behavior is unchanged (STAMP-ONLY milestone).
    processing_config: EligibleProcessingConfig | None = None
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


class ProcessorStatusRecord(BaseModel):
    """One ``processor_statuses`` row, shaped for a read response (P6-D).

    The full row (including ``payload``, ``fps_*``, ``error_*``, ``last_*``)
    so dashboards / verification scripts can read the ``payload.applied_geometry``
    lineage block stamped by the P6-C analytics worker. Distinct from
    ``ProcessorStatusResponse`` — that is the write-ack shape and intentionally
    omits the payload. ``account_id`` is intentionally omitted here too — the
    caller is already account-scoped.
    """

    id: UUID
    camera_id: UUID
    stream_session_id: UUID
    processor_id: str
    runtime: str
    package_id: str
    status: ProcessorStatusValue
    fps_input: float | None
    fps_processed: float | None
    last_frame_at: datetime | None
    last_observation_at: datetime | None
    error_code: str | None
    error_message: str | None
    payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class LatestProcessorStatusResponse(BaseModel):
    status: ProcessorStatusRecord


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


class ObservationRecord(BaseModel):
    """One ``observations`` row, shaped for a read response (P5-A).

    ``type`` is a plain ``str``, deliberately NOT ``ProcessorObservationType``:
    the DB column has no CHECK constraint (open vocabulary), so a read must
    never 422/500 on a row whose ``type`` predates or postdates the current
    write whitelist. ``account_id`` is intentionally omitted — the caller is
    already scoped to their own account, so echoing it adds nothing.
    """

    id: UUID
    camera_id: UUID
    stream_session_id: UUID
    processor_id: str | None
    package_id: str | None
    type: str
    payload: dict[str, Any]
    observed_at: datetime
    created_at: datetime


class RecentObservationsResponse(BaseModel):
    observations: list[ObservationRecord]


class PerceptionSnapshotIngestRequest(BaseModel):
    """``POST /v1/internal/processor/perception-snapshots`` request body (P5-C).

    No ``account_id`` / ``camera_id`` — the server derives tenant from the
    session, never trusting the body. ``snapshot`` is accepted as an opaque
    ``dict``: the envelope is validated strongly, but the nested
    objects/geometry are deliberately not deep-validated server-side so the
    snapshot shape can iterate without lock-stepping a server migration.
    """

    stream_session_id: UUID
    processor_id: str | None = Field(default=None, max_length=128)
    package_id: str | None = Field(default=None, max_length=128)
    observed_at: datetime
    snapshot: dict[str, Any] = Field(default_factory=dict)


class PerceptionSnapshotRecord(BaseModel):
    """One ``latest_perception_snapshots`` row, shaped for a read response.

    Latest visual state for one camera (upsert, one row per camera) — distinct
    from the append-only ``observations`` event log. ``account_id`` is omitted;
    the caller is already account-scoped.
    """

    id: UUID
    camera_id: UUID
    stream_session_id: UUID
    processor_id: str | None
    package_id: str | None
    snapshot: dict[str, Any]
    observed_at: datetime
    updated_at: datetime


class LatestPerceptionSnapshotResponse(BaseModel):
    snapshot: PerceptionSnapshotRecord


class PerceptionStreamTokenResponse(BaseModel):
    """Short-lived credentials for the perception-fanout WS hot path.

    ``ws_url`` is the full browser-reachable subscribe endpoint
    (``{fanout_public_ws_url}/v1/subscribe``); the client appends
    ``?token=...``. The token must never be logged or persisted.
    """

    token: str
    ws_url: str
    expires_at: datetime
