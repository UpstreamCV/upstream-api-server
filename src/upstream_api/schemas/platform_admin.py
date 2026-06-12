"""Response/request models for the /v1/admin/* platform-admin surface.

These are cross-tenant shapes consumed only by upstream-admin-panel. Tenant
rows always carry their account context (account_id + name/slug) because the
panel renders global lists where the tenant is a column, not a scope.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# /v1/admin/me
# ---------------------------------------------------------------------------


class AdminMeResponse(BaseModel):
    user_id: UUID
    email: str | None


# ---------------------------------------------------------------------------
# /v1/admin/system/health
# ---------------------------------------------------------------------------


class ApiHealth(BaseModel):
    status: str
    version: str
    environment: str


class DatabaseHealth(BaseModel):
    status: Literal["ok", "error"]
    latency_ms: float | None = None
    error: str | None = None


class StreamingServerHealth(BaseModel):
    status: Literal["ok", "unreachable", "error"]
    active_paths: int | None = None
    error: str | None = None


class ProcessorsHealth(BaseModel):
    active: int
    stale: int
    failed: int
    latest_update_at: datetime | None = None


class SystemHealthResponse(BaseModel):
    api: ApiHealth
    database: DatabaseHealth
    streaming_server: StreamingServerHealth
    processors: ProcessorsHealth


class SystemTimeseriesPoint(BaseModel):
    bucket: datetime
    sessions_started: int
    validation_failures: int
    streaming_minutes: float
    avg_fps: float | None
    alerts_fired: int
    workflow_runs: int
    objects_detected: int
    # Hourly mean of per-snapshot avg confidence; null = no detection metrics
    # that hour (distinct from low confidence).
    avg_detection_confidence: float | None


class SystemTimeseriesResponse(BaseModel):
    hours: int
    points: list[SystemTimeseriesPoint]


# ---------------------------------------------------------------------------
# /v1/admin/system/metrics — control-plane counters (in-process Prometheus
# registry, the same one GET /metrics serves). Process-lifetime values: they
# reset on restart, which is why the response carries the uptime.
# ---------------------------------------------------------------------------


class IngestWebhookMetrics(BaseModel):
    ok: int
    client_error: int
    server_error: int


class StreamAuthMetrics(BaseModel):
    allowed: int
    denied: int


class TimeToStreamingMetrics(BaseModel):
    count: int
    p50_seconds: float | None
    p95_seconds: float | None


class DbPoolMetrics(BaseModel):
    size: int
    in_use: int


class ControlPlaneMetricsResponse(BaseModel):
    process_uptime_seconds: float
    ingest_webhooks: IngestWebhookMetrics
    stream_auth: StreamAuthMetrics
    session_transitions_total: int
    sessions_failed_total: int
    time_to_streaming: TimeToStreamingMetrics
    db_pool: DbPoolMetrics


class HealthSample(BaseModel):
    sampled_at: datetime
    db_ok: bool
    db_latency_ms: float | None
    mediamtx_status: str
    mediamtx_active_paths: int | None
    processors_active: int
    processors_stale: int
    processors_failed: int
    active_sessions: int


class HealthHistoryResponse(BaseModel):
    minutes: int
    interval_seconds: int
    samples: list[HealthSample]


# ---------------------------------------------------------------------------
# /v1/admin/streams
# ---------------------------------------------------------------------------


class AdminHeartbeat(BaseModel):
    status: str
    fps: float | None
    bitrate_kbps: int | None
    width: int | None = None
    height: int | None = None
    codec: str | None = None
    last_error: str | None = None
    reported_at: datetime


class AdminStreamItem(BaseModel):
    id: UUID
    account_id: UUID
    account_name: str
    account_slug: str
    agent_id: UUID
    camera_id: UUID
    camera_name: str | None
    state: str
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    expires_at: datetime
    end_reason: str | None
    failure_code: str | None
    latest_heartbeat: AdminHeartbeat | None
    latest_validation_passed: bool | None
    processor_status: str | None


class AdminStreamListResponse(BaseModel):
    items: list[AdminStreamItem]
    total: int


class AdminStreamsSummaryResponse(BaseModel):
    by_state: dict[str, int]
    started_24h: int
    validation_failures_24h: int
    accounts_streaming: int
    open_reliability_findings: int


class AdminStreamValidation(BaseModel):
    passed: bool
    observed: dict[str, Any]
    expected: dict[str, Any]
    mismatch_fields: list[str]
    created_at: datetime


class AdminReliabilityFinding(BaseModel):
    finding_type: str
    category: str
    severity: str
    status: str
    title: str
    message: str
    last_seen_at: datetime


class AdminProcessorStatus(BaseModel):
    processor_id: str
    status: str
    fps_input: float | None
    fps_processed: float | None
    last_frame_at: datetime | None
    error_code: str | None
    error_message: str | None
    updated_at: datetime


class AdminStreamDetailResponse(BaseModel):
    session: AdminStreamItem
    requested: dict[str, Any]
    approved: dict[str, Any]
    validated: dict[str, Any]
    heartbeats: list[AdminHeartbeat]
    validations: list[AdminStreamValidation]
    reliability_findings: list[AdminReliabilityFinding]
    processor_statuses: list[AdminProcessorStatus]


# ---------------------------------------------------------------------------
# /v1/admin/accounts
# ---------------------------------------------------------------------------


class AdminAccountItem(BaseModel):
    id: UUID
    name: str
    slug: str
    plan: str
    status: str
    created_at: datetime
    member_count: int
    agent_count: int
    camera_count: int
    active_sessions: int


class AdminAccountListResponse(BaseModel):
    items: list[AdminAccountItem]
    total: int


class AdminAccountLimits(BaseModel):
    max_cameras: int
    max_concurrent_sessions: int
    max_fps: int
    max_width: int
    max_height: int
    max_bitrate_kbps: int
    allowed_codecs: list[str]
    monthly_minutes_quota: int
    snapshot_storage_mb: int
    updated_at: datetime


class AdminUsageTotal(BaseModel):
    event_type: str
    unit: str
    total_quantity: int


class AdminAccountDetailResponse(BaseModel):
    account: AdminAccountItem
    limits: AdminAccountLimits | None
    usage_30d: list[AdminUsageTotal]


class AdminAccountUpdate(BaseModel):
    """PATCH body for identity/plan/status. `deleted` is deliberately not
    settable from the panel — account deletion stays a manual, considered
    operation."""

    name: str | None = Field(default=None, min_length=1, max_length=200)
    slug: str | None = Field(
        default=None, min_length=2, max_length=80, pattern=r"^[a-z0-9][a-z0-9-]*$"
    )
    plan: Literal["free", "pro", "enterprise"] | None = None
    status: Literal["active", "suspended"] | None = None


class AdminAccountCreate(BaseModel):
    """POST body for provisioning a tenant directly from the panel.

    `slug` is derived from the name when omitted (same recipe as the signup
    trigger: lowercased, dashed, uuid suffix). `owner_email` optionally
    attaches a first owner — an existing auth user, or a new one when
    `owner_password` is also given.
    """

    name: str = Field(min_length=1, max_length=200)
    slug: str | None = Field(
        default=None, min_length=2, max_length=80, pattern=r"^[a-z0-9][a-z0-9-]*$"
    )
    plan: Literal["free", "pro", "enterprise"] = "free"
    owner_email: str | None = Field(default=None, max_length=320)
    owner_password: str | None = Field(default=None, min_length=8, max_length=200)


class AdminAccountLimitsUpdate(BaseModel):
    max_cameras: int | None = Field(default=None, ge=0)
    max_concurrent_sessions: int | None = Field(default=None, ge=0)
    max_fps: int | None = Field(default=None, ge=1)
    max_width: int | None = Field(default=None, ge=1)
    max_height: int | None = Field(default=None, ge=1)
    max_bitrate_kbps: int | None = Field(default=None, ge=1)
    allowed_codecs: list[str] | None = None
    monthly_minutes_quota: int | None = Field(default=None, ge=0)
    snapshot_storage_mb: int | None = Field(default=None, ge=0)


class AdminAccountMember(BaseModel):
    user_id: UUID
    role: str
    email: str | None
    created_at: datetime


class AdminMemberAdd(BaseModel):
    """POST body for attaching a member. When the email has no auth user yet,
    `password` must be provided so the user can be created (confirmed) via
    the GoTrue Admin API."""

    email: str = Field(max_length=320)
    role: Literal["owner", "admin", "viewer"]
    password: str | None = Field(default=None, min_length=8, max_length=200)


class AdminMemberUpdate(BaseModel):
    role: Literal["owner", "admin", "viewer"]


class AdminRecoveryLinkRequest(BaseModel):
    email: str = Field(max_length=320)


class AdminRecoveryLinkResponse(BaseModel):
    email: str
    action_link: str


class AdminApiTokenCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class AdminApiTokenCreateResponse(BaseModel):
    """`plaintext` is shown exactly once; it is never stored or audited."""

    token: AdminApiToken
    plaintext: str


class AdminPlatformAdminEntry(BaseModel):
    user_id: UUID
    email: str | None
    note: str | None
    created_at: datetime


class AdminPlatformAdminsResponse(BaseModel):
    admins: list[AdminPlatformAdminEntry]


class AdminPlatformAdminAdd(BaseModel):
    email: str = Field(max_length=320)
    note: str | None = Field(default=None, max_length=300)


class AdminAccountMembersResponse(BaseModel):
    members: list[AdminAccountMember]


class AdminApiToken(BaseModel):
    id: UUID
    name: str
    prefix: str
    last_four: str
    scopes: list[str]
    last_used_at: datetime | None
    expires_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


class AdminApiTokensResponse(BaseModel):
    tokens: list[AdminApiToken]


# ---------------------------------------------------------------------------
# /v1/admin/usage
# ---------------------------------------------------------------------------


class AdminUsageBucket(BaseModel):
    bucket_start: datetime
    account_id: UUID
    account_name: str
    event_type: str
    unit: str
    total_quantity: int


class AdminUsageResponse(BaseModel):
    buckets: list[AdminUsageBucket]


class AdminUsageSummaryRow(BaseModel):
    account_id: UUID
    account_name: str
    account_slug: str
    plan: str
    event_type: str
    unit: str
    total_quantity: int


class AdminUsageSummaryResponse(BaseModel):
    days: int
    rows: list[AdminUsageSummaryRow]


# ---------------------------------------------------------------------------
# /v1/admin/audit-log
# ---------------------------------------------------------------------------


class AdminAuditEntry(BaseModel):
    id: UUID
    admin_user_id: UUID
    action: str
    target_type: str
    target_id: UUID | None
    payload: dict[str, Any]
    request_id: str | None
    created_at: datetime


class AdminAuditLogResponse(BaseModel):
    items: list[AdminAuditEntry]
    total: int
