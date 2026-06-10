"""Agent bootstrap context — everything an agent needs to render account
status and decide what it may stream, fetched with nothing but its API token.

Deliberately excludes every secret-bearing field: no ``srt_url``,
``streamid``, session tokens, SRT passphrases, token ``prefix`` or ``hash``.
``BootstrapSession`` exists (instead of reusing ``StreamSessionResponse``)
precisely because the session response requires ``srt_url``/``streamid``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from upstream_api.schemas.cameras import CameraResponse


class BootstrapAccount(BaseModel):
    id: UUID
    name: str
    slug: str
    # Informational label only — enforcement always reads account_limits.
    plan: str
    status: str


class BootstrapToken(BaseModel):
    name: str
    scopes: list[str]
    last_four: str
    expires_at: datetime | None


class BootstrapAgent(BaseModel):
    id: UUID
    name: str
    status: str
    version: str | None
    last_seen_at: datetime | None


class BootstrapLimits(BaseModel):
    max_cameras: int
    max_concurrent_sessions: int
    max_fps: int
    max_width: int
    max_height: int
    max_bitrate_kbps: int
    allowed_codecs: list[str]
    allowed_stream_types: list[str]
    monthly_minutes_quota: int
    snapshot_storage_mb: int


class BootstrapUsage(BaseModel):
    camera_count: int
    active_session_count: int
    # Sum of completed-session durations this calendar month. Approximate:
    # in-flight session time is invisible until the session ends.
    monthly_minutes_used: int


class BootstrapSession(BaseModel):
    id: UUID
    camera_id: UUID
    agent_id: UUID
    state: str
    created_at: datetime
    expires_at: datetime
    started_at: datetime | None
    # Same dict shape as StreamSessionResponse.approved (lim_* columns).
    approved: dict[str, Any]


class BootstrapResponse(BaseModel):
    account: BootstrapAccount
    token: BootstrapToken
    # All agents registered under the account, newest-seen first.
    # Empty list == no agent registered yet.
    agents: list[BootstrapAgent]
    limits: BootstrapLimits
    usage: BootstrapUsage
    cameras: list[CameraResponse]
    active_sessions: list[BootstrapSession]
    server_time: datetime
