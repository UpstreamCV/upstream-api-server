from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class AccountResponse(BaseModel):
    id: UUID
    name: str
    slug: str
    plan: str
    status: str
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class AccountLimitsResponse(BaseModel):
    account_id: UUID
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
    created_at: datetime
    updated_at: datetime
