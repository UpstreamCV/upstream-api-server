from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

LatencyMode = Literal["low", "balanced", "stable"]


class CameraRegisterRequest(BaseModel):
    agent_id: UUID
    name: str = Field(min_length=1, max_length=128)
    external_id: str | None = Field(default=None, max_length=256)
    device_path: str | None = Field(default=None, max_length=512)
    source_type: str | None = Field(default=None, min_length=1, max_length=32)
    default_width: int = Field(ge=1, le=7680)
    default_height: int = Field(ge=1, le=4320)
    default_fps: int = Field(ge=1, le=240)
    default_codec: str = Field(min_length=1, max_length=32)
    default_bitrate_kbps: int = Field(ge=1, le=200_000)
    default_latency_mode: LatencyMode = "balanced"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CameraCreateRequest(BaseModel):
    """Dashboard-authored camera (no agent yet; an agent claims it later)."""

    name: str = Field(min_length=1, max_length=128)
    source_type: str = Field(min_length=1, max_length=32)
    device_path: str | None = Field(default=None, max_length=512)
    default_width: int = Field(ge=1, le=7680)
    default_height: int = Field(ge=1, le=4320)
    default_fps: int = Field(ge=1, le=240)
    default_codec: str = Field(default="h264", min_length=1, max_length=32)
    default_bitrate_kbps: int = Field(default=3000, ge=1, le=200_000)
    default_latency_mode: LatencyMode = "balanced"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CameraUpdateRequest(BaseModel):
    """Partial dashboard edit. Omitted fields are left unchanged."""

    name: str | None = Field(default=None, min_length=1, max_length=128)
    source_type: str | None = Field(default=None, min_length=1, max_length=32)
    device_path: str | None = Field(default=None, max_length=512)
    default_width: int | None = Field(default=None, ge=1, le=7680)
    default_height: int | None = Field(default=None, ge=1, le=4320)
    default_fps: int | None = Field(default=None, ge=1, le=240)
    default_codec: str | None = Field(default=None, min_length=1, max_length=32)
    default_bitrate_kbps: int | None = Field(default=None, ge=1, le=200_000)
    default_latency_mode: LatencyMode | None = None


class CameraClaimRequest(BaseModel):
    agent_id: UUID


class CameraResponse(BaseModel):
    id: UUID
    account_id: UUID
    agent_id: UUID | None
    name: str
    external_id: str | None
    device_path: str | None
    source_type: str | None = None
    default_width: int | None
    default_height: int | None
    default_fps: int | None
    default_codec: str | None
    default_bitrate_kbps: int | None
    default_latency_mode: LatencyMode
    metadata: dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime


class CameraListResponse(BaseModel):
    cameras: list[CameraResponse]
