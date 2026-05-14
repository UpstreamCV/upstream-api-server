from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

LatencyMode = Literal["low", "balanced", "stable"]


class RequestedStreamParams(BaseModel):
    width: int = Field(ge=1, le=7680)
    height: int = Field(ge=1, le=4320)
    fps: int = Field(ge=1, le=240)
    codec: str = Field(min_length=1, max_length=32)
    bitrate_kbps: int = Field(ge=1, le=200_000)
    latency_mode: LatencyMode = "balanced"


class StreamSessionStartRequest(BaseModel):
    camera_id: UUID
    requested: RequestedStreamParams


class StreamSessionResponse(BaseModel):
    id: UUID
    account_id: UUID
    agent_id: UUID
    camera_id: UUID
    state: str
    srt_url: str
    streamid: str
    expires_at: datetime
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    requested: dict[str, Any]
    approved: dict[str, Any]


class StreamSessionStartResponse(BaseModel):
    session: StreamSessionResponse
    srt_url: str
    session_token: str
    expires_at: datetime


class StreamSessionListResponse(BaseModel):
    sessions: list[StreamSessionResponse]
