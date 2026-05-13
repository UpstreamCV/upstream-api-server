from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

HeartbeatStatus = Literal["online", "streaming", "error", "reconnecting"]


class Heartbeat(BaseModel):
    camera_id: UUID | None = None
    session_id: UUID | None = None
    status: HeartbeatStatus
    fps: float | None = Field(default=None, ge=0, le=1000)
    bitrate_kbps: int | None = Field(default=None, ge=0, le=200_000)
    width: int | None = Field(default=None, ge=1, le=7680)
    height: int | None = Field(default=None, ge=1, le=4320)
    codec: str | None = Field(default=None, max_length=32)
    last_error: str | None = Field(default=None, max_length=512)
    agent_version: str | None = Field(default=None, max_length=64)
    reported_at: datetime


class HeartbeatBatchRequest(BaseModel):
    heartbeats: list[Heartbeat] = Field(min_length=1, max_length=100)


class HeartbeatBatchResponse(BaseModel):
    accepted: int
    server_time: datetime


class HeartbeatRecord(BaseModel):
    id: UUID
    agent_id: UUID
    camera_id: UUID | None
    session_id: UUID | None
    status: HeartbeatStatus
    fps: float | None
    bitrate_kbps: int | None
    width: int | None
    height: int | None
    codec: str | None
    last_error: str | None
    agent_version: str | None
    reported_at: datetime
    created_at: datetime


class HeartbeatListResponse(BaseModel):
    heartbeats: list[HeartbeatRecord]
