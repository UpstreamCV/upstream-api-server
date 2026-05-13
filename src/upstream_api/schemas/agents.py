from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class AgentRegisterRequest(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    hostname: str | None = None
    version: str | None = None
    os: str | None = None
    python_version: str | None = None
    capabilities: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    id: UUID
    account_id: UUID
    name: str
    hostname: str | None
    version: str | None
    os: str | None
    python_version: str | None
    capabilities: dict[str, Any]
    status: str
    last_seen_at: datetime | None
    created_at: datetime
    updated_at: datetime


class AgentRegisterResponse(BaseModel):
    agent: AgentResponse
    server_time: datetime


class AgentListResponse(BaseModel):
    agents: list[AgentResponse]
