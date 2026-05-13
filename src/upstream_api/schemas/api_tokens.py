from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class ApiTokenCreateRequest(BaseModel):
    account_id: UUID
    name: str = Field(min_length=1, max_length=128)
    scopes: list[str] = Field(default_factory=lambda: ["agent"])
    expires_at: datetime | None = None


class ApiTokenSummary(BaseModel):
    id: UUID
    account_id: UUID
    name: str
    prefix: str
    last_four: str
    scopes: list[str]
    last_used_at: datetime | None
    expires_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


class ApiTokenCreateResponse(BaseModel):
    token: ApiTokenSummary
    plaintext: str  # shown exactly once


class ApiTokenListResponse(BaseModel):
    tokens: list[ApiTokenSummary]
