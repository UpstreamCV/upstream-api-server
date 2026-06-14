"""Module 6 M6.3 — webhook destination schemas.

Config for where canonical events will eventually be delivered. The signing
secret is recoverable (encrypted at rest, not hashed) so the future delivery
worker can sign payloads; it is returned in plaintext ONLY on create and masked
everywhere else.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

DestinationType = Literal["webhook"]


class EventDestination(BaseModel):
    """A destination as returned by list/get/update — secret masked."""

    id: UUID
    account_id: UUID
    destination_type: DestinationType
    name: str
    url: str
    enabled: bool
    event_type_filters: list[str]
    camera_id_filters: list[UUID]
    workflow_id_filters: list[UUID]
    signing_secret_preview: str
    created_at: datetime
    updated_at: datetime
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    consecutive_failures: int
    disabled_at: datetime | None = None


class EventDestinationCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    url: str = Field(min_length=1, max_length=2048)
    enabled: bool = True
    event_type_filters: list[str] = Field(default_factory=list)
    camera_id_filters: list[UUID] = Field(default_factory=list)
    workflow_id_filters: list[UUID] = Field(default_factory=list)


class EventDestinationCreateResponse(EventDestination):
    """Create-only response: includes the plaintext signing secret, shown once."""

    signing_secret: str


class EventDestinationUpdate(BaseModel):
    """Partial update — only provided fields change. The signing secret is never
    rotated here (a later milestone)."""

    name: str | None = Field(default=None, min_length=1, max_length=200)
    url: str | None = Field(default=None, min_length=1, max_length=2048)
    enabled: bool | None = None
    event_type_filters: list[str] | None = None
    camera_id_filters: list[UUID] | None = None
    workflow_id_filters: list[UUID] | None = None


class EventDestinationListResponse(BaseModel):
    items: list[EventDestination]
