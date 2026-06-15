"""Module 6 M6.4 — webhook delivery attempt schemas.

Read-only view of the durable delivery queue. Carries no secret (signing
secrets live encrypted on event_destinations). Status vocabulary is
DB-constrained: pending | delivering | delivered | failed | dead.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class DeliveryAttempt(BaseModel):
    id: UUID
    account_id: UUID
    event_id: UUID
    destination_id: UUID
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    response_status_code: int | None = None
    response_body_preview: str | None = None
    error_message: str | None = None
    last_attempt_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class DeliveryAttemptListResponse(BaseModel):
    items: list[DeliveryAttempt]
    # created_at of the last item when the page is full — pass back as
    # ?before_created_at= for the next page (timestamp keyset, like audit-logs).
    next_before_created_at: datetime | None
