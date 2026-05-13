from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class StreamValidationResponse(BaseModel):
    id: UUID
    session_id: UUID
    passed: bool
    observed: dict[str, Any]
    expected: dict[str, Any]
    mismatch_fields: list[str]
    created_at: datetime


class StreamValidationListResponse(BaseModel):
    validations: list[StreamValidationResponse]
