from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class SnapshotUploadResponse(BaseModel):
    id: UUID
    camera_id: UUID
    session_id: UUID | None
    storage_key: str
    bytes: int
    width: int | None
    height: int | None
    captured_at: datetime
    created_at: datetime
