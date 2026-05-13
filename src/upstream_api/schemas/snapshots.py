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


class LatestFrameResponse(BaseModel):
    """Server-signed Supabase Storage URL for the most recent snapshot.

    `signed_url` expires at `expires_at` (~60 s window). Clients should
    re-fetch this endpoint before that time to refresh the URL.
    """

    storage_key: str
    bytes: int
    width: int | None
    height: int | None
    captured_at: datetime
    created_at: datetime
    signed_url: str
    expires_at: datetime
