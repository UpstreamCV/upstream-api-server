"""Account-level processing master switch -- request / response shapes.

When ``processing_enabled = false`` (account-wide), the processor's
eligibility SQL drops every session under this account, so no DeepStream
or mock workers spawn. The supervisor itself stays running (cheap HTTP
polling); GPU usage goes to zero.

Storage convention mirrors the existing ``cameras.metadata['package_id']``
pattern: a single JSONB key on the ``accounts`` row. Absent key = treated
as ``true`` (existing accounts continue processing exactly as before).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class ProcessingConfigResponse(BaseModel):
    account_id: UUID
    processing_enabled: bool
    updated_at: datetime


class ProcessingConfigUpdate(BaseModel):
    processing_enabled: bool
