from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel


class MembershipSummary(BaseModel):
    account_id: UUID
    account_name: str
    account_slug: str
    role: str


class CurrentUserResponse(BaseModel):
    user_id: UUID
    aal: str
    amr: list[str]
    memberships: list[MembershipSummary]
