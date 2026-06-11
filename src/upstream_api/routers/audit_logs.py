"""Module 7 M3 — tenant audit log reads.

Dashboard-only (Supabase user auth). Any account member can read — the
events describe account configuration the rest of the dashboard already
shows members (same read posture as alerts/usage). ip_address/user_agent
are stored but deliberately NOT returned here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query
from pydantic import BaseModel

from upstream_api.errors import ForbiddenError
from upstream_api.security.deps import CurrentUser, PoolDep
from upstream_api.services.processor import coerce_jsonb

router = APIRouter(prefix="/v1/audit-logs", tags=["audit-logs"])

_LIST_SQL = """
select id, actor_user_id, actor_type, event_type, resource_type,
       resource_id, summary, metadata, created_at
  from account_audit_logs
 where account_id = $1
   and ($2::timestamptz is null or created_at < $2)
   and ($3::text is null or event_type = $3)
   and ($4::text is null or resource_type = $4)
   and ($5::text is null or resource_id = $5)
 order by created_at desc, id desc
 limit $6
"""


class AuditLogActor(BaseModel):
    type: str
    user_id: UUID | None


class AuditLogItem(BaseModel):
    id: UUID
    created_at: datetime
    actor: AuditLogActor
    event_type: str
    resource_type: str | None
    resource_id: str | None
    summary: str
    metadata: dict[str, Any]


class AuditLogListResponse(BaseModel):
    items: list[AuditLogItem]
    # created_at of the last item when the page is full — pass back as
    # ?before= for the next page. Timestamp keyset, not an opaque token.
    next_cursor: datetime | None


def _resolve_account_for_user(user: CurrentUser, account_id: UUID | None) -> UUID:
    """Same helper shape as routers/usage.py / routers/workflows.py."""
    if account_id is not None:
        if account_id not in user.memberships:
            raise ForbiddenError(
                "User is not a member of this account.",
                code="forbidden_account",
            )
        return account_id
    if len(user.memberships) == 1:
        return next(iter(user.memberships))
    raise ForbiddenError(
        "account_id query parameter is required for users with multiple accounts.",
        code="account_id_required",
    )


@router.get("", response_model=AuditLogListResponse)
async def list_audit_logs(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    before: Annotated[datetime | None, Query()] = None,
    event_type: Annotated[str | None, Query()] = None,
    resource_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
) -> AuditLogListResponse:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _LIST_SQL,
            scope_account,
            before,
            event_type,
            resource_type,
            resource_id,
            limit,
        )
    items = [
        AuditLogItem(
            id=row["id"],
            created_at=row["created_at"],
            actor=AuditLogActor(type=row["actor_type"], user_id=row["actor_user_id"]),
            event_type=row["event_type"],
            resource_type=row["resource_type"],
            resource_id=row["resource_id"],
            summary=row["summary"],
            metadata=coerce_jsonb(row["metadata"]),
        )
        for row in rows
    ]
    next_cursor = items[-1].created_at if len(items) == limit else None
    return AuditLogListResponse(items=items, next_cursor=next_cursor)
