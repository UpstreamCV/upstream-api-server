from __future__ import annotations

import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.schemas.platform_admin import AdminAuditEntry, AdminAuditLogResponse
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep

router = APIRouter()


def _payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return json.loads(value) if value else {}


@router.get("/audit-log", response_model=AdminAuditLogResponse)
async def get_audit_log(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    admin_user_id: Annotated[UUID | None, Query()] = None,
    target_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AdminAuditLogResponse:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select id, admin_user_id, action, target_type, target_id,
                   payload, request_id, created_at,
                   count(*) over()::int as total
              from admin_audit_log
             where ($1::uuid is null or admin_user_id = $1)
               and ($2::uuid is null or target_id = $2)
             order by created_at desc
             limit $3 offset $4
            """,
            admin_user_id,
            target_id,
            limit,
            offset,
        )
    return AdminAuditLogResponse(
        items=[
            AdminAuditEntry(
                id=r["id"],
                admin_user_id=r["admin_user_id"],
                action=r["action"],
                target_type=r["target_type"],
                target_id=r["target_id"],
                payload=_payload_dict(r["payload"]),
                request_id=r["request_id"],
                created_at=r["created_at"],
            )
            for r in rows
        ],
        total=rows[0]["total"] if rows else 0,
    )
