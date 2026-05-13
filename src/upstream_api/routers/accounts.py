from __future__ import annotations

import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.accounts import AccountLimitsResponse, AccountResponse
from upstream_api.security.deps import CurrentCaller, PoolDep

router = APIRouter(prefix="/v1/account", tags=["accounts"])


@router.get("", response_model=AccountResponse)
async def get_account(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> AccountResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select id, name, slug, plan, status, metadata, created_at, updated_at
              from accounts
             where id = $1
            """,
            scope_account,
        )
    if row is None:
        raise NotFoundError("Account not found.", code="account_not_found")

    metadata = row["metadata"]
    if not isinstance(metadata, dict):
        metadata = json.loads(metadata) if metadata else {}

    return AccountResponse(
        id=row["id"],
        name=row["name"],
        slug=row["slug"],
        plan=row["plan"],
        status=row["status"],
        metadata=metadata,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.get("/limits", response_model=AccountLimitsResponse)
async def get_account_limits(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> AccountLimitsResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select account_id, max_cameras, max_concurrent_sessions, max_fps,
                   max_width, max_height, max_bitrate_kbps, allowed_codecs,
                   monthly_minutes_quota, snapshot_storage_mb,
                   created_at, updated_at
              from account_limits
             where account_id = $1
            """,
            scope_account,
        )
    if row is None:
        raise NotFoundError("Account limits not configured.", code="account_limits_missing")
    return AccountLimitsResponse(
        account_id=row["account_id"],
        max_cameras=row["max_cameras"],
        max_concurrent_sessions=row["max_concurrent_sessions"],
        max_fps=row["max_fps"],
        max_width=row["max_width"],
        max_height=row["max_height"],
        max_bitrate_kbps=row["max_bitrate_kbps"],
        allowed_codecs=list(row["allowed_codecs"]),
        monthly_minutes_quota=row["monthly_minutes_quota"],
        snapshot_storage_mb=row["snapshot_storage_mb"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
