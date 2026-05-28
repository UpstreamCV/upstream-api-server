from __future__ import annotations

import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.accounts import AccountLimitsResponse, AccountResponse
from upstream_api.schemas.processing_config import (
    ProcessingConfigResponse,
    ProcessingConfigUpdate,
)
from upstream_api.security.deps import CurrentCaller, PoolDep
from upstream_api.services.processing_config import (
    get_processing_config,
    update_processing_config,
)

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


@router.get("/processing-config", response_model=ProcessingConfigResponse)
async def get_account_processing_config(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ProcessingConfigResponse:
    """Read the account-level processing master switch.

    Default for an account that has never written the flag is ``true`` --
    every existing account continues to be processed exactly as before
    M3 shipped. The dashboard reads this on the account-settings page to
    render the toggle's current position.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        result = await get_processing_config(conn, account_id=scope_account)
    if result is None:
        raise NotFoundError("Account not found.", code="account_not_found")
    processing_enabled, updated_at = result
    return ProcessingConfigResponse(
        account_id=scope_account,
        processing_enabled=processing_enabled,
        updated_at=updated_at,
    )


@router.patch("/processing-config", response_model=ProcessingConfigResponse)
async def patch_account_processing_config(
    body: ProcessingConfigUpdate,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ProcessingConfigResponse:
    """Flip the account-level processing master switch.

    OFF -> processor's eligibility SQL drops every session under this
    account; existing workers drain gracefully on their next poll.
    Stream sessions stay alive (the streaming server keeps publishing
    snapshots); only worker-side processing pauses.

    ON -> any currently-streaming session under this account becomes
    eligible again within one poll cycle (~5s); the supervisor spawns
    workers normally.

    Same caller-auth pattern as ``GET /v1/account`` and
    ``GET /v1/account/limits`` (resolve the caller's account, no path
    parameter).
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        result = await update_processing_config(
            conn,
            account_id=scope_account,
            processing_enabled=body.processing_enabled,
        )
    if result is None:
        raise NotFoundError("Account not found.", code="account_not_found")
    processing_enabled, updated_at = result
    return ProcessingConfigResponse(
        account_id=scope_account,
        processing_enabled=processing_enabled,
        updated_at=updated_at,
    )
