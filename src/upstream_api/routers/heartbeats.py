from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.observability.context import agent_id_var
from upstream_api.schemas.heartbeats import (
    HeartbeatBatchRequest,
    HeartbeatBatchResponse,
    HeartbeatListResponse,
    HeartbeatRecord,
)
from upstream_api.security.deps import (
    CurrentAgent,
    CurrentCaller,
    PoolDep,
    require_agent_access,
)
from upstream_api.services.heartbeats import record_batch
from upstream_api.services.usage import record_event

router = APIRouter(prefix="/v1/heartbeats", tags=["heartbeats"])

# Mounted separately so the path matches the design doc:
# GET /v1/agents/{agent_id}/heartbeats and /v1/cameras/{camera_id}/heartbeats.
history_router = APIRouter(tags=["heartbeats"])


_HEARTBEAT_COLUMNS = (
    "id, agent_id, camera_id, session_id, status, fps, bitrate_kbps, "
    "width, height, codec, last_error, agent_version, reported_at, created_at"
)


def _row_to_heartbeat(row: dict) -> HeartbeatRecord:  # type: ignore[type-arg]
    return HeartbeatRecord(
        id=row["id"],
        agent_id=row["agent_id"],
        camera_id=row["camera_id"],
        session_id=row["session_id"],
        status=row["status"],
        fps=row["fps"],
        bitrate_kbps=row["bitrate_kbps"],
        width=row["width"],
        height=row["height"],
        codec=row["codec"],
        last_error=row["last_error"],
        agent_version=row["agent_version"],
        reported_at=row["reported_at"],
        created_at=row["created_at"],
    )


@router.post("", response_model=HeartbeatBatchResponse)
async def submit_heartbeats(
    body: HeartbeatBatchRequest,
    auth: CurrentAgent,
    pool: PoolDep,
    agent_id: Annotated[UUID, Query(description="The agent submitting these heartbeats.")],
) -> HeartbeatBatchResponse:
    """Submit a batch of heartbeats (max 100 per call).

    The ``agent_id`` query parameter identifies which agent (under the calling
    account's token) the heartbeats belong to. Cameras and sessions referenced
    in the batch must belong to that agent.
    """
    # Device-scoped tokens may only report heartbeats as their bound agent.
    require_agent_access(auth, agent_id)
    async with pool.acquire() as conn, conn.transaction():
        # Validate agent_id ownership before any insert. Cross-account agents
        # return the same 404 as unknown agents to avoid leaking existence.
        owner_row = await conn.fetchrow(
            "select account_id from agents where id = $1",
            agent_id,
        )
        if owner_row is None or owner_row["account_id"] != auth.account_id:
            raise NotFoundError("Agent not found.", code="agent_not_found")

        result = await record_batch(
            conn,
            account_id=auth.account_id,
            agent_id=agent_id,
            items=body.heartbeats,
        )
        await record_event(
            conn,
            account_id=auth.account_id,
            event_type="heartbeat_batch",
            quantity=result.accepted,
            unit="heartbeats",
        )

    agent_id_var.set(agent_id)
    return HeartbeatBatchResponse(
        accepted=result.accepted,
        server_time=datetime.now(tz=UTC),
    )


@history_router.get(
    "/v1/agents/{agent_id}/heartbeats",
    response_model=HeartbeatListResponse,
)
async def list_agent_heartbeats(
    agent_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    before: Annotated[datetime | None, Query()] = None,
) -> HeartbeatListResponse:
    """Heartbeat history for one agent. Newest first.

    Ownership check: the agent must belong to the resolved account.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from agents where id = $1",
            agent_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Agent not found.", code="agent_not_found")

        rows = await conn.fetch(
            f"""
            select {_HEARTBEAT_COLUMNS}
              from heartbeats
             where agent_id = $1
               and ($2::timestamptz is null or reported_at < $2)
             order by reported_at desc
             limit $3
            """,
            agent_id,
            before,
            limit,
        )
    return HeartbeatListResponse(heartbeats=[_row_to_heartbeat(r) for r in rows])


@history_router.get(
    "/v1/cameras/{camera_id}/heartbeats",
    response_model=HeartbeatListResponse,
)
async def list_camera_heartbeats(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    before: Annotated[datetime | None, Query()] = None,
) -> HeartbeatListResponse:
    """Heartbeat history for one camera. Newest first."""
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        rows = await conn.fetch(
            f"""
            select {_HEARTBEAT_COLUMNS}
              from heartbeats
             where camera_id = $1
               and ($2::timestamptz is null or reported_at < $2)
             order by reported_at desc
             limit $3
            """,
            camera_id,
            before,
            limit,
        )
    return HeartbeatListResponse(heartbeats=[_row_to_heartbeat(r) for r in rows])
