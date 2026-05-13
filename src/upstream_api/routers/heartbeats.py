from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.observability.context import agent_id_var
from upstream_api.schemas.heartbeats import (
    HeartbeatBatchRequest,
    HeartbeatBatchResponse,
)
from upstream_api.security.deps import CurrentAgent, PoolDep
from upstream_api.services.heartbeats import record_batch
from upstream_api.services.usage import record_event

router = APIRouter(prefix="/v1/heartbeats", tags=["heartbeats"])


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
    async with pool.acquire() as conn, conn.transaction():
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
