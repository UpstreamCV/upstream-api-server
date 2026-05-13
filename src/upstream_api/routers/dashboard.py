from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.schemas.dashboard import DashboardSummaryResponse
from upstream_api.security.deps import CurrentCaller, PoolDep

router = APIRouter(prefix="/v1/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummaryResponse)
async def get_summary(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> DashboardSummaryResponse:
    """Account-scoped overview counts for the dashboard's main page.

    Single round-trip; replaces the dashboard's client-side derivation
    across the three list endpoints.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select
              (select count(*)::int from cameras
                 where account_id = $1) as total_cameras,
              (select count(*)::int from cameras
                 where account_id = $1 and status = 'streaming') as streaming_cameras,
              (select count(*)::int from cameras
                 where account_id = $1 and status = 'idle') as idle_cameras,
              (select count(*)::int from cameras
                 where account_id = $1 and status = 'error') as cameras_with_errors,
              (select count(*)::int from agents
                 where account_id = $1
                   and last_seen_at > now() - interval '5 minutes') as online_agents,
              (select count(*)::int from agents
                 where account_id = $1) as total_agents,
              (select count(*)::int from stream_sessions
                 where account_id = $1 and state in ('approved', 'streaming')
                  ) as active_streams,
              (select count(*)::int from stream_validations v
                 join stream_sessions s on s.id = v.session_id
                where s.account_id = $1
                  and v.passed = false
                  and v.created_at > now() - interval '24 hours'
                  ) as validation_errors_24h
            """,
            scope_account,
        )
    assert row is not None
    return DashboardSummaryResponse(
        total_cameras=row["total_cameras"],
        online_cameras=row["online_agents"],  # heuristic: cameras attached to online agents
        offline_cameras=row["total_cameras"] - row["streaming_cameras"],
        streaming_cameras=row["streaming_cameras"],
        cameras_with_errors=row["cameras_with_errors"],
        active_streams=row["active_streams"],
        validation_errors_24h=row["validation_errors_24h"],
    )
