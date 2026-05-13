from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.observability.context import agent_id_var
from upstream_api.schemas.agents import (
    AgentListResponse,
    AgentRegisterRequest,
    AgentRegisterResponse,
    AgentResponse,
)
from upstream_api.security.deps import CurrentAgent, CurrentCaller, PoolDep

router = APIRouter(prefix="/v1/agents", tags=["agents"])


_AGENT_COLUMNS = (
    "id, account_id, name, hostname, version, os, python_version, "
    "capabilities, status, last_seen_at, created_at, updated_at"
)


def _row_to_agent(row: dict) -> AgentResponse:  # type: ignore[type-arg]
    return AgentResponse(
        id=row["id"],
        account_id=row["account_id"],
        name=row["name"],
        hostname=row["hostname"],
        version=row["version"],
        os=row["os"],
        python_version=row["python_version"],
        capabilities=row["capabilities"]
        if isinstance(row["capabilities"], dict)
        else json.loads(row["capabilities"]),
        status=row["status"],
        last_seen_at=row["last_seen_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.post("/register", response_model=AgentRegisterResponse)
async def register_agent(
    body: AgentRegisterRequest,
    auth: CurrentAgent,
    pool: PoolDep,
) -> AgentRegisterResponse:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"""
            insert into agents (
              account_id, name, hostname, version, os, python_version, capabilities, last_seen_at
            )
            values ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
            on conflict (account_id, name) do update
              set hostname = excluded.hostname,
                  version = excluded.version,
                  os = excluded.os,
                  python_version = excluded.python_version,
                  capabilities = excluded.capabilities,
                  last_seen_at = now()
            returning {_AGENT_COLUMNS}
            """,
            auth.account_id,
            body.name,
            body.hostname,
            body.version,
            body.os,
            body.python_version,
            json.dumps(body.capabilities),
        )
    assert row is not None
    agent_id_var.set(row["id"])

    return AgentRegisterResponse(agent=_row_to_agent(row), server_time=datetime.now(tz=UTC))


@router.get("", response_model=AgentListResponse)
async def list_agents(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[str | None, Query(max_length=32)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> AgentListResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            select {_AGENT_COLUMNS}
              from agents
             where account_id = $1
               and ($2::text is null or status = $2)
             order by created_at desc
             limit $3
            """,
            scope_account,
            status,
            limit,
        )
    return AgentListResponse(agents=[_row_to_agent(r) for r in rows])


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> AgentResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"select {_AGENT_COLUMNS} from agents where id = $1",
            agent_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Agent not found.", code="agent_not_found")
    return _row_to_agent(row)
