from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi import APIRouter

from upstream_api.observability.context import agent_id_var
from upstream_api.schemas.agents import (
    AgentRegisterRequest,
    AgentRegisterResponse,
    AgentResponse,
)
from upstream_api.security.deps import CurrentAgent, PoolDep

router = APIRouter(prefix="/v1/agents", tags=["agents"])


@router.post("/register", response_model=AgentRegisterResponse)
async def register_agent(
    body: AgentRegisterRequest,
    auth: CurrentAgent,
    pool: PoolDep,
) -> AgentRegisterResponse:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
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
            returning id, account_id, name, hostname, version, os, python_version,
                      capabilities, status, last_seen_at, created_at, updated_at
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

    agent = AgentResponse(
        id=row["id"],
        account_id=row["account_id"],
        name=row["name"],
        hostname=row["hostname"],
        version=row["version"],
        os=row["os"],
        python_version=row["python_version"],
        capabilities=row["capabilities"] if isinstance(row["capabilities"], dict)
        else json.loads(row["capabilities"]),
        status=row["status"],
        last_seen_at=row["last_seen_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
    return AgentRegisterResponse(agent=agent, server_time=datetime.now(tz=UTC))
