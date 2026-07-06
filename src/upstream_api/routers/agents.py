from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query

from upstream_api.config import get_settings
from upstream_api.errors import ForbiddenError, NotFoundError
from upstream_api.observability.context import agent_id_var
from upstream_api.schemas.agents import (
    AgentListResponse,
    AgentRegisterRequest,
    AgentRegisterResponse,
    AgentResponse,
)
from upstream_api.schemas.bootstrap import (
    BootstrapAccount,
    BootstrapAgent,
    BootstrapLimits,
    BootstrapResponse,
    BootstrapSession,
    BootstrapToken,
    BootstrapUsage,
)
from upstream_api.security.deps import CurrentAgent, CurrentCaller, PoolDep
from upstream_api.services.account_audit import try_record_account_audit_log
from upstream_api.services.agent_version_policy import build_agent_version_policy
from upstream_api.services.limits import load_account_limits

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
        # Device-scoped, already-bound tokens may only re-register their own
        # agent. Checked BEFORE the upsert so a stolen token can't even
        # create a differently-named agents row (the upsert would insert).
        if auth.is_device_scoped and auth.agent_id is not None:
            existing = await conn.fetchval(
                "select id from agents where account_id = $1 and name = $2",
                auth.account_id,
                body.name,
            )
            if existing != auth.agent_id:
                raise ForbiddenError(
                    "Device token is bound to a different agent.",
                    code="device_token_agent_mismatch",
                )

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

        # Trust-on-first-use bind: the first register through an unbound
        # device token locks the token to this agent, permanently. The
        # `agent_id is null` guard makes concurrent registers race-safe —
        # exactly one wins the bind; a loser targeting a DIFFERENT agent is
        # rejected (and its upsert rolled back), while a same-agent replay
        # sails through as a plain re-register. The partial unique index
        # api_tokens_one_active_per_agent_idx blocks the hijack case — an
        # unbound token adopting an agent that already has a live device
        # token (token rotation revokes the old one first, so it passes).
        if auth.is_device_scoped and auth.agent_id is None:
            try:
                bound = await conn.fetchrow(
                    """
                    update api_tokens
                       set agent_id = $2
                     where id = $1 and agent_id is null
                    returning agent_id
                    """,
                    auth.token_id,
                    row["id"],
                )
            except asyncpg.UniqueViolationError as exc:
                raise ForbiddenError(
                    "Agent already has an active device token; revoke it before binding a new one.",
                    code="device_token_agent_conflict",
                ) from exc
            if bound is None:
                current = await conn.fetchval(
                    "select agent_id from api_tokens where id = $1",
                    auth.token_id,
                )
                if current != row["id"]:
                    raise ForbiddenError(
                        "Device token is bound to a different agent.",
                        code="device_token_agent_mismatch",
                    )
            else:
                await try_record_account_audit_log(
                    conn,
                    account_id=auth.account_id,
                    actor_type="system",
                    event_type="api_token.bound",
                    resource_type="api_token",
                    resource_id=str(auth.token_id),
                    summary=f"Device token bound to agent {body.name!r} on first register.",
                    metadata={"agent_id": str(row["id"]), "agent_name": body.name},
                )
    agent_id_var.set(row["id"])

    return AgentRegisterResponse(agent=_row_to_agent(row), server_time=datetime.now(tz=UTC))


# NOTE: declared before GET /{agent_id} so "/bootstrap" isn't parsed as a UUID.
@router.get("/bootstrap", response_model=BootstrapResponse)
async def bootstrap(auth: CurrentAgent, pool: PoolDep) -> BootstrapResponse:
    """Server-side context for an agent holding nothing but its API token.

    One call answers: which account am I, what limits apply, which cameras
    exist (and their server-controlled config), what sessions are running,
    and how much of the account's capacity is in use. Everything is scoped
    to the token's account; no secret-bearing fields are returned (see
    ``schemas/bootstrap.py``).
    """
    # Local import to avoid a routers<->routers cycle if cameras ever imports
    # from this module; reuses the camera column list + response mapper.
    from upstream_api.routers.cameras import _CAMERA_COLUMNS, _camera_to_response

    # Bound device tokens see only their own agent's slice of the account —
    # the fleet's other agents/cameras/sessions are not this device's
    # business. Account-scoped tokens (and unbound device tokens, which
    # need the full picture exactly once, pre-register) keep the full view.
    scope_agent: UUID | None = auth.agent_id if auth.is_device_scoped else None

    async with pool.acquire() as conn:
        account_row = await conn.fetchrow(
            "select id, name, slug, plan, status from accounts where id = $1",
            auth.account_id,
        )
        if account_row is None:
            raise NotFoundError("Account not found.", code="account_not_found")

        token_row = await conn.fetchrow(
            "select name, scopes, last_four, expires_at from api_tokens where id = $1",
            auth.token_id,
        )
        if token_row is None:
            raise NotFoundError("Token not found.", code="token_not_found")

        agent_rows = await conn.fetch(
            """
            select id, name, status, version, last_seen_at
              from agents
             where account_id = $1
               and ($2::uuid is null or id = $2)
             order by last_seen_at desc nulls last, created_at desc
            """,
            auth.account_id,
            scope_agent,
        )

        limits = await load_account_limits(conn, auth.account_id)

        usage_row = await conn.fetchrow(
            """
            select
              (select count(*)::int from cameras
                where account_id = $1 and status <> 'disabled') as camera_count,
              (select count(*)::int from stream_sessions
                where account_id = $1
                  and state in ('approved', 'streaming')) as active_session_count,
              (select coalesce(sum(quantity), 0)::bigint from usage_events
                where account_id = $1
                  and event_type = 'session_ended'
                  and occurred_at >= date_trunc('month', now())) as monthly_seconds_used
            """,
            auth.account_id,
        )

        camera_rows = await conn.fetch(
            f"""
            select {_CAMERA_COLUMNS}
              from cameras
             where account_id = $1 and status <> 'disabled'
               and ($2::uuid is null or agent_id = $2)
             order by created_at desc
            """,
            auth.account_id,
            scope_agent,
        )

        session_rows = await conn.fetch(
            """
            select id, camera_id, agent_id, state, created_at, expires_at, started_at,
                   lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps,
                   lim_latency_mode
              from stream_sessions
             where account_id = $1 and state in ('approved', 'streaming')
               and ($2::uuid is null or agent_id = $2)
             order by created_at desc
            """,
            auth.account_id,
            scope_agent,
        )

    return BootstrapResponse(
        account=BootstrapAccount(
            id=account_row["id"],
            name=account_row["name"],
            slug=account_row["slug"],
            plan=account_row["plan"],
            status=account_row["status"],
        ),
        token=BootstrapToken(
            name=token_row["name"],
            scopes=list(token_row["scopes"]),
            last_four=token_row["last_four"],
            expires_at=token_row["expires_at"],
        ),
        agents=[
            BootstrapAgent(
                id=r["id"],
                name=r["name"],
                status=r["status"],
                version=r["version"],
                last_seen_at=r["last_seen_at"],
            )
            for r in agent_rows
        ],
        limits=BootstrapLimits(
            max_cameras=limits.max_cameras,
            max_concurrent_sessions=limits.max_concurrent_sessions,
            max_fps=limits.max_fps,
            max_width=limits.max_width,
            max_height=limits.max_height,
            max_bitrate_kbps=limits.max_bitrate_kbps,
            allowed_codecs=sorted(limits.allowed_codecs),
            allowed_stream_types=sorted(limits.allowed_stream_types),
            monthly_minutes_quota=limits.monthly_minutes_quota,
            snapshot_storage_mb=limits.snapshot_storage_mb,
        ),
        usage=BootstrapUsage(
            camera_count=usage_row["camera_count"],
            active_session_count=usage_row["active_session_count"],
            monthly_minutes_used=int(usage_row["monthly_seconds_used"]) // 60,
        ),
        cameras=[_camera_to_response(r) for r in camera_rows],
        active_sessions=[
            BootstrapSession(
                id=r["id"],
                camera_id=r["camera_id"],
                agent_id=r["agent_id"],
                state=r["state"],
                created_at=r["created_at"],
                expires_at=r["expires_at"],
                started_at=r["started_at"],
                approved={
                    "width": r["lim_width"],
                    "height": r["lim_height"],
                    "fps": r["lim_fps"],
                    "codec": r["lim_codec"],
                    "bitrate_kbps": r["lim_bitrate_kbps"],
                    "latency_mode": r["lim_latency_mode"],
                },
            )
            for r in session_rows
        ],
        server_time=datetime.now(tz=UTC),
        # Update M2: omitted unless this deployment configures a policy.
        agent_version_policy=build_agent_version_policy(get_settings()),
    )


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
