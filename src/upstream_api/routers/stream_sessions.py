from __future__ import annotations

import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.observability.context import session_id_var
from upstream_api.schemas.stream_sessions import (
    StreamSessionListResponse,
    StreamSessionResponse,
    StreamSessionStartRequest,
    StreamSessionStartResponse,
)
from upstream_api.schemas.stream_validations import (
    StreamValidationListResponse,
    StreamValidationResponse,
)
from upstream_api.security.deps import CurrentAgent, CurrentCaller, PoolDep, SettingsDep
from upstream_api.services.limits import LimitsSnapshot, RequestedStream
from upstream_api.services.session_lifecycle import (
    StreamSessionRow,
    start_session,
    stop_session,
)

router = APIRouter(prefix="/v1/stream-sessions", tags=["stream-sessions"])


def _row_to_response(row: StreamSessionRow) -> StreamSessionResponse:
    return StreamSessionResponse(
        id=row.id,
        account_id=row.account_id,
        agent_id=row.agent_id,
        camera_id=row.camera_id,
        state=row.state,
        srt_url=row.srt_url,
        streamid=row.streamid,
        expires_at=row.expires_at,
        started_at=row.started_at,
        ended_at=row.ended_at,
        requested={
            "width": row.requested.width,
            "height": row.requested.height,
            "fps": row.requested.fps,
            "codec": row.requested.codec,
            "bitrate_kbps": row.requested.bitrate_kbps,
            "latency_mode": row.requested.latency_mode,
        },
        approved={
            "width": row.approved.width,
            "height": row.approved.height,
            "fps": row.approved.fps,
            "codec": row.approved.codec,
            "bitrate_kbps": row.approved.bitrate_kbps,
            "latency_mode": row.approved.latency_mode,
        },
    )


_SESSION_COLUMNS = (
    "id, account_id, agent_id, camera_id, state, srt_url, streamid, "
    "expires_at, started_at, ended_at, "
    "req_width, req_height, req_fps, req_codec, req_bitrate_kbps, req_latency_mode, "
    "lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps, lim_latency_mode"
)


def _db_row_to_session(row: dict) -> StreamSessionRow:  # type: ignore[type-arg]
    return StreamSessionRow(
        id=row["id"],
        account_id=row["account_id"],
        agent_id=row["agent_id"],
        camera_id=row["camera_id"],
        state=row["state"],
        srt_url=row["srt_url"],
        streamid=row["streamid"],
        expires_at=row["expires_at"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        requested=RequestedStream(
            width=row["req_width"],
            height=row["req_height"],
            fps=row["req_fps"],
            codec=row["req_codec"],
            bitrate_kbps=row["req_bitrate_kbps"],
            latency_mode=row["req_latency_mode"],
        ),
        approved=LimitsSnapshot(
            width=row["lim_width"],
            height=row["lim_height"],
            fps=row["lim_fps"],
            codec=row["lim_codec"],
            bitrate_kbps=row["lim_bitrate_kbps"],
            latency_mode=row["lim_latency_mode"],
        ),
    )


@router.post("/start", response_model=StreamSessionStartResponse)
async def start(
    body: StreamSessionStartRequest,
    auth: CurrentAgent,
    pool: PoolDep,
    settings: SettingsDep,
) -> StreamSessionStartResponse:
    async with pool.acquire() as conn, conn.transaction():
        result = await start_session(
            conn,
            settings=settings,
            account_id=auth.account_id,
            camera_id=body.camera_id,
            requested=RequestedStream(
                width=body.requested.width,
                height=body.requested.height,
                fps=body.requested.fps,
                codec=body.requested.codec,
                bitrate_kbps=body.requested.bitrate_kbps,
                latency_mode=body.requested.latency_mode,
            ),
        )

    session_id_var.set(result.session.id)
    return StreamSessionStartResponse(
        session=_row_to_response(result.session),
        srt_url=result.srt_url,
        session_token=result.session_token,
        expires_at=result.claims.expires_at,
    )


@router.post("/{session_id}/stop", response_model=StreamSessionResponse)
async def stop(
    session_id: UUID,
    auth: CurrentAgent,
    pool: PoolDep,
) -> StreamSessionResponse:
    async with pool.acquire() as conn, conn.transaction():
        row = await stop_session(
            conn,
            account_id=auth.account_id,
            session_id=session_id,
        )
    return _row_to_response(row)


@router.get("", response_model=StreamSessionListResponse)
async def list_sessions(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    agent_id: Annotated[UUID | None, Query()] = None,
    camera_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[str | None, Query(max_length=32)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> StreamSessionListResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            select {_SESSION_COLUMNS}
              from stream_sessions
             where account_id = $1
               and ($2::uuid is null or agent_id = $2)
               and ($3::uuid is null or camera_id = $3)
               and ($4::text is null or state::text = $4)
             order by created_at desc
             limit $5
            """,
            scope_account,
            agent_id,
            camera_id,
            state,
            limit,
        )
    sessions = [_row_to_response(_db_row_to_session(r)) for r in rows]
    return StreamSessionListResponse(sessions=sessions)


@router.get("/{session_id}", response_model=StreamSessionResponse)
async def get_session(
    session_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> StreamSessionResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"select {_SESSION_COLUMNS} from stream_sessions where id = $1",
            session_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Session not found.", code="session_not_found")

    return _row_to_response(_db_row_to_session(row))


def _as_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    parsed = json.loads(value)  # type: ignore[arg-type]
    assert isinstance(parsed, dict)
    return parsed


@router.get(
    "/{session_id}/validations",
    response_model=StreamValidationListResponse,
)
async def list_session_validations(
    session_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> StreamValidationListResponse:
    """Validation history for one session. Newest first."""
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from stream_sessions where id = $1",
            session_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Session not found.", code="session_not_found")

        rows = await conn.fetch(
            """
            select id, session_id, passed, observed, expected, mismatch_fields,
                   created_at
              from stream_validations
             where session_id = $1
             order by created_at desc
             limit $2
            """,
            session_id,
            limit,
        )
    validations = [
        StreamValidationResponse(
            id=r["id"],
            session_id=r["session_id"],
            passed=r["passed"],
            observed=_as_dict(r["observed"]),
            expected=_as_dict(r["expected"]),
            mismatch_fields=list(r["mismatch_fields"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]
    return StreamValidationListResponse(validations=validations)
