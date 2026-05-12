from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter

from upstream_api.errors import NotFoundError
from upstream_api.observability.context import session_id_var
from upstream_api.schemas.stream_sessions import (
    StreamSessionResponse,
    StreamSessionStartRequest,
    StreamSessionStartResponse,
)
from upstream_api.security.deps import CurrentAgent, PoolDep, SettingsDep
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
        },
        approved={
            "width": row.approved.width,
            "height": row.approved.height,
            "fps": row.approved.fps,
            "codec": row.approved.codec,
            "bitrate_kbps": row.approved.bitrate_kbps,
        },
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


@router.get("/{session_id}", response_model=StreamSessionResponse)
async def get_session(
    session_id: UUID,
    auth: CurrentAgent,
    pool: PoolDep,
) -> StreamSessionResponse:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select id, account_id, agent_id, camera_id, state, srt_url, streamid,
                   expires_at, started_at, ended_at,
                   req_width, req_height, req_fps, req_codec, req_bitrate_kbps,
                   lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps
              from stream_sessions
             where id = $1
            """,
            session_id,
        )
    if row is None or row["account_id"] != auth.account_id:
        raise NotFoundError("Session not found.", code="session_not_found")

    streamsession = StreamSessionRow(
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
        ),
        approved=LimitsSnapshot(
            width=row["lim_width"],
            height=row["lim_height"],
            fps=row["lim_fps"],
            codec=row["lim_codec"],
            bitrate_kbps=row["lim_bitrate_kbps"],
        ),
    )
    return _row_to_response(streamsession)
