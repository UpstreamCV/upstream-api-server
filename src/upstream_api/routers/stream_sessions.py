from __future__ import annotations

import contextlib
import json
import re
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response

from upstream_api.errors import (
    BadGatewayError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    UnsupportedMediaTypeError,
)
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
from upstream_api.services.http_client import HttpClientDep
from upstream_api.services.limits import LimitsSnapshot, RequestedStream
from upstream_api.services.session_lifecycle import (
    StreamSessionRow,
    start_session,
    stop_session,
)
from upstream_api.services.whep_proxy import (
    UpstreamMediaMTXUnavailable,
    proxy_whep_delete,
    proxy_whep_offer,
)

# Resource id format on DELETE — comes from MediaMTX, so we deliberately accept
# any printable URL-safe id. The regex's job is to refuse path-traversal
# (slashes, dots, encoded characters) before we interpolate it into a URL.
_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9_~-]{1,128}$")
_STREAMID_PUBLISH_PREFIX = "publish:"

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
        created_at=row.created_at,
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
    "expires_at, created_at, started_at, ended_at, "
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
        created_at=row["created_at"],
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


# ---------------------------------------------------------------------------
# WHEP proxy — read-path auth for browser WebRTC playback.
#
# The dashboard's <LiveStreamPlayer> POSTs SDP offers here instead of hitting
# MediaMTX directly. We authenticate the caller (existing CurrentCaller dep),
# verify they're a member of the session's account, then forward the offer to
# MediaMTX over a private connection. MediaMTX itself must be firewalled in
# production — it has no auth on the read path.
# ---------------------------------------------------------------------------


def _mediamtx_path_from_streamid(streamid: str) -> str | None:
    """Return the MediaMTX path for a session, or None on a malformed streamid."""
    if not streamid.startswith(_STREAMID_PUBLISH_PREFIX):
        return None
    tail = streamid[len(_STREAMID_PUBLISH_PREFIX) :]
    # Defensive: reject any character that could escape the URL path segment.
    if not tail or "/" in tail or ".." in tail:
        return None
    return tail


@router.post(
    "/{session_id}/whep",
    status_code=201,
    responses={
        201: {"content": {"application/sdp": {}}, "description": "SDP answer"},
        404: {"description": "Session not found or stream not yet available"},
        409: {"description": "Session is not currently streaming"},
        415: {"description": "Content-Type must be application/sdp"},
        502: {"description": "Streaming server unreachable"},
    },
)
async def whep_offer(
    session_id: UUID,
    request: Request,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> Response:
    """Forward an SDP offer to MediaMTX after auth + ownership checks."""
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/sdp":
        raise UnsupportedMediaTypeError(
            "Content-Type must be application/sdp.",
            code="invalid_content_type",
        )

    sdp_offer = await request.body()
    if not sdp_offer:
        raise BadRequestError("Empty SDP offer.", code="invalid_sdp")

    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "select account_id, state, streamid from stream_sessions where id = $1",
            session_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Session not found.", code="session_not_found")
    if row["state"] != "streaming":
        raise ConflictError(
            "Session is not currently streaming.",
            code="session_not_streaming",
        )

    path = _mediamtx_path_from_streamid(row["streamid"])
    if path is None:
        # The session row's streamid format is owned by this server; an
        # unexpected shape means data corruption, not a client error.
        raise BadGatewayError(
            "Streaming server is misconfigured for this session.",
            code="upstream_unavailable",
        )

    try:
        result = await proxy_whep_offer(
            http,
            mediamtx_base_url=settings.mediamtx_base_url,
            path=path,
            sdp_offer=sdp_offer,
        )
    except UpstreamMediaMTXUnavailable as exc:
        raise BadGatewayError(
            "Streaming server is unreachable.",
            code="upstream_unavailable",
        ) from exc

    if result.status == 404:
        # Common race: api-server has marked the session streaming a beat
        # before MediaMTX registers the publisher's path. Dashboard retries.
        raise NotFoundError(
            "Stream not yet available; retry shortly.",
            code="stream_not_ready",
        )
    if not 200 <= result.status < 300:
        raise BadGatewayError(
            f"Streaming server rejected the offer (HTTP {result.status}).",
            code="upstream_unavailable",
        )
    if result.resource_id is None:
        raise BadGatewayError(
            "Streaming server returned an unparseable response.",
            code="upstream_unavailable",
        )

    response = Response(
        content=result.body,
        media_type="application/sdp",
        status_code=201,
    )
    # Bare resource id — the dashboard's whep-client appends it to the POST
    # URL to build the DELETE target. Avoids encoding the proxy prefix here.
    response.headers["Location"] = result.resource_id
    return response


@router.delete(
    "/{session_id}/whep/{resource_id}",
    status_code=204,
)
async def whep_delete(
    session_id: UUID,
    resource_id: str,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> Response:
    """Tear down a WHEP session at MediaMTX. Best-effort."""
    if not _RESOURCE_ID_RE.match(resource_id):
        raise BadRequestError("Invalid resource id.", code="invalid_resource_id")

    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "select account_id, streamid from stream_sessions where id = $1",
            session_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Session not found.", code="session_not_found")

    path = _mediamtx_path_from_streamid(row["streamid"])
    if path is None:
        # Nothing to clean up server-side.
        return Response(status_code=204)

    # Best-effort teardown. MediaMTX's ICE timeout reaps orphans on transport
    # failure, so a 502 here would be noise on the dashboard side.
    with contextlib.suppress(UpstreamMediaMTXUnavailable):
        await proxy_whep_delete(
            http,
            mediamtx_base_url=settings.mediamtx_base_url,
            path=path,
            resource_id=resource_id,
        )

    return Response(status_code=204)
