from __future__ import annotations

import asyncio
import contextlib
import json
import re
import time
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response

from upstream_api.errors import (
    BadGatewayError,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    LimitExceeded,
    NotFoundError,
    UnsupportedMediaTypeError,
)
from upstream_api.observability.context import session_id_var
from upstream_api.observability.logging import get_logger
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
from upstream_api.security.deps import (
    CurrentAgent,
    CurrentCaller,
    PoolDep,
    SettingsDep,
    require_caller_agent_access,
)
from upstream_api.security.streamid import extract_stream_key
from upstream_api.services.account_audit import try_record_account_audit_log
from upstream_api.services.http_client import HttpClientDep
from upstream_api.services.limits import LimitsSnapshot, RequestedStream
from upstream_api.services.mediamtx_provisioning import (
    MediaMTXProvisioningFailure,
    deprovision_path,
    provision_path,
)
from upstream_api.services.pull_provisioning import get_provision_state
from upstream_api.services.session_lifecycle import (
    StreamSessionRow,
    start_session,
    stop_session,
)
from upstream_api.services.stream_validation import mark_failed
from upstream_api.services.whep_proxy import (
    UpstreamMediaMTXUnavailable,
    proxy_whep_delete,
    proxy_whep_offer,
)

_provisionlog = get_logger("stream_sessions.mediamtx_provisioning")

# Resource id format on DELETE — comes from MediaMTX, so we deliberately accept
# any printable URL-safe id. The regex's job is to refuse path-traversal
# (slashes, dots, encoded characters) before we interpolate it into a URL.
_RESOURCE_ID_RE = re.compile(r"^[A-Za-z0-9_~-]{1,128}$")
_STREAMID_PUBLISH_PREFIX = "publish:"

router = APIRouter(prefix="/v1/stream-sessions", tags=["stream-sessions"])


async def _await_pull_provisioning(
    pool: Any,
    *,
    session_id: UUID,
    timeout_seconds: float,
    poll_interval: float,
) -> None:
    """Block until the streaming box acks the session's MediaMTX path.

    Pull mode's whole point is that the api-server never dials into the box's
    network. The cost is that provisioning becomes asynchronous, and the
    firmware's `/start` contract is synchronous. Rather than change the
    contract (which would mean an OTA to every Pi in the field), we absorb the
    asynchrony here: the caller still gets a usable `srt_url` or a real error.

    Terminates on whichever comes first:
      - the box acked            -> return, the path exists
      - the row went `failed`    -> 502 carrying the box's own reason
      - the budget expired       -> mark failed, 502

    The timeout branch marks the session `failed` so an ack that lands late
    cannot resurrect it — otherwise the box would create a path for a session
    the agent has already abandoned, which is precisely the orphan-path leak
    that left ten stale `sess_*` entries behind in the 2026-08 incident.
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        async with pool.acquire() as conn:
            current = await get_provision_state(conn, session_id=session_id)

        if current is None:
            raise NotFoundError("Session not found.", code="session_not_found")

        state, provisioned = current
        if provisioned:
            return
        if state != "approved":
            # The box acked a failure (or a sweeper terminated the row). The
            # ack handler already wrote failure_code/message; don't overwrite.
            _provisionlog.error(
                "mediamtx_provision_failed_pull_observed",
                session_id=str(session_id),
                state=state,
            )
            raise BadGatewayError(
                "Streaming server could not provision the path; session marked failed.",
                code="upstream_unavailable",
            )

        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(poll_interval)

    _provisionlog.error(
        "mediamtx_provision_timeout_pull",
        session_id=str(session_id),
        waited_seconds=timeout_seconds,
    )
    async with pool.acquire() as conn2, conn2.transaction():
        failed_state = await mark_failed(
            conn2,
            session_id=session_id,
            observed=None,
            mismatch_fields=[],
            failure_code="mediamtx_provision_failed",
            failure_message=(
                f"reason=provision_timeout waited={timeout_seconds:.1f}s "
                "(no streaming-box ack)"
            )[:512],
        )
    failed_state.transition.record()
    raise BadGatewayError(
        "Streaming server did not provision the path in time; session marked failed.",
        code="upstream_unavailable",
    )


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
    http: HttpClientDep,
) -> StreamSessionStartResponse:
    # A device-scoped token must be bound (register-first) and may only
    # stream cameras assigned to its bound agent; start_session enforces the
    # camera↔agent match once it has the camera row in hand.
    if auth.is_device_scoped and auth.agent_id is None:
        raise ForbiddenError(
            "Device token is not bound to an agent yet; call /v1/agents/register first.",
            code="device_token_unbound",
        )
    try:
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
                required_agent_id=auth.agent_id if auth.is_device_scoped else None,
            )
    except LimitExceeded as exc:
        # The rejected transaction rolled back, so the audit row needs its
        # own connection. Best-effort: the rejection is returned either way.
        async with pool.acquire() as audit_conn:
            await try_record_account_audit_log(
                audit_conn,
                account_id=auth.account_id,
                actor_type="system",
                event_type="stream_session.limit_rejected",
                resource_type="stream_session",
                resource_id=None,
                summary=f"Stream session start rejected: {exc.code}.",
                metadata={
                    "limit_code": exc.code,
                    "camera_id": str(body.camera_id),
                    **(exc.details or {}),
                },
            )
        raise

    session_id_var.set(result.session.id)
    # Metric facts record only after the transaction above committed —
    # a rollback must not count a transition (see TransitionRecord).
    result.transition.record()

    # Phase 2 / FU-6 — provision a dynamic MediaMTX path with the session's
    # SRT passphrase. Must happen AFTER the DB transaction commits so a
    # MediaMTX failure can't roll back the session row (we keep failed
    # rows around for the audit trail). On failure we mark the session
    # `failed` in a separate transaction and return 502; the agent will
    # retry `/start` and get a fresh attempt.
    stream_key = extract_stream_key(result.session.streamid)

    if settings.mediamtx_provisioning_mode == "pull":
        # Pull mode: the row is already the work order (provisioned_at IS
        # NULL). The box collects it on its own outbound poll and configures
        # its local MediaMTX, so we make no inbound call at all.
        #
        # We still BLOCK here until the box acks, which keeps this endpoint's
        # wire contract byte-identical to push mode: the agent receives either
        # a working srt_url or a clean error. That is what lets pull mode ship
        # without an OTA to the fleet — firmware behaviour is unchanged.
        await _await_pull_provisioning(
            pool,
            session_id=result.session.id,
            timeout_seconds=settings.provision_wait_seconds,
            poll_interval=settings.provision_poll_interval_seconds,
        )
        return StreamSessionStartResponse(
            session=_row_to_response(result.session),
            srt_url=result.srt_url,
            session_token=result.session_token,
            expires_at=result.claims.expires_at,
        )

    try:
        await provision_path(
            http,
            base_url=settings.mediamtx_api_base_url,
            path=stream_key,
            passphrase=result.srt_publish_passphrase,
        )
    except MediaMTXProvisioningFailure as exc:
        _provisionlog.error(
            "mediamtx_provision_failed",
            session_id=str(result.session.id),
            reason=exc.reason,
            status=exc.status,
        )
        async with pool.acquire() as conn2, conn2.transaction():
            failed_state = await mark_failed(
                conn2,
                session_id=result.session.id,
                observed=None,
                mismatch_fields=[],
                failure_code="mediamtx_provision_failed",
                failure_message=f"reason={exc.reason} status={exc.status}"[:512],
            )
        failed_state.transition.record()
        raise BadGatewayError(
            "Streaming server is unreachable; session marked failed.",
            code="upstream_unavailable",
        ) from exc

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
    settings: SettingsDep,
    http: HttpClientDep,
) -> StreamSessionResponse:
    if auth.is_device_scoped and auth.agent_id is None:
        raise ForbiddenError(
            "Device token is not bound to an agent yet; call /v1/agents/register first.",
            code="device_token_unbound",
        )
    async with pool.acquire() as conn, conn.transaction():
        row = await stop_session(
            conn,
            account_id=auth.account_id,
            session_id=session_id,
            required_agent_id=auth.agent_id if auth.is_device_scoped else None,
        )
    # After COMMIT — a rollback must not count the transition.
    row.transition.record()

    # Best-effort deprovision of the per-session MediaMTX path (Phase 2).
    # MediaMTX may be down or have already dropped the path on its own
    # restart — neither failure should cascade into the agent-facing
    # response. We log and continue.
    stream_key = extract_stream_key(row.streamid)
    try:
        await deprovision_path(
            http,
            base_url=settings.mediamtx_api_base_url,
            path=stream_key,
        )
    except MediaMTXProvisioningFailure as exc:
        _provisionlog.warning(
            "mediamtx_deprovision_failed",
            session_id=str(row.id),
            reason=exc.reason,
            status=exc.status,
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
            "select account_id, agent_id, state, streamid from stream_sessions where id = $1",
            session_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Session not found.", code="session_not_found")
    # Device-scoped tokens may only play back their own agent's sessions.
    require_caller_agent_access(caller, row["agent_id"])
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
            "select account_id, agent_id, streamid from stream_sessions where id = $1",
            session_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Session not found.", code="session_not_found")
    # Same binding rule as the offer path above.
    require_caller_agent_access(caller, row["agent_id"])

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
