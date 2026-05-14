"""MediaMTX ingest webhooks (Phase 2).

These endpoints are called by the streaming server during a stream's
lifecycle:

  - validate-stream:    SRT connection attempt -> "should I accept this?"
  - stream-started:     handshake + ffprobe done -> session is live
  - stream-ended:       SRT connection closed
  - validation-failed:  ffprobe mismatch / unsupported codec / etc.
  - lookup-session:     streamid -> session_id (read-only resolver)
  - mediamtx-auth:      MediaMTX 1.9.3 authHTTP deny gate (M4.1)

Most use ``X-Upstream-Webhook-Secret`` header auth via ``CurrentIngest``.
``mediamtx-auth`` is the exception: MediaMTX 1.9.3 cannot send custom
headers, so the secret rides in the URL path segment.
"""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import APIRouter, Path
from fastapi.responses import Response

from upstream_api.errors import ConflictError, NotFoundError
from upstream_api.observability.context import session_id_var
from upstream_api.observability.logging import get_logger
from upstream_api.schemas.ingest import (
    IngestActionResponse,
    LookupSessionRequest,
    LookupSessionResponse,
    MediaMTXAuthRequest,
    StreamEndedRequest,
    StreamStartedRequest,
    ValidateStreamRequest,
    ValidateStreamResponse,
    ValidationFailedRequest,
)
from upstream_api.security.deps import CurrentIngest, PoolDep, SettingsDep
from upstream_api.security.shared_secret import verify_shared_secret
from upstream_api.security.streamid import (
    build_streamid,
    extract_stream_key,
    is_valid_stream_key,
)
from upstream_api.services.stream_validation import (
    check_session_admit,
    load_session_by_streamid,
    load_session_state,
    mark_ended,
    mark_failed,
    mark_streaming,
)
from upstream_api.services.usage import record_event

_authlog = get_logger("ingest.mediamtx_auth")

# Streamid query forms the streaming server may fall back to if MediaMTX
# omits ``path`` for some future quirk. Compiled at module load to keep the
# hot path allocation-free.
_STREAMID_QUERY_FORM = re.compile(r"streamid=publish:(sess_[0-9a-f]{16})")
_STREAMID_BARE_FORM = re.compile(r"publish:(sess_[0-9a-f]{16})")

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])


@router.post("/validate-stream", response_model=ValidateStreamResponse)
async def validate_stream(
    body: ValidateStreamRequest,
    _ingest: CurrentIngest,
    pool: PoolDep,
) -> ValidateStreamResponse:
    """Synchronous preflight: should this streamid be allowed to publish?

    Confirms the streamid matches an active session row that's still in the
    `approved` state and not expired. Returns the approved parameters as
    `expected`; the streaming server must enforce these against what ffprobe
    actually observes.
    """
    try:
        extract_stream_key(body.streamid)
    except ValueError:
        return ValidateStreamResponse(
            accept=False, reason="Malformed streamid.", reason_code="streamid_malformed"
        )

    async with pool.acquire() as conn:
        session, reason_code = await check_session_admit(conn, body.streamid)

    if reason_code == "session_not_found":
        return ValidateStreamResponse(
            accept=False,
            reason="Session not found.",
            reason_code="session_not_found",
        )
    if reason_code == "stream_state_invalid":
        assert session is not None  # helper contract: row returned on this reason
        return ValidateStreamResponse(
            accept=False,
            session_id=session.id,
            reason=f"Session is in state '{session.state}', expected 'approved'.",
            reason_code="stream_state_invalid",
        )
    if reason_code == "session_expired":
        assert session is not None
        return ValidateStreamResponse(
            accept=False,
            session_id=session.id,
            reason="Session has expired.",
            reason_code="session_expired",
        )
    # accept path
    assert session is not None
    session_id_var.set(session.id)
    expected = {
        "codec": session.lim_codec,
        "width": session.lim_width,
        "height": session.lim_height,
        "fps": session.lim_fps,
        "bitrate_kbps": session.lim_bitrate_kbps,
        "latency_mode": session.lim_latency_mode,
    }
    return ValidateStreamResponse(accept=True, session_id=session.id, expected=expected)


@router.post("/stream-started", response_model=IngestActionResponse)
async def stream_started(
    body: StreamStartedRequest,
    _ingest: CurrentIngest,
    pool: PoolDep,
) -> IngestActionResponse:
    """Streaming server finished ffprobe and is now relaying the stream."""
    observed = body.observed.model_dump()
    async with pool.acquire() as conn, conn.transaction():
        state = await mark_streaming(
            conn,
            session_id=body.session_id,
            observed=observed,
            started_at=body.started_at,
        )
    session_id_var.set(body.session_id)
    return IngestActionResponse(session_id=body.session_id, state=state)


@router.post("/stream-ended", response_model=IngestActionResponse)
async def stream_ended(
    body: StreamEndedRequest,
    _ingest: CurrentIngest,
    pool: PoolDep,
) -> IngestActionResponse:
    """SRT connection closed; record duration and bytes for usage billing.

    Server-derived duration (from the session row's ``started_at``) wins over
    any value the caller supplies, since the DB is the source of truth and
    streaming-server hooks generally cannot track started_at across script
    invocations.
    """
    async with pool.acquire() as conn, conn.transaction():
        state, server_duration = await mark_ended(
            conn,
            session_id=body.session_id,
            ended_at=body.ended_at,
            reason=body.reason,
        )
        session = await load_session_state(conn, body.session_id)
        if session is None:
            raise ConflictError(
                "Session disappeared mid-write.",
                code="stream_state_invalid",
                details={"session_id": str(body.session_id)},
            )
        if server_duration is not None:
            quantity = int(server_duration)
        elif body.duration_seconds is not None:
            quantity = int(body.duration_seconds)
        else:
            quantity = 0
        await record_event(
            conn,
            account_id=session.account_id,
            event_type="stream_seconds",
            session_id=body.session_id,
            quantity=quantity,
            unit="seconds",
            metadata={
                "bytes_sent": body.bytes_sent,
                "reason": body.reason,
            },
        )
    session_id_var.set(body.session_id)
    return IngestActionResponse(session_id=body.session_id, state=state)


@router.post("/lookup-session", response_model=LookupSessionResponse)
async def lookup_session(
    body: LookupSessionRequest,
    _ingest: CurrentIngest,
    pool: PoolDep,
) -> LookupSessionResponse:
    """Resolve a streamid to its session identifiers.

    Used by streaming-server lifecycle hooks (``runOnReady`` /
    ``runOnNotReady``) to translate the SRT streamid they receive
    (``publish:sess_<hex>``) into the ``session_id`` they need to call
    ``stream-started`` / ``stream-ended``. Read-only and idempotent —
    independent from ``validate-stream``, which gates connection acceptance
    and only succeeds while the session is still ``approved``.
    """
    async with pool.acquire() as conn:
        session = await load_session_by_streamid(conn, body.streamid)
    if session is None:
        raise NotFoundError("Session not found.", code="session_not_found")
    session_id_var.set(session.id)
    return LookupSessionResponse(
        session_id=session.id,
        account_id=session.account_id,
        agent_id=session.agent_id,
        camera_id=session.camera_id,
        state=session.state,
    )


@router.post("/validation-failed", response_model=IngestActionResponse)
async def validation_failed(
    body: ValidationFailedRequest,
    _ingest: CurrentIngest,
    pool: PoolDep,
) -> IngestActionResponse:
    """ffprobe (or post-validation) rejected the stream; mark `failed`."""
    observed = body.observed.model_dump() if body.observed is not None else None
    async with pool.acquire() as conn, conn.transaction():
        state = await mark_failed(
            conn,
            session_id=body.session_id,
            observed=observed,
            mismatch_fields=list(body.mismatch_fields),
            failure_code=body.failure_code,
            failure_message=body.failure_message,
        )
    session_id_var.set(body.session_id)
    return IngestActionResponse(session_id=body.session_id, state=state)


def _redact_key(stream_key: str) -> str:
    """Truncate a stream key for log lines.

    Returns the first 12 chars (``sess_`` + 7 hex) plus ``…`` — enough for
    debugging which session a decision refers to, while still hiding 9 hex
    chars (~36 bits) of entropy from log readers.
    """
    return stream_key[:12] + "…" if len(stream_key) > 12 else stream_key


def _extract_stream_key(body: MediaMTXAuthRequest) -> str | None:
    """Pull the stream key from the MediaMTX payload.

    Primary source is ``body.path`` — MediaMTX 1.9.3 has already stripped
    the ``publish:`` action prefix for us, so for a valid SRT publish the
    path is exactly the ``sess_<hex>`` key. ``body.query`` is empty in the
    M4.1 verified behavior but we accept ``streamid=publish:<key>`` and the
    bare ``publish:<key>`` forms as a defensive fallback.
    """
    if body.path:
        return body.path
    if body.query:
        m = _STREAMID_QUERY_FORM.search(body.query)
        if m is not None:
            return m.group(1)
        m = _STREAMID_BARE_FORM.search(body.query)
        if m is not None:
            return m.group(1)
    return None


def _deny(reason_code: str, **fields: str) -> Response:
    """Log the deny decision and return an empty 401.

    MediaMTX 1.9.3 inspects only the status code (per
    ``mediamtx.yml.default:97-98``), so no body is sent.
    """
    _authlog.warning("mediamtx_auth_denied", reason_code=reason_code, **fields)
    return Response(status_code=401)


@router.post("/mediamtx-auth/{secret}", status_code=200, response_model=None)
async def mediamtx_auth(
    secret: Annotated[str, Path(min_length=1, max_length=512)],
    body: MediaMTXAuthRequest,
    settings: SettingsDep,
    pool: PoolDep,
) -> Response:
    """MediaMTX 1.9.3 authHTTP gate for the SRT publish deny check.

    Path-segment secret because MediaMTX 1.9.3 cannot send custom headers
    (``authHTTPHeaders`` is 1.11+). Returns **200 empty** on allow and
    **401 empty** on deny — MediaMTX inspects only the status code, so a
    JSON envelope would be ignored.

    Decision pipeline (fail-closed): secret check, ``action == "publish"``,
    ``protocol == "srt"``, stream key extracted from ``path`` (fallback:
    ``query``) and shape-validated, then handed to ``check_session_admit``
    for the same state/expiry rules as ``validate-stream``.

    Logging: only redacted ``stream_key``, ``action``, ``protocol``,
    ``reason_code``, and resolved ids. Never the presented secret, the
    URL, the full key, or the password field.
    """
    if not verify_shared_secret(secret, settings.mediamtx_webhook_secret.get_secret_value()):
        # Do NOT include the presented value in the log line.
        _authlog.warning("mediamtx_auth_denied", reason_code="invalid_webhook_secret")
        return Response(status_code=401)

    if body.action != "publish":
        return _deny("action_not_allowed", action=body.action)
    if body.protocol != "srt":
        return _deny("protocol_not_allowed", protocol=body.protocol)

    stream_key = _extract_stream_key(body)
    if stream_key is None or not is_valid_stream_key(stream_key):
        return _deny("streamid_malformed", action=body.action, protocol=body.protocol)

    streamid = build_streamid(stream_key)
    try:
        async with pool.acquire() as conn:
            session, reason_code = await check_session_admit(conn, streamid)
    except Exception:
        # Fail-closed on any DB / pool error. exception() captures the
        # traceback into the structured log without leaking the streamid.
        _authlog.exception(
            "mediamtx_auth_denied",
            reason_code="internal_error",
            stream_key=_redact_key(stream_key),
        )
        return Response(status_code=401)

    if reason_code is not None:
        # `session` is None only for ``session_not_found``; the other reject
        # reasons return the row alongside, so we can log its id.
        fields = {
            "action": body.action,
            "protocol": body.protocol,
            "stream_key": _redact_key(stream_key),
        }
        if session is not None:
            fields["session_id"] = str(session.id)
        return _deny(reason_code, **fields)

    assert session is not None
    session_id_var.set(session.id)
    _authlog.info(
        "mediamtx_auth_allowed",
        action=body.action,
        protocol=body.protocol,
        stream_key=_redact_key(stream_key),
        session_id=str(session.id),
        agent_id=str(session.agent_id),
        camera_id=str(session.camera_id),
        account_id=str(session.account_id),
    )
    return Response(status_code=200)


__all__ = ["router"]
