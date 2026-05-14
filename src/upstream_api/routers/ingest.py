"""MediaMTX ingest webhooks (Phase 2).

These four endpoints are called by the streaming server during a stream's
lifecycle:

  - validate-stream:    SRT connection attempt -> "should I accept this?"
  - stream-started:     handshake + ffprobe done -> session is live
  - stream-ended:       SRT connection closed
  - validation-failed:  ffprobe mismatch / unsupported codec / etc.

All four require the shared webhook secret. Stream identity is carried in
the streamid as a short opaque key (``publish:sess_<hex>``) minted by the
API and stored on the session row; validate-stream looks the session up by
the full streamid value rather than decoding a JWT.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter

from upstream_api.errors import ConflictError
from upstream_api.observability.context import session_id_var
from upstream_api.schemas.ingest import (
    IngestActionResponse,
    StreamEndedRequest,
    StreamStartedRequest,
    ValidateStreamRequest,
    ValidateStreamResponse,
    ValidationFailedRequest,
)
from upstream_api.security.deps import CurrentIngest, PoolDep
from upstream_api.security.streamid import extract_stream_key
from upstream_api.services.stream_validation import (
    load_session_by_streamid,
    load_session_state,
    mark_ended,
    mark_failed,
    mark_streaming,
)
from upstream_api.services.usage import record_event

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
        session = await load_session_by_streamid(conn, body.streamid)

    if session is None:
        return ValidateStreamResponse(
            accept=False,
            reason="Session not found.",
            reason_code="session_not_found",
        )
    if session.state != "approved":
        return ValidateStreamResponse(
            accept=False,
            session_id=session.id,
            reason=f"Session is in state '{session.state}', expected 'approved'.",
            reason_code="stream_state_invalid",
        )
    if session.expires_at < datetime.now(tz=UTC):
        return ValidateStreamResponse(
            accept=False,
            session_id=session.id,
            reason="Session has expired.",
            reason_code="session_expired",
        )

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
    """SRT connection closed; record duration and bytes for usage billing."""
    async with pool.acquire() as conn, conn.transaction():
        state, _duration = await mark_ended(
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
        await record_event(
            conn,
            account_id=session.account_id,
            event_type="stream_seconds",
            session_id=body.session_id,
            quantity=int(body.duration_seconds),
            unit="seconds",
            metadata={
                "bytes_sent": body.bytes_sent,
                "reason": body.reason,
            },
        )
    session_id_var.set(body.session_id)
    return IngestActionResponse(session_id=body.session_id, state=state)


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


__all__ = ["router"]
