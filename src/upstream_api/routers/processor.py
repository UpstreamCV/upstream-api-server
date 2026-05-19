"""M8.0 Processor Readiness Gate.

The safe API contract a future `upstream-processor` (not built yet) uses to
(1) discover only processable streams, (2) report its lifecycle/health, and
(3) post mock observations. Every endpoint is authed by the existing
`current_processor` shared-secret dependency — no new auth mechanism.

M8.0 does NOT decode video and does NOT run DeepStream. It only creates the
contract that M8.1 (mock processor) and M8.2 (DeepStream POC) consume.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.observability.context import session_id_var
from upstream_api.observability.logging import get_logger
from upstream_api.schemas.processor import (
    ApprovedStreamSummary,
    EligibleProcessorSession,
    ProcessorEligibleSessionsResponse,
    ProcessorObservationRequest,
    ProcessorObservationResponse,
    ProcessorStatusRequest,
    ProcessorStatusResponse,
    ProcessorValidationSummary,
)
from upstream_api.security.deps import CurrentProcessor, PoolDep, SettingsDep
from upstream_api.security.streamid import extract_stream_key
from upstream_api.services.processor import (
    coerce_jsonb,
    fetch_eligible_sessions,
    insert_observation,
    parse_iso_datetime,
    upsert_processor_status,
)
from upstream_api.services.stream_validation import load_session_state

_logger = get_logger("processor")

# M8.0 default. A camera may override via cameras.metadata->>'package_id'.
# No package-registry table yet (deferred to a later milestone).
_DEFAULT_PACKAGE_ID = "mock.detect.v0"
# M8.0 advertises a single runtime. Runtime selection (python/deepstream)
# lands with the processor supervisor (M8.5), not here.
_DEFAULT_RUNTIME = "mock"

router = APIRouter(prefix="/v1/internal/processor", tags=["processor"])


def _resolve_package_id(camera_metadata: object) -> str:
    meta = coerce_jsonb(camera_metadata)
    candidate = meta.get("package_id")
    if isinstance(candidate, str) and candidate.strip():
        return candidate
    return _DEFAULT_PACKAGE_ID


@router.get("/eligible-sessions", response_model=ProcessorEligibleSessionsResponse)
async def eligible_sessions(
    _auth: CurrentProcessor,
    pool: PoolDep,
    settings: SettingsDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
) -> ProcessorEligibleSessionsResponse:
    """Sessions safe for a processor to consume.

    A row is returned only if (enforced entirely in SQL — see
    ``services/processor._ELIGIBLE_SESSIONS_SQL``):

    - ``stream_sessions.state = 'streaming'``  (excludes approved / ended /
      expired / failed)
    - ``expires_at > now()``  (excludes a streaming row past its clock TTL)
    - a ``stream_validations`` row exists  (excludes streaming-with-no-
      validation)
    - the **latest** validation has ``passed = true``  (excludes
      streaming-then-failed, even if an older validation passed)

    The response never includes ``srt_url``, tokens, or secrets.
    """
    async with pool.acquire() as conn:
        rows = await fetch_eligible_sessions(conn, limit=limit)

    items: list[EligibleProcessorSession] = []
    for row in rows:
        try:
            stream_key = extract_stream_key(row["streamid"])
        except ValueError:
            # The streamid shape is owned by this server; a malformed value
            # is data corruption, not a client error. Skip the one row
            # rather than 500 the whole discovery call.
            _logger.warning(
                "eligible_session_bad_streamid",
                session_id=str(row["session_id"]),
            )
            continue

        items.append(
            EligibleProcessorSession(
                session_id=row["session_id"],
                stream_key=stream_key,
                camera_id=row["camera_id"],
                account_id=row["account_id"],
                internal_rtsp_url=f"{settings.mediamtx_rtsp_base}/{stream_key}",
                package_id=_resolve_package_id(row["camera_metadata"]),
                runtime=_DEFAULT_RUNTIME,
                approved=ApprovedStreamSummary(
                    codec=row["lim_codec"],
                    width=row["lim_width"],
                    height=row["lim_height"],
                    fps=row["lim_fps"],
                    bitrate_kbps=row["lim_bitrate_kbps"],
                ),
                validation=ProcessorValidationSummary(
                    passed=row["v_passed"],
                    observed=coerce_jsonb(row["v_observed"]),
                    expected=coerce_jsonb(row["v_expected"]),
                    mismatch_fields=list(row["v_mismatch_fields"]),
                    created_at=parse_iso_datetime(row["v_created_at"]),
                ),
                started_at=parse_iso_datetime(row["started_at"]),
            )
        )

    return ProcessorEligibleSessionsResponse(sessions=items)


@router.post("/status", response_model=ProcessorStatusResponse)
async def report_status(
    body: ProcessorStatusRequest,
    _auth: CurrentProcessor,
    pool: PoolDep,
) -> ProcessorStatusResponse:
    """Upsert the current status of one processor working one session.

    The session must exist; ``account_id`` / ``camera_id`` are derived from
    the session row, never trusted from the request. An invalid ``status``
    is rejected at the schema layer (422)."""
    async with pool.acquire() as conn, conn.transaction():
        session = await load_session_state(conn, body.stream_session_id)
        if session is None:
            raise NotFoundError("Session not found.", code="session_not_found")
        session_id_var.set(session.id)
        row = await upsert_processor_status(
            conn,
            account_id=session.account_id,
            camera_id=session.camera_id,
            req=body,
        )

    return ProcessorStatusResponse(
        id=row["id"],
        account_id=row["account_id"],
        camera_id=row["camera_id"],
        stream_session_id=row["stream_session_id"],
        processor_id=row["processor_id"],
        runtime=row["runtime"],
        package_id=row["package_id"],
        status=row["status"],
        created_at=parse_iso_datetime(row["created_at"]),
        updated_at=parse_iso_datetime(row["updated_at"]),
    )


@router.post("/observations", response_model=ProcessorObservationResponse)
async def post_observation(
    body: ProcessorObservationRequest,
    _auth: CurrentProcessor,
    pool: PoolDep,
) -> ProcessorObservationResponse:
    """Append one mock observation.

    The session must exist; ``account_id`` / ``camera_id`` are derived from
    the session row. M8.0 only accepts the three ``processor.mock_*`` types
    (rejected at the schema layer otherwise, 422)."""
    async with pool.acquire() as conn, conn.transaction():
        session = await load_session_state(conn, body.stream_session_id)
        if session is None:
            raise NotFoundError("Session not found.", code="session_not_found")
        session_id_var.set(session.id)
        new_id = await insert_observation(
            conn,
            account_id=session.account_id,
            camera_id=session.camera_id,
            req=body,
        )

    return ProcessorObservationResponse(id=new_id, count=1)
