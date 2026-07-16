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
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.observability.context import session_id_var
from upstream_api.observability.logging import get_logger
from upstream_api.schemas.processor import (
    ApprovedStreamSummary,
    EligibleGeometryConfig,
    EligibleProcessingConfig,
    EligibleProcessorSession,
    LatestPerceptionSnapshotResponse,
    PerceptionSnapshotIngestRequest,
    PerceptionSnapshotRecord,
    ProcessorEligibleSessionsResponse,
    ProcessorObservationRequest,
    ProcessorObservationResponse,
    ProcessorStatusRequest,
    ProcessorStatusResponse,
    ProcessorValidationSummary,
)
from upstream_api.security.deps import CurrentProcessor, PoolDep, SettingsDep
from upstream_api.security.streamid import extract_stream_key
from upstream_api.services.detection_metrics import (
    write_detection_metrics_for_perception_snapshot,
)
from upstream_api.services.processor import (
    coerce_jsonb,
    fetch_eligible_sessions,
    insert_observation,
    parse_iso_datetime,
    upsert_perception_snapshot,
    upsert_processor_status,
)
from upstream_api.services.rules import evaluate_observation
from upstream_api.services.stream_validation import SessionStateRow, load_session_state
from upstream_api.services.workflow_sequencer import evaluate_sequences

_logger = get_logger("processor")

# Runtime selection (python/deepstream/crowdcount) is done by the processor
# supervisor from the package_id via its package registry, not here — this
# field is vestigial (kept for the wire contract).
_DEFAULT_RUNTIME = "mock"

router = APIRouter(prefix="/v1/internal/processor", tags=["processor"])


def _resolve_package_id(camera_metadata: object) -> str | None:
    """The camera's assigned processing package, or None when it has none.

    A camera with no package has nothing for a processor to do, so it is
    excluded from eligible-sessions rather than defaulted to a placeholder
    (mock) detector.
    """
    meta = coerce_jsonb(camera_metadata)
    candidate = meta.get("package_id")
    if isinstance(candidate, str) and candidate.strip():
        return candidate
    return None


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

        package_id = _resolve_package_id(row["camera_metadata"])
        if package_id is None:
            # No package assigned -> nothing for a processor to do. Excluded
            # (previously defaulted to a mock placeholder detector, which ran a
            # fake detector on every unconfigured camera).
            continue

        items.append(
            EligibleProcessorSession(
                session_id=row["session_id"],
                stream_key=stream_key,
                camera_id=row["camera_id"],
                account_id=row["account_id"],
                external_id=row["external_id"],
                internal_rtsp_url=f"{settings.mediamtx_rtsp_base}/{stream_key}",
                package_id=package_id,
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
                geometry_config=(
                    EligibleGeometryConfig(
                        revision=row["gc_revision"],
                        config=coerce_jsonb(row["gc_config"]),
                    )
                    if row["gc_revision"] is not None
                    else None
                ),
                processing_config=(
                    EligibleProcessingConfig(
                        revision=row["pc_revision"],
                        config=coerce_jsonb(row["pc_config"]),
                    )
                    if row["pc_revision"] is not None
                    else None
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


async def _evaluate_rules_safe(
    conn: asyncpg.Connection,
    *,
    session: SessionStateRow,
    body: ProcessorObservationRequest,
    observation_id: UUID,
) -> None:
    """Evaluate user-authored rules for the just-ingested observation
    (Rules/Triggers M1).

    Best-effort alerting: this must NEVER fail the observation ingest. It
    runs in its own savepoint so any failure — including the rules/alerts
    tables not existing yet (before the migration applies) — rolls back only
    the alert writes and leaves the observation insert that already ran in
    the parent transaction intact. We log the failure with scalar ids only
    (never the observation payload or any secret).
    """
    try:
        async with conn.transaction():
            await evaluate_observation(
                conn,
                account_id=session.account_id,
                camera_id=session.camera_id,
                observation_id=observation_id,
                observation_type=body.type,
                payload=body.payload,
                observed_at=body.observed_at,
            )
    except Exception as exc:
        _logger.warning(
            "rule_evaluation_failed",
            camera_id=str(session.camera_id),
            error=type(exc).__name__,
        )


async def _evaluate_sequences_safe(
    conn: asyncpg.Connection,
    *,
    session: SessionStateRow,
    body: ProcessorObservationRequest,
    observation_id: UUID,
) -> None:
    """Workflow Engine M5: run the two-step sequencer for the
    just-ingested observation. Same savepoint contract as the rule
    evaluator — a failing (or not-yet-migrated) sequencer can NEVER fail
    observation ingest, and its own savepoint means a sequencer failure
    doesn't roll back rule evaluation (or vice versa)."""
    try:
        async with conn.transaction():
            await evaluate_sequences(
                conn,
                account_id=session.account_id,
                camera_id=session.camera_id,
                observation_id=observation_id,
                observation_type=body.type,
                payload=body.payload,
                observed_at=body.observed_at,
            )
    except Exception as exc:
        _logger.warning(
            "sequence_evaluation_failed",
            camera_id=str(session.camera_id),
            error=type(exc).__name__,
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
        # Rules/Triggers M1: evaluate user-authored rules against this
        # observation. Best-effort — isolated in a savepoint so it can never
        # break observation ingest.
        await _evaluate_rules_safe(
            conn,
            session=session,
            body=body,
            observation_id=new_id,
        )
        # Workflow Engine M5: two-step sequences, in their own savepoint.
        await _evaluate_sequences_safe(
            conn,
            session=session,
            body=body,
            observation_id=new_id,
        )

    return ProcessorObservationResponse(id=new_id, count=1)


async def _write_detection_metrics_safe(
    conn: asyncpg.Connection,
    *,
    session: SessionStateRow,
    body: PerceptionSnapshotIngestRequest,
    perception_snapshot_id: UUID,
) -> None:
    """Persist a compact detection-confidence summary for the snapshot (M5).

    Best-effort reliability analytics: this must NEVER fail the snapshot
    ingest. It runs in its own savepoint so any failure — including the M5
    ``snapshot_detection_metrics`` table not existing yet (before the migration
    applies) — rolls back only the metric write and leaves the snapshot upsert
    that already ran in the parent transaction intact. We log the failure with
    scalar ids only (never the snapshot payload or any secret).
    """
    try:
        async with conn.transaction():
            await write_detection_metrics_for_perception_snapshot(
                conn,
                account_id=session.account_id,
                camera_id=session.camera_id,
                stream_session_id=body.stream_session_id,
                perception_snapshot_id=perception_snapshot_id,
                processor_id=body.processor_id,
                package_id=body.package_id,
                observed_at=body.observed_at,
                snapshot=body.snapshot,
            )
    except Exception as exc:
        _logger.warning(
            "detection_metric_write_failed",
            camera_id=str(session.camera_id),
            error=type(exc).__name__,
        )


@router.post(
    "/perception-snapshots",
    response_model=LatestPerceptionSnapshotResponse,
)
async def post_perception_snapshot(
    body: PerceptionSnapshotIngestRequest,
    _auth: CurrentProcessor,
    pool: PoolDep,
) -> LatestPerceptionSnapshotResponse:
    """Upsert the latest perception snapshot for one camera (P5-C).

    The session must exist; ``account_id`` / ``camera_id`` are derived from
    the session row, never trusted from the request. One row per camera — a
    newer snapshot overwrites the older one. This is latest visual state,
    NOT the append-only ``observations`` event log."""
    async with pool.acquire() as conn, conn.transaction():
        session = await load_session_state(conn, body.stream_session_id)
        if session is None:
            raise NotFoundError("Session not found.", code="session_not_found")
        session_id_var.set(session.id)
        row = await upsert_perception_snapshot(
            conn,
            account_id=session.account_id,
            camera_id=session.camera_id,
            req=body,
        )
        # Reliability M5: persist a compact confidence summary for drift
        # detection. Best-effort — isolated in a savepoint so it can never
        # break snapshot ingest.
        await _write_detection_metrics_safe(
            conn,
            session=session,
            body=body,
            perception_snapshot_id=row["id"],
        )

    return LatestPerceptionSnapshotResponse(
        snapshot=PerceptionSnapshotRecord(
            id=row["id"],
            camera_id=row["camera_id"],
            stream_session_id=row["stream_session_id"],
            processor_id=row["processor_id"],
            package_id=row["package_id"],
            snapshot=coerce_jsonb(row["snapshot"]),
            observed_at=parse_iso_datetime(row["observed_at"]),
            updated_at=parse_iso_datetime(row["updated_at"]),
        )
    )
