"""Upstream Reliability M1a — observation ingest router.

``POST /v1/reliability/observations`` — the cloud-side receiving end for the
Upstream Collector. Auth + tenancy follow the dual-auth pattern: the
``reliability:ingest`` scope gates API tokens, ``caller.resolve_account`` derives
the account from the credential (never the request body), and any referenced
``camera_id`` is ownership-checked server-side (cross-account → 404, not 403, so
existence never leaks).

The batch + its observations are stored in one transaction; detectors then run
in a nested savepoint so a detector failure can NEVER break ingest. A dedupe
replay (same ``client_batch_id``) returns the original batch id + accepted count
and skips detector evaluation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.observability.logging import get_logger
from upstream_api.schemas.reliability_observations import (
    ReliabilityObservationBatchRequest,
    ReliabilityObservationBatchResponse,
    ReliabilityObservationItem,
)
from upstream_api.security.deps import CurrentCaller, PoolDep, require_token_scope
from upstream_api.services.reliability_observation_detectors import (
    evaluate_observation_batch,
)
from upstream_api.services.reliability_observation_ingest import record_observation_batch
from upstream_api.services.reliability_sources import upsert_sources_from_observation_batch

router = APIRouter(prefix="/v1/reliability", tags=["reliability"])

_logger = get_logger("reliability_ingest")

_CAMERA_OWNERSHIP_SQL = "select id from cameras where id = any($1::uuid[]) and account_id = $2"


async def _run_detectors_safe(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    source: str,
    batch_id: UUID,
    observations: list[ReliabilityObservationItem],
    now: datetime,
) -> None:
    """Run the observation detectors, isolated in a savepoint.

    Best-effort reliability analytics: this must NEVER fail the ingest. Any
    failure — including the cv_issues table not existing yet — rolls back only
    the detector/issue writes and leaves the observation rows that already
    committed in the parent transaction intact. We log scalar ids only.
    """
    try:
        async with conn.transaction():
            await evaluate_observation_batch(
                conn,
                account_id=account_id,
                source=source,
                batch_id=batch_id,
                observations=observations,
                now=now,
            )
    except Exception as exc:
        _logger.warning(
            "reliability_detector_failed",
            account_id=str(account_id),
            batch_id=str(batch_id),
            error=type(exc).__name__,
        )


async def _run_source_upsert_safe(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    source: str,
    batch_id: UUID,
    observations: list[ReliabilityObservationItem],
) -> None:
    """Project the batch into the cv_sources registry, isolated in a savepoint.

    The registry is a derived, product-critical projection of cv_observations
    (the durable system of record). It runs in its own savepoint — separate from
    the detector savepoint — so a pathological payload rolls back only the
    registry write and never rejects already-stored observations; the registry
    can be rebuilt from cv_observations. A failure here is projection drift (a
    stronger signal than best-effort analytics), so it is logged at error level.
    """
    try:
        async with conn.transaction():
            await upsert_sources_from_observation_batch(
                conn,
                account_id=account_id,
                source=source,
                observations=observations,
            )
    except Exception as exc:
        _logger.error(
            "reliability_source_upsert_failed",
            account_id=str(account_id),
            batch_id=str(batch_id),
            error=type(exc).__name__,
        )


@router.post("/observations", response_model=ReliabilityObservationBatchResponse)
async def ingest_observations(
    body: ReliabilityObservationBatchRequest,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ReliabilityObservationBatchResponse:
    """Ingest a batch of reliability observations.

    Requires the ``reliability:ingest`` scope for API tokens. The account is
    resolved from the credential; a spoofed body ``account_id`` is ignored
    (it is not a request field). Idempotent on ``client_batch_id``: a replay
    returns the original ``batch_id`` and accepted count.
    """
    require_token_scope(caller, "reliability:ingest")
    scope_account = caller.resolve_account(account_id)
    now = datetime.now(tz=UTC)
    camera_ids = {o.camera_id for o in body.observations if o.camera_id is not None}
    async with pool.acquire() as conn, conn.transaction():
        if camera_ids:
            rows = await conn.fetch(_CAMERA_OWNERSHIP_SQL, list(camera_ids), scope_account)
            if camera_ids - {r["id"] for r in rows}:
                # 404 (not 403) so a cross-account camera id never leaks existence.
                raise NotFoundError("Camera not found.", code="camera_not_found")
        result = await record_observation_batch(
            conn,
            account_id=scope_account,
            client_batch_id=body.client_batch_id,
            source=body.source,
            observations=body.observations,
        )
        if result.inserted:
            # Registry projection first (its own savepoint), then detectors — so
            # a detector-savepoint rollback can't lose the registry update. Both
            # are gated on the freshly-inserted batch, so idempotent replays
            # never double-count source counters.
            await _run_source_upsert_safe(
                conn,
                account_id=scope_account,
                source=body.source,
                batch_id=result.batch_id,
                observations=body.observations,
            )
            await _run_detectors_safe(
                conn,
                account_id=scope_account,
                source=body.source,
                batch_id=result.batch_id,
                observations=body.observations,
                now=now,
            )
    return ReliabilityObservationBatchResponse(
        accepted=result.accepted,
        batch_id=result.batch_id,
        server_time=datetime.now(tz=UTC),
    )
