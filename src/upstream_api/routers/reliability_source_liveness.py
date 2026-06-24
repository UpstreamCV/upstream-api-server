"""Reliability M2b — internal trigger for the source-liveness sweep.

Auth reuses the processor shared secret (``CurrentProcessor``): an internal
infrastructure surface like the processor / workflow-evaluator endpoints, so
the already-provisioned secret avoids a new credential. Safe to call repeatedly
— the sweep is idempotent. Useful for external cron when the in-process loop is
disabled (``UPSTREAM_SOURCE_LIVENESS_SWEEP_INTERVAL_SECONDS=0``) and for
deterministic tests (which never run the lifespan loop)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel

from upstream_api.security.deps import CurrentProcessor, PoolDep
from upstream_api.services.reliability_source_liveness import run_source_liveness_sweep

router = APIRouter(
    prefix="/v1/internal/reliability/source-liveness", tags=["reliability-source-liveness"]
)

_MAX_LIMIT = 5000


class SourceLivenessRunResponse(BaseModel):
    evaluated: int
    opened: int
    resolved: int
    errors: int
    by_status: dict[str, int]


@router.post("/run", response_model=SourceLivenessRunResponse)
async def run_sweep(
    _auth: CurrentProcessor,
    pool: PoolDep,
    limit: Annotated[int, Query(ge=1, le=_MAX_LIMIT)] = 500,
) -> SourceLivenessRunResponse:
    stats = await run_source_liveness_sweep(pool, limit=limit)
    return SourceLivenessRunResponse(
        evaluated=stats.evaluated,
        opened=stats.opened,
        resolved=stats.resolved,
        errors=stats.errors,
        by_status=stats.by_status,
    )
