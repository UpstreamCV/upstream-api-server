"""M10.1/M10.2 — internal trigger for the session-maintenance tick.

Auth reuses the processor shared secret (``CurrentProcessor``), the same
posture as ``/v1/internal/workflow-evaluator/run``: an internal
infrastructure surface reusing an already-provisioned secret. Safe to call
repeatedly — the sweep is single-flight behind an advisory lock and the
path reconcile is idempotent. Useful for external cron when the in-process
loop is disabled (``UPSTREAM_SESSION_MAINTENANCE_INTERVAL_SECONDS=0``), for
deterministic tests, and as the future landing spot for the streaming
server's boot ping (design doc 04 §3 — an immediate reconcile on MediaMTX
startup instead of waiting out the loop cadence).
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from upstream_api.security.deps import CurrentProcessor, PoolDep, SettingsDep
from upstream_api.services.http_client import HttpClientDep
from upstream_api.services.session_maintenance import run_session_maintenance

router = APIRouter(prefix="/v1/internal/session-maintenance", tags=["session-maintenance"])


class MaintenanceRunResponse(BaseModel):
    skipped: bool
    expired_approved: int
    expired_streaming: int
    would_expire: int
    stale_reported: int
    stale_swept: int
    paths_reprovisioned: int
    paths_extra: int
    paths_cleaned: int
    provision_errors: int
    deprovision_errors: int
    deferred_mutations: int
    reconcile_skipped: bool


@router.post("/run", response_model=MaintenanceRunResponse)
async def run_maintenance(
    _auth: CurrentProcessor,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
) -> MaintenanceRunResponse:
    stats = await run_session_maintenance(pool, http, settings)
    return MaintenanceRunResponse(
        skipped=stats.skipped,
        expired_approved=stats.expired_approved,
        expired_streaming=stats.expired_streaming,
        would_expire=stats.would_expire,
        stale_reported=stats.stale_reported,
        stale_swept=stats.stale_swept,
        paths_reprovisioned=stats.paths_reprovisioned,
        paths_extra=stats.paths_extra,
        paths_cleaned=stats.paths_cleaned,
        provision_errors=stats.provision_errors,
        deprovision_errors=stats.deprovision_errors,
        deferred_mutations=stats.deferred_mutations,
        reconcile_skipped=stats.reconcile_skipped,
    )
