from __future__ import annotations

from fastapi import APIRouter, Request

from upstream_api import __version__
from upstream_api.schemas.platform_admin import (
    ApiHealth,
    ProcessorsHealth,
    SystemHealthResponse,
)
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep, SettingsDep
from upstream_api.services.admin_health import (
    check_database,
    check_streaming_server,
    processor_rollup,
)

router = APIRouter()


@router.get("/system/health", response_model=SystemHealthResponse)
async def get_system_health(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    settings: SettingsDep,
    request: Request,
) -> SystemHealthResponse:
    """One-call health rollup for the panel's overview page.

    Component probes degrade to error-status payloads — this endpoint
    answers 200 even when the streaming server or DB is down, because the
    panel needs to RENDER that fact.
    """
    database = await check_database(pool)
    streaming_server = await check_streaming_server(
        request.app.state.http_client,
        base_url=settings.mediamtx_api_base_url,
    )
    # The processor rollup needs the DB; if the DB probe failed, don't
    # double-report the same outage as a query error.
    if database.status == "ok":
        processors = await processor_rollup(pool)
    else:
        processors = ProcessorsHealth(active=0, stale=0, failed=0)
    return SystemHealthResponse(
        api=ApiHealth(status="ok", version=__version__, environment=settings.environment),
        database=database,
        streaming_server=streaming_server,
        processors=processors,
    )
