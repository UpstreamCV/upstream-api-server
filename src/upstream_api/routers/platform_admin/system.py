from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from upstream_api import __version__
from upstream_api.observability import metrics as control_plane_metrics
from upstream_api.schemas.platform_admin import (
    ApiHealth,
    ControlPlaneMetricsResponse,
    HealthHistoryResponse,
    HealthSample,
    ProcessorsHealth,
    SystemHealthResponse,
    SystemTimeseriesPoint,
    SystemTimeseriesResponse,
)
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep, SettingsDep
from upstream_api.services.admin_health import (
    api_runtime,
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
        api=ApiHealth(
            status="ok",
            version=__version__,
            environment=settings.environment,
            runtime=api_runtime(),
        ),
        database=database,
        streaming_server=streaming_server,
        processors=processors,
    )


@router.get("/system/metrics", response_model=ControlPlaneMetricsResponse)
async def get_system_metrics(
    _admin: CurrentPlatformAdmin,
    request: Request,
) -> ControlPlaneMetricsResponse:
    """Control-plane counters for the panel — a parsed view of the same
    in-process Prometheus registry GET /metrics serves, so admins see the
    M9.3 observability data without scrape access to the monitoring box.

    Values are process-lifetime (reset on restart); the panel renders them
    as "since restart" using ``process_uptime_seconds``.
    """
    # Refresh pool-saturation gauges inline, same as routers/metrics.py
    # (absent under TestClient without lifespan — gauges just keep 0).
    pool = getattr(request.app.state, "db_pool", None)
    if pool is not None:
        size = pool.get_size()
        control_plane_metrics.db_pool_size.set(size)
        control_plane_metrics.db_pool_in_use.set(size - pool.get_idle_size())
    return ControlPlaneMetricsResponse(**control_plane_metrics.snapshot())


@router.get("/system/timeseries", response_model=SystemTimeseriesResponse)
async def get_system_timeseries(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    hours: Annotated[int, Query(ge=1, le=168)] = 24,
) -> SystemTimeseriesResponse:
    """Hourly platform activity for the System page graphs.

    Buckets come from generate_series so quiet hours are explicit zeros —
    a graph that silently skips empty hours misreads as healthy load.
    `avg_fps` is null for hours with no heartbeats (distinct from 0 fps,
    which would mean stalled streams).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            with hours as (
              select generate_series(
                       date_trunc('hour', now()) - ($1::int - 1) * interval '1 hour',
                       date_trunc('hour', now()),
                       interval '1 hour'
                     ) as bucket
            )
            select h.bucket,
                   (select count(*)::int from stream_sessions s
                     where s.created_at >= h.bucket
                       and s.created_at < h.bucket + interval '1 hour') as sessions_started,
                   (select count(*)::int from stream_validations v
                     where v.passed = false
                       and v.created_at >= h.bucket
                       and v.created_at < h.bucket + interval '1 hour') as validation_failures,
                   coalesce((select sum(u.quantity) / 60.0 from usage_events u
                     where u.event_type = 'session_ended'
                       and u.unit = 'seconds'
                       and u.occurred_at >= h.bucket
                       and u.occurred_at < h.bucket + interval '1 hour'), 0)::float
                     as streaming_minutes,
                   (select avg(hb.fps)::float from heartbeats hb
                     where hb.reported_at >= h.bucket
                       and hb.reported_at < h.bucket + interval '1 hour'
                       and hb.fps is not null) as avg_fps,
                   (select count(*)::int from alerts a
                     where a.created_at >= h.bucket
                       and a.created_at < h.bucket + interval '1 hour') as alerts_fired,
                   (select count(*)::int from workflow_runs w
                     where w.started_at >= h.bucket
                       and w.started_at < h.bucket + interval '1 hour') as workflow_runs,
                   coalesce((select sum(m.object_count)::int
                      from snapshot_detection_metrics m
                     where m.observed_at >= h.bucket
                       and m.observed_at < h.bucket + interval '1 hour'), 0)
                     as objects_detected,
                   (select avg(m.avg_confidence)::float
                      from snapshot_detection_metrics m
                     where m.observed_at >= h.bucket
                       and m.observed_at < h.bucket + interval '1 hour'
                       and m.avg_confidence is not null) as avg_detection_confidence
              from hours h
             order by h.bucket
            """,
            hours,
        )
    return SystemTimeseriesResponse(
        hours=hours,
        points=[
            SystemTimeseriesPoint(
                bucket=r["bucket"],
                sessions_started=r["sessions_started"],
                validation_failures=r["validation_failures"],
                streaming_minutes=round(r["streaming_minutes"], 1),
                avg_fps=round(r["avg_fps"], 1) if r["avg_fps"] is not None else None,
                alerts_fired=r["alerts_fired"],
                workflow_runs=r["workflow_runs"],
                objects_detected=r["objects_detected"],
                avg_detection_confidence=(
                    round(r["avg_detection_confidence"], 3)
                    if r["avg_detection_confidence"] is not None
                    else None
                ),
            )
            for r in rows
        ],
    )


@router.get("/system/health/history", response_model=HealthHistoryResponse)
async def get_health_history(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    settings: SettingsDep,
    minutes: Annotated[int, Query(ge=5, le=1440)] = 60,
) -> HealthHistoryResponse:
    """Server-collected health samples for the live graphs.

    Source: the in-process sampler loop (services/health_sampler.py). The
    panel reads this instead of building client-side ring buffers, so the
    history is shared across tabs and exists while nobody is watching.
    Capped at 2000 rows (a 24h window at the 60 s default is 1440).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select sampled_at, db_ok, db_latency_ms, mediamtx_status,
                   mediamtx_active_paths, processors_active, processors_stale,
                   processors_failed, active_sessions
              from system_health_samples
             where sampled_at > now() - make_interval(mins => $1)
             order by sampled_at
             limit 2000
            """,
            minutes,
        )
    return HealthHistoryResponse(
        minutes=minutes,
        interval_seconds=settings.health_sample_interval_seconds,
        samples=[
            HealthSample(
                sampled_at=r["sampled_at"],
                db_ok=r["db_ok"],
                db_latency_ms=r["db_latency_ms"],
                mediamtx_status=r["mediamtx_status"],
                mediamtx_active_paths=r["mediamtx_active_paths"],
                processors_active=r["processors_active"],
                processors_stale=r["processors_stale"],
                processors_failed=r["processors_failed"],
                active_sessions=r["active_sessions"],
            )
            for r in rows
        ],
    )
