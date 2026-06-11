"""In-process health sampler — server-side collection for the System graphs.

Every tick the API probes its own components (the same probes the on-demand
/v1/admin/system/health rollup uses) and appends a row to
``system_health_samples``. The admin panel reads that history instead of
collecting samples client-side, so DB-latency / uptime graphs exist even
when no panel tab is open.

Deployment posture mirrors ``workflow_evaluator_loop``: gated by
``UPSTREAM_HEALTH_SAMPLE_INTERVAL_SECONDS`` (0 disables), single-instance
deployments only — with replicas, enable on exactly one.

Retention is handled inline: roughly hourly (every ``_PRUNE_EVERY`` ticks)
the loop deletes samples older than 7 days. At a 60 s interval that's about
10k rows/week — small enough that an index-backed range delete is cheap.
"""

from __future__ import annotations

import asyncio

import asyncpg
import httpx

from upstream_api.config import Settings
from upstream_api.observability.logging import get_logger
from upstream_api.services.admin_health import (
    check_database,
    check_streaming_server,
    processor_rollup,
)

_log = get_logger("health_sampler")

_RETENTION = "7 days"
_PRUNE_EVERY = 60  # ticks


async def record_sample(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
) -> None:
    """Probe every component once and append a sample row.

    A failed DB probe still records (db_ok=false, null latency) as long as
    the INSERT itself can reach the database — a sample that says "the DB
    was slow/down" is exactly what the graph is for.
    """
    database = await check_database(pool)
    streaming = await check_streaming_server(http, base_url=settings.mediamtx_api_base_url)
    if database.status == "ok":
        processors = await processor_rollup(pool)
        async with pool.acquire() as conn:
            active_sessions = await conn.fetchval(
                """
                select count(*)::int from stream_sessions
                 where state in ('approved', 'streaming')
                """
            )
    else:
        processors = None
        active_sessions = 0

    async with pool.acquire() as conn:
        await conn.execute(
            """
            insert into system_health_samples
                (db_ok, db_latency_ms, mediamtx_status, mediamtx_active_paths,
                 processors_active, processors_stale, processors_failed,
                 active_sessions)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            database.status == "ok",
            database.latency_ms,
            streaming.status,
            streaming.active_paths,
            processors.active if processors else 0,
            processors.stale if processors else 0,
            processors.failed if processors else 0,
            active_sessions or 0,
        )


async def prune_samples(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f"delete from system_health_samples where sampled_at < now() - interval '{_RETENTION}'"
        )


async def health_sampler_loop(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
    interval_seconds: int,
) -> None:
    """Run until cancelled. One failed tick logs and keeps looping — a
    sampler that dies on the first DB blip defeats its purpose."""
    tick = 0
    while True:
        try:
            await record_sample(pool, http, settings)
            tick += 1
            if tick % _PRUNE_EVERY == 0:
                await prune_samples(pool)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log.warning("health_sample_failed", error=str(exc))
        await asyncio.sleep(interval_seconds)
