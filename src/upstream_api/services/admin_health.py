"""System-health probes for GET /v1/admin/system/health.

Each probe degrades to an error-status payload instead of raising — the
health rollup must always render, even (especially) when a component is
down. Timeouts are short so the endpoint stays snappy: 0.5 s for the DB
(mirrors /readyz), 1 s for the MediaMTX HTTP API probe.
"""

from __future__ import annotations

import asyncio
import os
import resource
import sys
import time

import asyncpg
import httpx

from upstream_api.observability.metrics import process_started_at
from upstream_api.schemas.platform_admin import (
    ApiRuntime,
    DatabaseHealth,
    ProcessorsHealth,
    StreamingServerHealth,
)

_DB_TIMEOUT_SECONDS = 0.5
_MEDIAMTX_TIMEOUT_SECONDS = 1.0

# A processor row older than this is "stale": the worker stopped upserting
# its status but never reported failed/stopped. Matches the dashboard's
# online-agent heuristic window.
_PROCESSOR_STALE_INTERVAL = "5 minutes"


def _rss_mb() -> float | None:
    """Current resident memory of this process, in MB.

    Prefers /proc/self/status (current RSS — what production's Linux
    container has); falls back to getrusage's PEAK RSS elsewhere (macOS
    dev, where ru_maxrss is bytes; Linux reports KB).
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        pass
    try:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ValueError, OSError):
        return None
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(peak / divisor, 1)


def api_runtime() -> ApiRuntime:
    """Process-level vitals for the API component of the health rollup.

    Everything degrades to None rather than failing the rollup — these are
    nice-to-have params on a tile, not probes.
    """
    try:
        load_1m: float | None = round(os.getloadavg()[0], 2)
    except OSError:
        load_1m = None
    return ApiRuntime(
        uptime_seconds=round(max(0.0, time.time() - process_started_at), 1),
        memory_rss_mb=_rss_mb(),
        load_1m=load_1m,
    )


async def check_database(pool: asyncpg.Pool) -> DatabaseHealth:
    started = time.monotonic()
    try:
        async with asyncio.timeout(_DB_TIMEOUT_SECONDS):
            async with pool.acquire() as conn:
                await conn.fetchval("select 1")
    except (TimeoutError, Exception) as exc:
        return DatabaseHealth(status="error", error=str(exc))
    return DatabaseHealth(status="ok", latency_ms=round((time.monotonic() - started) * 1000, 1))


async def check_streaming_server(
    client: httpx.AsyncClient,
    *,
    base_url: str,
) -> StreamingServerHealth:
    """Probe MediaMTX's path-list API (the same admin API used for
    per-session provisioning, so reachability here implies provisioning
    works too)."""
    url = f"{base_url.rstrip('/')}/v3/paths/list"
    try:
        response = await client.get(url, timeout=_MEDIAMTX_TIMEOUT_SECONDS)
    except httpx.HTTPError as exc:
        return StreamingServerHealth(status="unreachable", error=str(exc))
    if response.status_code != 200:
        return StreamingServerHealth(
            status="error",
            error=f"HTTP {response.status_code}: {response.text[:200]}",
        )
    try:
        body = response.json()
        active_paths = int(body.get("itemCount", len(body.get("items", []))))
    except (ValueError, TypeError, AttributeError):
        active_paths = None
    return StreamingServerHealth(status="ok", active_paths=active_paths)


async def processor_rollup(pool: asyncpg.Pool) -> ProcessorsHealth:
    """Counts over processor_statuses for sessions that are still active.

    Rows for ended sessions are history, not health — a processor that
    finished its session and stopped is not 'stale'.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            select
              count(*) filter (
                where p.status in ('starting', 'attached', 'processing', 'lagging')
                  and p.updated_at > now() - interval '{_PROCESSOR_STALE_INTERVAL}'
              )::int as active,
              count(*) filter (
                where p.status in ('starting', 'attached', 'processing', 'lagging')
                  and p.updated_at <= now() - interval '{_PROCESSOR_STALE_INTERVAL}'
              )::int as stale,
              count(*) filter (where p.status = 'failed')::int as failed,
              max(p.updated_at) as latest_update_at
              from processor_statuses p
              join stream_sessions s on s.id = p.stream_session_id
             where s.state in ('approved', 'streaming')
            """
        )
    assert row is not None
    return ProcessorsHealth(
        active=row["active"],
        stale=row["stale"],
        failed=row["failed"],
        latest_update_at=row["latest_update_at"],
    )
