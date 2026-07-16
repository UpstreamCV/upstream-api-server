from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.schemas.dashboard import (
    DashboardSummaryResponse,
    FleetTrendPoint,
    FleetTrendRange,
    FleetTrendResponse,
    FleetTrendSummary,
)
from upstream_api.security.deps import CurrentCaller, PoolDep

router = APIRouter(prefix="/v1/dashboard", tags=["dashboard"])

# Hours per supported trend range. Hourly buckets for both, matching the
# per-camera confidence trend (GET /v1/cameras/{id}/reliability/confidence-trend)
# so the two trend surfaces stay shaped alike.
_TREND_HOURS: dict[str, int] = {"24h": 24, "7d": 168}

# One row per hour in the window, each camera counted once by the status of its
# LAST heartbeat in that hour.
#
# generate_series supplies every bucket so quiet hours come back as zeros rather
# than missing rows -- the client must not have to distinguish "no data" from
# "nothing happened", and a left join alone would drop empty hours entirely.
#
# The window is inclusive of the CURRENT partial hour: this chart's x-axis ends
# at "Now" and the newest heartbeat is the freshest truth we have. (The
# confidence trend excludes its partial hour because a partial ROLLUP understates
# an average; a latest-status count has no such distortion.)
_FLEET_TREND_SQL = """
with bounds as (
  select
    date_trunc('hour', now()) - make_interval(hours => $2::int - 1) as first_bucket,
    date_trunc('hour', now()) as last_bucket
),
buckets as (
  select generate_series(b.first_bucket, b.last_bucket, interval '1 hour') as bucket_start
  from bounds b
),
latest_per_camera as (
  select distinct on (h.camera_id, date_trunc('hour', h.reported_at))
    date_trunc('hour', h.reported_at) as bucket_start,
    h.camera_id,
    h.status
  from heartbeats h, bounds b
  where h.account_id = $1
    and h.camera_id is not null
    and h.reported_at >= b.first_bucket
    and h.reported_at < b.last_bucket + interval '1 hour'
  order by h.camera_id, date_trunc('hour', h.reported_at), h.reported_at desc
)
select
  b.bucket_start,
  b.bucket_start + interval '1 hour' as bucket_end,
  count(*) filter (where l.status = 'error')::int as critical,
  count(*) filter (where l.status = 'reconnecting')::int as review,
  count(*) filter (where l.status in ('online', 'streaming'))::int as healthy,
  count(l.camera_id)::int as reporting_cameras
from buckets b
left join latest_per_camera l on l.bucket_start = b.bucket_start
group by b.bucket_start
order by b.bucket_start
"""


@router.get("/summary", response_model=DashboardSummaryResponse)
async def get_summary(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> DashboardSummaryResponse:
    """Account-scoped overview counts for the dashboard's main page.

    Single round-trip; replaces the dashboard's client-side derivation
    across the three list endpoints.

    `online_cameras` counts CAMERAS whose owning agent is fresh (<5 min). It
    previously returned the online-AGENT count outright, under a comment calling
    itself a "heuristic: cameras attached to online agents" -- but no join to
    cameras existed, so a 2-agent/40-camera account reported 2 online cameras,
    and that value drives the dashboard's headline "Active Cameras" tile. The
    join below is what the comment always claimed. `offline_cameras` is now its
    complement (it was total - streaming, which called every idle-but-connected
    camera offline and made the two fields fail to sum to total_cameras).
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select
              (select count(*)::int from cameras
                 where account_id = $1) as total_cameras,
              (select count(*)::int from cameras
                 where account_id = $1 and status = 'streaming') as streaming_cameras,
              (select count(*)::int from cameras
                 where account_id = $1 and status = 'idle') as idle_cameras,
              (select count(*)::int from cameras
                 where account_id = $1 and status = 'error') as cameras_with_errors,
              (select count(*)::int from cameras c
                 join agents a on a.id = c.agent_id
                where c.account_id = $1
                  and a.last_seen_at > now() - interval '5 minutes') as online_cameras,
              (select count(*)::int from agents
                 where account_id = $1
                   and last_seen_at > now() - interval '5 minutes') as online_agents,
              (select count(*)::int from agents
                 where account_id = $1) as total_agents,
              (select count(*)::int from stream_sessions
                 where account_id = $1 and state in ('approved', 'streaming')
                  ) as active_streams,
              (select count(*)::int from stream_validations v
                 join stream_sessions s on s.id = v.session_id
                where s.account_id = $1
                  and v.passed = false
                  and v.created_at > now() - interval '24 hours'
                  ) as validation_errors_24h
            """,
            scope_account,
        )
    assert row is not None
    return DashboardSummaryResponse(
        total_cameras=row["total_cameras"],
        online_cameras=row["online_cameras"],
        offline_cameras=row["total_cameras"] - row["online_cameras"],
        streaming_cameras=row["streaming_cameras"],
        cameras_with_errors=row["cameras_with_errors"],
        active_streams=row["active_streams"],
        validation_errors_24h=row["validation_errors_24h"],
    )


@router.get("/fleet-trend", response_model=FleetTrendResponse)
async def get_fleet_trend(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    range: Annotated[FleetTrendRange, Query()] = "24h",
) -> FleetTrendResponse:
    """Account-scoped fleet health over a trailing 24h/7d window, hourly.

    Derived live from `heartbeats`, which already historizes per-camera status
    with `reported_at` -- there is no rollup table and no sampler to run, so the
    series is populated from existing data the moment this ships. (`cameras`
    carries only mutable current state, so nothing else in the schema can answer
    "how did the fleet look at 3am".)

    Each camera is counted once per hour by its LAST heartbeat in that hour, so
    critical/review/healthy partition `reporting_cameras` and can never
    double-count a chatty camera.
    """
    scope_account = caller.resolve_account(account_id)
    hours = _TREND_HOURS[range]

    async with pool.acquire() as conn:
        rows = await conn.fetch(_FLEET_TREND_SQL, scope_account, hours)
        total_cameras = await conn.fetchval(
            "select count(*)::int from cameras where account_id = $1",
            scope_account,
        )

    points = [
        FleetTrendPoint(
            bucket_start=row["bucket_start"],
            bucket_end=row["bucket_end"],
            critical=row["critical"],
            review=row["review"],
            healthy=row["healthy"],
            reporting_cameras=row["reporting_cameras"],
        )
        for row in rows
    ]
    latest = points[-1] if points else None

    return FleetTrendResponse(
        range=range,
        bucket_size="1h",
        points=points,
        summary=FleetTrendSummary(
            point_count=len(points),
            latest_bucket_start=latest.bucket_start if latest else None,
            latest_critical=latest.critical if latest else 0,
            latest_review=latest.review if latest else 0,
            latest_healthy=latest.healthy if latest else 0,
            total_cameras=total_cameras or 0,
        ),
    )
