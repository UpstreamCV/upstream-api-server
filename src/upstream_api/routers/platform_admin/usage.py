from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import BadRequestError
from upstream_api.schemas.platform_admin import (
    AdminUsageBucket,
    AdminUsageResponse,
    AdminUsageSummaryResponse,
    AdminUsageSummaryRow,
)
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep

router = APIRouter()

_MAX_RANGE = timedelta(days=366)


@router.get("/usage", response_model=AdminUsageResponse)
async def get_usage(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    start: Annotated[datetime | None, Query()] = None,
    end: Annotated[datetime | None, Query()] = None,
    bucket: Annotated[Literal["day", "week", "month"], Query()] = "day",
    account_id: Annotated[UUID | None, Query()] = None,
    event_type: Annotated[str | None, Query(max_length=100)] = None,
) -> AdminUsageResponse:
    """Time-bucketed usage_events aggregates across all accounts.

    Defaults to the last 30 days. `bucket` is passed to date_trunc as a
    parameter (not interpolated), so the Literal validation is belt and
    the parameterization is suspenders.
    """
    end_at = end or datetime.now(tz=UTC)
    start_at = start or end_at - timedelta(days=30)
    if start_at >= end_at:
        raise BadRequestError("start must be before end.", code="invalid_range")
    if end_at - start_at > _MAX_RANGE:
        raise BadRequestError("Range too large (max 366 days).", code="range_too_large")

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select date_trunc($1, u.occurred_at) as bucket_start,
                   u.account_id, a.name as account_name,
                   u.event_type, u.unit,
                   sum(u.quantity)::bigint as total_quantity
              from usage_events u
              join accounts a on a.id = u.account_id
             where u.occurred_at >= $2
               and u.occurred_at < $3
               and ($4::uuid is null or u.account_id = $4)
               and ($5::text is null or u.event_type = $5)
             group by 1, 2, 3, 4, 5
             order by 1 desc, 6 desc
            """,
            bucket,
            start_at,
            end_at,
            account_id,
            event_type,
        )
    return AdminUsageResponse(
        buckets=[
            AdminUsageBucket(
                bucket_start=r["bucket_start"],
                account_id=r["account_id"],
                account_name=r["account_name"],
                event_type=r["event_type"],
                unit=r["unit"],
                total_quantity=r["total_quantity"],
            )
            for r in rows
        ]
    )


@router.get("/usage/summary", response_model=AdminUsageSummaryResponse)
async def get_usage_summary(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    days: Annotated[int, Query(ge=1, le=365)] = 30,
) -> AdminUsageSummaryResponse:
    """Per-account usage totals for the trailing window — the panel's
    'who is consuming what' table."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select u.account_id, a.name as account_name,
                   a.slug::text as account_slug, a.plan,
                   u.event_type, u.unit,
                   sum(u.quantity)::bigint as total_quantity
              from usage_events u
              join accounts a on a.id = u.account_id
             where u.occurred_at > now() - make_interval(days => $1)
             group by 1, 2, 3, 4, 5, 6
             order by 7 desc
            """,
            days,
        )
    return AdminUsageSummaryResponse(
        days=days,
        rows=[
            AdminUsageSummaryRow(
                account_id=r["account_id"],
                account_name=r["account_name"],
                account_slug=r["account_slug"],
                plan=r["plan"],
                event_type=r["event_type"],
                unit=r["unit"],
                total_quantity=r["total_quantity"],
            )
            for r in rows
        ],
    )
