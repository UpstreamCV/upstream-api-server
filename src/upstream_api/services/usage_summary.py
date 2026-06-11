"""Module 7 M1 — usage summary aggregation.

One round-trip computing every counter the billing/usage page shows. Each
counter deliberately reuses the predicate of the code that OWNS that
number, so the page can never disagree with enforcement or history:

- cameras: ``status <> 'disabled'`` — the max_cameras enforcement predicate.
- active_streams: ``state in ('approved','streaming')`` — the
  max_concurrent_sessions enforcement predicate.
- stream_minutes: sum of usage_events ``session_ended`` quantities in the
  period, seconds // 60 — the agent-bootstrap convention. ``stream_seconds``
  events are the streaming-server's report of the SAME durations and must
  never be added on top. Completed sessions only: an in-flight session
  contributes nothing until it ends (documented undercount).
- workflow_runs / alerts: created in the period (account+created indexes).
- rules / workflows: ALL current definitions (enabled or not — there is no
  soft delete; disabled still occupies the account's configuration).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.errors import NotFoundError
from upstream_api.schemas.usage import (
    PlanSummary,
    UsageCounters,
    UsageLimits,
    UsagePeriod,
    UsageStates,
    UsageSummaryResponse,
    usage_state,
)
from upstream_api.services.usage import current_usage_period

_ACCOUNT_AND_LIMITS_SQL = """
select a.plan, a.status,
       l.max_cameras, l.max_concurrent_sessions,
       l.monthly_minutes_quota, l.allowed_stream_types
  from accounts a
  join account_limits l on l.account_id = a.id
 where a.id = $1
"""

_USAGE_COUNTERS_SQL = """
select
  (select count(*)::int from cameras
    where account_id = $1 and status <> 'disabled') as cameras,
  (select count(*)::int from stream_sessions
    where account_id = $1
      and state in ('approved', 'streaming')) as active_streams,
  (select coalesce(sum(quantity), 0)::bigint from usage_events
    where account_id = $1
      and event_type = 'session_ended'
      and occurred_at >= $2
      and occurred_at < $3) as stream_seconds_completed,
  (select count(*)::int from workflow_runs
    where account_id = $1
      and created_at >= $2 and created_at < $3) as workflow_runs,
  (select count(*)::int from alerts
    where account_id = $1
      and created_at >= $2 and created_at < $3) as alerts,
  (select count(*)::int from rules where account_id = $1) as rules,
  (select count(*)::int from workflows where account_id = $1) as workflows
"""


async def build_usage_summary(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    now: datetime | None = None,
) -> UsageSummaryResponse:
    period_start, period_end = current_usage_period(now)

    account_row: dict[str, Any] | None = await conn.fetchrow(_ACCOUNT_AND_LIMITS_SQL, account_id)
    if account_row is None:
        # Account missing OR its limits row missing — same error code the
        # rest of the limits stack uses for the latter.
        raise NotFoundError("Account limits not configured.", code="account_limits_missing")

    usage_row: dict[str, Any] | None = await conn.fetchrow(
        _USAGE_COUNTERS_SQL, account_id, period_start, period_end
    )
    assert usage_row is not None  # aggregate select always returns one row

    plan_key = account_row["plan"] or "free"
    stream_minutes = int(usage_row["stream_seconds_completed"]) // 60
    limits = UsageLimits(
        max_cameras=account_row["max_cameras"],
        max_concurrent_sessions=account_row["max_concurrent_sessions"],
        monthly_minutes_quota=account_row["monthly_minutes_quota"],
        allowed_stream_types=sorted(account_row["allowed_stream_types"]),
    )
    usage = UsageCounters(
        cameras=usage_row["cameras"],
        active_streams=usage_row["active_streams"],
        stream_minutes=stream_minutes,
        workflow_runs=usage_row["workflow_runs"],
        alerts=usage_row["alerts"],
        rules=usage_row["rules"],
        workflows=usage_row["workflows"],
    )
    return UsageSummaryResponse(
        plan=PlanSummary(
            key=plan_key,
            label=plan_key.replace("_", " ").title(),
            status=account_row["status"],
        ),
        period=UsagePeriod(start=period_start, end=period_end),
        limits=limits,
        usage=usage,
        states=UsageStates(
            cameras=usage_state(usage.cameras, limits.max_cameras),
            active_streams=usage_state(usage.active_streams, limits.max_concurrent_sessions),
            stream_minutes=usage_state(usage.stream_minutes, limits.monthly_minutes_quota),
        ),
    )
