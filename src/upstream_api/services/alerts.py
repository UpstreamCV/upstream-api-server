"""Rules/Triggers M1 — alert inbox reads and read/resolve transitions.

Alerts are written only by the rule evaluator (services/rules.py); this
module owns the user-facing lifecycle. account_id is NOT selected onto read
responses (the caller is already account-scoped).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

_LIST_ALERTS_SQL = """
select
    id, camera_id, rule_id, observation_id, severity, status,
    title, message, payload, created_at, read_at, resolved_at
  from alerts
 where account_id = $1
   and ($2::text is null or status = $2)
   and ($3::uuid is null or camera_id = $3)
 order by created_at desc
 limit $4
"""

# read_at is set once; re-reading an already-read alert is a no-op.
_MARK_READ_SQL = """
update alerts
   set read_at = coalesce(read_at, now())
 where account_id = $1
   and id = $2
returning id, camera_id, rule_id, observation_id, severity, status,
          title, message, payload, created_at, read_at, resolved_at
"""

# Resolving also stamps read_at — a resolved-but-unread alert makes no sense
# in the inbox. Resolving an already-resolved alert is a no-op.
_RESOLVE_SQL = """
update alerts
   set status      = 'resolved',
       resolved_at = coalesce(resolved_at, now()),
       read_at     = coalesce(read_at, now())
 where account_id = $1
   and id = $2
returning id, camera_id, rule_id, observation_id, severity, status,
          title, message, payload, created_at, read_at, resolved_at
"""


async def list_alerts(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    status: str | None,
    camera_id: UUID | None,
    limit: int,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_LIST_ALERTS_SQL, account_id, status, camera_id, limit)
    return [dict(r) for r in rows]


async def mark_alert_read(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    alert_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(_MARK_READ_SQL, account_id, alert_id)
    return dict(row) if row is not None else None


async def resolve_alert(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    alert_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(_RESOLVE_SQL, account_id, alert_id)
    return dict(row) if row is not None else None
