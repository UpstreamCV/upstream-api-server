"""Append-only billing ledger writes."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.logging import get_logger

_logger = get_logger("usage")


async def record_event(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_type: str,
    session_id: UUID | None = None,
    quantity: float = 1.0,
    unit: str = "count",
    metadata: dict[str, Any] | None = None,
) -> None:
    # TODO(phase5): drop the savepoint + UndefinedTableError swallow once
    # migration 0005 ships the usage_events table. Phase 1-4 endpoints call
    # this function before the table exists, and a bare ``except`` in a
    # transaction would leave the outer transaction aborted — we must wrap the
    # insert in a savepoint so the rollback is scoped to this call.
    try:
        async with conn.transaction():
            await conn.execute(
                """
                insert into usage_events
                  (account_id, event_type, session_id, quantity, unit, occurred_at, metadata)
                values ($1, $2, $3, $4, $5, now(), $6::jsonb)
                """,
                account_id,
                event_type,
                session_id,
                quantity,
                unit,
                None if metadata is None else json.dumps(metadata),
            )
    except asyncpg.UndefinedTableError:
        _logger.debug(
            "usage_events_table_missing",
            event_type=event_type,
            account_id=str(account_id),
        )
        return
