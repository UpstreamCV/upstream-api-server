"""Append-only billing ledger writes."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.logging import get_logger

_logger = get_logger("usage")

# Cached result of `to_regclass('public.usage_events') is not null`. ``None``
# until the first call probes; ``False`` while the table is missing (Phase
# 1-4); ``True`` once the Phase-5 migration has shipped. Caching avoids the
# probe on every call while still letting unrelated SQL errors propagate.
_table_exists: bool | None = None
_probe_lock = asyncio.Lock()


def reset_usage_table_cache() -> None:
    """Reset the cached probe result. Tests only."""
    global _table_exists
    _table_exists = None


async def _probe_table_exists(conn: asyncpg.Connection) -> bool:
    global _table_exists
    if _table_exists is not None:
        return _table_exists
    async with _probe_lock:
        if _table_exists is None:
            _table_exists = bool(
                await conn.fetchval("select to_regclass('public.usage_events') is not null")
            )
    return _table_exists


async def record_event(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_type: str,
    session_id: UUID | None = None,
    quantity: int | float = 1,
    unit: str = "count",
    metadata: dict[str, Any] | None = None,
) -> None:
    if not await _probe_table_exists(conn):
        _logger.debug(
            "usage_events_table_missing",
            event_type=event_type,
            account_id=str(account_id),
        )
        return
    # `quantity` is bigint in the schema; coerce here so callers can pass a
    # float (e.g. raw duration in seconds) without tripping asyncpg's strict
    # type check on the bind.
    await conn.execute(
        """
        insert into usage_events
          (account_id, event_type, session_id, quantity, unit, occurred_at, metadata)
        values ($1, $2, $3, $4, $5, now(), $6::jsonb)
        """,
        account_id,
        event_type,
        session_id,
        int(quantity),
        unit,
        None if metadata is None else json.dumps(metadata),
    )
