"""Account-level processing master switch -- DB helpers.

One toggle per account, stored as a key in ``accounts.metadata`` JSONB. We
read it via the existing ``coerce_jsonb`` helper (same one M1 needed for
the reliability-evidence fix) so the value round-trips correctly whether
asyncpg returns the row's metadata as ``str`` or ``dict``.

Default semantics: an account with no key set is treated as ``true``. This
keeps every account that existed before M3 processing exactly as before.

Write path: PATCH merges the flag into existing metadata via JSONB
concatenation, preserving any other keys (e.g. workspace branding,
feature flags) the account row already carries.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.services.processor import coerce_jsonb


def _read_processing_enabled(metadata: Any) -> bool:
    """Walk the JSONB and return the effective flag.

    Missing key, missing metadata, and non-bool values all collapse to
    ``True`` -- the existing behavior. The dashboard toggle will write
    explicit bools, so non-bool values shouldn't occur in practice but we
    fail safe (process the camera) rather than silently dropping workers.
    """
    if not isinstance(metadata, dict):
        try:
            metadata = coerce_jsonb(metadata)
        except Exception:
            return True
    value = metadata.get("processing_enabled")
    if isinstance(value, bool):
        return value
    return True


async def get_processing_config(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
) -> tuple[bool, datetime] | None:
    """Return ``(processing_enabled, updated_at)`` for ``account_id``.

    ``None`` only when the account row itself is missing -- the router
    converts that into a 404.
    """
    row = await conn.fetchrow(
        "select metadata, updated_at from accounts where id = $1",
        account_id,
    )
    if row is None:
        return None
    return _read_processing_enabled(row["metadata"]), row["updated_at"]


async def update_processing_config(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    processing_enabled: bool,
) -> tuple[bool, datetime] | None:
    """Set ``metadata['processing_enabled']`` on the account row.

    JSONB concat preserves every other key the metadata might carry. The
    update is idempotent -- flipping to the same value bumps ``updated_at``
    via the existing ``set_updated_at`` trigger but is otherwise a no-op.
    """
    patch = json.dumps({"processing_enabled": processing_enabled})
    row = await conn.fetchrow(
        """
        update accounts
           set metadata = coalesce(metadata, '{}'::jsonb) || $2::jsonb
         where id = $1
        returning metadata, updated_at
        """,
        account_id,
        patch,
    )
    if row is None:
        return None
    return _read_processing_enabled(row["metadata"]), row["updated_at"]
