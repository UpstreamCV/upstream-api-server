"""Append-only audit trail for mutating /v1/admin/* calls.

Every PATCH on the platform-admin surface records who did it, what it
targeted, and a {before, after} diff. `request_id` ties the row back to the
structured access logs for the same request.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.context import request_id_var


async def record_admin_action(
    conn: asyncpg.Connection,
    *,
    admin_user_id: UUID,
    action: str,
    target_type: str,
    target_id: UUID | None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        """
        insert into admin_audit_log
            (admin_user_id, action, target_type, target_id, payload, request_id)
        values ($1, $2, $3, $4, $5, $6)
        """,
        admin_user_id,
        action,
        target_type,
        target_id,
        json.dumps({"before": before or {}, "after": after or {}}, default=str),
        request_id_var.get("") or None,
    )
