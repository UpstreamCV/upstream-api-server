"""Module 7 M3 — tenant audit log writes.

Account-facing audit history (who created this camera, who revoked that
token, what changed our plan). NOT the platform-admin trail — that's
``services/admin_audit.py`` with a different audience and table.

Write posture (mirrors ``services/usage.py``):

- A ``to_regclass`` probe lets code deploy before the migration — missing
  table means the write is skipped with a debug log, never an error.
- ``try_record_account_audit_log`` additionally swallows unexpected insert
  errors so an audit failure can never fail the user's action. Callers
  inside an open transaction should know an unexpected DB error still
  aborts that transaction (asyncpg semantics); the probe covers the only
  realistic failure (pre-migration deploy), so this is best-effort in the
  same sense record_event is.

Sanitization: ``metadata`` passes through ``_sanitize_metadata`` which
drops any key that smells like secret material (recursively). Raw tokens,
auth headers, Stripe secrets, card data, raw webhook payloads, passwords,
and recovery links must never reach this module in the first place — the
sanitizer is the backstop, not the policy.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.context import request_id_var
from upstream_api.observability.logging import get_logger

_logger = get_logger("account_audit")

ACTOR_TYPES = ("user", "system", "stripe", "internal")

# Any metadata key containing one of these substrings is dropped.
_FORBIDDEN_KEY_PARTS = (
    "token",
    "secret",
    "password",
    "authorization",
    "auth_header",
    "api_key",
    "apikey",
    "signature",
    "recovery",
    "card",
    "cvc",
    "pan",
    "credential",
)
# Exact keys that are fine despite containing a forbidden part.
_ALLOWED_EXACT_KEYS = frozenset({"token_id", "token_name", "token_prefix_last_four"})

_MAX_METADATA_DEPTH = 4

_table_exists: bool | None = None
_probe_lock = asyncio.Lock()


def reset_audit_table_cache() -> None:
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
                await conn.fetchval("select to_regclass('public.account_audit_logs') is not null")
            )
    return _table_exists


def _sanitize_metadata(value: Any, depth: int = 0) -> Any:
    if depth >= _MAX_METADATA_DEPTH:
        return None
    if isinstance(value, dict):
        clean: dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            lowered = key_str.lower()
            if lowered not in _ALLOWED_EXACT_KEYS and any(
                part in lowered for part in _FORBIDDEN_KEY_PARTS
            ):
                continue
            clean[key_str] = _sanitize_metadata(item, depth + 1)
        return clean
    if isinstance(value, list):
        return [_sanitize_metadata(item, depth + 1) for item in value[:50]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


# Public alias so other writers (e.g. services/events.py) reuse the SAME
# secret-stripping policy rather than re-deriving the forbidden-key list.
sanitize_metadata = _sanitize_metadata


_INSERT_SQL = """
insert into account_audit_logs (
    account_id, actor_user_id, actor_type, event_type,
    resource_type, resource_id, summary, metadata,
    request_id, ip_address, user_agent
)
values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10::inet, $11)
"""


async def record_account_audit_log(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_type: str,
    summary: str,
    actor_user_id: UUID | None = None,
    actor_type: str = "user",
    resource_type: str | None = None,
    resource_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> None:
    if not await _probe_table_exists(conn):
        _logger.debug(
            "account_audit_table_missing",
            event_type=event_type,
            account_id=str(account_id),
        )
        return
    clean_metadata = _sanitize_metadata(metadata or {})
    await conn.execute(
        _INSERT_SQL,
        account_id,
        actor_user_id,
        actor_type,
        event_type,
        resource_type,
        resource_id,
        summary,
        json.dumps(clean_metadata, default=str),
        request_id or request_id_var.get("") or None,
        ip_address,
        user_agent,
    )


async def try_record_account_audit_log(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_type: str,
    summary: str,
    actor_user_id: UUID | None = None,
    actor_type: str = "user",
    resource_type: str | None = None,
    resource_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> None:
    """Best-effort wrapper: an audit failure must not fail the user action."""
    try:
        await record_account_audit_log(
            conn,
            account_id=account_id,
            event_type=event_type,
            summary=summary,
            actor_user_id=actor_user_id,
            actor_type=actor_type,
            resource_type=resource_type,
            resource_id=resource_id,
            metadata=metadata,
            request_id=request_id,
            ip_address=ip_address,
            user_agent=user_agent,
        )
    except Exception:
        _logger.warning(
            "account_audit_write_failed",
            event_type=event_type,
            account_id=str(account_id),
            exc_info=True,
        )
