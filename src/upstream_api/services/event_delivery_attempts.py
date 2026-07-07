"""Module 6 M6.4 — webhook delivery attempts: the durable delivery queue.

This module owns the DB side of delivery:

- ``enqueue_delivery_attempts_for_event`` — called inside the event-insert
  transaction (best-effort savepoint) to create one ``pending`` attempt per
  matching enabled webhook destination. Matching is done in Python
  (``destination_matches_event``) so it is unit-testable.
- ``claim_due_attempts`` — ``FOR UPDATE SKIP LOCKED`` batch claim (multi-worker
  safe), flipping rows to ``delivering`` and incrementing ``attempt_count``.
- ``load_delivery_contexts`` — joins event + destination and pgcrypto-decrypts
  the signing secret (the ONLY place the secret is decrypted; never logged).
- ``record_delivery_result`` + ``update_destination_health`` — persist outcome.
- read helpers for the minimal debug API.

The HTTP send / signing / retry policy lives in ``services/webhook_delivery.py``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.config import get_settings
from upstream_api.observability.logging import get_logger
from upstream_api.schemas.event_delivery_attempts import DeliveryAttempt
from upstream_api.schemas.events import CanonicalEvent

_logger = get_logger("event_delivery_attempts")

# Read-path bounds (mirrors the events reader).
DEFAULT_ATTEMPT_LIMIT = 50
MAX_ATTEMPT_LIMIT = 100


# ---------------------------------------------------------------------------
# Matching (pure, unit-testable). Empty filter = "all".
# ---------------------------------------------------------------------------


def destination_matches_event(
    *,
    event_type_filters: list[str],
    camera_id_filters: list[UUID],
    workflow_id_filters: list[UUID],
    event: CanonicalEvent,
) -> bool:
    if event_type_filters and event.event_type not in event_type_filters:
        return False
    if camera_id_filters and (event.camera_id is None or event.camera_id not in camera_id_filters):
        return False
    return not (
        workflow_id_filters
        and (event.workflow_id is None or event.workflow_id not in workflow_id_filters)
    )


_ENABLED_WEBHOOK_DESTS_SQL = """
select id, event_type_filters, camera_id_filters, workflow_id_filters
  from event_destinations
 where account_id = $1
   and enabled = true
   and destination_type = 'webhook'
"""

_INSERT_ATTEMPT_SQL = """
insert into event_delivery_attempts (account_id, event_id, destination_id, max_attempts)
values ($1, $2, $3, $4)
on conflict (event_id, destination_id) do nothing
returning id
"""


async def enqueue_delivery_attempts_for_event(
    conn: asyncpg.Connection,
    *,
    event: CanonicalEvent,
) -> int:
    """Create a pending delivery attempt per matching enabled webhook
    destination, in the caller's transaction. Best-effort: isolated in a
    savepoint so an enqueue failure never breaks event creation. Returns the
    number of attempts enqueued (0 on any failure)."""
    try:
        async with conn.transaction():
            dests = await conn.fetch(_ENABLED_WEBHOOK_DESTS_SQL, event.account_id)
            max_attempts = get_settings().webhook_delivery_max_attempts
            enqueued = 0
            for d in dests:
                if not destination_matches_event(
                    event_type_filters=list(d["event_type_filters"]),
                    camera_id_filters=list(d["camera_id_filters"]),
                    workflow_id_filters=list(d["workflow_id_filters"]),
                    event=event,
                ):
                    continue
                attempt_id = await conn.fetchval(
                    _INSERT_ATTEMPT_SQL,
                    event.account_id,
                    event.id,
                    d["id"],
                    max_attempts,
                )
                if attempt_id is not None:
                    enqueued += 1
            return enqueued
    except Exception as exc:  # best-effort: enqueue must never fail event insert
        _logger.warning(
            "enqueue_delivery_attempts_failed",
            event_id=str(event.id),
            error=type(exc).__name__,
        )
        return 0


# ---------------------------------------------------------------------------
# Claiming (multi-worker safe).
# ---------------------------------------------------------------------------

_CLAIM_SQL = """
with due as (
  select id
    from event_delivery_attempts
   where status in ('pending', 'failed')
     and next_attempt_at <= now()
     and attempt_count < max_attempts
   order by next_attempt_at asc, created_at asc
   for update skip locked
   limit $1
)
update event_delivery_attempts a
   set status = 'delivering',
       started_at = now(),
       last_attempt_at = now(),
       attempt_count = a.attempt_count + 1,
       updated_at = now()
  from due
 where a.id = due.id
returning a.id, a.account_id, a.event_id, a.destination_id, a.attempt_count, a.max_attempts
"""


async def claim_due_attempts(conn: asyncpg.Connection, *, limit: int) -> list[dict[str, Any]]:
    """Claim up to ``limit`` due attempts with FOR UPDATE SKIP LOCKED, flipping
    them to 'delivering' and bumping attempt_count. Call inside a transaction so
    the claim commits before the (slow) HTTP send releases the row lock."""
    rows = await conn.fetch(_CLAIM_SQL, limit)
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Delivery context (decrypts the signing secret — the only such place).
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DeliveryContext:
    attempt_id: UUID
    account_id: UUID
    destination_id: UUID
    attempt_count: int
    max_attempts: int
    url: str
    secret: str  # plaintext signing secret — in-memory only, never logged
    event: CanonicalEvent


_LOAD_CONTEXT_SQL = """
select a.id as attempt_id, a.account_id, a.destination_id,
       a.attempt_count, a.max_attempts,
       d.url as url,
       pgp_sym_decrypt(d.signing_secret_encrypted, $2) as secret,
       e.id as e_id, e.seq, e.event_type, e.schema_version,
       e.camera_id, e.session_id, e.source, e.source_observation_id,
       e.workflow_run_id, e.source_alert_id, e.source_finding_id, e.rule_id,
       e.workflow_id, e.dedupe_key, e.occurred_at, e.created_at as e_created_at,
       e.payload, e.evidence, e.reliability_context
  from event_delivery_attempts a
  join event_destinations d on d.id = a.destination_id
  join events e on e.id = a.event_id
 where a.id = any($1::uuid[])
"""


def _jsonb_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    parsed = json.loads(value)  # type: ignore[arg-type]
    assert isinstance(parsed, dict)
    return parsed


def _jsonb_optional(value: object) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    parsed = json.loads(value)  # type: ignore[arg-type]
    assert isinstance(parsed, dict)
    return parsed


def _context_from_row(row: Any) -> DeliveryContext:
    event = CanonicalEvent(
        id=row["e_id"],
        seq=row["seq"],
        event_type=row["event_type"],
        schema_version=row["schema_version"],
        account_id=row["account_id"],
        camera_id=row["camera_id"],
        session_id=row["session_id"],
        source=row["source"],
        source_observation_id=row["source_observation_id"],
        workflow_run_id=row["workflow_run_id"],
        source_alert_id=row["source_alert_id"],
        source_finding_id=row["source_finding_id"],
        rule_id=row["rule_id"],
        workflow_id=row["workflow_id"],
        dedupe_key=row["dedupe_key"],
        occurred_at=row["occurred_at"],
        created_at=row["e_created_at"],
        payload=_jsonb_dict(row["payload"]),
        evidence=_jsonb_dict(row["evidence"]),
        reliability_context=_jsonb_optional(row["reliability_context"]),
    )
    secret = row["secret"]
    if isinstance(secret, (bytes, bytearray, memoryview)):
        secret = bytes(secret).decode()
    return DeliveryContext(
        attempt_id=row["attempt_id"],
        account_id=row["account_id"],
        destination_id=row["destination_id"],
        attempt_count=row["attempt_count"],
        max_attempts=row["max_attempts"],
        url=row["url"],
        secret=secret,
        event=event,
    )


async def load_delivery_contexts(
    conn: asyncpg.Connection, *, attempt_ids: list[UUID], enc_key: str
) -> list[DeliveryContext]:
    if not attempt_ids:
        return []
    rows = await conn.fetch(_LOAD_CONTEXT_SQL, attempt_ids, enc_key)
    return [_context_from_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Recording results + destination health.
# ---------------------------------------------------------------------------

_RECORD_RESULT_SQL = """
update event_delivery_attempts
   set status = $2,
       response_status_code = $3,
       response_body_preview = $4,
       error_message = $5,
       next_attempt_at = coalesce($6, next_attempt_at),
       completed_at = $7,
       updated_at = now()
 where id = $1
"""

_HEALTH_SUCCESS_SQL = """
update event_destinations
   set last_success_at = now(), consecutive_failures = 0
 where id = $1
"""

_HEALTH_FAILURE_SQL = """
update event_destinations
   set last_failure_at = now(), consecutive_failures = consecutive_failures + 1
 where id = $1
"""


async def record_delivery_result(
    conn: asyncpg.Connection,
    *,
    attempt_id: UUID,
    destination_id: UUID,
    status: str,
    response_status_code: int | None,
    response_body_preview: str | None,
    error_message: str | None,
    next_attempt_at: datetime | None,
    completed_at: datetime,
) -> None:
    """Persist an attempt outcome and update destination health, in one txn."""
    async with conn.transaction():
        await conn.execute(
            _RECORD_RESULT_SQL,
            attempt_id,
            status,
            response_status_code,
            response_body_preview,
            error_message,
            next_attempt_at,
            completed_at,
        )
        if status == "delivered":
            await conn.execute(_HEALTH_SUCCESS_SQL, destination_id)
        else:
            await conn.execute(_HEALTH_FAILURE_SQL, destination_id)


# ---------------------------------------------------------------------------
# Read API.
# ---------------------------------------------------------------------------

_ATTEMPT_COLUMNS = """
    id, account_id, event_id, destination_id, status, attempt_count, max_attempts,
    next_attempt_at, started_at, completed_at, response_status_code,
    response_body_preview, error_message, last_attempt_at, created_at, updated_at
"""

_LIST_ATTEMPTS_SQL = f"""
select
{_ATTEMPT_COLUMNS}
  from event_delivery_attempts
 where account_id = $1
   and ($2::uuid is null or event_id = $2)
   and ($3::uuid is null or destination_id = $3)
   and ($4::text is null or status = $4)
   and ($5::timestamptz is null or created_at < $5)
 order by created_at desc, id desc
 limit $6
"""


def _row_to_attempt(row: Any) -> DeliveryAttempt:
    return DeliveryAttempt(
        id=row["id"],
        account_id=row["account_id"],
        event_id=row["event_id"],
        destination_id=row["destination_id"],
        status=row["status"],
        attempt_count=row["attempt_count"],
        max_attempts=row["max_attempts"],
        next_attempt_at=row["next_attempt_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        response_status_code=row["response_status_code"],
        response_body_preview=row["response_body_preview"],
        error_message=row["error_message"],
        last_attempt_at=row["last_attempt_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def list_delivery_attempts(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_id: UUID | None = None,
    destination_id: UUID | None = None,
    status: str | None = None,
    limit: int = DEFAULT_ATTEMPT_LIMIT,
    before_created_at: datetime | None = None,
) -> tuple[list[DeliveryAttempt], datetime | None]:
    limit = max(1, min(limit, MAX_ATTEMPT_LIMIT))
    rows = await conn.fetch(
        _LIST_ATTEMPTS_SQL,
        account_id,
        event_id,
        destination_id,
        status,
        before_created_at,
        limit,
    )
    items = [_row_to_attempt(r) for r in rows]
    next_cursor = items[-1].created_at if len(items) == limit else None
    return items, next_cursor
