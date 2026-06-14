"""Module 6 M6.1 — canonical event ledger writes.

The durable, append-only record of meaningful product events. M6.1 emits only
workflow-run-derived events: ``finalize_workflow_run`` (services/workflow_runs.py)
calls ``emit_workflow_run_event`` inside the producing transaction, so an event
commits atomically with the run finalization (transactional outbox) and a
rollback drops both.

Design constraints (from the M6.1 brief and the surrounding codebase):

- ``insert_event`` runs on the caller's connection and NEVER opens its own
  transaction — it must join whatever transaction the producer already holds.
- Emission is purely additive: ``emit_workflow_run_event`` wraps the insert in
  a savepoint and is best-effort, so a ledger failure (e.g. the table not yet
  migrated, or an unexpected error) can never roll back or otherwise change the
  existing workflow-run / alert behavior. The savepoint only isolates the
  event's OWN failure; a successfully-inserted event still rides — and rolls
  back with — the outer transaction.
- Idempotency: ``dedupe_key`` is stable per (account, run, status,
  action_result); the unique index + ``ON CONFLICT DO NOTHING`` absorb a
  repeated finalization (the documented double-fire across two API workers, or
  a retry) without inserting a duplicate.
- Payloads are sanitized through the same secret-stripping policy as the audit
  log (``account_audit.sanitize_metadata``); secrets / raw image data / huge
  blobs must never reach this module in the first place — the sanitizer is the
  backstop, not the policy.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.logging import get_logger
from upstream_api.schemas.events import EVENT_SCHEMA_VERSION, CanonicalEvent
from upstream_api.services.account_audit import sanitize_metadata

# Read-path bounds (M6.2). Mirrors the audit-logs keyset reader.
DEFAULT_EVENT_LIMIT = 50
MAX_EVENT_LIMIT = 100

_logger = get_logger("events")


# ---------------------------------------------------------------------------
# Pure mapping helpers (no I/O) — testable in isolation.
# ---------------------------------------------------------------------------


def workflow_run_event_type(status: str, action_result: str | None) -> str:
    """Map a finalized workflow_run ``(status, action_result)`` to a canonical
    ``event_type``.

    M6.1's finalize seam only ever produces ``alert.created`` (a run completed
    by creating an alert) and ``workflow.cooldown_suppressed`` (a match
    suppressed inside its cooldown window). The remaining branches are defined
    for future producers that will route through this seam — sequence
    completion/failure/timeout and run start — so the vocabulary is stable
    before those land. The fallback never silently drops an event.
    """
    result = (action_result or "").lower()
    if action_result == "alert_created":
        return "alert.created"
    if status == "suppressed":
        return "workflow.cooldown_suppressed"
    if status == "completed":
        return "workflow.succeeded"
    if status == "failed":
        if "timeout" in result or "timed_out" in result:
            return "workflow.timed_out"
        return "workflow.failed"
    if status == "active":
        return "workflow.started"
    return "workflow.finalized"


def workflow_run_dedupe_key(
    workflow_run_id: UUID, status: str, action_result: str | None
) -> str:
    """Stable idempotency key for a run finalization. A run is finalized once
    with a deterministic (status, action_result), so re-finalizing produces the
    same key and is absorbed by ``ON CONFLICT (account_id, dedupe_key)``."""
    return f"workflow_run:{workflow_run_id}:{status}:{action_result or 'none'}"


# ---------------------------------------------------------------------------
# Ledger insert.
# ---------------------------------------------------------------------------

_INSERT_EVENT_SQL = """
insert into events (
    account_id, camera_id, session_id, event_type, schema_version, source,
    source_observation_id, workflow_run_id, source_alert_id, source_finding_id,
    rule_id, workflow_id, dedupe_key, occurred_at,
    payload, evidence, reliability_context
)
values (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10,
    $11, $12, $13, $14,
    $15::jsonb, $16::jsonb, $17::jsonb
)
on conflict (account_id, dedupe_key) do nothing
returning
    id, seq, account_id, camera_id, session_id, event_type, schema_version,
    source, source_observation_id, workflow_run_id, source_alert_id,
    source_finding_id, rule_id, workflow_id, dedupe_key, occurred_at,
    created_at, payload, evidence, reliability_context
"""


async def insert_event(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_type: str,
    source: str,
    dedupe_key: str,
    occurred_at: datetime,
    camera_id: UUID | None = None,
    session_id: UUID | None = None,
    workflow_run_id: UUID | None = None,
    workflow_id: UUID | None = None,
    rule_id: UUID | None = None,
    source_observation_id: UUID | None = None,
    source_alert_id: UUID | None = None,
    source_finding_id: UUID | None = None,
    payload: dict[str, Any] | None = None,
    evidence: dict[str, Any] | None = None,
    reliability_context: dict[str, Any] | None = None,
    schema_version: int = EVENT_SCHEMA_VERSION,
) -> dict[str, Any] | None:
    """Append one canonical event on the caller's connection/transaction.

    Idempotent via ``ON CONFLICT (account_id, dedupe_key) DO NOTHING``. Returns
    the inserted row as a dict, or ``None`` when the row already existed (a
    duplicate ``dedupe_key`` for the account). NEVER opens its own transaction
    — it must participate in whatever transaction the producer holds.
    """
    if not event_type:
        raise ValueError("event_type is required")
    if not source:
        raise ValueError("source is required")
    if not dedupe_key:
        raise ValueError("dedupe_key is required")

    clean_payload = sanitize_metadata(payload or {})
    clean_evidence = sanitize_metadata(evidence or {})
    clean_context = (
        sanitize_metadata(reliability_context) if reliability_context is not None else None
    )

    row = await conn.fetchrow(
        _INSERT_EVENT_SQL,
        account_id,
        camera_id,
        session_id,
        event_type,
        schema_version,
        source,
        source_observation_id,
        workflow_run_id,
        source_alert_id,
        source_finding_id,
        rule_id,
        workflow_id,
        dedupe_key,
        occurred_at,
        json.dumps(clean_payload, default=str),
        json.dumps(clean_evidence, default=str),
        json.dumps(clean_context, default=str) if clean_context is not None else None,
    )
    if row is None:
        _logger.debug(
            "event_deduped",
            event_type=event_type,
            account_id=str(account_id),
            dedupe_key=dedupe_key,
        )
        return None
    _logger.debug(
        "event_inserted",
        event_type=event_type,
        account_id=str(account_id),
        seq=row["seq"],
    )
    return dict(row)


# ---------------------------------------------------------------------------
# Workflow-run finalization → canonical event (the M6.1 emit seam).
# ---------------------------------------------------------------------------


async def emit_workflow_run_event(
    conn: asyncpg.Connection,
    *,
    workflow_run_id: UUID,
    status: str,
    action_result: str | None,
    account_id: UUID,
    occurred_at: datetime,
    camera_id: UUID | None = None,
    source: str = "workflow",
    rule_id: UUID | None = None,
    workflow_id: UUID | None = None,
    session_id: UUID | None = None,
    source_observation_id: UUID | None = None,
    source_alert_id: UUID | None = None,
    trigger_type: str | None = None,
    evidence: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Emit the canonical event for a just-finalized workflow run.

    Best-effort by design: the insert runs in its own savepoint so a ledger
    failure (table not yet migrated, or any unexpected error) is isolated and
    logged, never rolling back the run finalization / alert. A successful
    insert still belongs to the outer transaction and rolls back with it.

    The event ``payload`` carries the finalize context plus the run's rule
    snapshot; ``evidence`` carries the run's triggering evidence (the matched
    observation payload, or the timer condition evidence). Both are sanitized.
    """
    event_type = workflow_run_event_type(status, action_result)
    dedupe_key = workflow_run_dedupe_key(workflow_run_id, status, action_result)

    event_payload: dict[str, Any] = {
        "status": status,
        "action_result": action_result,
    }
    if trigger_type is not None:
        event_payload["trigger_type"] = trigger_type
    if payload:
        event_payload.update(payload)

    try:
        async with conn.transaction():
            return await insert_event(
                conn,
                account_id=account_id,
                event_type=event_type,
                source=source,
                dedupe_key=dedupe_key,
                occurred_at=occurred_at,
                camera_id=camera_id,
                session_id=session_id,
                workflow_run_id=workflow_run_id,
                workflow_id=workflow_id,
                rule_id=rule_id,
                source_observation_id=source_observation_id,
                source_alert_id=source_alert_id,
                payload=event_payload,
                evidence=evidence or {},
            )
    except Exception as exc:  # emission is best-effort: it must never fail the run.
        _logger.warning(
            "workflow_run_event_emit_failed",
            workflow_run_id=str(workflow_run_id),
            event_type=event_type,
            error=type(exc).__name__,
        )
        return None


# ---------------------------------------------------------------------------
# Read path (M6.2): account-scoped list + single-event fetch.
# ---------------------------------------------------------------------------

_EVENT_COLUMNS = """
    id, seq, account_id, camera_id, session_id, event_type, schema_version,
    source, source_observation_id, workflow_run_id, source_alert_id,
    source_finding_id, rule_id, workflow_id, dedupe_key, occurred_at,
    created_at, payload, evidence, reliability_context
"""

# Keyset pagination on the monotonic per-account ``seq``, newest-first. Every
# filter is a parameterized "null means unfiltered" predicate so one prepared
# statement serves all filter combinations (same shape as the audit-logs and
# workflow-runs readers).
_LIST_EVENTS_SQL = f"""
select
{_EVENT_COLUMNS}
  from events
 where account_id = $1
   and ($2::uuid is null or camera_id = $2)
   and ($3::text is null or event_type = $3)
   and ($4::text is null or source = $4)
   and ($5::uuid is null or rule_id = $5)
   and ($6::uuid is null or workflow_id = $6)
   and ($7::uuid is null or workflow_run_id = $7)
   and ($8::uuid is null or session_id = $8)
   and ($9::timestamptz is null or occurred_at >= $9)
   and ($10::timestamptz is null or occurred_at < $10)
   and ($11::bigint is null or seq < $11)
   and ($12::bigint is null or seq > $12)
   and ($13::boolean is null or (evidence <> '{{}}'::jsonb) = $13)
 order by seq desc
 limit $14
"""

_GET_EVENT_SQL = f"""
select
{_EVENT_COLUMNS}
  from events
 where account_id = $1
   and id = $2
"""


def _jsonb_dict(value: object) -> dict[str, Any]:
    """asyncpg returns jsonb as ``str`` unless a codec is registered; the fake
    pool in tests hands back ``dict``. Accept both (local copy to avoid a
    services.processor import cycle)."""
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    parsed = json.loads(value)  # type: ignore[arg-type]
    assert isinstance(parsed, dict)
    return parsed


def _jsonb_optional(value: object) -> dict[str, Any] | None:
    """Like ``_jsonb_dict`` but preserves SQL NULL as ``None`` (for the
    nullable ``reliability_context`` column)."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    parsed = json.loads(value)  # type: ignore[arg-type]
    assert isinstance(parsed, dict)
    return parsed


def _row_to_event(row: Any) -> CanonicalEvent:
    return CanonicalEvent(
        id=row["id"],
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
        created_at=row["created_at"],
        payload=_jsonb_dict(row["payload"]),
        evidence=_jsonb_dict(row["evidence"]),
        reliability_context=_jsonb_optional(row["reliability_context"]),
    )


async def list_events(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID | None = None,
    event_type: str | None = None,
    source: str | None = None,
    rule_id: UUID | None = None,
    workflow_id: UUID | None = None,
    workflow_run_id: UUID | None = None,
    session_id: UUID | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    has_evidence: bool | None = None,
    limit: int = DEFAULT_EVENT_LIMIT,
    before_seq: int | None = None,
    after_seq: int | None = None,
) -> tuple[list[CanonicalEvent], int | None]:
    """List account-scoped canonical events, newest-first by ``seq``.

    Keyset pagination: ``before_seq`` returns the next (older) page
    (``seq < before_seq``); ``after_seq`` returns newer events
    (``seq > after_seq``) for polling. Returns ``(items, next_before_seq)``,
    where ``next_before_seq`` is the cursor for the next page, or ``None`` when
    the page was not full (no more rows). ``account_id`` is always enforced.
    """
    limit = max(1, min(limit, MAX_EVENT_LIMIT))
    rows = await conn.fetch(
        _LIST_EVENTS_SQL,
        account_id,
        camera_id,
        event_type,
        source,
        rule_id,
        workflow_id,
        workflow_run_id,
        session_id,
        start_time,
        end_time,
        before_seq,
        after_seq,
        has_evidence,
        limit,
    )
    items = [_row_to_event(r) for r in rows]
    next_before_seq = items[-1].seq if len(items) == limit else None
    return items, next_before_seq


async def get_event(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    event_id: UUID,
) -> CanonicalEvent | None:
    """Fetch one account-scoped event by id. Returns ``None`` when the event
    does not exist OR belongs to another account — the router turns ``None``
    into a 404 so cross-account existence never leaks."""
    row = await conn.fetchrow(_GET_EVENT_SQL, account_id, event_id)
    return _row_to_event(row) if row is not None else None
