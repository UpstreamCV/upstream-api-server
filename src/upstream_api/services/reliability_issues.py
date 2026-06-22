"""Upstream Reliability M1a — issue lifecycle DB helpers.

Mirrors services/reliability_repo.py (the processor-side findings lifecycle) but
keyed on ``(account_id, dedupe_key)`` instead of ``(camera_id, dedupe_key)`` —
the camera/source is encoded in the ``dedupe_key`` value, so an account-level
key still separates distinct entities. ``account_id`` is NEVER taken from the
request body; the caller passes the account resolved from the auth credential.

Why select-then-upsert instead of ``INSERT ... ON CONFLICT``: the "one OPEN row
per (account_id, dedupe_key)" invariant is a *partial* unique index
(``where status = 'open'``), which Postgres won't accept as an ON CONFLICT
arbiter. We lock the open row explicitly (``SELECT ... FOR UPDATE``) then UPDATE
it or INSERT a fresh one — resolved rows stack as history, and a recurrence
after resolution opens a new row.

Opening a new issue and resolving an issue each promote a canonical event
(``reliability.issue_opened`` / ``reliability.issue_resolved``) via the
best-effort emit seam in services/events.py, so issue lifecycle rides the
existing webhook delivery / event read API with no new plumbing. A re-open
(refresh of an already-open row) does NOT emit — the event fires once per
transition.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.services.events import emit_reliability_issue_event

# Returned by every read/write helper so the router shapes responses without
# picking columns at the call site.
_RETURNING_COLUMNS = """
  id, camera_id, source_batch_id, external_camera_key,
  issue_type, category, severity, status,
  title, message, likely_cause, recommended_action,
  evidence, dedupe_key, detector_version, source_observation_ids,
  first_seen_at, last_seen_at, resolved_at, resolved_reason,
  created_at, updated_at
"""

# Lock-then-decide: pick an open row by dedupe_key under a row lock so a
# concurrent detector can't insert a second open row in parallel.
_SELECT_OPEN_FOR_UPDATE_SQL = """
select id
  from cv_issues
 where account_id = $1
   and dedupe_key = $2
   and status     = 'open'
 for update
"""

_INSERT_OPEN_SQL = f"""
insert into cv_issues (
    account_id, camera_id, source_batch_id, external_camera_key,
    issue_type, category, severity, status,
    title, message, likely_cause, recommended_action,
    evidence, dedupe_key, detector_version, source_observation_ids
)
values (
    $1, $2, $3, $4,
    $5, $6, $7, 'open',
    $8, $9, $10, $11,
    $12::jsonb, $13, $14, $15::jsonb
)
returning {_RETURNING_COLUMNS}
"""

# Re-opens refresh in place; first_seen_at is preserved (original incident),
# issue_type / category / dedupe_key are stable identity and not touched.
_UPDATE_OPEN_SQL = f"""
update cv_issues
   set camera_id              = $3,
       source_batch_id        = $4,
       external_camera_key    = $5,
       severity               = $6,
       title                  = $7,
       message                = $8,
       likely_cause           = $9,
       recommended_action     = $10,
       evidence               = $11::jsonb,
       detector_version       = $12,
       source_observation_ids = $13::jsonb,
       last_seen_at           = now()
 where id = $1
   and account_id = $2
returning {_RETURNING_COLUMNS}
"""

_RESOLVE_BY_DEDUPE_SQL = f"""
update cv_issues
   set status          = 'resolved',
       resolved_at     = now(),
       resolved_reason = $3
 where account_id = $1
   and dedupe_key = any($2::text[])
   and status     = 'open'
returning {_RETURNING_COLUMNS}
"""

_SELECT_BY_ID_FOR_UPDATE_SQL = f"""
select {_RETURNING_COLUMNS}
  from cv_issues
 where account_id = $1
   and id = $2
 for update
"""

_RESOLVE_BY_ID_SQL = f"""
update cv_issues
   set status          = 'resolved',
       resolved_at     = now(),
       resolved_reason = $3
 where account_id = $1
   and id = $2
   and status = 'open'
returning {_RETURNING_COLUMNS}
"""

_LIST_SQL = f"""
select {_RETURNING_COLUMNS}
  from cv_issues
 where account_id = $1
   and ($2::text is null or status = $2)
 order by last_seen_at desc
 limit $3
"""

_GET_SQL = f"""
select {_RETURNING_COLUMNS}
  from cv_issues
 where account_id = $1
   and id = $2
"""


def _ids_json(source_observation_ids: list[UUID] | None) -> str | None:
    if source_observation_ids is None:
        return None
    return json.dumps([str(i) for i in source_observation_ids])


async def upsert_open_issue(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    dedupe_key: str,
    issue_type: str,
    category: str,
    severity: str,
    title: str,
    message: str,
    likely_cause: str | None,
    recommended_action: str | None,
    evidence: dict[str, Any],
    detector_version: str,
    camera_id: UUID | None,
    external_camera_key: str | None,
    source_batch_id: UUID | None,
    source_observation_ids: list[UUID] | None,
    occurred_at: datetime,
) -> tuple[dict[str, Any], bool]:
    """Open or refresh an issue under ``dedupe_key``.

    Returns ``(row, opened_new)``. On a fresh INSERT (``opened_new=True``) a
    ``reliability.issue_opened`` event is emitted; a refresh of an existing open
    row does not emit. Must run inside the caller's transaction so the SELECT
    FOR UPDATE keeps a concurrent detector from racing in a duplicate open row.
    """
    evidence_json = json.dumps(evidence, default=str)
    ids_json = _ids_json(source_observation_ids)
    existing = await conn.fetchrow(_SELECT_OPEN_FOR_UPDATE_SQL, account_id, dedupe_key)
    if existing is not None:
        row = await conn.fetchrow(
            _UPDATE_OPEN_SQL,
            existing["id"],
            account_id,
            camera_id,
            source_batch_id,
            external_camera_key,
            severity,
            title,
            message,
            likely_cause,
            recommended_action,
            evidence_json,
            detector_version,
            ids_json,
        )
        assert row is not None  # UPDATE ... RETURNING on the locked row.
        return dict(row), False

    row = await conn.fetchrow(
        _INSERT_OPEN_SQL,
        account_id,
        camera_id,
        source_batch_id,
        external_camera_key,
        issue_type,
        category,
        severity,
        title,
        message,
        likely_cause,
        recommended_action,
        evidence_json,
        dedupe_key,
        detector_version,
        ids_json,
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    await emit_reliability_issue_event(
        conn,
        issue_id=row["id"],
        kind="opened",
        account_id=account_id,
        occurred_at=occurred_at,
        camera_id=camera_id,
        payload={
            "issue_type": issue_type,
            "category": category,
            "severity": severity,
            "title": title,
            "dedupe_key": dedupe_key,
        },
        evidence=evidence,
    )
    return dict(row), True


async def resolve_open_issues_by_dedupe(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    dedupe_keys: list[str],
    reason: str,
    occurred_at: datetime,
) -> list[dict[str, Any]]:
    """Flip all matching open issues to ``resolved`` and emit a resolved event
    per row actually flipped. No-op (and no round-trip) on an empty key list.
    Used by detectors when a previously-flagged condition has recovered."""
    if not dedupe_keys:
        return []
    rows = await conn.fetch(_RESOLVE_BY_DEDUPE_SQL, account_id, dedupe_keys, reason)
    for r in rows:
        await emit_reliability_issue_event(
            conn,
            issue_id=r["id"],
            kind="resolved",
            account_id=account_id,
            occurred_at=occurred_at,
            camera_id=r["camera_id"],
            payload={"resolved_reason": reason},
        )
    return [dict(r) for r in rows]


async def resolve_issue_by_id(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    issue_id: UUID,
    reason: str,
    occurred_at: datetime,
) -> dict[str, Any] | None:
    """Resolve one account-scoped issue by id (the manual resolve endpoint).

    Returns ``None`` when the issue does not exist OR belongs to another account
    (the router turns ``None`` into a 404 so cross-account existence never
    leaks). Idempotent: an already-resolved issue is returned unchanged and emits
    no second event. Must run inside the caller's transaction.
    """
    existing = await conn.fetchrow(_SELECT_BY_ID_FOR_UPDATE_SQL, account_id, issue_id)
    if existing is None:
        return None
    if existing["status"] != "open":
        return dict(existing)
    row = await conn.fetchrow(_RESOLVE_BY_ID_SQL, account_id, issue_id, reason)
    assert row is not None  # the locked row was open; the UPDATE matches it.
    await emit_reliability_issue_event(
        conn,
        issue_id=issue_id,
        kind="resolved",
        account_id=account_id,
        occurred_at=occurred_at,
        camera_id=row["camera_id"],
        payload={"resolved_reason": reason},
    )
    return dict(row)


async def list_issues(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    status: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Account-scoped issues, newest-seen first, optionally filtered by status."""
    rows = await conn.fetch(_LIST_SQL, account_id, status, limit)
    return [dict(r) for r in rows]


async def get_issue(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    issue_id: UUID,
) -> dict[str, Any] | None:
    """Fetch one account-scoped issue. ``None`` when missing or cross-account
    (the router turns it into a 404)."""
    row = await conn.fetchrow(_GET_SQL, account_id, issue_id)
    return dict(row) if row is not None else None
