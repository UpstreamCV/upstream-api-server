"""Reliability findings — DB helpers.

The same upsert posture as services/processor.py: SQL lives here, the router
just passes the tenant columns it has already derived from the camera row.
account_id is NEVER taken from the request body; the caller passes the
account_id resolved through the camera-ownership check.

Why a select-then-upsert dance instead of ``INSERT ... ON CONFLICT``:

The "one OPEN row per (camera_id, dedupe_key)" invariant is a *partial* unique
index (``where status = 'open'``). Postgres does not let a partial unique index
be the conflict target of ``INSERT ... ON CONFLICT``; it requires a regular
unique constraint. Rather than relax the index to a full unique that would
forbid stacking resolved-history rows, we do the lookup explicitly under a row
lock and then either UPDATE the open row or INSERT a fresh one. Concurrent
writers serialize at the index lookup + insert, and one of them gets the
unique-index violation that we trap as "the other writer just opened it".
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

# Returned by every read/write helper so the router can shape responses
# without picking columns at the call site.
_RETURNING_COLUMNS = """
  id, camera_id, stream_session_id, processor_status_id,
  finding_type, category, severity, status,
  title, message, likely_cause, recommended_action,
  evidence, dedupe_key, detector_version,
  first_seen_at, last_seen_at, resolved_at, resolved_reason,
  created_at, updated_at
"""

# Lock-then-decide: pick an open row by dedupe_key, holding a row lock for the
# duration of the transaction so a concurrent evaluator can't insert a second
# open row in parallel. NULL row → caller inserts; non-NULL → caller updates.
_SELECT_OPEN_FOR_UPDATE_SQL = """
select id
  from reliability_findings
 where account_id = $1
   and camera_id  = $2
   and dedupe_key = $3
   and status     = 'open'
 for update
"""

_INSERT_OPEN_SQL = f"""
insert into reliability_findings (
    account_id, camera_id, stream_session_id, processor_status_id,
    finding_type, category, severity, status,
    title, message, likely_cause, recommended_action,
    evidence, dedupe_key, detector_version
)
values (
    $1, $2, $3, $4,
    $5, $6, $7, 'open',
    $8, $9, $10, $11,
    $12::jsonb, $13, $14
)
returning {_RETURNING_COLUMNS}
"""

# Re-opens never insert: if a row is already 'open', refresh it instead. The
# severity / message / evidence / linked-session columns are refreshed because
# the same dedupe_key can describe a worse condition the second time around
# (e.g. processor was lagging, now it's silent). first_seen_at is preserved on
# purpose — it's the original-incident timestamp.
_UPDATE_OPEN_SQL = f"""
update reliability_findings
   set stream_session_id   = $4,
       processor_status_id = $5,
       severity            = $6,
       title               = $7,
       message             = $8,
       likely_cause        = $9,
       recommended_action  = $10,
       evidence            = $11::jsonb,
       detector_version    = $12,
       last_seen_at        = now()
 where id = $1
   and account_id = $2
   and camera_id  = $3
returning {_RETURNING_COLUMNS}
"""

_RESOLVE_OPEN_SQL = f"""
update reliability_findings
   set status          = 'resolved',
       resolved_at     = now(),
       resolved_reason = $4
 where account_id = $1
   and camera_id  = $2
   and dedupe_key = any($3::text[])
   and status     = 'open'
returning {_RETURNING_COLUMNS}
"""

# Severity ordering for `order by`. critical first, info last. Postgres has no
# enum here (the column is plain text with a CHECK), so we map via case.
#
# Two SQL variants by status:
#   - 'open'      -> severity-desc then last_seen_at-desc (priority first)
#   - any other   -> last_seen_at-desc only; resolved history orders newest-
#                    resolved first because operators reading the history
#                    care about recency, not original severity
_LIST_OPEN_SQL = f"""
select {_RETURNING_COLUMNS}
  from reliability_findings
 where account_id = $1
   and camera_id  = $2
   and status     = 'open'
 order by case severity
            when 'critical' then 0
            when 'warning'  then 1
            when 'info'     then 2
            else 3
          end,
          last_seen_at desc
 limit $3
"""

_LIST_BY_STATUS_SQL = f"""
select {_RETURNING_COLUMNS}
  from reliability_findings
 where account_id = $1
   and camera_id  = $2
   and status     = $3
 order by coalesce(resolved_at, last_seen_at) desc
 limit $4
"""


async def upsert_open_finding(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    dedupe_key: str,
    finding_type: str,
    category: str,
    severity: str,
    title: str,
    message: str,
    likely_cause: str | None,
    recommended_action: str | None,
    evidence: dict[str, Any],
    detector_version: str,
    stream_session_id: UUID | None,
    processor_status_id: UUID | None,
) -> dict[str, Any]:
    """Open or refresh a finding under ``dedupe_key``.

    If a row with the same ``(camera_id, dedupe_key)`` is already ``open``,
    refresh it (severity / message / evidence / last_seen_at). Otherwise
    insert a new ``open`` row. ``first_seen_at`` is preserved across refreshes.

    Must run inside the caller's transaction so the SELECT FOR UPDATE keeps
    a concurrent evaluator from racing in a duplicate open row.
    """
    evidence_json = json.dumps(evidence, default=str)
    existing = await conn.fetchrow(
        _SELECT_OPEN_FOR_UPDATE_SQL,
        account_id,
        camera_id,
        dedupe_key,
    )
    if existing is not None:
        row = await conn.fetchrow(
            _UPDATE_OPEN_SQL,
            existing["id"],
            account_id,
            camera_id,
            stream_session_id,
            processor_status_id,
            severity,
            title,
            message,
            likely_cause,
            recommended_action,
            evidence_json,
            detector_version,
        )
        assert row is not None  # UPDATE ... RETURNING on the locked row.
        return dict(row)

    row = await conn.fetchrow(
        _INSERT_OPEN_SQL,
        account_id,
        camera_id,
        stream_session_id,
        processor_status_id,
        finding_type,
        category,
        severity,
        title,
        message,
        likely_cause,
        recommended_action,
        evidence_json,
        dedupe_key,
        detector_version,
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return dict(row)


async def resolve_open_findings(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    dedupe_keys: list[str],
    reason: str,
) -> list[dict[str, Any]]:
    """Flip all matching open rows to ``resolved`` and stamp ``resolved_at``.

    No-op when ``dedupe_keys`` is empty — the array-equal short-circuits in SQL
    but Python skipping the call avoids a needless round-trip.
    """
    if not dedupe_keys:
        return []
    rows = await conn.fetch(
        _RESOLVE_OPEN_SQL,
        account_id,
        camera_id,
        dedupe_keys,
        reason,
    )
    return [dict(r) for r in rows]


async def list_open_findings_for_camera(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    limit: int,
) -> list[dict[str, Any]]:
    """Open findings for one camera, severity-desc then most-recently-seen.

    Kept as the dedicated open path because the summary endpoint and the
    reliability evaluator both depend on the severity-first ordering. For
    any other status (resolved/suppressed/ignored) callers use
    ``list_findings_for_camera``.
    """
    rows = await conn.fetch(_LIST_OPEN_SQL, account_id, camera_id, limit)
    return [dict(r) for r in rows]


async def list_findings_for_camera(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    status: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Findings for one camera filtered by ``status``, recency-first.

    Used by the Reliability tab's "Recently resolved" section. Ordering is
    ``coalesce(resolved_at, last_seen_at) desc`` -- resolved rows have a
    resolved_at; suppressed/ignored may not. Recency is what operators
    care about for history. Open rows go through ``list_open_findings_for_camera``
    instead so the summary's severity-first ordering stays canonical.
    """
    rows = await conn.fetch(_LIST_BY_STATUS_SQL, account_id, camera_id, status, limit)
    return [dict(r) for r in rows]
