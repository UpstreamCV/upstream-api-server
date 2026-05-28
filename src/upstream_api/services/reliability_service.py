"""Reliability findings — request-shaping service.

Sits between the router and the evaluator + repo. The router stays thin; this
module:

1. Calls the evaluator (which writes findings).
2. Reads back the open findings (single source of truth: the table).
3. Rolls them up into the summary endpoint's shape.

Kept separate from the evaluator so the rules don't need to know about
HTTP-facing concepts like "top_findings" or "highest_severity".
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.config import Settings
from upstream_api.schemas.reliability import (
    ReliabilityFindingRecord,
    ReliabilityFindingsResponse,
    ReliabilitySummaryResponse,
    ReliabilitySummaryStatus,
)
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.reliability_evaluator import (
    evaluate_camera,
    has_recent_evaluable_signal,
)
from upstream_api.services.reliability_repo import list_open_findings_for_camera

# How many findings the summary surfaces. The card only renders the highest-
# severity one today; three leaves headroom for a "see all" affordance without
# another round-trip.
_TOP_FINDINGS_LIMIT = 3

# Map severity → rank for the categorical roll-up. The repo already orders by
# severity desc when listing, so list[0] is the highest-severity row when
# present.
_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}


def _summary_status_for(
    *,
    findings: list[dict[str, Any]],
    has_signal: bool,
) -> ReliabilitySummaryStatus:
    """Roll a list of open findings up into one of the five summary verdicts."""
    if findings:
        top_severity = min(_SEVERITY_RANK[f["severity"]] for f in findings)
        if top_severity == 0:
            return "critical"
        if top_severity == 1:
            return "needs_review"
        return "info"
    return "healthy" if has_signal else "unknown"


def _to_record(row: dict[str, Any]) -> ReliabilityFindingRecord:
    return ReliabilityFindingRecord(
        id=row["id"],
        camera_id=row["camera_id"],
        stream_session_id=row["stream_session_id"],
        processor_status_id=row["processor_status_id"],
        finding_type=row["finding_type"],
        category=row["category"],
        severity=row["severity"],
        status=row["status"],
        title=row["title"],
        message=row["message"],
        likely_cause=row["likely_cause"],
        recommended_action=row["recommended_action"],
        # asyncpg returns JSONB as ``str`` unless a codec is registered; the
        # fake pool in tests hands back ``dict`` or ``str`` depending on the
        # construction path. ``coerce_jsonb`` (same helper the processor read
        # path uses) handles both. The previous ``isinstance(..., dict) else {}``
        # short-circuited to {} in production, silently dropping every
        # finding's evidence in the API response.
        evidence=coerce_jsonb(row["evidence"]),
        dedupe_key=row["dedupe_key"],
        detector_version=row["detector_version"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        resolved_at=row["resolved_at"],
        resolved_reason=row["resolved_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def get_summary_for_camera(
    conn: asyncpg.Connection,
    *,
    settings: Settings,
    account_id: UUID,
    camera_id: UUID,
    now: datetime | None = None,
) -> ReliabilitySummaryResponse:
    """Evaluate the camera and return the summary shape the dashboard expects.

    Must run inside a transaction — the evaluator's upserts take row locks.
    ``settings`` is threaded down so the brightness rule can hit Supabase
    Storage; the rest of the service has no use for it.
    """
    evaluated_at, facts = await evaluate_camera(
        conn,
        settings=settings,
        account_id=account_id,
        camera_id=camera_id,
        now=now,
    )
    open_rows = await list_open_findings_for_camera(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        limit=_TOP_FINDINGS_LIMIT,
    )
    status = _summary_status_for(
        findings=open_rows,
        has_signal=has_recent_evaluable_signal(facts),
    )
    top = [_to_record(r) for r in open_rows]
    highest_severity = top[0].severity if top else None
    recommended = top[0].recommended_action if top else None

    return ReliabilitySummaryResponse(
        camera_id=camera_id,
        status=status,
        highest_severity=highest_severity,
        open_findings_count=len(open_rows),
        top_findings=top,
        recommended_next_action=recommended,
        last_evaluated_at=evaluated_at,
    )


async def get_findings_for_camera(
    conn: asyncpg.Connection,
    *,
    settings: Settings,
    account_id: UUID,
    camera_id: UUID,
    limit: int,
    now: datetime | None = None,
) -> ReliabilityFindingsResponse:
    """Evaluate, then return all open findings (severity-desc, last_seen_at-desc).

    Mirrors the summary call so the findings endpoint is also "evaluate on
    read" — a dashboard that hits findings/ before summary/ still gets fresh
    state without needing both calls.
    """
    evaluated_at, _facts = await evaluate_camera(
        conn,
        settings=settings,
        account_id=account_id,
        camera_id=camera_id,
        now=now,
    )
    rows = await list_open_findings_for_camera(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        limit=limit,
    )
    return ReliabilityFindingsResponse(
        findings=[_to_record(r) for r in rows],
        last_evaluated_at=evaluated_at,
    )
