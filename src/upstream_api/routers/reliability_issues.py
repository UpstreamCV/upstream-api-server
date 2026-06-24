"""Upstream Reliability M1a — issue read + resolve router.

Account-wide reliability issues inbox surface:

- ``GET  /v1/reliability/issues``            — list (optional status filter)
- ``GET  /v1/reliability/issues/{id}``       — fetch one (404 if cross-account)
- ``POST /v1/reliability/issues/{id}/resolve`` — resolve an open issue

Reads require ``reliability:read`` for API tokens (dashboard JWT users are gated
by account membership). The resolve mutation requires ``reliability:write`` for
API tokens and owner/admin role for JWT users — same posture as
``event_destinations._resolve_for_write``. The account is always resolved from
the credential; cross-account/unknown ids return 404 so existence never leaks.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.reliability_observations import (
    CollectorStatusResponse,
    ReliabilityIssueRecord,
    ReliabilityIssueResolveRequest,
    ReliabilityIssuesResponse,
    ReliabilitySourceRecord,
    ReliabilitySourcesResponse,
    SourceKind,
    SourceStatusHint,
)
from upstream_api.security.deps import (
    CallerAuth,
    CurrentCaller,
    PoolDep,
    require_account_role,
    require_token_scope,
)
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.reliability_collector_status import get_collector_status
from upstream_api.services.reliability_issues import (
    get_issue,
    list_issues,
    resolve_issue_by_id,
)
from upstream_api.services.reliability_sources import list_sources

router = APIRouter(prefix="/v1/reliability", tags=["reliability"])

_ISSUES_DEFAULT_LIMIT = 50
_ISSUES_MAX_LIMIT = 200

_SOURCES_DEFAULT_LIMIT = 100
_SOURCES_MAX_LIMIT = 500

IssueStatus = Literal["open", "resolved", "suppressed", "ignored"]


def _coerce_id_list(value: Any) -> list[str] | None:
    """``source_observation_ids`` is a jsonb array; asyncpg hands back a JSON
    string, the test fake a list, SQL NULL a ``None``."""
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return value
    parsed = json.loads(value)
    assert isinstance(parsed, list)
    return parsed


def _record(row: dict[str, Any]) -> ReliabilityIssueRecord:
    return ReliabilityIssueRecord(
        id=row["id"],
        camera_id=row["camera_id"],
        source_batch_id=row["source_batch_id"],
        external_camera_key=row["external_camera_key"],
        issue_type=row["issue_type"],
        category=row["category"],
        severity=row["severity"],
        status=row["status"],
        title=row["title"],
        message=row["message"],
        likely_cause=row["likely_cause"],
        recommended_action=row["recommended_action"],
        evidence=coerce_jsonb(row["evidence"]),
        dedupe_key=row["dedupe_key"],
        detector_version=row["detector_version"],
        source_observation_ids=_coerce_id_list(row["source_observation_ids"]),
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        resolved_at=row["resolved_at"],
        resolved_reason=row["resolved_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _resolve_account_for_write(caller: CallerAuth, account_id: UUID | None) -> UUID:
    # API tokens are authorized by scope; dashboard users by owner/admin role.
    require_token_scope(caller, "reliability:write")
    account = caller.resolve_account(account_id)
    if caller.agent is None:
        assert caller.user is not None
        require_account_role(caller.user, account, "owner", "admin")
    return account


@router.get("/collector-status", response_model=CollectorStatusResponse)
async def get_collector_status_summary(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CollectorStatusResponse:
    """Account-scoped collector ingest health (M1c).

    Cloud-visible summary of the cv_* tables for the dashboard's
    /agents/collector page: last ingest, recent batches/observations, active
    sources, open issues, and the latest collector heartbeat. Bounded to the
    last 24h. Empty account → empty/null, not 404."""
    require_token_scope(caller, "reliability:read")
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        data = await get_collector_status(conn, account_id=scope_account)
    return CollectorStatusResponse.model_validate(data)


@router.get("/sources", response_model=ReliabilitySourcesResponse)
async def list_reliability_sources(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    source_kind: Annotated[SourceKind | None, Query()] = None,
    status_hint: Annotated[SourceStatusHint | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=_SOURCES_MAX_LIMIT)] = _SOURCES_DEFAULT_LIMIT,
) -> ReliabilitySourcesResponse:
    """Account-scoped collector source registry (M2a), most-recently-seen first.

    Durable per-(source, external_camera_key) identity projected from the
    observation stream: when each source last sent data, what types, latest
    model, totals, a display-only ``status_hint``, and open-issue count. These
    are external collector/CV sources, not managed Upstream cameras. Optional
    ``source_kind`` / ``status_hint`` filters (bogus → 422). Empty account →
    empty list, not 404."""
    require_token_scope(caller, "reliability:read")
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        rows = await list_sources(
            conn,
            account_id=scope_account,
            source_kind=source_kind,
            status_hint=status_hint,
            limit=limit,
        )
    return ReliabilitySourcesResponse(
        sources=[ReliabilitySourceRecord.model_validate(r) for r in rows]
    )


@router.get("/issues", response_model=ReliabilityIssuesResponse)
async def list_reliability_issues(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[IssueStatus | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=_ISSUES_MAX_LIMIT)] = _ISSUES_DEFAULT_LIMIT,
) -> ReliabilityIssuesResponse:
    """Account-scoped reliability issues, newest-seen first. Optional ``status``
    filter; a bogus status returns 422 (Pydantic ``Literal``)."""
    require_token_scope(caller, "reliability:read")
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        rows = await list_issues(conn, account_id=scope_account, status=status, limit=limit)
    return ReliabilityIssuesResponse(issues=[_record(r) for r in rows])


@router.get("/issues/{issue_id}", response_model=ReliabilityIssueRecord)
async def get_reliability_issue(
    issue_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ReliabilityIssueRecord:
    """One account-scoped issue. Cross-account or unknown id returns 404."""
    require_token_scope(caller, "reliability:read")
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await get_issue(conn, account_id=scope_account, issue_id=issue_id)
    if row is None:
        raise NotFoundError("Issue not found.", code="issue_not_found")
    return _record(row)


@router.post("/issues/{issue_id}/resolve", response_model=ReliabilityIssueRecord)
async def resolve_reliability_issue(
    issue_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    body: ReliabilityIssueResolveRequest | None = None,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ReliabilityIssueRecord:
    """Resolve an open issue (idempotent — re-resolving an already-resolved
    issue is a no-op that emits no second event). Requires ``reliability:write``
    (tokens) / owner|admin (JWT users). Unknown/cross-account id returns 404."""
    scope_account = _resolve_account_for_write(caller, account_id)
    reason = (body.resolved_reason if body else None) or "resolved_by_user"
    now = datetime.now(tz=UTC)
    async with pool.acquire() as conn, conn.transaction():
        row = await resolve_issue_by_id(
            conn,
            account_id=scope_account,
            issue_id=issue_id,
            reason=reason,
            occurred_at=now,
        )
    if row is None:
        raise NotFoundError("Issue not found.", code="issue_not_found")
    return _record(row)
