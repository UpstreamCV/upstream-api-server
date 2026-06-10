"""Workflow Engine M2 — workflow run history reads.

Dashboard-only (Supabase user auth), any member. M2 is read-path only:
runs are created and finalized exclusively by the rule evaluator on the
observation ingest path. Mutation APIs (review/acknowledge) are deferred
until something produces the 'action_required' status.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ForbiddenError, NotFoundError
from upstream_api.schemas.workflow_runs import (
    WorkflowRunAlertRef,
    WorkflowRunListResponse,
    WorkflowRunStatus,
    WorkflowRunSummary,
)
from upstream_api.security.deps import CurrentUser, PoolDep
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.workflow_runs import fetch_workflow_run, list_workflow_runs

router = APIRouter(prefix="/v1/workflow-runs", tags=["workflow-runs"])


def _resolve_account_for_user(user: CurrentUser, account_id: UUID | None) -> UUID:
    """Pick the account scope for a user-only endpoint.

    Mirrors `CallerAuth.resolve_account` semantics but for routes that don't
    accept agent tokens (same helper shape as routers/api_tokens.py).
    """
    if account_id is not None:
        if account_id not in user.memberships:
            raise ForbiddenError(
                "User is not a member of this account.",
                code="forbidden_account",
            )
        return account_id
    if len(user.memberships) == 1:
        return next(iter(user.memberships))
    raise ForbiddenError(
        "account_id query parameter is required for users with multiple accounts.",
        code="account_id_required",
    )


def _summary(row: dict[str, Any]) -> WorkflowRunSummary:
    alert = (
        WorkflowRunAlertRef(
            id=row["alert_id"],
            status=row["alert_status"],
            severity=row["alert_severity"],
        )
        if row["alert_id"] is not None
        else None
    )
    return WorkflowRunSummary(
        id=row["id"],
        camera_id=row["camera_id"],
        rule_id=row["rule_id"],
        trigger_observation_id=row["trigger_observation_id"],
        workflow_type=row["workflow_type"],
        status=row["status"],
        title=row["title"],
        summary=row["summary"],
        trigger_type=row["trigger_type"],
        action_result=row["action_result"],
        evidence=coerce_jsonb(row["evidence"]),
        payload=coerce_jsonb(row["payload"]),
        alert=alert,
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.get("", response_model=WorkflowRunListResponse)
async def list_runs_endpoint(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[WorkflowRunStatus | None, Query()] = None,
    camera_id: Annotated[UUID | None, Query()] = None,
    rule_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> WorkflowRunListResponse:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        rows = await list_workflow_runs(
            conn,
            account_id=scope_account,
            status=status,
            camera_id=camera_id,
            rule_id=rule_id,
            limit=limit,
        )
    return WorkflowRunListResponse(runs=[_summary(r) for r in rows])


@router.get("/{workflow_run_id}", response_model=WorkflowRunSummary)
async def get_run_endpoint(
    workflow_run_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> WorkflowRunSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        row = await fetch_workflow_run(
            conn, account_id=scope_account, workflow_run_id=workflow_run_id
        )
    if row is None:
        # 404 for both "doesn't exist" and cross-tenant — anti-enumeration.
        raise NotFoundError("Workflow run not found.", code="workflow_run_not_found")
    return _summary(row)
