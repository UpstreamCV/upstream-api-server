"""Workflow Engine M5 — workflow definition CRUD.

Dashboard-only (Supabase user auth). Mutations are owner/admin; reads any
member. ``workflow_type`` is immutable after create. Mirrors the rules
router's posture exactly (camera-ownership 404, 422 invalid config with
problem details, anti-enumeration 404s).
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query

from upstream_api.errors import ApiError, ForbiddenError, NotFoundError
from upstream_api.schemas.workflows import (
    WorkflowCreateRequest,
    WorkflowListResponse,
    WorkflowPatchRequest,
    WorkflowSummary,
    validate_sequence_config,
)
from upstream_api.security.deps import CurrentUser, PoolDep, require_account_role
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.workflows import (
    delete_workflow,
    fetch_workflow,
    insert_workflow,
    list_workflows,
    update_workflow,
)

router = APIRouter(prefix="/v1/workflows", tags=["workflows"])


class InvalidWorkflowConfigError(ApiError):
    status_code = 422
    code = "invalid_workflow_config"


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


def _check_config(workflow_type: str, config: dict[str, Any]) -> None:
    # 'sequence' is the only type the create schema accepts; PATCH validates
    # against the stored type, which can only be 'sequence' today.
    problems = (
        validate_sequence_config(config)
        if workflow_type == "sequence"
        else [f"Unsupported workflow_type: {workflow_type!r}."]
    )
    if problems:
        raise InvalidWorkflowConfigError(
            "Workflow config is invalid.",
            details={"problems": problems},
        )


async def _require_owned_camera(
    conn: asyncpg.Connection, *, account_id: UUID, camera_id: UUID
) -> None:
    # 404 for both "doesn't exist" and "belongs to another account" prevents
    # camera-id enumeration — same posture as the rules router.
    row = await conn.fetchrow(
        "select account_id from cameras where id = $1",
        camera_id,
    )
    if row is None or row["account_id"] != account_id:
        raise NotFoundError("Camera not found.", code="camera_not_found")


def _summary(row: dict[str, Any]) -> WorkflowSummary:
    return WorkflowSummary(
        id=row["id"],
        camera_id=row["camera_id"],
        name=row["name"],
        description=row["description"],
        workflow_type=row["workflow_type"],
        enabled=row["enabled"],
        config=coerce_jsonb(row["config"]),
        severity=row["severity"],
        cooldown_seconds=row["cooldown_seconds"],
        created_by=row["created_by"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.post("", response_model=WorkflowSummary, status_code=201)
async def create_workflow(
    body: WorkflowCreateRequest,
    user: CurrentUser,
    pool: PoolDep,
) -> WorkflowSummary:
    require_account_role(user, body.account_id, "owner", "admin")
    _check_config(body.workflow_type, body.config)

    async with pool.acquire() as conn, conn.transaction():
        await _require_owned_camera(conn, account_id=body.account_id, camera_id=body.camera_id)
        row = await insert_workflow(
            conn,
            account_id=body.account_id,
            created_by=user.user_id,
            req=body,
        )
    return _summary(row)


@router.get("", response_model=WorkflowListResponse)
async def list_workflows_endpoint(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    camera_id: Annotated[UUID | None, Query()] = None,
    workflow_type: Annotated[str | None, Query(alias="type")] = None,
) -> WorkflowListResponse:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        rows = await list_workflows(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            workflow_type=workflow_type,
        )
    return WorkflowListResponse(workflows=[_summary(r) for r in rows])


@router.get("/{workflow_id}", response_model=WorkflowSummary)
async def get_workflow(
    workflow_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> WorkflowSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        row = await fetch_workflow(conn, account_id=scope_account, workflow_id=workflow_id)
    if row is None:
        raise NotFoundError("Workflow not found.", code="workflow_not_found")
    return _summary(row)


@router.patch("/{workflow_id}", response_model=WorkflowSummary)
async def patch_workflow(
    workflow_id: UUID,
    body: WorkflowPatchRequest,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> WorkflowSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    require_account_role(user, scope_account, "owner", "admin")

    async with pool.acquire() as conn, conn.transaction():
        existing = await fetch_workflow(conn, account_id=scope_account, workflow_id=workflow_id)
        if existing is None:
            raise NotFoundError("Workflow not found.", code="workflow_not_found")
        if body.config is not None:
            # Validate against the stored (immutable) type, not client input.
            _check_config(existing["workflow_type"], body.config)
        row = await update_workflow(
            conn,
            account_id=scope_account,
            workflow_id=workflow_id,
            name=body.name,
            description=body.description,
            enabled=body.enabled,
            config=body.config,
            severity=body.severity,
            cooldown_seconds=body.cooldown_seconds,
        )
    assert row is not None  # Row existed inside the same transaction.
    return _summary(row)


@router.delete("/{workflow_id}", status_code=204)
async def delete_workflow_endpoint(
    workflow_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> None:
    scope_account = _resolve_account_for_user(user, account_id)
    require_account_role(user, scope_account, "owner", "admin")

    async with pool.acquire() as conn, conn.transaction():
        deleted = await delete_workflow(conn, account_id=scope_account, workflow_id=workflow_id)
    if not deleted:
        raise NotFoundError("Workflow not found.", code="workflow_not_found")
