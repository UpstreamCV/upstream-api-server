"""Rules/Triggers M1 — user-facing rule CRUD.

Dashboard-only (Supabase user auth). Mutations are owner/admin; reads are any
member. ``rule_type`` is immutable after create — delete and recreate to
change it (keeps per-type config validation single-shot).
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query

from upstream_api.errors import ApiError, ForbiddenError, NotFoundError
from upstream_api.schemas.rules import (
    RuleCreateRequest,
    RuleListResponse,
    RulePatchRequest,
    RuleSummary,
    validate_rule_config,
)
from upstream_api.security.deps import CurrentUser, PoolDep, require_account_role
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.rules import (
    delete_rule,
    fetch_rule,
    insert_rule,
    list_rules,
    update_rule,
)

router = APIRouter(prefix="/v1/rules", tags=["rules"])


class InvalidRuleConfigError(ApiError):
    status_code = 422
    code = "invalid_rule_config"


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


def _check_config(rule_type: str, config: dict[str, Any]) -> None:
    problems = validate_rule_config(rule_type, config)
    if problems:
        raise InvalidRuleConfigError(
            "Rule config is invalid.",
            details={"problems": problems},
        )


async def _require_owned_camera(
    conn: asyncpg.Connection, *, account_id: UUID, camera_id: UUID
) -> None:
    # 404 for both "doesn't exist" and "belongs to another account" prevents
    # camera-id enumeration — same posture as the observations read path.
    row = await conn.fetchrow(
        "select account_id from cameras where id = $1",
        camera_id,
    )
    if row is None or row["account_id"] != account_id:
        raise NotFoundError("Camera not found.", code="camera_not_found")


def _summary(row: dict[str, Any]) -> RuleSummary:
    return RuleSummary(
        id=row["id"],
        camera_id=row["camera_id"],
        name=row["name"],
        rule_type=row["rule_type"],
        enabled=row["enabled"],
        config=coerce_jsonb(row["config"]),
        severity=row["severity"],
        cooldown_seconds=row["cooldown_seconds"],
        created_by=row["created_by"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.post("", response_model=RuleSummary, status_code=201)
async def create_rule(
    body: RuleCreateRequest,
    user: CurrentUser,
    pool: PoolDep,
) -> RuleSummary:
    require_account_role(user, body.account_id, "owner", "admin")
    _check_config(body.rule_type, body.config)

    async with pool.acquire() as conn, conn.transaction():
        await _require_owned_camera(conn, account_id=body.account_id, camera_id=body.camera_id)
        row = await insert_rule(
            conn,
            account_id=body.account_id,
            created_by=user.user_id,
            req=body,
        )
    return _summary(row)


@router.get("", response_model=RuleListResponse)
async def list_rules_endpoint(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    camera_id: Annotated[UUID | None, Query()] = None,
) -> RuleListResponse:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        rows = await list_rules(conn, account_id=scope_account, camera_id=camera_id)
    return RuleListResponse(rules=[_summary(r) for r in rows])


@router.get("/{rule_id}", response_model=RuleSummary)
async def get_rule(
    rule_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> RuleSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        row = await fetch_rule(conn, account_id=scope_account, rule_id=rule_id)
    if row is None:
        raise NotFoundError("Rule not found.", code="rule_not_found")
    return _summary(row)


@router.patch("/{rule_id}", response_model=RuleSummary)
async def patch_rule(
    rule_id: UUID,
    body: RulePatchRequest,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> RuleSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    require_account_role(user, scope_account, "owner", "admin")

    async with pool.acquire() as conn, conn.transaction():
        existing = await fetch_rule(conn, account_id=scope_account, rule_id=rule_id)
        if existing is None:
            raise NotFoundError("Rule not found.", code="rule_not_found")
        if body.config is not None:
            # Validate against the stored (immutable) type, not client input.
            _check_config(existing["rule_type"], body.config)
        row = await update_rule(
            conn,
            account_id=scope_account,
            rule_id=rule_id,
            name=body.name,
            enabled=body.enabled,
            config=body.config,
            severity=body.severity,
            cooldown_seconds=body.cooldown_seconds,
        )
    assert row is not None  # Row existed inside the same transaction.
    return _summary(row)


@router.delete("/{rule_id}", status_code=204)
async def delete_rule_endpoint(
    rule_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> None:
    scope_account = _resolve_account_for_user(user, account_id)
    require_account_role(user, scope_account, "owner", "admin")

    async with pool.acquire() as conn, conn.transaction():
        deleted = await delete_rule(conn, account_id=scope_account, rule_id=rule_id)
    if not deleted:
        raise NotFoundError("Rule not found.", code="rule_not_found")
