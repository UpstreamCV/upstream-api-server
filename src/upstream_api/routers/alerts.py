"""Rules/Triggers M1 — the alert inbox.

Dashboard-only (Supabase user auth). Any member can list and mark alerts
read; resolving is owner/admin (it is a workflow decision, not a view
state). Alerts are written only by the rule evaluator (services/rules.py).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ForbiddenError, NotFoundError
from upstream_api.schemas.alerts import AlertListResponse, AlertSummary
from upstream_api.security.deps import CurrentUser, PoolDep, require_account_role
from upstream_api.services.alerts import list_alerts, mark_alert_read, resolve_alert
from upstream_api.services.processor import coerce_jsonb

router = APIRouter(prefix="/v1/alerts", tags=["alerts"])


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


def _summary(row: dict[str, Any]) -> AlertSummary:
    return AlertSummary(
        id=row["id"],
        camera_id=row["camera_id"],
        rule_id=row["rule_id"],
        observation_id=row["observation_id"],
        workflow_run_id=row["workflow_run_id"],
        severity=row["severity"],
        status=row["status"],
        title=row["title"],
        message=row["message"],
        payload=coerce_jsonb(row["payload"]),
        created_at=row["created_at"],
        read_at=row["read_at"],
        resolved_at=row["resolved_at"],
    )


@router.get("", response_model=AlertListResponse)
async def list_alerts_endpoint(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[Literal["open", "resolved"] | None, Query()] = None,
    camera_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> AlertListResponse:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        rows = await list_alerts(
            conn,
            account_id=scope_account,
            status=status,
            camera_id=camera_id,
            limit=limit,
        )
    return AlertListResponse(alerts=[_summary(r) for r in rows])


@router.post("/{alert_id}/read", response_model=AlertSummary)
async def mark_read_endpoint(
    alert_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> AlertSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn, conn.transaction():
        row = await mark_alert_read(conn, account_id=scope_account, alert_id=alert_id)
    if row is None:
        # 404 for both "doesn't exist" and cross-tenant — anti-enumeration.
        raise NotFoundError("Alert not found.", code="alert_not_found")
    return _summary(row)


@router.post("/{alert_id}/resolve", response_model=AlertSummary)
async def resolve_endpoint(
    alert_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> AlertSummary:
    scope_account = _resolve_account_for_user(user, account_id)
    require_account_role(user, scope_account, "owner", "admin")
    async with pool.acquire() as conn, conn.transaction():
        row = await resolve_alert(conn, account_id=scope_account, alert_id=alert_id)
    if row is None:
        raise NotFoundError("Alert not found.", code="alert_not_found")
    return _summary(row)
