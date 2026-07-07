"""Module 6 M6.4 — minimal read API for webhook delivery attempts + internal drain.

Read-only, account-scoped views for debugging deliveries without a dashboard:
- GET /v1/event-delivery-attempts                          (flat, filtered)
- GET /v1/events/{event_id}/delivery-attempts              (per event)
- GET /v1/event-destinations/{id}/delivery-attempts        (per destination)

Reads accept a dashboard member or an API token holding ``events:read`` OR
``destinations:read``. Cross-account ids simply yield an empty list (no leak).

POST /v1/internal/event-deliveries/drain runs one drain cycle on demand
(shared-secret auth, like the workflow-evaluator drain) — for cron / smoke tests
when the in-process loop is disabled.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from pydantic import BaseModel

from upstream_api.errors import ForbiddenError
from upstream_api.schemas.event_delivery_attempts import DeliveryAttemptListResponse
from upstream_api.security.deps import (
    CallerAuth,
    CurrentCaller,
    CurrentProcessor,
    PoolDep,
    SettingsDep,
)
from upstream_api.services.event_delivery_attempts import (
    DEFAULT_ATTEMPT_LIMIT,
    MAX_ATTEMPT_LIMIT,
    list_delivery_attempts,
)
from upstream_api.services.http_client import HttpClientDep
from upstream_api.services.webhook_delivery import drain_due_deliveries

router = APIRouter(tags=["event-delivery-attempts"])

_READ_SCOPES = {"events:read", "destinations:read"}


def _resolve_for_read(caller: CallerAuth, account_id: UUID | None) -> UUID:
    if caller.agent is not None and not (_READ_SCOPES & set(caller.agent.scopes)):
        raise ForbiddenError(
            "API token is missing a required scope.",
            code="forbidden.scope",
            details={"required_any": sorted(_READ_SCOPES)},
        )
    return caller.resolve_account(account_id)


async def _list(
    pool: PoolDep,
    *,
    account_id: UUID,
    event_id: UUID | None,
    destination_id: UUID | None,
    status: str | None,
    limit: int,
    before_created_at: datetime | None,
) -> DeliveryAttemptListResponse:
    async with pool.acquire() as conn:
        items, cursor = await list_delivery_attempts(
            conn,
            account_id=account_id,
            event_id=event_id,
            destination_id=destination_id,
            status=status,
            limit=limit,
            before_created_at=before_created_at,
        )
    return DeliveryAttemptListResponse(items=items, next_before_created_at=cursor)


@router.get("/v1/event-delivery-attempts", response_model=DeliveryAttemptListResponse)
async def list_attempts_endpoint(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[str | None, Query()] = None,
    event_id: Annotated[UUID | None, Query()] = None,
    destination_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ATTEMPT_LIMIT)] = DEFAULT_ATTEMPT_LIMIT,
    before_created_at: Annotated[datetime | None, Query()] = None,
) -> DeliveryAttemptListResponse:
    scope_account = _resolve_for_read(caller, account_id)
    return await _list(
        pool,
        account_id=scope_account,
        event_id=event_id,
        destination_id=destination_id,
        status=status,
        limit=limit,
        before_created_at=before_created_at,
    )


@router.get("/v1/events/{event_id}/delivery-attempts", response_model=DeliveryAttemptListResponse)
async def list_event_attempts_endpoint(
    event_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ATTEMPT_LIMIT)] = DEFAULT_ATTEMPT_LIMIT,
    before_created_at: Annotated[datetime | None, Query()] = None,
) -> DeliveryAttemptListResponse:
    scope_account = _resolve_for_read(caller, account_id)
    return await _list(
        pool,
        account_id=scope_account,
        event_id=event_id,
        destination_id=None,
        status=status,
        limit=limit,
        before_created_at=before_created_at,
    )


@router.get(
    "/v1/event-destinations/{destination_id}/delivery-attempts",
    response_model=DeliveryAttemptListResponse,
)
async def list_destination_attempts_endpoint(
    destination_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ATTEMPT_LIMIT)] = DEFAULT_ATTEMPT_LIMIT,
    before_created_at: Annotated[datetime | None, Query()] = None,
) -> DeliveryAttemptListResponse:
    scope_account = _resolve_for_read(caller, account_id)
    return await _list(
        pool,
        account_id=scope_account,
        event_id=None,
        destination_id=destination_id,
        status=status,
        limit=limit,
        before_created_at=before_created_at,
    )


class DrainResponse(BaseModel):
    claimed: int
    delivered: int
    failed: int
    dead: int
    errors: int


@router.post("/v1/internal/event-deliveries/drain", response_model=DrainResponse)
async def drain_endpoint(
    _auth: CurrentProcessor,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
) -> DrainResponse:
    stats = await drain_due_deliveries(pool, http, settings)
    return DrainResponse(
        claimed=stats.claimed,
        delivered=stats.delivered,
        failed=stats.failed,
        dead=stats.dead,
        errors=stats.errors,
    )
