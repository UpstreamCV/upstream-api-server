"""Module 6 M6.2 — Event REST API.

Read access to the canonical event ledger (M6.1). Two endpoints:

- ``GET /v1/events`` — keyset-paginated (by ``seq``), newest-first, filtered.
- ``GET /v1/events/{event_id}`` — one full canonical event.

Dual auth via ``CurrentCaller``: dashboard JWT users (gated by account
membership) and developer API tokens (gated by the ``events:read`` scope).
Account scoping is enforced in application code (``where account_id = $1``);
cross-account ids return 404, never leaking existence. Read-only — this module
never writes or mutates events.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.events import CanonicalEvent, EventListResponse
from upstream_api.security.deps import CurrentCaller, PoolDep, require_token_scope
from upstream_api.services.events import (
    DEFAULT_EVENT_LIMIT,
    MAX_EVENT_LIMIT,
    get_event,
    list_events,
)

router = APIRouter(prefix="/v1/events", tags=["events"])


@router.get("", response_model=EventListResponse)
async def list_events_endpoint(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    camera_id: Annotated[UUID | None, Query()] = None,
    event_type: Annotated[str | None, Query()] = None,
    source: Annotated[str | None, Query()] = None,
    rule_id: Annotated[UUID | None, Query()] = None,
    workflow_id: Annotated[UUID | None, Query()] = None,
    workflow_run_id: Annotated[UUID | None, Query()] = None,
    session_id: Annotated[UUID | None, Query()] = None,
    start_time: Annotated[datetime | None, Query()] = None,
    end_time: Annotated[datetime | None, Query()] = None,
    has_evidence: Annotated[bool | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_EVENT_LIMIT)] = DEFAULT_EVENT_LIMIT,
    before_seq: Annotated[int | None, Query(ge=1)] = None,
    after_seq: Annotated[int | None, Query(ge=0)] = None,
) -> EventListResponse:
    require_token_scope(caller, "events:read")
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        items, next_before_seq = await list_events(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            event_type=event_type,
            source=source,
            rule_id=rule_id,
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            session_id=session_id,
            start_time=start_time,
            end_time=end_time,
            has_evidence=has_evidence,
            limit=limit,
            before_seq=before_seq,
            after_seq=after_seq,
        )
    return EventListResponse(items=items, next_before_seq=next_before_seq)


@router.get("/{event_id}", response_model=CanonicalEvent)
async def get_event_endpoint(
    event_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CanonicalEvent:
    require_token_scope(caller, "events:read")
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        event = await get_event(conn, account_id=scope_account, event_id=event_id)
    if event is None:
        # 404 for both unknown and cross-account ids — never leak existence.
        raise NotFoundError("Event not found.", code="event_not_found")
    return event
