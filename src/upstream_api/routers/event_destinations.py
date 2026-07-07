"""Module 6 M6.3 — webhook destination REST API.

Account-scoped CRUD for webhook destinations (where canonical events will
eventually be delivered). Config only — no outbound delivery.

Auth: dual via ``CurrentCaller``.
- Reads require ``destinations:read`` for API tokens; dashboard users are gated
  by account membership.
- Writes require ``destinations:write`` for API tokens; dashboard users must be
  account owner/admin.

The signing secret is generated server-side, stored pgcrypto-encrypted, and
returned in plaintext ONLY from the create endpoint; every other response
carries just the masked preview. Unknown/cross-account ids return 404.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ApiError, NotFoundError
from upstream_api.schemas.event_destinations import (
    EventDestination,
    EventDestinationCreate,
    EventDestinationCreateResponse,
    EventDestinationListResponse,
    EventDestinationUpdate,
)
from upstream_api.security.deps import (
    CallerAuth,
    CurrentCaller,
    PoolDep,
    SettingsDep,
    require_account_role,
    require_token_scope,
)
from upstream_api.services.account_audit import try_record_account_audit_log
from upstream_api.services.event_destinations import (
    create_destination,
    delete_destination,
    get_destination,
    list_destinations,
    update_destination,
)

router = APIRouter(prefix="/v1/event-destinations", tags=["event-destinations"])


def _resolve_for_read(caller: CallerAuth, account_id: UUID | None) -> UUID:
    require_token_scope(caller, "destinations:read")
    return caller.resolve_account(account_id)


def _resolve_for_write(caller: CallerAuth, account_id: UUID | None) -> UUID:
    # API tokens are authorized by scope; dashboard users by owner/admin role.
    require_token_scope(caller, "destinations:write")
    account = caller.resolve_account(account_id)
    if caller.agent is None:
        assert caller.user is not None
        require_account_role(caller.user, account, "owner", "admin")
    return account


def _actor(caller: CallerAuth) -> tuple[UUID | None, str]:
    if caller.user is not None:
        return caller.user.user_id, "user"
    return None, "internal"


@router.get("", response_model=EventDestinationListResponse)
async def list_destinations_endpoint(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    enabled: Annotated[bool | None, Query()] = None,
    destination_type: Annotated[str | None, Query()] = None,
) -> EventDestinationListResponse:
    scope_account = _resolve_for_read(caller, account_id)
    async with pool.acquire() as conn:
        items = await list_destinations(
            conn,
            account_id=scope_account,
            enabled=enabled,
            destination_type=destination_type,
        )
    return EventDestinationListResponse(items=items)


@router.post("", response_model=EventDestinationCreateResponse, status_code=201)
async def create_destination_endpoint(
    body: EventDestinationCreate,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> EventDestinationCreateResponse:
    scope_account = _resolve_for_write(caller, account_id)
    enc_key = settings.webhook_secret_enc_key
    if enc_key is None:
        raise ApiError(
            "Webhook signing-secret encryption key is not configured.",
            code="webhook_encryption_unconfigured",
            status_code=503,
        )
    env_label = "live" if settings.is_production else "test"
    actor_user_id, actor_type = _actor(caller)
    async with pool.acquire() as conn, conn.transaction():
        dest, plaintext = await create_destination(
            conn,
            account_id=scope_account,
            created_by=actor_user_id,
            payload=body,
            enc_key=enc_key.get_secret_value(),
            env_label=env_label,
        )
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            actor_user_id=actor_user_id,
            actor_type=actor_type,
            event_type="event_destination.created",
            resource_type="event_destination",
            resource_id=str(dest.id),
            summary=f"Webhook destination {dest.name!r} created.",
            metadata={"name": dest.name, "url": dest.url, "enabled": dest.enabled},
        )
    return EventDestinationCreateResponse(**dest.model_dump(), signing_secret=plaintext)


@router.get("/{destination_id}", response_model=EventDestination)
async def get_destination_endpoint(
    destination_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> EventDestination:
    scope_account = _resolve_for_read(caller, account_id)
    async with pool.acquire() as conn:
        dest = await get_destination(conn, account_id=scope_account, destination_id=destination_id)
    if dest is None:
        raise NotFoundError("Event destination not found.", code="event_destination_not_found")
    return dest


@router.patch("/{destination_id}", response_model=EventDestination)
async def update_destination_endpoint(
    destination_id: UUID,
    body: EventDestinationUpdate,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> EventDestination:
    scope_account = _resolve_for_write(caller, account_id)
    actor_user_id, actor_type = _actor(caller)
    async with pool.acquire() as conn, conn.transaction():
        dest = await update_destination(
            conn, account_id=scope_account, destination_id=destination_id, patch=body
        )
        if dest is None:
            raise NotFoundError("Event destination not found.", code="event_destination_not_found")
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            actor_user_id=actor_user_id,
            actor_type=actor_type,
            event_type="event_destination.updated",
            resource_type="event_destination",
            resource_id=str(dest.id),
            summary=f"Webhook destination {dest.name!r} updated.",
            metadata={
                "enabled": dest.enabled,
                "fields": sorted(body.model_dump(exclude_none=True)),
            },
        )
    return dest


@router.delete("/{destination_id}", status_code=204)
async def delete_destination_endpoint(
    destination_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> None:
    scope_account = _resolve_for_write(caller, account_id)
    actor_user_id, actor_type = _actor(caller)
    async with pool.acquire() as conn, conn.transaction():
        deleted = await delete_destination(
            conn, account_id=scope_account, destination_id=destination_id
        )
        if not deleted:
            raise NotFoundError("Event destination not found.", code="event_destination_not_found")
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            actor_user_id=actor_user_id,
            actor_type=actor_type,
            event_type="event_destination.deleted",
            resource_type="event_destination",
            resource_id=str(destination_id),
            summary="Webhook destination deleted.",
            metadata={"destination_id": str(destination_id)},
        )
