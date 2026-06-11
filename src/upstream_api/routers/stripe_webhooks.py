"""Module 7 M2 — Stripe webhook receiver.

POST /v1/webhooks/stripe. No user auth — authentication IS the signature
check against the RAW request body (Stripe-Signature header +
UPSTREAM_STRIPE_WEBHOOK_SECRET). Never parse-then-reserialize before
verifying; the signature covers the exact bytes.

Idempotency: every event id is recorded first (insert ... on conflict do
nothing); duplicate deliveries ack 200 without reprocessing. Unhandled
event types ack 200 too — returning errors would just make Stripe retry
events we don't care about.

Handled events:
- checkout.session.completed  -> link customer/subscription to the account
  (metadata.account_id), then fetch the subscription from Stripe and sync.
  The session payload's subscription object is incomplete — never trust it.
- customer.subscription.created/updated/deleted -> sync status, price,
  period, cancel_at_period_end; entitled statuses apply the plan mapping;
  deleted downgrades to free (see services/billing.py for the contract).
- invoice.payment_failed -> record past_due so the dashboard warns
  immediately; plan/limits untouched (grace until Stripe cancels).

Logging: event ids and types only — payloads can contain billing
addresses and must never hit logs.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Request
from pydantic import BaseModel

from upstream_api.observability.logging import get_logger
from upstream_api.security.deps import PoolDep
from upstream_api.services.billing import (
    find_account_by_customer,
    mark_payment_failed,
    record_stripe_event,
    sync_subscription_to_account,
    upsert_customer,
)
from upstream_api.services.stripe_gateway import (
    InvalidWebhookSignatureError,
    StripeGatewayDep,
)

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])

_logger = get_logger("stripe_webhooks")

_SUBSCRIPTION_EVENTS = frozenset(
    {
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    }
)


class WebhookAck(BaseModel):
    received: bool = True
    duplicate: bool = False


def _account_id_from(obj: dict[str, Any]) -> UUID | None:
    metadata = obj.get("metadata") or {}
    raw = metadata.get("account_id")
    try:
        return UUID(raw) if isinstance(raw, str) else None
    except ValueError:
        return None


async def _resolve_account(conn: asyncpg.Connection, obj: dict[str, Any]) -> UUID | None:
    """Metadata account_id first (we stamp it everywhere), customer-id
    lookup second (covers subscriptions created via the Portal)."""
    account_id = _account_id_from(obj)
    if account_id is not None:
        return account_id
    customer = obj.get("customer")
    if isinstance(customer, str) and customer:
        return await find_account_by_customer(conn, customer)
    return None


async def _handle_checkout_completed(
    conn: asyncpg.Connection, gateway: StripeGatewayDep, obj: dict[str, Any]
) -> None:
    account_id = _account_id_from(obj)
    if account_id is None:
        _logger.warning("stripe_checkout_missing_account", session_id=obj.get("id"))
        return
    customer = obj.get("customer")
    if isinstance(customer, str) and customer:
        await upsert_customer(conn, account_id=account_id, customer_id=customer)
    subscription_id = obj.get("subscription")
    if isinstance(subscription_id, str) and subscription_id:
        subscription = await gateway.retrieve_subscription(subscription_id)
        await sync_subscription_to_account(conn, account_id=account_id, subscription=subscription)


async def _handle_subscription_event(conn: asyncpg.Connection, obj: dict[str, Any]) -> None:
    account_id = await _resolve_account(conn, obj)
    if account_id is None:
        _logger.warning(
            "stripe_subscription_unmatched",
            subscription_id=obj.get("id"),
        )
        return
    await sync_subscription_to_account(conn, account_id=account_id, subscription=obj)


async def _handle_payment_failed(conn: asyncpg.Connection, obj: dict[str, Any]) -> None:
    account_id = await _resolve_account(conn, obj)
    if account_id is None:
        _logger.warning("stripe_invoice_unmatched", invoice_id=obj.get("id"))
        return
    await mark_payment_failed(conn, account_id=account_id)


@router.post("/stripe", response_model=WebhookAck)
async def stripe_webhook(
    request: Request,
    pool: PoolDep,
    gateway: StripeGatewayDep,
) -> WebhookAck:
    payload = await request.body()
    signature = request.headers.get("stripe-signature")
    if not signature:
        raise InvalidWebhookSignatureError("Missing Stripe-Signature header.")
    event = gateway.construct_event(payload, signature)

    event_id = str(event.get("id"))
    event_type = str(event.get("type"))
    obj: dict[str, Any] = dict((event.get("data") or {}).get("object") or {})

    async with pool.acquire() as conn, conn.transaction():
        first_delivery = await record_stripe_event(conn, event_id=event_id, event_type=event_type)
        if not first_delivery:
            return WebhookAck(duplicate=True)

        if event_type == "checkout.session.completed":
            await _handle_checkout_completed(conn, gateway, obj)
        elif event_type in _SUBSCRIPTION_EVENTS:
            await _handle_subscription_event(conn, obj)
        elif event_type == "invoice.payment_failed":
            await _handle_payment_failed(conn, obj)
        # Anything else: recorded + acked, deliberately unprocessed.

    _logger.info("stripe_event_processed", event_id=event_id, event_type=event_type)
    return WebhookAck()
