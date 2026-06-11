"""Module 7 M2 — Stripe Checkout + Customer Portal session endpoints.

Both endpoints are owner/admin only (they spend money / change the
subscription) and return a Stripe-hosted URL the dashboard redirects to.
No card data passes through Upstream.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

import asyncpg
from fastapi import APIRouter
from pydantic import BaseModel

from upstream_api.config import get_settings
from upstream_api.errors import ApiError, NotFoundError
from upstream_api.security.deps import CurrentUser, PoolDep, require_account_role
from upstream_api.services.billing import get_billing_row, upsert_customer
from upstream_api.services.plans import price_id_for_plan
from upstream_api.services.stripe_gateway import (
    StripeGatewayDep,
    StripeNotConfiguredError,
)

router = APIRouter(prefix="/v1/billing", tags=["billing"])


class InvalidPlanError(ApiError):
    status_code = 422
    code = "invalid_plan"


class NoBillingCustomerError(ApiError):
    status_code = 409
    code = "billing_no_customer"


class CheckoutRequest(BaseModel):
    account_id: UUID
    plan: Literal["starter", "pro"]


class PortalRequest(BaseModel):
    account_id: UUID


class BillingUrlResponse(BaseModel):
    url: str


async def _ensure_customer(
    conn: asyncpg.Connection,
    gateway: StripeGatewayDep,
    *,
    account_id: UUID,
) -> str:
    """Return the account's Stripe customer id, creating one on first use."""
    billing = await get_billing_row(conn, account_id)
    existing = billing.get("stripe_customer_id") if billing else None
    if isinstance(existing, str) and existing:
        return existing
    account_name = await conn.fetchval("select name from accounts where id = $1", account_id)
    if account_name is None:
        raise NotFoundError("Account not found.", code="account_not_found")
    customer_id = await gateway.create_customer(name=str(account_name), account_id=account_id)
    await upsert_customer(conn, account_id=account_id, customer_id=customer_id)
    return customer_id


@router.post("/checkout", response_model=BillingUrlResponse)
async def create_checkout(
    body: CheckoutRequest,
    user: CurrentUser,
    pool: PoolDep,
    gateway: StripeGatewayDep,
) -> BillingUrlResponse:
    require_account_role(user, body.account_id, "owner", "admin")
    settings = get_settings()
    price_id = price_id_for_plan(settings, body.plan)
    if price_id is None:
        # Key present but the price for this plan isn't wired up.
        raise StripeNotConfiguredError(f"No Stripe price configured for plan {body.plan!r}.")

    async with pool.acquire() as conn, conn.transaction():
        customer_id = await _ensure_customer(conn, gateway, account_id=body.account_id)

    billing_url = f"{settings.dashboard_base_url}/dashboard/billing"
    url = await gateway.create_checkout_session(
        customer_id=customer_id,
        price_id=price_id,
        account_id=body.account_id,
        plan_key=body.plan,
        success_url=f"{billing_url}?checkout=success",
        cancel_url=f"{billing_url}?checkout=canceled",
    )
    return BillingUrlResponse(url=url)


@router.post("/portal", response_model=BillingUrlResponse)
async def create_portal(
    body: PortalRequest,
    user: CurrentUser,
    pool: PoolDep,
    gateway: StripeGatewayDep,
) -> BillingUrlResponse:
    require_account_role(user, body.account_id, "owner", "admin")

    async with pool.acquire() as conn:
        billing = await get_billing_row(conn, body.account_id)
    customer_id = billing.get("stripe_customer_id") if billing else None
    if not isinstance(customer_id, str) or not customer_id:
        raise NoBillingCustomerError(
            "This account has no billing profile yet — start a checkout first."
        )

    settings = get_settings()
    url = await gateway.create_portal_session(
        customer_id=customer_id,
        return_url=f"{settings.dashboard_base_url}/dashboard/billing",
    )
    return BillingUrlResponse(url=url)
