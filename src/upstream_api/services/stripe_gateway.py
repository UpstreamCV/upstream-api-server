"""Module 7 M2 — thin wrapper around the Stripe SDK.

Everything Stripe-shaped goes through this gateway so routes depend on an
injectable object (tests override ``get_stripe_gateway`` with a fake — no
real Stripe calls in tests, ever). The stripe-python SDK is synchronous;
calls run in a worker thread via ``asyncio.to_thread`` so they can't block
the event loop.

Security posture: card/payment data never touches Upstream — Checkout and
the Customer Portal are Stripe-hosted pages. We store only customer /
subscription / price identifiers. Never log raw event payloads (they can
contain billing addresses); log ids and types only.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import stripe
from fastapi import Depends

from upstream_api.config import get_settings
from upstream_api.errors import ApiError


class StripeNotConfiguredError(ApiError):
    status_code = 503
    code = "billing_not_configured"


class InvalidWebhookSignatureError(ApiError):
    status_code = 400
    code = "invalid_webhook_signature"


@dataclass(frozen=True)
class StripeGateway:
    secret_key: str
    webhook_secret: str | None

    async def create_customer(self, *, name: str, account_id: UUID) -> str:
        customer = await asyncio.to_thread(
            stripe.Customer.create,
            api_key=self.secret_key,
            name=name,
            metadata={"account_id": str(account_id)},
        )
        return str(customer["id"])

    async def create_checkout_session(
        self,
        *,
        customer_id: str,
        price_id: str,
        account_id: UUID,
        plan_key: str,
        success_url: str,
        cancel_url: str,
    ) -> str:
        metadata = {"account_id": str(account_id), "plan_key": plan_key}
        session = await asyncio.to_thread(
            stripe.checkout.Session.create,
            api_key=self.secret_key,
            mode="subscription",
            customer=customer_id,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata=metadata,
            # Stamp the subscription too, so subscription.* webhook events
            # carry the account_id without a customer-id lookup.
            subscription_data={"metadata": metadata},
        )
        return str(session["url"])

    async def create_portal_session(self, *, customer_id: str, return_url: str) -> str:
        session = await asyncio.to_thread(
            stripe.billing_portal.Session.create,
            api_key=self.secret_key,
            customer=customer_id,
            return_url=return_url,
        )
        return str(session["url"])

    async def retrieve_subscription(self, subscription_id: str) -> dict[str, Any]:
        subscription = await asyncio.to_thread(
            stripe.Subscription.retrieve,
            subscription_id,
            api_key=self.secret_key,
        )
        # StripeObject is dict-like but not a Mapping to mypy; round-trip
        # through its serializer for a plain dict.
        data: dict[str, Any] = subscription.to_dict()
        return data

    def construct_event(self, payload: bytes, signature: str) -> dict[str, Any]:
        """Verify the webhook signature against the RAW request body."""
        if self.webhook_secret is None:
            raise StripeNotConfiguredError("Stripe webhook secret is not configured.")
        try:
            event = stripe.Webhook.construct_event(  # type: ignore[no-untyped-call]
                payload, signature, self.webhook_secret
            )
        except stripe.SignatureVerificationError as exc:
            raise InvalidWebhookSignatureError(
                "Stripe webhook signature verification failed."
            ) from exc
        return dict(event)


def get_stripe_gateway() -> StripeGateway:
    settings = get_settings()
    if settings.stripe_secret_key is None:
        raise StripeNotConfiguredError("Billing is not configured on this deployment.")
    webhook_secret = settings.stripe_webhook_secret
    return StripeGateway(
        secret_key=settings.stripe_secret_key.get_secret_value(),
        webhook_secret=(webhook_secret.get_secret_value() if webhook_secret else None),
    )


StripeGatewayDep = Annotated[StripeGateway, Depends(get_stripe_gateway)]
