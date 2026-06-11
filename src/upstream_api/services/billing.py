"""Module 7 M2 — billing state persistence + subscription sync.

The sync contract (documented behavior, pinned by tests):

- status ``active`` / ``trialing`` with a known price -> accounts.plan and
  account_limits are OVERWRITTEN from the plan mapping. Unknown price ->
  billing state recorded, plan/limits untouched (logged; a misconfigured
  price env must not corrupt limits).
- ``cancel_at_period_end`` true while active -> nothing downgrades yet;
  the flag is recorded and the dashboard shows "cancels on <date>". The
  downgrade happens when Stripe actually deletes the subscription at
  period end.
- status ``canceled`` (subscription deleted) -> plan + limits downgrade to
  'free'. Cameras/workflows/rules are NEVER deleted — the free limits
  simply gate new activity; existing over-limit resources stay.
- ``past_due`` / ``unpaid`` / ``incomplete*`` -> status recorded, plan and
  limits left alone (grace). Stripe's dunning settings decide when the
  subscription actually cancels, which then triggers the downgrade.

Webhook idempotency: ``record_stripe_event`` inserts the Stripe event id
with ON CONFLICT DO NOTHING — a duplicate delivery returns False and the
handler skips. Same single-statement-guard posture as the rest of the
codebase.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.config import get_settings
from upstream_api.observability.logging import get_logger
from upstream_api.services.account_audit import try_record_account_audit_log
from upstream_api.services.plans import apply_plan_to_account, plan_for_price_id

_logger = get_logger("billing")

# Subscription statuses that keep (or grant) the paid plan's limits.
_ENTITLED_STATUSES = frozenset({"active", "trialing"})

_GET_BILLING_SQL = """
select account_id, stripe_customer_id, stripe_subscription_id,
       stripe_price_id, stripe_subscription_status,
       cancel_at_period_end, current_period_start, current_period_end
  from account_billing
 where account_id = $1
"""

_UPSERT_CUSTOMER_SQL = """
insert into account_billing (account_id, stripe_customer_id)
values ($1, $2)
on conflict (account_id)
do update set stripe_customer_id = excluded.stripe_customer_id
"""

_SYNC_SUBSCRIPTION_SQL = """
insert into account_billing (
    account_id, stripe_customer_id, stripe_subscription_id, stripe_price_id,
    stripe_subscription_status, cancel_at_period_end,
    current_period_start, current_period_end
)
values ($1, $2, $3, $4, $5, $6, $7, $8)
on conflict (account_id)
do update set
    stripe_customer_id = coalesce(excluded.stripe_customer_id,
                                  account_billing.stripe_customer_id),
    stripe_subscription_id = excluded.stripe_subscription_id,
    stripe_price_id = excluded.stripe_price_id,
    stripe_subscription_status = excluded.stripe_subscription_status,
    cancel_at_period_end = excluded.cancel_at_period_end,
    current_period_start = excluded.current_period_start,
    current_period_end = excluded.current_period_end
"""

_ACCOUNT_BY_CUSTOMER_SQL = """
select account_id from account_billing where stripe_customer_id = $1
"""

_RECORD_EVENT_SQL = """
insert into stripe_events (id, event_type)
values ($1, $2)
on conflict (id) do nothing
returning id
"""

_SET_SUBSCRIPTION_STATUS_SQL = """
update account_billing
   set stripe_subscription_status = $2
 where account_id = $1
"""


async def get_billing_row(conn: asyncpg.Connection, account_id: UUID) -> dict[str, Any] | None:
    row = await conn.fetchrow(_GET_BILLING_SQL, account_id)
    return dict(row) if row is not None else None


async def upsert_customer(conn: asyncpg.Connection, *, account_id: UUID, customer_id: str) -> None:
    await conn.execute(_UPSERT_CUSTOMER_SQL, account_id, customer_id)


async def find_account_by_customer(conn: asyncpg.Connection, customer_id: str) -> UUID | None:
    value = await conn.fetchval(_ACCOUNT_BY_CUSTOMER_SQL, customer_id)
    return value if isinstance(value, UUID) else None


async def record_stripe_event(conn: asyncpg.Connection, *, event_id: str, event_type: str) -> bool:
    """True when this is the first delivery; False on a duplicate."""
    inserted = await conn.fetchval(_RECORD_EVENT_SQL, event_id, event_type)
    return inserted is not None


async def mark_payment_failed(conn: asyncpg.Connection, *, account_id: UUID) -> None:
    """invoice.payment_failed: record the problem, keep plan/limits (grace).

    Stripe sends customer.subscription.updated with the authoritative
    status alongside; this just makes the dashboard warn immediately even
    if that event is delayed.
    """
    await conn.execute(_SET_SUBSCRIPTION_STATUS_SQL, account_id, "past_due")
    await try_record_account_audit_log(
        conn,
        account_id=account_id,
        actor_type="stripe",
        event_type="billing.payment_failed",
        resource_type="billing",
        summary="A subscription payment failed (Stripe).",
        metadata={"subscription_status": "past_due"},
    )


def _epoch_to_dt(value: Any) -> datetime | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, tz=UTC)
    return None


def _subscription_fields(subscription: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields we persist, tolerating both Stripe API shapes.

    Newer Stripe API versions moved current_period_start/end from the
    subscription onto its items; read the item first, fall back to the
    top level.
    """
    items = subscription.get("items") or {}
    data = items.get("data") or []
    first_item: dict[str, Any] = data[0] if data else {}
    price = first_item.get("price") or {}
    customer = subscription.get("customer")
    return {
        "subscription_id": subscription.get("id"),
        "customer_id": customer if isinstance(customer, str) else None,
        "status": subscription.get("status"),
        "price_id": price.get("id"),
        "cancel_at_period_end": bool(subscription.get("cancel_at_period_end")),
        "period_start": _epoch_to_dt(
            first_item.get("current_period_start") or subscription.get("current_period_start")
        ),
        "period_end": _epoch_to_dt(
            first_item.get("current_period_end") or subscription.get("current_period_end")
        ),
    }


async def sync_subscription_to_account(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    subscription: dict[str, Any],
) -> None:
    fields = _subscription_fields(subscription)
    await conn.execute(
        _SYNC_SUBSCRIPTION_SQL,
        account_id,
        fields["customer_id"],
        fields["subscription_id"],
        fields["price_id"],
        fields["status"],
        fields["cancel_at_period_end"],
        fields["period_start"],
        fields["period_end"],
    )

    await try_record_account_audit_log(
        conn,
        account_id=account_id,
        actor_type="stripe",
        event_type="billing.subscription_synced",
        resource_type="stripe_subscription",
        resource_id=fields["subscription_id"],
        summary=f"Stripe subscription synced (status: {fields['status']}).",
        metadata={
            "subscription_status": fields["status"],
            "cancel_at_period_end": fields["cancel_at_period_end"],
        },
    )

    status = fields["status"]
    if status in _ENTITLED_STATUSES:
        plan_key = (
            plan_for_price_id(get_settings(), fields["price_id"]) if fields["price_id"] else None
        )
        if plan_key is None:
            _logger.warning(
                "stripe_price_unmapped",
                account_id=str(account_id),
                price_id=fields["price_id"],
                subscription_id=fields["subscription_id"],
            )
            return
        await _apply_plan_with_audit(conn, account_id=account_id, plan_key=plan_key)
        _logger.info(
            "billing_plan_applied",
            account_id=str(account_id),
            plan=plan_key,
            subscription_status=status,
        )
    elif status == "canceled":
        await try_record_account_audit_log(
            conn,
            account_id=account_id,
            actor_type="stripe",
            event_type="billing.subscription_canceled",
            resource_type="stripe_subscription",
            resource_id=fields["subscription_id"],
            summary="Stripe subscription canceled — account downgraded to the free plan.",
            metadata={},
        )
        await _apply_plan_with_audit(conn, account_id=account_id, plan_key="free")
        _logger.info(
            "billing_plan_downgraded",
            account_id=str(account_id),
            subscription_id=fields["subscription_id"],
        )
    # past_due / unpaid / incomplete*: state recorded above; limits stay.


async def _apply_plan_with_audit(
    conn: asyncpg.Connection, *, account_id: UUID, plan_key: str
) -> None:
    """apply_plan_to_account + audit events. plan_changed only fires when
    the plan actually changes; limits_updated fires on every apply (the
    limits row is overwritten even for a same-plan re-sync)."""
    old_plan = await conn.fetchval("select plan from accounts where id = $1", account_id)
    await apply_plan_to_account(conn, account_id=account_id, plan_key=plan_key)
    if old_plan != plan_key:
        await try_record_account_audit_log(
            conn,
            account_id=account_id,
            actor_type="stripe",
            event_type="billing.plan_changed",
            resource_type="account",
            resource_id=str(account_id),
            summary=f"Plan changed from {old_plan!r} to {plan_key!r}.",
            metadata={"old_plan": old_plan, "new_plan": plan_key},
        )
    await try_record_account_audit_log(
        conn,
        account_id=account_id,
        actor_type="stripe",
        event_type="billing.limits_updated",
        resource_type="account_limits",
        resource_id=str(account_id),
        summary=f"Account limits set from the {plan_key!r} plan mapping.",
        metadata={"plan": plan_key},
    )
