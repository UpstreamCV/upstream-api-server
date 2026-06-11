"""Module 7 M2 — Stripe checkout/portal/webhook tests.

Stripe is NEVER called: routes get a FakeGateway via dependency_overrides
and settings access is monkeypatched. Layers:

1. /v1/billing/checkout + /v1/billing/portal — auth, roles, tenancy,
   plan validation, customer create/reuse, returned URLs.
2. /v1/webhooks/stripe — signature, idempotent duplicate delivery,
   checkout.session.completed linking, subscription sync (plan + limits
   applied / downgraded), invoice.payment_failed.
3. Usage summary billing block derivation.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.schemas.usage import BillingSummary
from upstream_api.security.deps import UserAuth, current_user
from upstream_api.services.plans import PLAN_LIMITS, plan_for_price_id
from upstream_api.services.stripe_gateway import (
    InvalidWebhookSignatureError,
    get_stripe_gateway,
)
from upstream_api.services.usage_summary import _derive_billing

_PRICE_STARTER = "price_starter_test"
_PRICE_PRO = "price_pro_test"

_FAKE_SETTINGS = SimpleNamespace(
    stripe_secret_key=SimpleNamespace(get_secret_value=lambda: "sk_test"),
    stripe_webhook_secret=SimpleNamespace(get_secret_value=lambda: "whsec_test"),
    stripe_price_starter_monthly=_PRICE_STARTER,
    stripe_price_pro_monthly=_PRICE_PRO,
    dashboard_base_url="https://app.test",
)


@pytest.fixture(autouse=True)
def _stripe_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("upstream_api.routers.billing.get_settings", lambda: _FAKE_SETTINGS)
    monkeypatch.setattr("upstream_api.services.billing.get_settings", lambda: _FAKE_SETTINGS)


def _subscription(
    account_id: UUID,
    *,
    status: str = "active",
    price: str = _PRICE_STARTER,
    cancel_at_period_end: bool = False,
) -> dict[str, Any]:
    return {
        "id": "sub_123",
        "customer": "cus_123",
        "status": status,
        "cancel_at_period_end": cancel_at_period_end,
        "items": {
            "data": [
                {
                    "price": {"id": price},
                    "current_period_start": 1_765_000_000,
                    "current_period_end": 1_767_592_000,
                }
            ]
        },
        "metadata": {"account_id": str(account_id)},
    }


class FakeGateway:
    def __init__(self, subscription: dict[str, Any] | None = None) -> None:
        self.subscription = subscription
        self.created_customers: list[dict[str, Any]] = []
        self.checkout_calls: list[dict[str, Any]] = []
        self.portal_calls: list[dict[str, Any]] = []

    async def create_customer(self, *, name: str, account_id: UUID) -> str:
        self.created_customers.append({"name": name, "account_id": account_id})
        return "cus_new"

    async def create_checkout_session(self, **kwargs: Any) -> str:
        self.checkout_calls.append(kwargs)
        return "https://checkout.stripe.com/c/test"

    async def create_portal_session(self, **kwargs: Any) -> str:
        self.portal_calls.append(kwargs)
        return "https://billing.stripe.com/p/test"

    async def retrieve_subscription(self, subscription_id: str) -> dict[str, Any]:
        assert self.subscription is not None
        return self.subscription

    def construct_event(self, payload: bytes, signature: str) -> dict[str, Any]:
        if signature != "valid":
            raise InvalidWebhookSignatureError("bad signature")
        return dict(json.loads(payload))


class _Tx:
    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Billing-flavored fake. Mutations are recorded AND folded back into
    `billing_row` so multi-step handlers see their own writes."""

    def __init__(
        self,
        *,
        billing_row: dict[str, Any] | None = None,
        account_name: str | None = "Acme Co",
        customer_to_account: dict[str, UUID] | None = None,
    ) -> None:
        self.billing_row = billing_row
        self.account_name = account_name
        self.customer_to_account = customer_to_account or {}
        self.recorded_events: set[str] = set()
        self.customer_upserts: list[tuple[Any, ...]] = []
        self.sync_upserts: list[tuple[Any, ...]] = []
        self.plan_updates: list[tuple[Any, ...]] = []
        self.limits_updates: list[tuple[Any, ...]] = []
        self.status_updates: list[tuple[Any, ...]] = []

    def transaction(self) -> _Tx:
        return _Tx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "from account_billing" in query
        return self.billing_row

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "select name from accounts" in query:
            return self.account_name
        if "insert into stripe_events" in query:
            if args[0] in self.recorded_events:
                return None
            self.recorded_events.add(args[0])
            return args[0]
        if "where stripe_customer_id" in query:
            return self.customer_to_account.get(args[0])
        raise AssertionError(f"unexpected fetchval: {query}")

    async def execute(self, query: str, *args: Any) -> str:
        if "insert into account_billing" in query:
            if "stripe_subscription_id" in query:
                self.sync_upserts.append(args)
                self.billing_row = {
                    "account_id": args[0],
                    "stripe_customer_id": args[1]
                    or (self.billing_row or {}).get("stripe_customer_id"),
                    "stripe_subscription_id": args[2],
                    "stripe_price_id": args[3],
                    "stripe_subscription_status": args[4],
                    "cancel_at_period_end": args[5],
                    "current_period_start": args[6],
                    "current_period_end": args[7],
                }
            else:
                self.customer_upserts.append(args)
                self.billing_row = {
                    **(self.billing_row or {}),
                    "account_id": args[0],
                    "stripe_customer_id": args[1],
                }
            return "INSERT 0 1"
        if "update accounts set plan" in query:
            self.plan_updates.append(args)
            return "UPDATE 1"
        if "update account_limits" in query:
            self.limits_updates.append(args)
            return "UPDATE 1"
        if "set stripe_subscription_status" in query:
            self.status_updates.append(args)
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute: {query}")


class AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    async def __aenter__(self) -> Any:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _user(account_id: UUID, role: str = "owner") -> UserAuth:
    return UserAuth(user_id=uuid4(), memberships={account_id: role}, aal="aal1", amr=("password",))


def _client(conn: FakeConn, gateway: FakeGateway, user: UserAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[get_stripe_gateway] = lambda: gateway
    if user is not None:
        app.dependency_overrides[current_user] = lambda: user
    return TestClient(app)


# ---------------------------------------------------------------------------
# Checkout.
# ---------------------------------------------------------------------------


def test_checkout_requires_auth() -> None:
    account = uuid4()
    r = _client(FakeConn(), FakeGateway(), None).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "starter"},
    )
    assert r.status_code == 401


def test_checkout_viewer_forbidden() -> None:
    account = uuid4()
    r = _client(FakeConn(), FakeGateway(), _user(account, role="viewer")).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "starter"},
    )
    assert r.status_code == 403


def test_checkout_cross_tenant_forbidden() -> None:
    member_of, other = uuid4(), uuid4()
    r = _client(FakeConn(), FakeGateway(), _user(member_of)).post(
        "/v1/billing/checkout",
        json={"account_id": str(other), "plan": "starter"},
    )
    assert r.status_code == 403


def test_checkout_unknown_plan_rejected() -> None:
    account = uuid4()
    r = _client(FakeConn(), FakeGateway(), _user(account)).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "enterprise"},
    )
    assert r.status_code == 422  # Literal["starter","pro"] rejects at schema


def test_checkout_creates_customer_when_missing() -> None:
    account = uuid4()
    conn = FakeConn(billing_row=None)
    gateway = FakeGateway()
    r = _client(conn, gateway, _user(account)).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "starter"},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"url": "https://checkout.stripe.com/c/test"}
    assert len(gateway.created_customers) == 1
    assert gateway.created_customers[0]["account_id"] == account
    assert conn.customer_upserts == [(account, "cus_new")]
    call = gateway.checkout_calls[0]
    assert call["customer_id"] == "cus_new"
    assert call["price_id"] == _PRICE_STARTER
    assert call["plan_key"] == "starter"
    assert call["account_id"] == account
    assert call["success_url"].startswith("https://app.test/dashboard/billing")


def test_checkout_reuses_existing_customer() -> None:
    account = uuid4()
    conn = FakeConn(billing_row={"account_id": account, "stripe_customer_id": "cus_existing"})
    gateway = FakeGateway()
    r = _client(conn, gateway, _user(account)).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "pro"},
    )
    assert r.status_code == 200
    assert gateway.created_customers == []
    assert conn.customer_upserts == []
    assert gateway.checkout_calls[0]["customer_id"] == "cus_existing"
    assert gateway.checkout_calls[0]["price_id"] == _PRICE_PRO


def test_checkout_admin_allowed() -> None:
    account = uuid4()
    r = _client(FakeConn(), FakeGateway(), _user(account, role="admin")).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "starter"},
    )
    assert r.status_code == 200


def test_checkout_price_not_configured_503(monkeypatch: pytest.MonkeyPatch) -> None:
    no_price = SimpleNamespace(**{**_FAKE_SETTINGS.__dict__})
    no_price.stripe_price_starter_monthly = None
    monkeypatch.setattr("upstream_api.routers.billing.get_settings", lambda: no_price)
    account = uuid4()
    r = _client(FakeConn(), FakeGateway(), _user(account)).post(
        "/v1/billing/checkout",
        json={"account_id": str(account), "plan": "starter"},
    )
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "billing_not_configured"


# ---------------------------------------------------------------------------
# Portal.
# ---------------------------------------------------------------------------


def test_portal_requires_auth() -> None:
    r = _client(FakeConn(), FakeGateway(), None).post(
        "/v1/billing/portal", json={"account_id": str(uuid4())}
    )
    assert r.status_code == 401


def test_portal_viewer_forbidden() -> None:
    account = uuid4()
    r = _client(FakeConn(), FakeGateway(), _user(account, role="viewer")).post(
        "/v1/billing/portal", json={"account_id": str(account)}
    )
    assert r.status_code == 403


def test_portal_requires_existing_customer() -> None:
    account = uuid4()
    r = _client(FakeConn(billing_row=None), FakeGateway(), _user(account)).post(
        "/v1/billing/portal", json={"account_id": str(account)}
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "billing_no_customer"


def test_portal_returns_url() -> None:
    account = uuid4()
    conn = FakeConn(billing_row={"account_id": account, "stripe_customer_id": "cus_123"})
    gateway = FakeGateway()
    r = _client(conn, gateway, _user(account)).post(
        "/v1/billing/portal", json={"account_id": str(account)}
    )
    assert r.status_code == 200
    assert r.json() == {"url": "https://billing.stripe.com/p/test"}
    assert gateway.portal_calls[0]["customer_id"] == "cus_123"
    assert gateway.portal_calls[0]["return_url"] == ("https://app.test/dashboard/billing")


# ---------------------------------------------------------------------------
# Webhook.
# ---------------------------------------------------------------------------


def _post_event(
    conn: FakeConn,
    gateway: FakeGateway,
    event: dict[str, Any],
    *,
    signature: str = "valid",
) -> Any:
    client = _client(conn, gateway, None)  # no user auth on webhooks
    return client.post(
        "/v1/webhooks/stripe",
        content=json.dumps(event),
        headers={"stripe-signature": signature},
    )


def _event(event_type: str, obj: dict[str, Any], *, event_id: str = "evt_1") -> dict[str, Any]:
    return {"id": event_id, "type": event_type, "data": {"object": obj}}


def test_webhook_rejects_invalid_signature() -> None:
    conn = FakeConn()
    r = _post_event(
        conn,
        FakeGateway(),
        _event("customer.subscription.updated", {}),
        signature="tampered",
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_webhook_signature"
    assert conn.recorded_events == set()


def test_webhook_rejects_missing_signature() -> None:
    client = _client(FakeConn(), FakeGateway(), None)
    r = client.post("/v1/webhooks/stripe", content=b"{}")
    assert r.status_code == 400


def test_webhook_checkout_completed_links_and_syncs() -> None:
    account = uuid4()
    conn = FakeConn()
    gateway = FakeGateway(subscription=_subscription(account))
    session_obj = {
        "id": "cs_1",
        "customer": "cus_123",
        "subscription": "sub_123",
        "metadata": {"account_id": str(account)},
    }
    r = _post_event(conn, gateway, _event("checkout.session.completed", session_obj))
    assert r.status_code == 200, r.text
    assert conn.customer_upserts == [(account, "cus_123")]
    assert len(conn.sync_upserts) == 1
    # Active starter subscription -> plan + limits applied.
    assert conn.plan_updates == [(account, "starter")]
    assert len(conn.limits_updates) == 1
    starter = PLAN_LIMITS["starter"]
    limits_args = conn.limits_updates[0]
    assert limits_args[1] == starter.max_cameras
    assert limits_args[9] == starter.monthly_minutes_quota


def test_webhook_subscription_updated_applies_pro_plan() -> None:
    account = uuid4()
    conn = FakeConn()
    sub = _subscription(account, price=_PRICE_PRO)
    r = _post_event(conn, FakeGateway(), _event("customer.subscription.updated", sub))
    assert r.status_code == 200
    assert conn.plan_updates == [(account, "pro")]
    row = conn.billing_row
    assert row is not None
    assert row["stripe_subscription_status"] == "active"
    assert row["stripe_price_id"] == _PRICE_PRO
    assert row["current_period_end"] is not None


def test_webhook_subscription_resolved_by_customer_when_no_metadata() -> None:
    account = uuid4()
    conn = FakeConn(customer_to_account={"cus_123": account})
    sub = _subscription(account)
    sub["metadata"] = {}
    r = _post_event(conn, FakeGateway(), _event("customer.subscription.updated", sub))
    assert r.status_code == 200
    assert conn.plan_updates == [(account, "starter")]


def test_webhook_subscription_deleted_downgrades_to_free() -> None:
    account = uuid4()
    conn = FakeConn()
    sub = _subscription(account, status="canceled")
    r = _post_event(conn, FakeGateway(), _event("customer.subscription.deleted", sub))
    assert r.status_code == 200
    assert conn.plan_updates == [(account, "free")]
    free = PLAN_LIMITS["free"]
    assert conn.limits_updates[0][1] == free.max_cameras
    assert conn.limits_updates[0][9] == free.monthly_minutes_quota


def test_webhook_past_due_records_status_but_keeps_limits() -> None:
    account = uuid4()
    conn = FakeConn()
    sub = _subscription(account, status="past_due")
    r = _post_event(conn, FakeGateway(), _event("customer.subscription.updated", sub))
    assert r.status_code == 200
    assert conn.plan_updates == []  # grace: no plan/limit change
    assert conn.limits_updates == []
    assert conn.billing_row is not None
    assert conn.billing_row["stripe_subscription_status"] == "past_due"


def test_webhook_cancel_at_period_end_keeps_plan() -> None:
    account = uuid4()
    conn = FakeConn()
    sub = _subscription(account, cancel_at_period_end=True)
    r = _post_event(conn, FakeGateway(), _event("customer.subscription.updated", sub))
    assert r.status_code == 200
    # Still active -> plan stays applied; flag recorded for the dashboard.
    assert conn.plan_updates == [(account, "starter")]
    assert conn.billing_row is not None
    assert conn.billing_row["cancel_at_period_end"] is True


def test_webhook_duplicate_event_processed_once() -> None:
    account = uuid4()
    conn = FakeConn()
    sub = _subscription(account)
    event = _event("customer.subscription.updated", sub, event_id="evt_dup")
    first = _post_event(conn, FakeGateway(), event)
    second = _post_event(conn, FakeGateway(), event)
    assert first.json() == {"received": True, "duplicate": False}
    assert second.json() == {"received": True, "duplicate": True}
    assert len(conn.sync_upserts) == 1
    assert len(conn.plan_updates) == 1


def test_webhook_unmapped_price_records_state_but_not_limits() -> None:
    account = uuid4()
    conn = FakeConn()
    sub = _subscription(account, price="price_unknown")
    r = _post_event(conn, FakeGateway(), _event("customer.subscription.updated", sub))
    assert r.status_code == 200
    assert len(conn.sync_upserts) == 1  # state recorded
    assert conn.plan_updates == []  # but limits never corrupted
    assert conn.limits_updates == []


def test_webhook_payment_failed_marks_past_due() -> None:
    account = uuid4()
    conn = FakeConn(customer_to_account={"cus_123": account})
    invoice = {"id": "in_1", "customer": "cus_123", "metadata": {}}
    r = _post_event(conn, FakeGateway(), _event("invoice.payment_failed", invoice))
    assert r.status_code == 200
    assert conn.status_updates == [(account, "past_due")]
    assert conn.plan_updates == []


def test_webhook_unhandled_event_acked() -> None:
    conn = FakeConn()
    r = _post_event(conn, FakeGateway(), _event("charge.refunded", {"id": "ch_1"}))
    assert r.status_code == 200
    assert "evt_1" in conn.recorded_events


def test_plan_for_price_id_mapping() -> None:
    assert plan_for_price_id(_FAKE_SETTINGS, _PRICE_STARTER) == "starter"  # type: ignore[arg-type]
    assert plan_for_price_id(_FAKE_SETTINGS, _PRICE_PRO) == "pro"  # type: ignore[arg-type]
    assert plan_for_price_id(_FAKE_SETTINGS, "price_other") is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Usage summary billing block derivation.
# ---------------------------------------------------------------------------


def _derive(row: dict[str, Any] | None, *, configured: bool = True) -> BillingSummary:
    import upstream_api.services.usage_summary as mod

    original = mod.get_settings
    mod.get_settings = lambda: SimpleNamespace(  # type: ignore[assignment]
        stripe_secret_key="sk" if configured else None
    )
    try:
        return _derive_billing(row)
    finally:
        mod.get_settings = original  # type: ignore[assignment]


def test_billing_block_none_before_any_checkout() -> None:
    block = _derive(None)
    assert block.status == "none"
    assert block.management_available is False
    assert block.configured is True


def test_billing_block_not_configured() -> None:
    block = _derive(None, configured=False)
    assert block.configured is False
    assert block.management_available is False


def test_billing_block_active_with_management() -> None:
    block = _derive(
        {
            "stripe_customer_id": "cus_1",
            "stripe_subscription_id": "sub_1",
            "stripe_subscription_status": "active",
            "cancel_at_period_end": False,
            "current_period_start": None,
            "current_period_end": None,
        }
    )
    assert block.status == "active"
    assert block.management_available is True


def test_billing_block_payment_trouble_maps_to_past_due() -> None:
    for raw in ("past_due", "unpaid", "incomplete"):
        block = _derive(
            {
                "stripe_customer_id": "cus_1",
                "stripe_subscription_id": "sub_1",
                "stripe_subscription_status": raw,
                "cancel_at_period_end": False,
                "current_period_start": None,
                "current_period_end": None,
            }
        )
        assert block.status == "past_due", raw
        assert block.subscription_status == raw


def test_billing_block_canceled() -> None:
    block = _derive(
        {
            "stripe_customer_id": "cus_1",
            "stripe_subscription_id": "sub_1",
            "stripe_subscription_status": "canceled",
            "cancel_at_period_end": False,
            "current_period_start": None,
            "current_period_end": None,
        }
    )
    assert block.status == "canceled"
    assert block.management_available is True  # portal still reachable


# ---------------------------------------------------------------------------
# Real-SDK conversion regression (found in production, 2026-06-11): a live
# stripe.Event is dict-LIKE but not a dict — dict(event) raises KeyError: 0
# and StripeObject has no .get(). The gateway must hand plain recursive
# dicts to the webhook handlers.
# ---------------------------------------------------------------------------


def test_construct_event_returns_plain_recursive_dicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import stripe

    from upstream_api.services.stripe_gateway import StripeGateway

    real_event = stripe.Event.construct_from(
        {
            "id": "evt_real",
            "type": "customer.subscription.updated",
            "data": {
                "object": {
                    "id": "sub_1",
                    "customer": "cus_1",
                    "status": "active",
                    "cancel_at_period_end": False,
                    "metadata": {"account_id": str(uuid4())},
                    "items": {
                        "data": [
                            {
                                "price": {"id": _PRICE_STARTER},
                                "current_period_start": 1_765_000_000,
                                "current_period_end": 1_767_592_000,
                            }
                        ]
                    },
                }
            },
        },
        "sk_test_dummy",
    )
    monkeypatch.setattr(stripe.Webhook, "construct_event", lambda *a, **k: real_event)
    gateway = StripeGateway(secret_key="sk_test_dummy", webhook_secret="whsec_x")
    event = gateway.construct_event(b"{}", "sig")
    # The exact downstream access pattern the webhook router uses:
    assert type(event) is dict
    obj = dict((event.get("data") or {}).get("object") or {})
    assert obj["id"] == "sub_1"
    items = obj.get("items") or {}
    assert type(items) is dict  # recursion reached nested objects
    assert items["data"][0]["price"]["id"] == _PRICE_STARTER


async def test_retrieve_subscription_returns_plain_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import stripe

    from upstream_api.services.billing import _subscription_fields
    from upstream_api.services.stripe_gateway import StripeGateway

    real_sub = stripe.Subscription.construct_from(
        {
            "id": "sub_2",
            "customer": "cus_2",
            "status": "active",
            "cancel_at_period_end": False,
            "items": {
                "data": [
                    {
                        "price": {"id": _PRICE_PRO},
                        "current_period_start": 1_765_000_000,
                        "current_period_end": 1_767_592_000,
                    }
                ]
            },
        },
        "sk_test_dummy",
    )
    monkeypatch.setattr(stripe.Subscription, "retrieve", lambda *a, **k: real_sub)
    gateway = StripeGateway(secret_key="sk_test_dummy", webhook_secret=None)
    sub = await gateway.retrieve_subscription("sub_2")
    assert type(sub) is dict
    fields = _subscription_fields(sub)
    assert fields["price_id"] == _PRICE_PRO
    assert fields["period_end"] is not None
