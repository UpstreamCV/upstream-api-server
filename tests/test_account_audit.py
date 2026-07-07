"""Module 7 M3 — tenant audit log tests.

Layers:

1. Metadata sanitization + the audit service (insert shape, missing-table
   probe, best-effort wrapper).
2. ``GET /v1/audit-logs`` — auth, tenancy, filters, pagination.
3. Mutation-point coverage: every instrumented route writes the right
   event with safe metadata (tokens, rules, workflows, billing, webhook
   sync, session limit rejection).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, UserAuth, current_agent, current_user
from upstream_api.services.account_audit import (
    _sanitize_metadata,
    record_account_audit_log,
    reset_audit_table_cache,
    try_record_account_audit_log,
)
from upstream_api.services.stripe_gateway import get_stripe_gateway


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    reset_audit_table_cache()


# ---------------------------------------------------------------------------
# Sanitization — pure.
# ---------------------------------------------------------------------------


def test_sanitize_strips_forbidden_keys() -> None:
    dirty = {
        "name": "Cam 1",
        "token": "upstream_live_abc",
        "api_key": "sk_test_x",
        "Authorization": "Bearer x",
        "stripe_secret": "whsec_x",
        "password": "hunter2",
        "card_number": "4242",
        "recovery_link": "https://x",
        "webhook_signature": "t=1,v1=abc",
        "nested": {"client_secret": "x", "ok": 1},
    }
    clean = _sanitize_metadata(dirty)
    assert clean == {"name": "Cam 1", "nested": {"ok": 1}}


def test_sanitize_keeps_allowed_exact_keys() -> None:
    clean = _sanitize_metadata({"token_id": "abc", "token_name": "CI", "scopes": ["agent"]})
    assert clean == {"token_id": "abc", "token_name": "CI", "scopes": ["agent"]}


def test_sanitize_depth_limit_and_non_json_values() -> None:
    deep = {"a": {"b": {"c": {"d": {"e": 1}}}}}
    clean = _sanitize_metadata(deep)
    assert clean["a"]["b"]["c"]["d"] is None
    assert _sanitize_metadata({"when": datetime(2026, 6, 11, tzinfo=UTC)})["when"].startswith(
        "2026-"
    )


# ---------------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------------


class ServiceFakeConn:
    def __init__(self, *, table_exists: bool = True, insert_raises: bool = False) -> None:
        self.table_exists = table_exists
        self.insert_raises = insert_raises
        self.inserts: list[tuple[Any, ...]] = []

    async def fetchval(self, query: str, *args: Any) -> Any:
        assert "to_regclass" in query
        return self.table_exists

    async def execute(self, query: str, *args: Any) -> str:
        assert "insert into account_audit_logs" in query
        if self.insert_raises:
            raise RuntimeError("boom")
        self.inserts.append(args)
        return "INSERT 0 1"


async def test_service_inserts_row() -> None:
    conn = ServiceFakeConn()
    account, actor = uuid4(), uuid4()
    await record_account_audit_log(
        conn,
        account_id=account,
        actor_user_id=actor,
        event_type="camera.created",
        resource_type="camera",
        resource_id="cam-1",
        summary="Camera created.",
        metadata={"name": "Cam", "token": "SECRET"},
        request_id="req-1",
    )
    assert len(conn.inserts) == 1
    args = conn.inserts[0]
    assert args[0] == account
    assert args[1] == actor
    assert args[2] == "user"
    assert args[3] == "camera.created"
    assert json.loads(args[7]) == {"name": "Cam"}  # token stripped
    assert args[8] == "req-1"


async def test_service_skips_when_table_missing() -> None:
    conn = ServiceFakeConn(table_exists=False)
    await record_account_audit_log(
        conn,
        account_id=uuid4(),
        event_type="x",
        summary="x",
    )
    assert conn.inserts == []


async def test_try_record_swallows_errors() -> None:
    conn = ServiceFakeConn(insert_raises=True)
    await try_record_account_audit_log(
        conn,
        account_id=uuid4(),
        event_type="x",
        summary="x",
    )  # must not raise


# ---------------------------------------------------------------------------
# Route plumbing fakes.
# ---------------------------------------------------------------------------


class _Tx:
    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


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


def _client(
    conn: Any,
    *,
    user: UserAuth | None = None,
    agent: AgentAuth | None = None,
    gateway: Any | None = None,
) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if user is not None:
        app.dependency_overrides[current_user] = lambda: user
    if agent is not None:
        app.dependency_overrides[current_agent] = lambda: agent
    if gateway is not None:
        app.dependency_overrides[get_stripe_gateway] = lambda: gateway
    return TestClient(app)


class AuditCapturingConn:
    """Base fake: records audit inserts, answers the probe. Subclasses /
    instances handle route-specific queries via handler lists."""

    def __init__(self) -> None:
        self.audit_rows: list[tuple[Any, ...]] = []
        self.fetchrow_handlers: list[tuple[str, Any]] = []
        self.fetchval_handlers: list[tuple[str, Any]] = []
        self.execute_log: list[str] = []

    def transaction(self) -> _Tx:
        return _Tx()

    async def fetchrow(self, query: str, *args: Any) -> Any:
        for needle, result in self.fetchrow_handlers:
            if needle in query:
                return result(args) if callable(result) else result
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "to_regclass" in query:
            return True
        for needle, result in self.fetchval_handlers:
            if needle in query:
                return result(args) if callable(result) else result
        raise AssertionError(f"unexpected fetchval: {query}")

    async def execute(self, query: str, *args: Any) -> str:
        if "insert into account_audit_logs" in query:
            self.audit_rows.append(args)
            return "INSERT 0 1"
        self.execute_log.append(query)
        if query.strip().startswith("delete"):
            return "DELETE 1"
        return "UPDATE 1"

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        raise AssertionError(f"unexpected fetch: {query}")

    def audit_events(self) -> list[str]:
        return [row[3] for row in self.audit_rows]

    def audit_metadata(self, index: int = 0) -> dict[str, Any]:
        return dict(json.loads(self.audit_rows[index][7]))


# ---------------------------------------------------------------------------
# GET /v1/audit-logs.
# ---------------------------------------------------------------------------


def _log_row(created_at: datetime, event_type: str = "camera.created") -> dict[str, Any]:
    return {
        "id": uuid4(),
        "actor_user_id": uuid4(),
        "actor_type": "user",
        "event_type": event_type,
        "resource_type": "camera",
        "resource_id": "cam-1",
        "summary": "Camera created.",
        "metadata": {"name": "Cam"},
        "created_at": created_at,
    }


class ListFakeConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.query_args: tuple[Any, ...] | None = None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from account_audit_logs" in query
        self.query_args = args
        return self.rows[: args[5]]


def test_list_requires_auth() -> None:
    r = _client(ListFakeConn([])).get("/v1/audit-logs")
    assert r.status_code == 401


def test_list_cross_tenant_forbidden() -> None:
    member_of, requested = uuid4(), uuid4()
    r = _client(ListFakeConn([]), user=_user(member_of)).get(
        "/v1/audit-logs", params={"account_id": str(requested)}
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden_account"


def test_list_viewer_member_can_read() -> None:
    account = uuid4()
    now = datetime.now(tz=UTC)
    r = _client(ListFakeConn([_log_row(now)]), user=_user(account, "viewer")).get(
        "/v1/audit-logs", params={"account_id": str(account)}
    )
    assert r.status_code == 200
    item = r.json()["items"][0]
    assert item["event_type"] == "camera.created"
    assert item["actor"]["type"] == "user"
    assert item["summary"] == "Camera created."
    # ip/user_agent never appear in dashboard responses.
    assert "ip_address" not in item and "user_agent" not in item


def test_list_filters_bound_into_query() -> None:
    account = uuid4()
    conn = ListFakeConn([])
    before = datetime.now(tz=UTC)
    r = _client(conn, user=_user(account)).get(
        "/v1/audit-logs",
        params={
            "account_id": str(account),
            "event_type": "rule.updated",
            "resource_type": "rule",
            "resource_id": "abc",
            "before": before.isoformat(),
            "limit": 10,
        },
    )
    assert r.status_code == 200
    assert conn.query_args is not None
    assert conn.query_args[1] == before
    assert conn.query_args[2] == "rule.updated"
    assert conn.query_args[3] == "rule"
    assert conn.query_args[4] == "abc"
    assert conn.query_args[5] == 10


def test_list_limit_max_100() -> None:
    account = uuid4()
    r = _client(ListFakeConn([]), user=_user(account)).get(
        "/v1/audit-logs", params={"account_id": str(account), "limit": 101}
    )
    assert r.status_code == 422


def test_list_pagination_cursor() -> None:
    account = uuid4()
    now = datetime.now(tz=UTC)
    rows = [_log_row(now - timedelta(minutes=i)) for i in range(5)]
    full = _client(ListFakeConn(rows), user=_user(account)).get(
        "/v1/audit-logs", params={"account_id": str(account), "limit": 5}
    )
    assert full.json()["next_cursor"] is not None
    partial = _client(ListFakeConn(rows), user=_user(account)).get(
        "/v1/audit-logs", params={"account_id": str(account), "limit": 50}
    )
    assert partial.json()["next_cursor"] is None


# ---------------------------------------------------------------------------
# Mutation points — API tokens.
# ---------------------------------------------------------------------------


def _token_conn(account: UUID) -> AuditCapturingConn:
    conn = AuditCapturingConn()
    token_id = uuid4()
    conn.fetchrow_handlers = [
        (
            "insert into api_tokens",
            lambda args: {
                "id": token_id,
                "account_id": args[0],
                "name": args[1],
                "prefix": args[2],
                "last_four": args[4],
                "scopes": list(args[5]),
                "agent_id": None,
                "last_used_at": None,
                "expires_at": None,
                "revoked_at": None,
                "created_at": datetime.now(tz=UTC),
            },
        ),
        (
            "select t.revoked_at, t.account_id, t.name",
            {"revoked_at": None, "account_id": account, "name": "CI token"},
        ),
        ("select role from account_members", {"role": "owner"}),
        ("from account_members", {"role": "owner"}),
    ]
    return conn


def test_token_create_audited_without_raw_token() -> None:
    account = uuid4()
    conn = _token_conn(account)
    r = _client(conn, user=_user(account)).post(
        "/v1/api-tokens",
        json={"account_id": str(account), "name": "CI token", "scopes": ["agent"]},
    )
    assert r.status_code == 201, r.text
    plaintext = r.json()["plaintext"]
    assert conn.audit_events() == ["api_token.created"]
    row = conn.audit_rows[0]
    assert row[3] == "api_token.created"
    # The raw token must appear NOWHERE in the audit row.
    flat = json.dumps([str(value) for value in row])
    assert plaintext not in flat
    assert conn.audit_metadata() == {"token_name": "CI token", "scopes": ["agent"]}


def test_token_revoke_audited() -> None:
    account = uuid4()
    conn = _token_conn(account)
    r = _client(conn, user=_user(account)).delete(f"/v1/api-tokens/{uuid4()}")
    assert r.status_code == 204
    assert conn.audit_events() == ["api_token.revoked"]


# ---------------------------------------------------------------------------
# Mutation points — rules + workflows (create/update/delete/enable).
# ---------------------------------------------------------------------------


_RULE_CONFIG: dict[str, Any] = {}
_SEQ_CONFIG = {
    "start": {"event_type": "processor.zone_entered"},
    "then": {"within_seconds": 30, "event_type": "processor.zone_exited"},
}


def _rule_row(account: UUID, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid4(),
        "account_id": account,
        "camera_id": uuid4(),
        "name": "Zone watch",
        "rule_type": "zone_entered",
        "enabled": True,
        "config": _RULE_CONFIG,
        "severity": "warning",
        "cooldown_seconds": 300,
        "created_by": None,
        "created_at": datetime.now(tz=UTC),
        "updated_at": datetime.now(tz=UTC),
    }
    row.update(overrides)
    return row


def _workflow_row(account: UUID, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid4(),
        "account_id": account,
        "camera_id": uuid4(),
        "name": "Dock check",
        "description": None,
        "workflow_type": "sequence",
        "enabled": True,
        "config": _SEQ_CONFIG,
        "severity": "warning",
        "cooldown_seconds": 300,
        "created_by": None,
        "created_at": datetime.now(tz=UTC),
        "updated_at": datetime.now(tz=UTC),
    }
    row.update(overrides)
    return row


def test_rule_create_update_delete_audited() -> None:
    account, camera = uuid4(), uuid4()
    conn = AuditCapturingConn()
    rule = _rule_row(account, camera_id=camera)
    conn.fetchrow_handlers = [
        ("from cameras", {"account_id": account}),
        ("insert into rules", rule),
        ("update rules", rule),
        ("from rules", rule),
    ]
    conn.fetchval_handlers = [("delete from rules", rule["id"])]
    client = _client(conn, user=_user(account))

    r = client.post(
        "/v1/rules",
        json={
            "account_id": str(account),
            "camera_id": str(camera),
            "name": "Zone watch",
            "rule_type": "zone_entered",
            "config": _RULE_CONFIG,
        },
    )
    assert r.status_code == 201, r.text
    r = client.patch(
        f"/v1/rules/{rule['id']}",
        params={"account_id": str(account)},
        json={"name": "Zone watch 2"},
    )
    assert r.status_code == 200, r.text
    r = client.delete(f"/v1/rules/{rule['id']}", params={"account_id": str(account)})
    assert r.status_code == 204
    assert conn.audit_events() == ["rule.created", "rule.updated", "rule.deleted"]
    assert conn.audit_metadata(1)["changed_fields"] == ["name"]


def test_workflow_lifecycle_audited_with_enable_disable() -> None:
    account, camera = uuid4(), uuid4()
    conn = AuditCapturingConn()
    wf = _workflow_row(account, camera_id=camera)
    disabled = dict(wf, enabled=False)
    conn.fetchrow_handlers = [
        ("from cameras", {"account_id": account}),
        ("insert into workflows", wf),
        ("update workflows", disabled),
        ("from workflows", wf),
    ]
    conn.fetchval_handlers = [("delete from workflows", wf["id"])]
    client = _client(conn, user=_user(account))

    r = client.post(
        "/v1/workflows",
        json={
            "account_id": str(account),
            "camera_id": str(camera),
            "name": "Dock check",
            "workflow_type": "sequence",
            "config": _SEQ_CONFIG,
        },
    )
    assert r.status_code == 201, r.text
    r = client.patch(
        f"/v1/workflows/{wf['id']}",
        params={"account_id": str(account)},
        json={"enabled": False},
    )
    assert r.status_code == 200, r.text
    r = client.delete(f"/v1/workflows/{wf['id']}", params={"account_id": str(account)})
    assert r.status_code == 204
    assert conn.audit_events() == [
        "workflow.created",
        "workflow.disabled",
        "workflow.deleted",
    ]


# ---------------------------------------------------------------------------
# Mutation points — billing checkout/portal + webhook sync.
# ---------------------------------------------------------------------------


class FakeGateway:
    def __init__(self, subscription: dict[str, Any] | None = None) -> None:
        self.subscription = subscription

    async def create_customer(self, **_: Any) -> str:
        return "cus_new"

    async def create_checkout_session(self, **_: Any) -> str:
        return "https://checkout.stripe.com/c/test"

    async def create_portal_session(self, **_: Any) -> str:
        return "https://billing.stripe.com/p/test"

    async def retrieve_subscription(self, _sid: str) -> dict[str, Any]:
        assert self.subscription is not None
        return self.subscription

    def construct_event(self, payload: bytes, signature: str) -> dict[str, Any]:
        return dict(json.loads(payload))


@pytest.fixture(autouse=True)
def _billing_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = SimpleNamespace(
        stripe_secret_key=SimpleNamespace(get_secret_value=lambda: "sk_test"),
        stripe_webhook_secret=SimpleNamespace(get_secret_value=lambda: "whsec"),
        stripe_price_starter_monthly="price_starter_test",
        stripe_price_pro_monthly="price_pro_test",
        dashboard_base_url="https://app.test",
    )
    monkeypatch.setattr("upstream_api.routers.billing.get_settings", lambda: fake)
    monkeypatch.setattr("upstream_api.services.billing.get_settings", lambda: fake)


def _billing_conn(account: UUID) -> AuditCapturingConn:
    conn = AuditCapturingConn()
    conn.fetchrow_handlers = [
        (
            "from account_billing",
            {"account_id": account, "stripe_customer_id": "cus_123"},
        ),
    ]
    conn.fetchval_handlers = [
        ("select plan from accounts", "free"),
        ("insert into stripe_events", lambda args: args[0]),
        ("where stripe_customer_id", account),
    ]
    return conn


def test_checkout_and_portal_audited() -> None:
    account = uuid4()
    conn = _billing_conn(account)
    client = _client(conn, user=_user(account), gateway=FakeGateway())
    assert (
        client.post(
            "/v1/billing/checkout",
            json={"account_id": str(account), "plan": "starter"},
        ).status_code
        == 200
    )
    assert client.post("/v1/billing/portal", json={"account_id": str(account)}).status_code == 200
    assert conn.audit_events() == [
        "billing.checkout_started",
        "billing.portal_opened",
    ]
    assert conn.audit_metadata(0) == {"plan": "starter"}
    # Checkout/portal actor is the signed-in user, not stripe.
    assert conn.audit_rows[0][2] == "user"


def _subscription(account: UUID, *, status: str = "active") -> dict[str, Any]:
    return {
        "id": "sub_1",
        "customer": "cus_123",
        "status": status,
        "cancel_at_period_end": False,
        "items": {
            "data": [
                {
                    "price": {"id": "price_starter_test"},
                    "current_period_start": 1_765_000_000,
                    "current_period_end": 1_767_592_000,
                }
            ]
        },
        "metadata": {"account_id": str(account)},
    }


def _post_webhook(conn: AuditCapturingConn, event: dict[str, Any]) -> Any:
    client = _client(conn, gateway=FakeGateway())
    return client.post(
        "/v1/webhooks/stripe",
        content=json.dumps(event),
        headers={"stripe-signature": "ok"},
    )


def test_webhook_sync_emits_billing_events() -> None:
    account = uuid4()
    conn = _billing_conn(account)
    r = _post_webhook(
        conn,
        {
            "id": "evt_1",
            "type": "customer.subscription.updated",
            "data": {"object": _subscription(account)},
        },
    )
    assert r.status_code == 200, r.text
    events = conn.audit_events()
    assert events == [
        "billing.subscription_synced",
        "billing.plan_changed",
        "billing.limits_updated",
    ]
    # Webhook-driven events are actor_type stripe with no user.
    assert all(row[2] == "stripe" and row[1] is None for row in conn.audit_rows)
    plan_changed = conn.audit_metadata(1)
    assert plan_changed == {"old_plan": "free", "new_plan": "starter"}


def test_webhook_cancel_emits_canceled_and_downgrade_events() -> None:
    account = uuid4()
    conn = _billing_conn(account)
    r = _post_webhook(
        conn,
        {
            "id": "evt_2",
            "type": "customer.subscription.deleted",
            "data": {"object": _subscription(account, status="canceled")},
        },
    )
    assert r.status_code == 200
    events = conn.audit_events()
    assert "billing.subscription_canceled" in events
    assert "billing.limits_updated" in events
    # old_plan == new_plan == free -> no plan_changed event.
    assert "billing.plan_changed" not in events


def test_webhook_payment_failed_audited() -> None:
    account = uuid4()
    conn = _billing_conn(account)
    r = _post_webhook(
        conn,
        {
            "id": "evt_3",
            "type": "invoice.payment_failed",
            "data": {"object": {"id": "in_1", "customer": "cus_123", "metadata": {}}},
        },
    )
    assert r.status_code == 200
    assert conn.audit_events() == ["billing.payment_failed"]


# ---------------------------------------------------------------------------
# Mutation points — session limit rejection.
# ---------------------------------------------------------------------------


class SessionRejectConn(AuditCapturingConn):
    """Limits row with everything maxed out -> concurrent-session rejection."""

    def __init__(self, account: UUID) -> None:
        super().__init__()
        self.fetchrow_handlers = [
            (
                "from cameras",
                {
                    "id": uuid4(),
                    "account_id": account,
                    "agent_id": uuid4(),
                    "source_type": "webcam",
                },
            ),
            (
                "from account_limits",
                {
                    "max_cameras": 5,
                    "max_concurrent_sessions": 1,
                    "max_fps": 60,
                    "max_width": 1920,
                    "max_height": 1080,
                    "max_bitrate_kbps": 8000,
                    "allowed_codecs": ["h264"],
                    "allowed_stream_types": ["webcam"],
                    "monthly_minutes_quota": 600,
                    "snapshot_storage_mb": 1024,
                    "session_ttl_seconds": None,
                    "shared_inference_enabled": False,
                },
            ),
        ]
        self.fetchval_handlers = [
            ("status from accounts", "active"),  # suspension check
            ("from usage_events", 0),
            ("exists(", False),  # camera_already_streaming pre-check
            ("count(*)", 1),  # already at the 1-session cap
        ]


def test_session_limit_rejection_audited() -> None:
    account = uuid4()
    conn = SessionRejectConn(account)
    agent = AgentAuth(account_id=account, token_id=uuid4(), scopes=("agent",))
    r = _client(conn, agent=agent).post(
        "/v1/stream-sessions/start",
        json={
            "camera_id": str(uuid4()),
            "requested": {
                "width": 1280,
                "height": 720,
                "fps": 30,
                "codec": "h264",
                "bitrate_kbps": 2500,
                "latency_mode": "balanced",
            },
        },
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "limit_exceeded.max_concurrent_sessions"
    assert conn.audit_events() == ["stream_session.limit_rejected"]
    metadata = conn.audit_metadata()
    assert metadata["limit_code"] == "limit_exceeded.max_concurrent_sessions"
    assert metadata["active"] == 1
    assert metadata["max"] == 1
    assert conn.audit_rows[0][2] == "system"  # agent-initiated, no user actor
