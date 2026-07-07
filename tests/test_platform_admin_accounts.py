"""Router tests for /v1/admin/accounts* — list, detail, audited PATCHes.

Same pattern as test_list_endpoints.py: TestClient + dependency_overrides
for the admin identity and the pool; FakeConn answers by query content and
captures executes so the audit insert can be asserted.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import PlatformAdminAuth, current_platform_admin

_ADMIN = PlatformAdminAuth(user_id=uuid4(), email="admin@example.com", aal="aal1", amr=())


def _account_item_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid4(),
        "name": "Acme",
        "slug": "acme",
        "plan": "free",
        "status": "active",
        "created_at": datetime.now(tz=UTC),
        "member_count": 2,
        "agent_count": 1,
        "camera_count": 3,
        "active_sessions": 1,
        "total": 1,
    }
    row.update(overrides)
    return row


def _limits_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "max_cameras": 1,
        "max_concurrent_sessions": 1,
        "max_fps": 30,
        "max_width": 1920,
        "max_height": 1080,
        "max_bitrate_kbps": 6000,
        "allowed_codecs": ["h264"],
        "monthly_minutes_quota": 600,
        "snapshot_storage_mb": 1024,
        "session_ttl_seconds": None,
        "shared_inference_enabled": False,
        "updated_at": datetime.now(tz=UTC),
    }
    row.update(overrides)
    return row


class FakeConn:
    def __init__(
        self,
        *,
        account_row: dict[str, Any] | None = None,
        limits_row: dict[str, Any] | None = None,
    ) -> None:
        self.account_row = account_row
        self.limits_row = limits_row
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetches.append((query, args))
        if "from accounts a" in query and self.account_row is not None:
            return [self.account_row]
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetches.append((query, args))
        if "from accounts where id = $1 for update" in query:
            if self.account_row is None:
                return None
            return {
                "name": self.account_row["name"],
                "slug": self.account_row["slug"],
                "plan": self.account_row["plan"],
                "status": self.account_row["status"],
            }
        if "update account_limits" in query:
            assert self.limits_row is not None
            return self.limits_row
        if "from account_limits" in query:
            return self.limits_row
        if "from accounts a" in query:
            return self.account_row
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        return "OK"


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class AcquireCtx:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_platform_admin] = lambda: _ADMIN
    return TestClient(app)


def test_admin_routes_require_platform_admin() -> None:
    """Without an override the real dep runs and rejects a missing header."""
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(FakeConn())
    client = TestClient(app)
    r = client.get("/v1/admin/accounts")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


def test_list_accounts() -> None:
    row = _account_item_row()
    client = _client(FakeConn(account_row=row))
    r = client.get("/v1/admin/accounts")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["items"][0]["slug"] == "acme"
    assert body["items"][0]["active_sessions"] == 1


def test_account_detail_includes_limits_and_usage() -> None:
    row = _account_item_row()
    client = _client(FakeConn(account_row=row, limits_row=_limits_row()))
    r = client.get(f"/v1/admin/accounts/{row['id']}")
    assert r.status_code == 200
    body = r.json()
    assert body["account"]["name"] == "Acme"
    assert body["limits"]["max_fps"] == 30
    assert body["usage_30d"] == []


def test_account_detail_404() -> None:
    client = _client(FakeConn(account_row=None))
    r = client.get(f"/v1/admin/accounts/{uuid4()}")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "account_not_found"


def test_patch_account_plan_writes_audit_row() -> None:
    row = _account_item_row(plan="pro")
    conn = FakeConn(account_row=row)
    client = _client(conn)
    r = client.patch(f"/v1/admin/accounts/{row['id']}", json={"plan": "pro"})
    assert r.status_code == 200, r.text
    audit_inserts = [q for q, _ in conn.executes if "insert into admin_audit_log" in q]
    assert len(audit_inserts) == 1
    # The audit row carries the acting admin and the action verb.
    _, args = next((q, a) for q, a in conn.executes if "insert into admin_audit_log" in q)
    assert args[0] == _ADMIN.user_id
    assert args[1] == "account.plan.update"


def test_patch_account_rejects_deleted_status() -> None:
    """`deleted` is not in the Literal — pydantic rejects it before the DB."""
    row = _account_item_row()
    client = _client(FakeConn(account_row=row))
    r = client.patch(f"/v1/admin/accounts/{row['id']}", json={"status": "deleted"})
    assert r.status_code == 422


def test_patch_account_empty_body_is_400() -> None:
    row = _account_item_row()
    client = _client(FakeConn(account_row=row))
    r = client.patch(f"/v1/admin/accounts/{row['id']}", json={})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "empty_update"


def test_patch_limits_writes_audit_row() -> None:
    row = _account_item_row()
    updated = _limits_row(max_fps=60)
    conn = FakeConn(account_row=row, limits_row=updated)
    client = _client(conn)
    r = client.patch(f"/v1/admin/accounts/{row['id']}/limits", json={"max_fps": 60})
    assert r.status_code == 200, r.text
    assert r.json()["max_fps"] == 60
    audit_inserts = [a for q, a in conn.executes if "insert into admin_audit_log" in q]
    assert len(audit_inserts) == 1
    assert audit_inserts[0][1] == "account.limits.update"


def test_patch_limits_rejects_unknown_field() -> None:
    row = _account_item_row()
    client = _client(FakeConn(account_row=row, limits_row=_limits_row()))
    r = client.patch(
        f"/v1/admin/accounts/{row['id']}/limits",
        json={"not_a_limit": 5},
    )
    # Unknown fields are ignored by pydantic -> empty update -> 400.
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "empty_update"


def test_patch_limits_rejects_negative_value() -> None:
    row = _account_item_row()
    client = _client(FakeConn(account_row=row, limits_row=_limits_row()))
    r = client.patch(f"/v1/admin/accounts/{row['id']}/limits", json={"max_fps": -1})
    assert r.status_code == 422
