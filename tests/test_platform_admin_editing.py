"""Tests for the /v1/admin editing surface: account create/identity edit,
member management, token issue/revoke, platform-admins CRUD, force-stop, and
the GoTrue admin service.

Same FakePool pattern as the other platform-admin router tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import PlatformAdminAuth, current_platform_admin
from upstream_api.services.supabase_admin import create_auth_user, generate_recovery_link

_ADMIN = PlatformAdminAuth(user_id=uuid4(), email="admin@example.com", aal="aal1", amr=())


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Answers by query content. Configure per-test via the dicts below."""

    def __init__(self) -> None:
        self.fetchrow_results: list[tuple[str, dict[str, Any] | None]] = []
        self.fetchval_results: list[tuple[str, Any]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        self.raise_unique_on: str | None = None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        for needle, result in self.fetchrow_results:
            if needle in query:
                return result
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        for needle, result in self.fetchval_results:
            if needle in query:
                return result
        return None

    async def fetch(self, _query: str, *args: Any) -> list[dict[str, Any]]:
        return []

    async def execute(self, query: str, *args: Any) -> str:
        if self.raise_unique_on and self.raise_unique_on in query:
            raise asyncpg.UniqueViolationError("duplicate")
        self.executes.append((query, args))
        return "OK"


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


def _account_item_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid4(),
        "name": "Acme",
        "slug": "acme",
        "plan": "free",
        "status": "active",
        "created_at": datetime.now(tz=UTC),
        "member_count": 1,
        "agent_count": 0,
        "camera_count": 0,
        "active_sessions": 0,
    }
    row.update(overrides)
    return row


def _client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(500))
    )
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_platform_admin] = lambda: _ADMIN
    return TestClient(app)


def _audit_actions(conn: FakeConn) -> list[str]:
    return [a[1] for q, a in conn.executes if "insert into admin_audit_log" in q]


# ---------------------------------------------------------------------------
# Account create / identity edit
# ---------------------------------------------------------------------------


def test_create_account_without_owner() -> None:
    conn = FakeConn()
    conn.fetchrow_results = [("from accounts a", _account_item_row(name="New Co"))]
    client = _client(conn)
    r = client.post("/v1/admin/accounts", json={"name": "New Co", "plan": "pro"})
    assert r.status_code == 201, r.text
    assert any("insert into accounts" in q for q, _ in conn.executes)
    assert any("insert into account_limits" in q for q, _ in conn.executes)
    assert "account.create" in _audit_actions(conn)


def test_create_account_slug_conflict_is_409() -> None:
    conn = FakeConn()
    conn.raise_unique_on = "insert into accounts"
    client = _client(conn)
    r = client.post("/v1/admin/accounts", json={"name": "Acme", "slug": "acme"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "slug_taken"


def test_create_account_unknown_owner_without_password_is_400() -> None:
    conn = FakeConn()  # auth.users lookup returns None
    client = _client(conn)
    r = client.post(
        "/v1/admin/accounts",
        json={"name": "Acme", "owner_email": "nobody@example.com"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "owner_not_found"


def test_patch_account_name_and_slug() -> None:
    row = _account_item_row(name="Renamed", slug="renamed")
    conn = FakeConn()
    conn.fetchrow_results = [
        ("for update", {"name": "Acme", "slug": "acme", "plan": "free", "status": "active"}),
        ("from accounts a", row),
    ]
    client = _client(conn)
    r = client.patch(
        f"/v1/admin/accounts/{row['id']}",
        json={"name": "Renamed", "slug": "renamed"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "Renamed"
    assert _audit_actions(conn) == ["account.update"]


def test_patch_account_rejects_bad_slug() -> None:
    client = _client(FakeConn())
    r = client.patch(f"/v1/admin/accounts/{uuid4()}", json={"slug": "Bad Slug!"})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------


def test_add_member_existing_user() -> None:
    user_id = uuid4()
    account_id = uuid4()
    conn = FakeConn()
    conn.fetchval_results = [
        ("from accounts where id", 1),
        ("from auth.users", user_id),
    ]
    conn.fetchrow_results = [
        ("insert into account_members", {"created_at": datetime.now(tz=UTC)}),
    ]
    client = _client(conn)
    r = client.post(
        f"/v1/admin/accounts/{account_id}/members",
        json={"email": "user@example.com", "role": "viewer"},
    )
    assert r.status_code == 201, r.text
    assert r.json()["role"] == "viewer"
    assert "account.member.add" in _audit_actions(conn)


def test_add_member_unknown_user_without_password_is_400() -> None:
    conn = FakeConn()
    conn.fetchval_results = [("from accounts where id", 1), ("from auth.users", None)]
    client = _client(conn)
    r = client.post(
        f"/v1/admin/accounts/{uuid4()}/members",
        json={"email": "new@example.com", "role": "viewer"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "member_user_not_found"


def test_add_member_duplicate_is_409() -> None:
    conn = FakeConn()
    conn.fetchval_results = [
        ("from accounts where id", 1),
        ("from auth.users", uuid4()),
    ]
    # insert ... returning -> None (conflict do nothing)
    client = _client(conn)
    r = client.post(
        f"/v1/admin/accounts/{uuid4()}/members",
        json={"email": "user@example.com", "role": "admin"},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "member_exists"


def test_demote_last_owner_is_409() -> None:
    conn = FakeConn()
    conn.fetchrow_results = [
        ("for update", {"role": "owner", "created_at": datetime.now(tz=UTC)}),
    ]
    conn.fetchval_results = [("count(*)", 1)]
    client = _client(conn)
    r = client.patch(
        f"/v1/admin/accounts/{uuid4()}/members/{uuid4()}",
        json={"role": "viewer"},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "last_owner"


def test_remove_last_owner_is_409() -> None:
    conn = FakeConn()
    conn.fetchrow_results = [("for update", {"role": "owner"})]
    conn.fetchval_results = [("count(*)", 1)]
    client = _client(conn)
    r = client.delete(f"/v1/admin/accounts/{uuid4()}/members/{uuid4()}")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "last_owner"


def test_remove_member_ok() -> None:
    conn = FakeConn()
    conn.fetchrow_results = [("for update", {"role": "viewer"})]
    conn.fetchval_results = [("count(*)", 2)]
    client = _client(conn)
    r = client.delete(f"/v1/admin/accounts/{uuid4()}/members/{uuid4()}")
    assert r.status_code == 204
    assert "account.member.remove" in _audit_actions(conn)


# ---------------------------------------------------------------------------
# API tokens
# ---------------------------------------------------------------------------


def test_create_token_returns_plaintext_and_audits_without_it() -> None:
    account_id = uuid4()
    conn = FakeConn()
    conn.fetchval_results = [("from accounts where id", 1)]
    conn.fetchrow_results = [
        (
            "insert into api_tokens",
            {
                "id": uuid4(),
                "name": "onboarding",
                "prefix": "ups_test_ABCD1234",
                "last_four": "wxyz",
                "scopes": ["agent"],
                "last_used_at": None,
                "expires_at": None,
                "revoked_at": None,
                "created_at": datetime.now(tz=UTC),
            },
        ),
    ]
    client = _client(conn)
    r = client.post(
        f"/v1/admin/accounts/{account_id}/api-tokens",
        json={"name": "onboarding"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["plaintext"].startswith("ups_")
    # plaintext never appears in any executed SQL argument (audit included)
    for _q, args in conn.executes:
        assert body["plaintext"] not in [str(a) for a in args]
    assert "account.api_token.create" in _audit_actions(conn)


def test_revoke_token_is_idempotent() -> None:
    conn = FakeConn()
    conn.fetchrow_results = [
        ("select revoked_at", {"revoked_at": datetime.now(tz=UTC), "prefix": "p"}),
    ]
    client = _client(conn)
    r = client.delete(f"/v1/admin/accounts/{uuid4()}/api-tokens/{uuid4()}")
    assert r.status_code == 204
    assert _audit_actions(conn) == []  # already revoked: no new audit row


# ---------------------------------------------------------------------------
# platform-admins CRUD
# ---------------------------------------------------------------------------


def test_add_platform_admin_unknown_user_is_400() -> None:
    conn = FakeConn()
    client = _client(conn)
    r = client.post(
        "/v1/admin/platform-admins",
        json={"email": "ghost@example.com"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "user_not_found"


def test_add_platform_admin_ok() -> None:
    conn = FakeConn()
    conn.fetchval_results = [("from auth.users", uuid4())]
    conn.fetchrow_results = [
        ("insert into platform_admins", {"created_at": datetime.now(tz=UTC)}),
    ]
    client = _client(conn)
    r = client.post(
        "/v1/admin/platform-admins",
        json={"email": "new-admin@example.com", "note": "ops"},
    )
    assert r.status_code == 201, r.text
    assert "platform_admin.add" in _audit_actions(conn)


def test_cannot_remove_own_admin_access() -> None:
    client = _client(FakeConn())
    r = client.delete(f"/v1/admin/platform-admins/{_ADMIN.user_id}")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "cannot_remove_self"


def test_remove_other_admin_ok() -> None:
    conn = FakeConn()
    conn.fetchrow_results = [("delete from platform_admins", {"note": "ops"})]
    client = _client(conn)
    r = client.delete(f"/v1/admin/platform-admins/{uuid4()}")
    assert r.status_code == 204
    assert "platform_admin.remove" in _audit_actions(conn)


# ---------------------------------------------------------------------------
# Force-stop
# ---------------------------------------------------------------------------


def test_force_stop_unknown_session_is_404() -> None:
    client = _client(FakeConn())
    r = client.post(f"/v1/admin/streams/{uuid4()}/stop")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


# ---------------------------------------------------------------------------
# supabase_admin service (httpx.MockTransport)
# ---------------------------------------------------------------------------


async def test_create_auth_user_ok() -> None:
    user_id = uuid4()

    expected_key = get_settings().supabase_service_role_key.get_secret_value()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/auth/v1/admin/users"
        assert request.headers["apikey"] == expected_key
        return httpx.Response(200, json={"id": str(user_id), "email": "a@b.co"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        created = await create_auth_user(
            client, get_settings(), email="a@b.co", password="password123"
        )
    assert created.user_id == UUID(str(user_id))


async def test_create_auth_user_422_is_bad_request() -> None:
    from upstream_api.errors import BadRequestError

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, text="weak password")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        try:
            await create_auth_user(client, get_settings(), email="a@b.co", password="pw123456")
            raise AssertionError("expected BadRequestError")
        except BadRequestError as exc:
            assert exc.code == "auth_user_invalid"


async def test_generate_recovery_link_ok() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/auth/v1/admin/generate_link"
        return httpx.Response(200, json={"action_link": "https://x/recover?token=t"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        link = await generate_recovery_link(client, get_settings(), email="a@b.co")
    assert link.startswith("https://")
