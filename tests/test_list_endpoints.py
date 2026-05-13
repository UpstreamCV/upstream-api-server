"""End-to-end tests for the new GET list endpoints (Phase 4).

These exercise the ``CurrentCaller`` dual-auth dep — i.e. that agents are
single-account scoped, users with one membership resolve implicitly, users
with many memberships must pass ``?account_id=``, and a user passing an
account they're not in gets 403.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import (
    AgentAuth,
    CallerAuth,
    UserAuth,
    current_caller,
)


class FakeConn:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.last_account_filter: UUID | None = None

    async def fetch(self, _query: str, *args: Any) -> list[dict[str, Any]]:
        # First arg in every list query is the account_id filter; capture it.
        self.last_account_filter = args[0] if args else None
        return self.rows


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


def _agent_row(account_id: UUID) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "name": "agent-1",
        "hostname": "host",
        "version": "0.1.0",
        "os": "linux",
        "python_version": "3.11",
        "capabilities": {},
        "status": "active",
        "last_seen_at": datetime.now(tz=UTC),
        "created_at": datetime.now(tz=UTC),
        "updated_at": datetime.now(tz=UTC),
    }


def _client(
    conn: FakeConn,
    *,
    agent: AgentAuth | None = None,
    user: UserAuth | None = None,
) -> TestClient:
    caller = CallerAuth(agent=agent, user=user)
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


def _user_auth(memberships: dict[UUID, str]) -> UserAuth:
    return UserAuth(user_id=uuid4(), memberships=memberships, aal="aal1", amr=())


def _agent_auth(account_id: UUID) -> AgentAuth:
    return AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",))


# ----------------------------------------------------------------------------
# Agent token: single-account-scoped, query param ignored.
# ----------------------------------------------------------------------------


def test_agent_lists_only_its_account() -> None:
    account = uuid4()
    conn = FakeConn(rows=[_agent_row(account)])
    client = _client(conn, agent=_agent_auth(account))
    r = client.get("/v1/agents")
    assert r.status_code == 200
    assert len(r.json()["agents"]) == 1
    # Even with a stray ?account_id= the agent's own account wins.
    r2 = client.get(f"/v1/agents?account_id={uuid4()}")
    assert r2.status_code == 200
    assert conn.last_account_filter == account


# ----------------------------------------------------------------------------
# User token: account resolution.
# ----------------------------------------------------------------------------


def test_user_with_single_membership_resolves_implicitly() -> None:
    account = uuid4()
    conn = FakeConn(rows=[_agent_row(account)])
    client = _client(conn, user=_user_auth({account: "owner"}))
    r = client.get("/v1/agents")
    assert r.status_code == 200
    assert conn.last_account_filter == account


def test_user_with_multiple_memberships_requires_account_id() -> None:
    conn = FakeConn(rows=[])
    client = _client(
        conn,
        user=_user_auth({uuid4(): "owner", uuid4(): "viewer"}),
    )
    r = client.get("/v1/agents")
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "account_id_required"


def test_user_passing_foreign_account_id_is_rejected() -> None:
    own = uuid4()
    foreign = uuid4()
    conn = FakeConn(rows=[])
    client = _client(conn, user=_user_auth({own: "owner"}))
    r = client.get(f"/v1/agents?account_id={foreign}")
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden_account"


def test_user_passing_owned_account_id_is_accepted() -> None:
    own = uuid4()
    other = uuid4()
    conn = FakeConn(rows=[_agent_row(own)])
    client = _client(conn, user=_user_auth({own: "owner", other: "viewer"}))
    r = client.get(f"/v1/agents?account_id={own}")
    assert r.status_code == 200
    assert conn.last_account_filter == own
