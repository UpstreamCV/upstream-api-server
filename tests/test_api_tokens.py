from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.api_tokens import (
    issue_token,
    issue_token_async,
    split_token,
    verify_secret,
    verify_secret_async,
)
from upstream_api.security.deps import (
    UserAuth,
    clear_membership_cache,
    current_agent,
    current_user,
)


def test_issue_token_returns_unique_plaintext_and_prefix() -> None:
    a = issue_token()
    b = issue_token()
    assert a.plaintext != b.plaintext
    assert a.hash != b.hash
    # CRITICAL: prefixes must differ so the DB unique constraint is respected.
    assert a.prefix != b.prefix
    assert a.prefix.startswith("ups_live_")
    assert len(a.prefix.split("_")) == 3
    assert len(a.last_four) == 4


def test_split_token_recovers_prefix_and_secret() -> None:
    issued = issue_token(environment="test")
    prefix, secret = split_token(issued.plaintext)
    assert prefix == issued.prefix
    assert prefix.startswith("ups_test_")
    assert issued.plaintext == f"{prefix}_{secret}"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "abc",
        "ups_",
        "ups_live",
        "ups_live_",
        "ups_live_abc",  # only 3 segments — missing secret
        "ups__abc_xyz",  # empty env segment
        "_live_abc_xyz",  # empty leading segment
    ],
)
def test_split_token_rejects_invalid_shapes(bad: str) -> None:
    with pytest.raises(ValueError):
        split_token(bad)


def test_verify_secret_round_trip() -> None:
    issued = issue_token()
    _, secret = split_token(issued.plaintext)
    assert verify_secret(secret, issued.hash) is True
    assert verify_secret(secret + "x", issued.hash) is False
    assert verify_secret("wrong", issued.hash) is False


async def test_verify_secret_async_matches_sync() -> None:
    issued = await issue_token_async(environment="test")
    _, secret = split_token(issued.plaintext)
    assert await verify_secret_async(secret, issued.hash) is True
    assert await verify_secret_async("wrong", issued.hash) is False


# --- last_used_at throttling -------------------------------------------------


class _FakeConn:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, _query: str, *_args: Any) -> dict[str, Any] | None:
        return self._row

    async def fetch(self, _query: str, *_args: Any) -> list[dict[str, Any]]:
        return []

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        return "UPDATE 1"


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


class _AcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


async def test_current_agent_writes_last_used_when_null() -> None:
    issued = await issue_token_async(environment="test")
    row = {
        "id": uuid4(),
        "account_id": uuid4(),
        "hash": issued.hash,
        "scopes": ["agent"],
        "expires_at": None,
        "revoked_at": None,
        "last_used_at": None,
    }
    conn = _FakeConn(row)
    pool = _FakePool(conn)

    await current_agent(pool=pool, authorization=f"Bearer {issued.plaintext}")

    assert any("update api_tokens" in q for q, _ in conn.executes)


async def test_current_agent_skips_last_used_when_recent() -> None:
    issued = await issue_token_async(environment="test")
    row = {
        "id": uuid4(),
        "account_id": uuid4(),
        "hash": issued.hash,
        "scopes": ["agent"],
        "expires_at": None,
        "revoked_at": None,
        "last_used_at": datetime.now(tz=UTC) - timedelta(seconds=5),
    }
    conn = _FakeConn(row)
    pool = _FakePool(conn)

    await current_agent(pool=pool, authorization=f"Bearer {issued.plaintext}")

    assert not any("update api_tokens" in q for q, _ in conn.executes), (
        "Update should be throttled when last_used_at is within the refresh window"
    )


# --- DELETE /v1/api-tokens/{id}: 404 vs 403 oracle ---------------------------


@pytest.fixture
def _client_with_overrides() -> TestClient:
    """TestClient with current_user + pool stubbed to a fake that returns no row."""
    clear_membership_cache()
    app = create_app()

    fake_user = UserAuth(
        user_id=uuid4(),
        memberships={},  # not an admin of anything
        aal="aal1",
        amr=(),
    )

    class _NoRowConn:
        async def fetchrow(self, *_a: Any, **_k: Any) -> None:
            return None

        async def execute(self, *_a: Any, **_k: Any) -> str:
            return "OK"

        def transaction(self) -> _Tx:
            return _Tx()

    class _Tx:
        async def __aenter__(self) -> _Tx:
            return self

        async def __aexit__(self, *_exc: Any) -> None:
            return None

    pool = _FakePool(_NoRowConn())  # type: ignore[arg-type]

    app.dependency_overrides[current_user] = lambda: fake_user
    app.dependency_overrides[get_pool] = lambda: pool

    client = TestClient(app)
    return client


def test_revoke_returns_404_for_unknown_token_id(_client_with_overrides: TestClient) -> None:
    response = _client_with_overrides.delete(f"/v1/api-tokens/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "token_not_found"


def test_revoke_returns_404_for_non_admin_caller(_client_with_overrides: TestClient) -> None:
    # The fake pool always returns no row from the JOIN — same shape whether
    # the token doesn't exist or the caller isn't an admin. That's the point:
    # the response body must be identical.
    a = _client_with_overrides.delete(f"/v1/api-tokens/{uuid4()}")
    b = _client_with_overrides.delete(f"/v1/api-tokens/{uuid4()}")
    assert a.status_code == b.status_code == 404
    assert a.json()["error"]["code"] == b.json()["error"]["code"] == "token_not_found"
    assert a.json()["error"]["message"] == b.json()["error"]["message"]
