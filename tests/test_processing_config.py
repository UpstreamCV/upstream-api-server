"""Account-level processing master switch -- endpoint tests.

GET / PATCH ``/v1/account/processing-config`` round-trip cases. Auth +
account scoping follow the existing ``/v1/account`` pattern; the auth tests
under ``test_caller_auth`` already cover the generic 401 / 403 paths so
this file focuses on the new behavior:

- default-true semantics (absent key in metadata)
- explicit values round-trip through GET
- PATCH writes the flag without clobbering other metadata keys
- 404 on unknown account
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller


class FakeTx:
    async def __aenter__(self) -> FakeTx:
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


class FakeAccountConn:
    """Backs the GET + PATCH paths.

    Stores the metadata dict in-place so PATCH-then-GET round-trips work
    against the same conn instance.
    """

    def __init__(
        self,
        *,
        exists: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.exists = exists
        # Match asyncpg's JSONB return shape: a JSON string (this is what
        # production sees; tests that mishandle this catch the M1-style
        # ``coerce_jsonb`` bug we already fixed once).
        self.metadata_json: str = json.dumps(metadata or {})
        self.updated_at = datetime.now(tz=UTC)
        self.update_calls: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTx:
        return FakeTx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "select metadata, updated_at from accounts" in query:
            if not self.exists:
                return None
            return {"metadata": self.metadata_json, "updated_at": self.updated_at}
        if "update accounts" in query:
            self.update_calls.append(args)
            if not self.exists:
                return None
            patch = json.loads(args[1])  # PATCH body serialized at the service layer
            merged = json.loads(self.metadata_json)
            merged.update(patch)
            self.metadata_json = json.dumps(merged)
            self.updated_at = datetime.now(tz=UTC)
            return {"metadata": self.metadata_json, "updated_at": self.updated_at}
        return None


def _client(conn: Any, *, account_id: UUID | None = None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id or uuid4(), token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET
# ---------------------------------------------------------------------------


def test_get_returns_true_when_key_absent() -> None:
    """Default semantics: an account that has never written the flag is
    treated as enabled. Critical for backward compatibility -- every existing
    account continues being processed after M3 ships."""
    account = uuid4()
    conn = FakeAccountConn(metadata={})
    r = _client(conn, account_id=account).get("/v1/account/processing-config")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["account_id"] == str(account)
    assert body["processing_enabled"] is True
    assert body["updated_at"] is not None


def test_get_returns_true_when_explicitly_true() -> None:
    account = uuid4()
    conn = FakeAccountConn(metadata={"processing_enabled": True})
    r = _client(conn, account_id=account).get("/v1/account/processing-config")
    assert r.json()["processing_enabled"] is True


def test_get_returns_false_when_explicitly_false() -> None:
    account = uuid4()
    conn = FakeAccountConn(metadata={"processing_enabled": False})
    r = _client(conn, account_id=account).get("/v1/account/processing-config")
    assert r.json()["processing_enabled"] is False


def test_get_treats_unrelated_metadata_keys_as_true() -> None:
    """Other metadata keys (workspace branding, feature flags) on the row
    must not collapse the processing flag. Absent key still means default."""
    account = uuid4()
    conn = FakeAccountConn(metadata={"display_name": "Floor 3", "color": "#ff0000"})
    r = _client(conn, account_id=account).get("/v1/account/processing-config")
    assert r.json()["processing_enabled"] is True


def test_get_returns_404_when_account_missing() -> None:
    conn = FakeAccountConn(exists=False)
    r = _client(conn, account_id=uuid4()).get("/v1/account/processing-config")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "account_not_found"


# ---------------------------------------------------------------------------
# PATCH
# ---------------------------------------------------------------------------


def test_patch_turns_processing_off() -> None:
    """The dashboard's master-switch-OFF click."""
    account = uuid4()
    conn = FakeAccountConn(metadata={"processing_enabled": True})
    r = _client(conn, account_id=account).patch(
        "/v1/account/processing-config",
        json={"processing_enabled": False},
    )
    assert r.status_code == 200, r.text
    assert r.json()["processing_enabled"] is False


def test_patch_turns_processing_on() -> None:
    """The dashboard's master-switch-ON click. From False, back to True."""
    account = uuid4()
    conn = FakeAccountConn(metadata={"processing_enabled": False})
    r = _client(conn, account_id=account).patch(
        "/v1/account/processing-config",
        json={"processing_enabled": True},
    )
    assert r.status_code == 200, r.text
    assert r.json()["processing_enabled"] is True


def test_patch_preserves_other_metadata_keys() -> None:
    """The JSONB concat in update_processing_config must NOT drop other
    keys the account row carries. If this ever regresses, operators who
    rely on metadata for workspace settings lose them on first toggle."""
    account = uuid4()
    conn = FakeAccountConn(metadata={"display_name": "Floor 3", "color": "#ff0000"})
    _client(conn, account_id=account).patch(
        "/v1/account/processing-config",
        json={"processing_enabled": False},
    )
    merged = json.loads(conn.metadata_json)
    assert merged["display_name"] == "Floor 3"
    assert merged["color"] == "#ff0000"
    assert merged["processing_enabled"] is False


def test_patch_then_get_round_trips() -> None:
    """Write-then-read through the same conn instance returns the new value
    (the JSONB string in the fake gets re-parsed on each GET, matching prod
    asyncpg behavior)."""
    account = uuid4()
    conn = FakeAccountConn(metadata={"processing_enabled": True})
    client = _client(conn, account_id=account)
    client.patch("/v1/account/processing-config", json={"processing_enabled": False})
    r = client.get("/v1/account/processing-config")
    assert r.json()["processing_enabled"] is False


def test_patch_returns_404_when_account_missing() -> None:
    conn = FakeAccountConn(exists=False)
    r = _client(conn, account_id=uuid4()).patch(
        "/v1/account/processing-config", json={"processing_enabled": False}
    )
    assert r.status_code == 404
