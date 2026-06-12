"""Route-level tests for POST /v1/cameras/{camera_id}/perception/stream-token.

The perception-fanout hot path's auth boundary: Supabase JWT (or agent
token) + camera-ownership checks happen here, then the browser presents
the minted HMAC token to the perception-fanout sidecar
(upstream-streaming-server), which verifies signature + expiry only.

Pattern mirrors tests/test_perception_snapshots.py (read side):
TestClient + a fake asyncpg pool, no real DB. Fanout settings are
injected per-test via monkeypatch (the conftest autouse fixture clears
the settings cache between tests).
"""

from __future__ import annotations

import time
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller
from upstream_api.security.fanout_token import verify_fanout_token

TOKEN_SECRET = "test-fanout-token-secret-32-bytes!!"
WS_BASE = "ws://127.0.0.1:18990"


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


class FakeCameraConn:
    def __init__(self, *, camera_account_id: UUID | None) -> None:
        self.camera_account_id = camera_account_id

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras where id" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}
        return None


def _user_client(conn: Any, *, account_id: UUID, user_id: UUID | None = None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=None,
        user=UserAuth(
            user_id=user_id or uuid4(),
            memberships={account_id: "owner"},
            aal="aal1",
            amr=("password",),
        ),
    )
    return TestClient(app)


def _agent_client(conn: Any, *, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def _no_auth_client(conn: Any) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


@pytest.fixture()
def fanout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UPSTREAM_FANOUT_TOKEN_SECRET", TOKEN_SECRET)
    monkeypatch.setenv("UPSTREAM_FANOUT_PUBLIC_WS_URL", WS_BASE)


_PATH = "/v1/cameras/{camera_id}/perception/stream-token"


def test_mint_happy_path_token_verifies(fanout_env: None) -> None:
    account, camera, user = uuid4(), uuid4(), uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _user_client(conn, account_id=account, user_id=user).post(
        _PATH.format(camera_id=camera)
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body.keys()) == {"token", "ws_url", "expires_at"}
    assert body["ws_url"] == f"{WS_BASE}/v1/subscribe"

    claims = verify_fanout_token(body["token"], TOKEN_SECRET, now=time.time())
    assert claims is not None
    assert claims.camera_id == str(camera)
    assert claims.account_id == str(account)
    assert claims.subject == str(user)


def test_mint_expiry_matches_ttl(fanout_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UPSTREAM_FANOUT_TOKEN_TTL_SECONDS", "120")
    account, camera = uuid4(), uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _user_client(conn, account_id=account).post(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    claims = verify_fanout_token(r.json()["token"], TOKEN_SECRET, now=time.time())
    assert claims is not None
    assert 115 <= claims.expires_at - time.time() <= 121
    assert claims.expires_at - claims.issued_at == 120


def test_mint_agent_caller_gets_agent_subject(fanout_env: None) -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _agent_client(conn, account_id=account).post(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    claims = verify_fanout_token(r.json()["token"], TOKEN_SECRET, now=time.time())
    assert claims is not None
    assert claims.subject.startswith("agent-token:")


def test_mint_unknown_camera_404(fanout_env: None) -> None:
    conn = FakeCameraConn(camera_account_id=None)
    r = _user_client(conn, account_id=uuid4()).post(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_mint_cross_tenant_camera_404(fanout_env: None) -> None:
    caller_account, other_account = uuid4(), uuid4()
    conn = FakeCameraConn(camera_account_id=other_account)
    r = _user_client(conn, account_id=caller_account).post(
        _PATH.format(camera_id=uuid4())
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_mint_unauthenticated_401(fanout_env: None) -> None:
    conn = FakeCameraConn(camera_account_id=uuid4())
    r = _no_auth_client(conn).post(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


def test_mint_unconfigured_503(monkeypatch: pytest.MonkeyPatch) -> None:
    # setenv("") rather than delenv: Settings also reads the repo .env file,
    # so deleting the OS env var alone is not hermetic on a dev machine whose
    # .env sets the fanout vars. An explicit empty value overrides .env and
    # (by design) counts as unconfigured.
    monkeypatch.setenv("UPSTREAM_FANOUT_TOKEN_SECRET", "")
    monkeypatch.setenv("UPSTREAM_FANOUT_PUBLIC_WS_URL", "")
    account = uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _user_client(conn, account_id=account).post(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "perception_stream_not_configured"


def test_mint_empty_secret_is_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty-string env var yields SecretStr("") rather than None; it must
    503 like an unset secret — the sidecar refuses to start on the same empty
    value, so minting would produce tokens nothing can verify."""
    monkeypatch.setenv("UPSTREAM_FANOUT_TOKEN_SECRET", "")
    monkeypatch.setenv("UPSTREAM_FANOUT_PUBLIC_WS_URL", WS_BASE)
    account = uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _user_client(conn, account_id=account).post(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "perception_stream_not_configured"


def test_mint_secret_alone_is_not_enough(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both settings are required: a secret without a public ws_url would
    mint tokens the browser has no address for."""
    monkeypatch.setenv("UPSTREAM_FANOUT_TOKEN_SECRET", TOKEN_SECRET)
    monkeypatch.setenv("UPSTREAM_FANOUT_PUBLIC_WS_URL", "")
    account = uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _user_client(conn, account_id=account).post(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "perception_stream_not_configured"


def test_token_not_logged_in_response_headers(fanout_env: None) -> None:
    """The token may only appear in the JSON body — defensive pin against a
    refactor that echoes it into a header or cookie."""
    account, camera = uuid4(), uuid4()
    conn = FakeCameraConn(camera_account_id=account)
    r = _user_client(conn, account_id=account).post(_PATH.format(camera_id=camera))
    token = r.json()["token"]
    for value in r.headers.values():
        assert token not in value
