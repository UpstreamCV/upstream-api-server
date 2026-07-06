"""GET /v1/agents/bootstrap — agent token context endpoint.

Covers: full payload assembly, account scoping of every query, auth failures
(missing / invalid / revoked tokens via the REAL ``current_agent`` dependency),
and the no-secrets guarantee.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.api_tokens import issue_token
from upstream_api.security.deps import AgentAuth, current_agent

ACCOUNT_ID = uuid4()
TOKEN_ID = uuid4()
AGENT_ID = uuid4()
CAMERA_ID = uuid4()
SESSION_ID = uuid4()

_NOW = datetime.now(tz=UTC)


def _account_row() -> dict[str, Any]:
    return {
        "id": ACCOUNT_ID,
        "name": "Josh Demo",
        "slug": "josh-demo-abc123",
        "plan": "free",
        "status": "active",
    }


def _token_row() -> dict[str, Any]:
    return {
        "name": "macbook token",
        "scopes": ["agent"],
        "last_four": "WXYZ",
        "expires_at": None,
    }


def _agent_row() -> dict[str, Any]:
    return {
        "id": AGENT_ID,
        "name": "upstream-agent-abcd1234",
        "status": "active",
        "version": "0.1.0",
        "last_seen_at": _NOW,
    }


def _limits_row() -> dict[str, Any]:
    return {
        "max_cameras": 5,
        "max_concurrent_sessions": 5,
        "max_fps": 60,
        "max_width": 1920,
        "max_height": 1080,
        "max_bitrate_kbps": 8000,
        "allowed_codecs": ["h264", "h265"],
        "allowed_stream_types": ["webcam", "rtsp"],
        "monthly_minutes_quota": 600,
        "snapshot_storage_mb": 1024,
        "session_ttl_seconds": None,
    }


def _usage_row() -> dict[str, Any]:
    return {
        "camera_count": 1,
        "active_session_count": 1,
        "monthly_seconds_used": 359,  # 5m59s -> 5 minutes
    }


def _camera_row() -> dict[str, Any]:
    return {
        "id": CAMERA_ID,
        "account_id": ACCOUNT_ID,
        "agent_id": AGENT_ID,
        "name": "Front Door",
        "external_id": "webcam-0",
        "device_path": "webcam:0",
        "source_type": "webcam",
        "default_width": 1280,
        "default_height": 720,
        "default_fps": 30,
        "default_codec": "h264",
        "default_bitrate_kbps": 3000,
        "default_latency_mode": "balanced",
        "metadata": {},
        "status": "streaming",
        "created_at": _NOW,
        "updated_at": _NOW,
    }


def _session_row() -> dict[str, Any]:
    return {
        "id": SESSION_ID,
        "camera_id": CAMERA_ID,
        "agent_id": AGENT_ID,
        "state": "streaming",
        "created_at": _NOW,
        "expires_at": _NOW + timedelta(hours=4),
        "started_at": _NOW,
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


class FakeConn:
    """Dispatches on query substrings; records the account bind of each query."""

    def __init__(self) -> None:
        self.account_binds: list[Any] = []
        self.api_token_row: dict[str, Any] | None = None

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from api_tokens" in query and "prefix" in query:
            # Real current_agent dependency path (auth tests).
            return self.api_token_row
        if "from api_tokens" in query:
            # Bound by token_id, not account_id — not part of the scoping check.
            return _token_row()
        self.account_binds.append(args[0] if args else None)
        if "from accounts" in query:
            return _account_row()
        if "from account_limits" in query:
            return _limits_row()
        if "monthly_seconds_used" in query:
            return _usage_row()
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.account_binds.append(args[0] if args else None)
        if "from agents" in query:
            return [_agent_row()]
        if "from account_limits" in query:
            return []
        if "from cameras" in query:
            return [_camera_row()]
        if "from stream_sessions" in query:
            return [_session_row()]
        return []

    async def fetchval(self, query: str, *args: Any) -> Any:
        return None

    async def execute(self, query: str, *args: Any) -> str:
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


def _client(
    conn: FakeConn, *, account_id: UUID = ACCOUNT_ID, override_auth: bool = True
) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if override_auth:
        auth = AgentAuth(account_id=account_id, token_id=TOKEN_ID, scopes=("agent",))
        app.dependency_overrides[current_agent] = lambda: auth
    return TestClient(app)


def test_bootstrap_happy_path_returns_all_blocks() -> None:
    conn = FakeConn()
    r = _client(conn).get("/v1/agents/bootstrap")
    assert r.status_code == 200
    body = r.json()

    assert body["account"]["id"] == str(ACCOUNT_ID)
    assert body["account"]["plan"] == "free"
    assert body["token"]["scopes"] == ["agent"]
    assert body["token"]["last_four"] == "WXYZ"
    assert [a["id"] for a in body["agents"]] == [str(AGENT_ID)]
    assert body["limits"]["allowed_stream_types"] == ["rtsp", "webcam"]
    assert body["limits"]["max_concurrent_sessions"] == 5
    assert body["usage"]["camera_count"] == 1
    assert body["usage"]["active_session_count"] == 1
    assert body["usage"]["monthly_minutes_used"] == 5  # floor(359 / 60)
    assert [c["id"] for c in body["cameras"]] == [str(CAMERA_ID)]
    assert body["cameras"][0]["source_type"] == "webcam"
    assert body["active_sessions"][0]["approved"]["width"] == 1280
    assert "server_time" in body


def test_bootstrap_agents_empty_when_not_registered() -> None:
    conn = FakeConn()

    async def _no_agents(self: FakeConn, query: str, *args: Any) -> list[dict[str, Any]]:
        self.account_binds.append(args[0] if args else None)
        if "from agents" in query:
            return []
        if "from cameras" in query:
            return []
        if "from stream_sessions" in query:
            return []
        return []

    conn.fetch = _no_agents.__get__(conn)  # type: ignore[method-assign]
    r = _client(conn).get("/v1/agents/bootstrap")
    assert r.status_code == 200
    assert r.json()["agents"] == []
    assert r.json()["cameras"] == []
    assert r.json()["active_sessions"] == []


def test_bootstrap_is_scoped_to_token_account() -> None:
    conn = FakeConn()
    r = _client(conn).get("/v1/agents/bootstrap")
    assert r.status_code == 200
    assert conn.account_binds  # every query ran with a bind
    assert all(bind == ACCOUNT_ID for bind in conn.account_binds)


def test_bootstrap_rejects_missing_token() -> None:
    conn = FakeConn()
    r = _client(conn, override_auth=False).get("/v1/agents/bootstrap")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


def test_bootstrap_rejects_unknown_token() -> None:
    conn = FakeConn()
    conn.api_token_row = None
    issued = issue_token(environment="test")
    r = _client(conn, override_auth=False).get(
        "/v1/agents/bootstrap",
        headers={"Authorization": f"Bearer {issued.plaintext}"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_token"


def test_bootstrap_rejects_revoked_token() -> None:
    conn = FakeConn()
    issued = issue_token(environment="test")
    conn.api_token_row = {
        "id": TOKEN_ID,
        "account_id": ACCOUNT_ID,
        "hash": issued.hash,
        "scopes": ["agent"],
        "expires_at": None,
        "revoked_at": _NOW,
        "last_used_at": None,
    }
    r = _client(conn, override_auth=False).get(
        "/v1/agents/bootstrap",
        headers={"Authorization": f"Bearer {issued.plaintext}"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "token_revoked"


def test_bootstrap_response_contains_no_secrets() -> None:
    """Seeded rows are realistic; a leak of any secret-bearing field would show."""
    conn = FakeConn()
    r = _client(conn).get("/v1/agents/bootstrap")
    assert r.status_code == 200
    text = r.text.lower()
    needles = ("srt_url", "srt://", "passphrase", "streamid", "session_token", '"hash"', "prefix")
    for needle in needles:
        assert needle not in text, f"bootstrap response leaked: {needle}"


def test_bootstrap_route_not_shadowed_by_agent_id_route() -> None:
    conn = FakeConn()
    r = _client(conn).get("/v1/agents/bootstrap")
    # A route-order bug would parse "bootstrap" as a UUID -> 422.
    assert r.status_code == 200


# --- update M2: agent version policy ----------------------------------------


def test_bootstrap_version_policy_null_when_unconfigured() -> None:
    conn = FakeConn()
    r = _client(conn).get("/v1/agents/bootstrap")
    assert r.status_code == 200
    assert r.json()["agent_version_policy"] is None


def test_bootstrap_version_policy_present_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from upstream_api.config import get_settings

    settings = get_settings().model_copy(
        update={
            "agent_latest_version": "0.2.0",
            "agent_min_supported_version": "0.1.0",
            "agent_update_severity_hint": "recommended",
            "agent_release_notes_url": "https://example.test/releases",
            "agent_install_hint": "pip install --upgrade upstream-agent",
        }
    )
    monkeypatch.setattr("upstream_api.routers.agents.get_settings", lambda: settings)
    conn = FakeConn()
    r = _client(conn).get("/v1/agents/bootstrap")
    assert r.status_code == 200
    policy = r.json()["agent_version_policy"]
    assert policy == {
        "latest_version": "0.2.0",
        "minimum_supported_version": "0.1.0",
        "severity_hint": "recommended",
        "release_notes_url": "https://example.test/releases",
        "install_hint": "pip install --upgrade upstream-agent",
    }


def test_bootstrap_version_policy_partial_config_still_ships(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Either of latest/minimum alone is enough to ship the policy; the
    other fields ride along as null."""
    from upstream_api.config import get_settings

    settings = get_settings().model_copy(update={"agent_latest_version": "0.2.0"})
    monkeypatch.setattr("upstream_api.routers.agents.get_settings", lambda: settings)
    r = _client(FakeConn()).get("/v1/agents/bootstrap")
    policy = r.json()["agent_version_policy"]
    assert policy["latest_version"] == "0.2.0"
    assert policy["minimum_supported_version"] is None
