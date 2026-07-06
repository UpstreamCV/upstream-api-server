"""Device-scoped token binding: TOFU bind at register + per-route enforcement.

The unit layer covers ``require_agent_access`` truth-table; the router layer
covers the trust-on-first-use bind inside ``POST /v1/agents/register`` and
that every agent-authed write path rejects a mis-bound device token BEFORE
touching the target resource.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.errors import ForbiddenError
from upstream_api.security.deps import (
    AgentAuth,
    CallerAuth,
    current_agent,
    current_caller,
    require_agent_access,
)
from upstream_api.services import account_audit

# ---------------------------------------------------------------------------
# require_agent_access truth table
# ---------------------------------------------------------------------------


def _auth(*, scopes: tuple[str, ...], agent_id: UUID | None = None) -> AgentAuth:
    return AgentAuth(account_id=uuid4(), token_id=uuid4(), scopes=scopes, agent_id=agent_id)


def test_account_scoped_token_passes_any_agent() -> None:
    require_agent_access(_auth(scopes=("agent",)), uuid4())


def test_unbound_device_token_is_rejected() -> None:
    with pytest.raises(ForbiddenError) as exc:
        require_agent_access(_auth(scopes=("device",)), uuid4())
    assert exc.value.code == "device_token_unbound"


def test_bound_device_token_mismatch_is_rejected() -> None:
    with pytest.raises(ForbiddenError) as exc:
        require_agent_access(_auth(scopes=("device",), agent_id=uuid4()), uuid4())
    assert exc.value.code == "device_token_agent_mismatch"


def test_bound_device_token_matching_agent_passes() -> None:
    agent_id = uuid4()
    require_agent_access(_auth(scopes=("device",), agent_id=agent_id), agent_id)


# ---------------------------------------------------------------------------
# Router-level fakes
# ---------------------------------------------------------------------------


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Substring-dispatched fake for the register/bind flow."""

    def __init__(self) -> None:
        self.account_id = uuid4()
        self.agent_id = uuid4()
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        # POST /register upsert returns this agents row.
        self.agent_row_id = self.agent_id
        # `select id from agents where account_id = $1 and name = $2`
        self.existing_agent_by_name: UUID | None = None
        # `update api_tokens ... where id = $1 and agent_id is null`
        self.bind_succeeds = True
        # fallback read when the bind update returns no row
        self.token_agent_id: UUID | None = None
        self.bind_calls: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "select id from agents where account_id" in query:
            return self.existing_agent_by_name
        if "select agent_id from api_tokens" in query:
            return self.token_agent_id
        if "to_regclass" in query:
            # Audit table present (so the bind audit write is observable);
            # usage_events absent (so record_event stays a no-op).
            return "account_audit_logs" in query
        return None

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "insert into agents" in query:
            now = datetime.now(tz=UTC)
            return {
                "id": self.agent_row_id,
                "account_id": args[0],
                "name": args[1],
                "hostname": args[2],
                "version": args[3],
                "os": args[4],
                "python_version": args[5],
                "capabilities": {},
                "status": "online",
                "last_seen_at": now,
                "created_at": now,
                "updated_at": now,
            }
        if "update api_tokens" in query and "agent_id is null" in query:
            self.bind_calls.append(args)
            if self.bind_succeeds:
                return {"agent_id": args[1]}
            return None
        return None

    async def execute(self, query: str, *args: Any) -> str:
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


@pytest.fixture(autouse=True)
def _reset_audit_cache() -> None:
    account_audit.reset_audit_table_cache()


def _client(conn: FakeConn, auth: AgentAuth) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_agent] = lambda: auth
    # Dual-auth (CurrentCaller) endpoints resolve the same agent token.
    app.dependency_overrides[current_caller] = lambda: CallerAuth(agent=auth, user=None)
    return TestClient(app)


def _register_body(name: str = "T1234") -> dict[str, Any]:
    return {
        "name": name,
        "hostname": "pi-zero",
        "version": "1.0.0",
        "os": "linux",
        "python_version": "3.11.9",
        "capabilities": {},
    }


_HDRS = {"Authorization": "Bearer test"}


# ---------------------------------------------------------------------------
# TOFU bind at /v1/agents/register
# ---------------------------------------------------------------------------


def test_register_binds_unbound_device_token() -> None:
    conn = FakeConn()
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post("/v1/agents/register", json=_register_body(), headers=_HDRS)
    assert r.status_code == 200, r.text
    # The bind UPDATE ran with (token_id, upserted agent id).
    assert conn.bind_calls == [(auth.token_id, conn.agent_row_id)]
    # And the bind was audited.
    assert any("api_token.bound" in str(args) for _, args in conn.executes)


def test_register_rebind_race_same_agent_is_ok() -> None:
    """Bind race where a concurrent register already bound the token to the
    SAME agent: the update returns no row but the current binding matches."""
    conn = FakeConn()
    conn.bind_succeeds = False
    conn.token_agent_id = conn.agent_row_id
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post("/v1/agents/register", json=_register_body(), headers=_HDRS)
    assert r.status_code == 200, r.text


def test_register_rebind_race_other_agent_is_403() -> None:
    conn = FakeConn()
    conn.bind_succeeds = False
    conn.token_agent_id = uuid4()  # bound elsewhere by the race winner
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post("/v1/agents/register", json=_register_body(), headers=_HDRS)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_register_bound_token_other_agent_name_is_403() -> None:
    conn = FakeConn()
    bound_agent = uuid4()
    conn.existing_agent_by_name = uuid4()  # the name resolves to someone else
    auth = AgentAuth(
        account_id=conn.account_id,
        token_id=uuid4(),
        scopes=("device",),
        agent_id=bound_agent,
    )
    r = _client(conn, auth).post(
        "/v1/agents/register", json=_register_body("other-device"), headers=_HDRS
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_register_bound_token_same_agent_reregisters() -> None:
    conn = FakeConn()
    conn.existing_agent_by_name = conn.agent_row_id
    auth = AgentAuth(
        account_id=conn.account_id,
        token_id=uuid4(),
        scopes=("device",),
        agent_id=conn.agent_row_id,
    )
    r = _client(conn, auth).post("/v1/agents/register", json=_register_body(), headers=_HDRS)
    assert r.status_code == 200, r.text
    # No re-bind attempt for an already-bound token.
    assert conn.bind_calls == []


def test_register_account_token_never_binds() -> None:
    conn = FakeConn()
    auth = AgentAuth(account_id=conn.account_id, token_id=uuid4(), scopes=("agent",))
    r = _client(conn, auth).post("/v1/agents/register", json=_register_body(), headers=_HDRS)
    assert r.status_code == 200, r.text
    assert conn.bind_calls == []


# ---------------------------------------------------------------------------
# Enforcement on the other agent-authed write paths. The mismatch cases are
# rejected BEFORE any DB access, so the bare FakeConn suffices.
# ---------------------------------------------------------------------------


def _bound_elsewhere(conn: FakeConn) -> AgentAuth:
    return AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=uuid4()
    )


def test_heartbeats_reject_mismatched_device_token() -> None:
    conn = FakeConn()
    r = _client(conn, _bound_elsewhere(conn)).post(
        f"/v1/heartbeats?agent_id={uuid4()}",
        json={
            "heartbeats": [{"status": "online", "reported_at": datetime.now(tz=UTC).isoformat()}]
        },
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_camera_register_rejects_mismatched_device_token() -> None:
    conn = FakeConn()
    r = _client(conn, _bound_elsewhere(conn)).post(
        "/v1/cameras/register",
        json={
            "agent_id": str(uuid4()),
            "name": "front-door",
            "default_width": 1280,
            "default_height": 720,
            "default_fps": 30,
            "default_codec": "h264",
            "default_bitrate_kbps": 3000,
        },
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_camera_claim_rejects_unbound_device_token() -> None:
    conn = FakeConn()
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post(
        f"/v1/cameras/{uuid4()}/claim",
        json={"agent_id": str(uuid4())},
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_unbound"


def test_session_start_rejects_unbound_device_token() -> None:
    conn = FakeConn()
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post(
        "/v1/stream-sessions/start",
        json={
            "camera_id": str(uuid4()),
            "requested": {
                "width": 1280,
                "height": 720,
                "fps": 30,
                "codec": "h264",
                "bitrate_kbps": 3000,
            },
        },
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_unbound"


def test_session_stop_rejects_unbound_device_token() -> None:
    conn = FakeConn()
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post(f"/v1/stream-sessions/{uuid4()}/stop", headers=_HDRS)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_unbound"


def test_snapshot_upload_rejects_mismatched_device_token() -> None:
    conn = FakeConn()
    r = _client(conn, _bound_elsewhere(conn)).post(
        "/v1/snapshots",
        data={
            "camera_id": str(uuid4()),
            "captured_at": datetime.now(tz=UTC).isoformat(),
            "agent_id": str(uuid4()),
        },
        files={"file": ("f.jpg", b"\xff\xd8\xff\xe0fake", "image/jpeg")},
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


# ---------------------------------------------------------------------------
# TOFU hijack: one active bound token per agent (DB unique index)
# ---------------------------------------------------------------------------


def test_register_hijack_of_actively_bound_agent_is_403() -> None:
    """An unbound token registering under a name whose agent already has a
    live device token hits the partial unique index -> 403, upsert rolled
    back."""
    import asyncpg

    class HijackConn(FakeConn):
        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            if "update api_tokens" in query and "agent_id is null" in query:
                raise asyncpg.UniqueViolationError("one active token per agent")
            return await super().fetchrow(query, *args)

    conn = HijackConn()
    auth = AgentAuth(
        account_id=conn.account_id, token_id=uuid4(), scopes=("device",), agent_id=None
    )
    r = _client(conn, auth).post(
        "/v1/agents/register", json=_register_body("victim-device"), headers=_HDRS
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_conflict"


# ---------------------------------------------------------------------------
# Cross-agent enforcement with a BOUND token (service level: the camera /
# session rows resolve to a different agent than the binding)
# ---------------------------------------------------------------------------


async def test_start_session_rejects_camera_of_other_agent() -> None:
    from upstream_api.config import get_settings
    from upstream_api.services.limits import RequestedStream
    from upstream_api.services.session_lifecycle import start_session

    account_id = uuid4()
    camera_agent = uuid4()
    bound_agent = uuid4()

    class Conn:
        def transaction(self) -> FakeTransaction:
            return FakeTransaction()

        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            if "from cameras" in query:
                return {
                    "id": args[0],
                    "account_id": account_id,
                    "agent_id": camera_agent,
                    "source_type": "webcam",
                }
            return None

    with pytest.raises(ForbiddenError) as exc:
        await start_session(
            Conn(),
            settings=get_settings(),
            account_id=account_id,
            camera_id=uuid4(),
            requested=RequestedStream(
                width=1280, height=720, fps=30, codec="h264", bitrate_kbps=2500
            ),
            required_agent_id=bound_agent,
        )
    assert exc.value.code == "device_token_agent_mismatch"


async def test_stop_session_rejects_session_of_other_agent() -> None:
    from upstream_api.services.session_lifecycle import stop_session

    account_id = uuid4()
    session_agent = uuid4()
    bound_agent = uuid4()

    class Conn:
        def transaction(self) -> FakeTransaction:
            return FakeTransaction()

        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            if "from stream_sessions" in query and "for update" in query:
                return {
                    "id": args[0],
                    "account_id": account_id,
                    "agent_id": session_agent,
                    "camera_id": uuid4(),
                    "state": "streaming",
                    "srt_url": "srt://x",
                    "streamid": "publish:sess_" + "0" * 16,
                    "expires_at": datetime.now(tz=UTC),
                    "created_at": datetime.now(tz=UTC),
                    "started_at": None,
                    "ended_at": None,
                    "req_width": 1280,
                    "req_height": 720,
                    "req_fps": 30,
                    "req_codec": "h264",
                    "req_bitrate_kbps": 2500,
                    "req_latency_mode": "balanced",
                    "lim_width": 1280,
                    "lim_height": 720,
                    "lim_fps": 30,
                    "lim_codec": "h264",
                    "lim_bitrate_kbps": 2500,
                    "lim_latency_mode": "balanced",
                }
            return None

    with pytest.raises(ForbiddenError) as exc:
        await stop_session(
            Conn(),
            account_id=account_id,
            session_id=uuid4(),
            required_agent_id=bound_agent,
        )
    assert exc.value.code == "device_token_agent_mismatch"


# ---------------------------------------------------------------------------
# CurrentCaller surfaces: config writes, perception token, WHEP, account switch
# ---------------------------------------------------------------------------


class CameraOwnerConn(FakeConn):
    """FakeConn whose camera owner-row lookup returns a configurable agent."""

    def __init__(self, *, camera_agent_id: UUID | None) -> None:
        super().__init__()
        self.camera_agent_id = camera_agent_id

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras where id" in query:
            return {"account_id": self.account_id, "agent_id": self.camera_agent_id}
        if "from stream_sessions where id" in query:
            return {
                "account_id": self.account_id,
                "agent_id": self.camera_agent_id,
                "state": "streaming",
                "streamid": "publish:sess_" + "0" * 16,
            }
        return await super().fetchrow(query, *args)


def test_geometry_put_rejects_mismatched_device_token() -> None:
    conn = CameraOwnerConn(camera_agent_id=uuid4())
    r = _client(conn, _bound_elsewhere(conn)).put(
        f"/v1/cameras/{uuid4()}/geometry-config",
        json={
            "config": {
                "version": 1,
                "frame": {
                    "width": 1280,
                    "height": 720,
                    "coordinate_space": "normalized",
                },
                "rois": [],
                "lines": [],
            }
        },
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_processing_config_put_rejects_mismatched_device_token() -> None:
    conn = CameraOwnerConn(camera_agent_id=uuid4())
    r = _client(conn, _bound_elsewhere(conn)).put(
        f"/v1/cameras/{uuid4()}/processing-config",
        json={
            "config": {
                "version": 1,
                "model_key": "trafficcamnet_v1",
                "selected_classes": [],
                "tracker": None,
                "confidence_threshold": None,
                "processing_fps": None,
            }
        },
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_perception_stream_token_rejects_mismatched_device_token() -> None:
    from upstream_api.config import Settings, get_settings

    conn = CameraOwnerConn(camera_agent_id=uuid4())
    auth = _bound_elsewhere(conn)
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_agent] = lambda: auth
    app.dependency_overrides[current_caller] = lambda: CallerAuth(agent=auth, user=None)
    base = get_settings()
    configured: Settings = base.model_copy(
        update={
            "fanout_token_secret": base.session_jwt_secret,
            "fanout_public_ws_url": "ws://fanout.test",
        }
    )
    app.dependency_overrides[get_settings] = lambda: configured
    r = TestClient(app).post(f"/v1/cameras/{uuid4()}/perception/stream-token", headers=_HDRS)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_account_processing_switch_forbidden_for_device_tokens() -> None:
    conn = FakeConn()
    # Even a BOUND device token must not flip the account-wide switch.
    r = _client(conn, _bound_elsewhere(conn)).patch(
        "/v1/account/processing-config",
        json={"processing_enabled": False},
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_forbidden"


def test_whep_offer_rejects_mismatched_device_token() -> None:
    conn = CameraOwnerConn(camera_agent_id=uuid4())
    r = _client(conn, _bound_elsewhere(conn)).post(
        f"/v1/stream-sessions/{uuid4()}/whep",
        content=b"v=0 fake sdp",
        headers={**_HDRS, "Content-Type": "application/sdp"},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


def test_whep_delete_rejects_mismatched_device_token() -> None:
    conn = CameraOwnerConn(camera_agent_id=uuid4())
    r = _client(conn, _bound_elsewhere(conn)).delete(
        f"/v1/stream-sessions/{uuid4()}/whep/resource123",
        headers=_HDRS,
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "device_token_agent_mismatch"


# ---------------------------------------------------------------------------
# Bootstrap scoping: a bound device token sees only its own agent's slice
# ---------------------------------------------------------------------------


def test_bootstrap_scopes_queries_to_bound_agent() -> None:
    bound_agent = uuid4()

    class BootstrapConn(FakeConn):
        def __init__(self) -> None:
            super().__init__()
            self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []

        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            if "from accounts where id" in query:
                return {
                    "id": args[0],
                    "name": "Fleet",
                    "slug": "fleet",
                    "plan": "pro",
                    "status": "active",
                }
            if "from api_tokens where id" in query:
                return {
                    "name": "T1234",
                    "scopes": ["device"],
                    "last_four": "ABCD",
                    "expires_at": None,
                }
            if "from account_limits" in query:
                return {
                    "max_cameras": 10,
                    "max_concurrent_sessions": 5,
                    "max_fps": 60,
                    "max_width": 4096,
                    "max_height": 2160,
                    "max_bitrate_kbps": 12000,
                    "allowed_codecs": ["h264"],
                    "allowed_stream_types": ["webcam", "rtsp"],
                    "monthly_minutes_quota": 600,
                    "snapshot_storage_mb": 1024,
                    "session_ttl_seconds": None,
                }
            if "camera_count" in query:
                return {
                    "camera_count": 1,
                    "active_session_count": 0,
                    "monthly_seconds_used": 0,
                }
            return await super().fetchrow(query, *args)

        async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
            self.fetch_calls.append((query, args))
            return []

    conn = BootstrapConn()
    auth = AgentAuth(
        account_id=conn.account_id,
        token_id=uuid4(),
        scopes=("device",),
        agent_id=bound_agent,
    )
    r = _client(conn, auth).get("/v1/agents/bootstrap", headers=_HDRS)
    assert r.status_code == 200, r.text
    # All three list queries (agents, cameras, sessions) must be scoped to
    # the bound agent via their $2 parameter.
    assert len(conn.fetch_calls) == 3
    for query, args in conn.fetch_calls:
        assert "$2" in query
        assert args[1] == bound_agent
