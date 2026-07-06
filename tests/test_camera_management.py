"""Dashboard camera management: POST /v1/cameras, PATCH /v1/cameras/{id},
agent claim flow, and the camera_unassigned guard at session start."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.errors import ConflictError
from upstream_api.security.deps import (
    AgentAuth,
    CallerAuth,
    UserAuth,
    current_agent,
    current_caller,
)
from upstream_api.services.limits import RequestedStream
from upstream_api.services.session_lifecycle import start_session

ACCOUNT_ID = uuid4()
AGENT_ID = uuid4()
CAMERA_ID = uuid4()

_NOW = datetime.now(tz=UTC)


def _limits_row() -> dict[str, Any]:
    return {
        "max_cameras": 5,
        "max_concurrent_sessions": 5,
        "max_fps": 60,
        "max_width": 1920,
        "max_height": 1080,
        "max_bitrate_kbps": 8000,
        "allowed_codecs": ["h264"],
        "allowed_stream_types": ["webcam", "rtsp"],
        "monthly_minutes_quota": 600,
        "snapshot_storage_mb": 1024,
        "session_ttl_seconds": None,
    }


def _camera_row(
    *,
    agent_id: UUID | None = None,
    account_id: UUID = ACCOUNT_ID,
    source_type: str | None = "webcam",
) -> dict[str, Any]:
    return {
        "id": CAMERA_ID,
        "account_id": account_id,
        "agent_id": agent_id,
        "name": "Front Door",
        "external_id": None,
        "device_path": "webcam:0",
        "source_type": source_type,
        "default_width": 1280,
        "default_height": 720,
        "default_fps": 30,
        "default_codec": "h264",
        "default_bitrate_kbps": 3000,
        "default_latency_mode": "balanced",
        "metadata": {},
        "status": "idle",
        "created_at": _NOW,
        "updated_at": _NOW,
    }


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    def __init__(
        self,
        *,
        camera: dict[str, Any] | None = None,
        agent_account_id: UUID | None = ACCOUNT_ID,
        camera_count: int = 0,
        claim_update_returns: bool = True,
    ) -> None:
        self.camera = camera
        self.agent_account_id = agent_account_id
        self.camera_count = camera_count
        self.claim_update_returns = claim_update_returns
        self.insert_sql: str = ""
        self.insert_args: tuple[Any, ...] | None = None
        self.update_sql: str = ""
        self.update_args: tuple[Any, ...] | None = None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from agents" in query:
            if self.agent_account_id is None:
                return None
            return {"account_id": self.agent_account_id}
        if "from account_limits" in query:
            return _limits_row()
        if "from cameras" in query:
            return self.camera
        if "insert into cameras" in query:
            self.insert_sql = query
            self.insert_args = args
            row = _camera_row(agent_id=None, source_type=args[3])
            row["name"] = args[1]
            return row
        if "update cameras" in query and "agent_id = $3" in query:
            self.update_sql = query
            self.update_args = args
            if not self.claim_update_returns:
                return None
            return _camera_row(agent_id=args[2])
        if "update cameras" in query:
            self.update_sql = query
            self.update_args = args
            return _camera_row(agent_id=self.camera["agent_id"] if self.camera else None)
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "count(*)" in query:
            return self.camera_count
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


def _user_client(conn: FakeConn, *, role: str = "owner") -> TestClient:
    app = create_app()
    user = UserAuth(user_id=uuid4(), memberships={ACCOUNT_ID: role}, aal="aal1", amr=())
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(agent=None, user=user)
    return TestClient(app)


def _agent_client(conn: FakeConn) -> TestClient:
    app = create_app()
    auth = AgentAuth(account_id=ACCOUNT_ID, token_id=uuid4(), scopes=("agent",))
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_agent] = lambda: auth
    return TestClient(app)


# ----------------------------------------------------------------------------
# POST /v1/cameras — dashboard create
# ----------------------------------------------------------------------------


def _create_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": "Front Door",
        "source_type": "webcam",
        "device_path": "webcam:0",
        "default_width": 1280,
        "default_height": 720,
        "default_fps": 30,
    }
    body.update(overrides)
    return body


def test_user_creates_unassigned_camera() -> None:
    conn = FakeConn()
    r = _user_client(conn).post("/v1/cameras", json=_create_body())
    assert r.status_code == 201
    assert r.json()["agent_id"] is None
    assert r.json()["source_type"] == "webcam"
    assert "values ($1, null" in conn.insert_sql  # created unassigned


def test_viewer_role_cannot_create_camera() -> None:
    conn = FakeConn()
    r = _user_client(conn, role="viewer").post("/v1/cameras", json=_create_body())
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.role"


def test_agent_token_cannot_use_dashboard_create() -> None:
    conn = FakeConn()
    app = create_app()
    agent = AgentAuth(account_id=ACCOUNT_ID, token_id=uuid4(), scopes=("agent",))
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(agent=agent, user=None)
    r = TestClient(app).post("/v1/cameras", json=_create_body())
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.user_required"


def test_create_rejects_disallowed_stream_type() -> None:
    conn = FakeConn()
    r = _user_client(conn).post("/v1/cameras", json=_create_body(source_type="file"))
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "limit_exceeded.stream_type_not_allowed"


def test_create_enforces_camera_slot_limit() -> None:
    conn = FakeConn(camera_count=5)
    r = _user_client(conn).post("/v1/cameras", json=_create_body())
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "limit_exceeded.max_cameras"


def test_create_rejects_over_limit_defaults() -> None:
    conn = FakeConn()
    body = _create_body(default_width=3840, default_height=2160)
    r = _user_client(conn).post("/v1/cameras", json=body)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "limit_exceeded.resolution_too_high"


# ----------------------------------------------------------------------------
# PATCH /v1/cameras/{id}
# ----------------------------------------------------------------------------


def test_user_patches_camera_config() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=AGENT_ID))
    r = _user_client(conn).patch(f"/v1/cameras/{CAMERA_ID}", json={"default_fps": 15})
    assert r.status_code == 200
    assert conn.update_args is not None


def test_patch_cross_account_camera_is_404() -> None:
    conn = FakeConn(camera=_camera_row(account_id=uuid4()))
    r = _user_client(conn).patch(f"/v1/cameras/{CAMERA_ID}", json={"default_fps": 15})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_patch_rejects_merged_over_limit_config() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=AGENT_ID))
    r = _user_client(conn).patch(f"/v1/cameras/{CAMERA_ID}", json={"default_fps": 120})
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "limit_exceeded.fps_too_high"


def test_viewer_role_cannot_patch_camera() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=AGENT_ID))
    client = _user_client(conn, role="viewer")
    r = client.patch(f"/v1/cameras/{CAMERA_ID}", json={"default_fps": 15})
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.role"


# ----------------------------------------------------------------------------
# POST /v1/cameras/{id}/claim
# ----------------------------------------------------------------------------


def test_agent_claims_unassigned_camera() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=None))
    r = _agent_client(conn).post(f"/v1/cameras/{CAMERA_ID}/claim", json={"agent_id": str(AGENT_ID)})
    assert r.status_code == 200
    assert r.json()["agent_id"] == str(AGENT_ID)
    assert conn.update_sql and "agent_id is null" in conn.update_sql


def test_claim_is_idempotent_for_same_agent() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=AGENT_ID))
    r = _agent_client(conn).post(f"/v1/cameras/{CAMERA_ID}/claim", json={"agent_id": str(AGENT_ID)})
    assert r.status_code == 200
    assert conn.update_args is None  # no write needed


def test_claim_conflicts_when_held_by_other_agent() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=uuid4()))
    r = _agent_client(conn).post(f"/v1/cameras/{CAMERA_ID}/claim", json={"agent_id": str(AGENT_ID)})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "camera_claimed_by_other_agent"


def test_claim_race_lost_is_conflict() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=None), claim_update_returns=False)
    r = _agent_client(conn).post(f"/v1/cameras/{CAMERA_ID}/claim", json={"agent_id": str(AGENT_ID)})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "camera_claimed_by_other_agent"


def test_claim_rejects_foreign_agent() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=None), agent_account_id=uuid4())
    r = _agent_client(conn).post(f"/v1/cameras/{CAMERA_ID}/claim", json={"agent_id": str(AGENT_ID)})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "agent_account_mismatch"


def test_claim_cross_account_camera_is_404() -> None:
    conn = FakeConn(camera=_camera_row(agent_id=None, account_id=uuid4()))
    r = _agent_client(conn).post(f"/v1/cameras/{CAMERA_ID}/claim", json={"agent_id": str(AGENT_ID)})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


# ----------------------------------------------------------------------------
# start_session rejects unassigned cameras
# ----------------------------------------------------------------------------


class UnassignedCameraConn:
    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return {
                "id": args[0],
                "account_id": ACCOUNT_ID,
                "agent_id": None,
                "source_type": "webcam",
            }
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        return False

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"


async def test_session_start_rejects_unassigned_camera() -> None:
    conn = UnassignedCameraConn()
    requested = RequestedStream(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=3000)
    with pytest.raises(ConflictError) as exc:
        await start_session(
            conn,
            settings=get_settings(),
            account_id=ACCOUNT_ID,
            camera_id=CAMERA_ID,
            requested=requested,
        )
    assert exc.value.code == "camera_unassigned"
