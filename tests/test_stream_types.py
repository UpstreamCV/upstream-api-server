"""allowed_stream_types enforcement at camera registration + session start."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.errors import ForbiddenError, LimitExceeded
from upstream_api.security.deps import (
    AgentAuth,
    CallerAuth,
    UserAuth,
    current_agent,
    current_caller,
)
from upstream_api.services.limits import (
    AccountLimits,
    check_execution_mode_allowed,
    check_stream_type_allowed,
)

ACCOUNT_ID = uuid4()
AGENT_ID = uuid4()

_NOW = datetime.now(tz=UTC)


def _limits(
    allowed_stream_types: tuple[str, ...] = ("webcam", "rtsp"),
    *,
    shared_inference_enabled: bool = False,
) -> AccountLimits:
    return AccountLimits(
        max_cameras=5,
        max_concurrent_sessions=5,
        max_fps=60,
        max_width=1920,
        max_height=1080,
        max_bitrate_kbps=8000,
        allowed_codecs=frozenset({"h264"}),
        allowed_stream_types=frozenset(allowed_stream_types),
        monthly_minutes_quota=600,
        snapshot_storage_mb=1024,
        shared_inference_enabled=shared_inference_enabled,
    )


# ----------------------------------------------------------------------------
# Unit: check_stream_type_allowed
# ----------------------------------------------------------------------------


def test_stream_type_check_allows_listed_type() -> None:
    check_stream_type_allowed(_limits(), "webcam")  # no raise


def test_stream_type_check_is_case_insensitive() -> None:
    check_stream_type_allowed(_limits(("WebCam",)), "WEBCAM")  # no raise


def test_stream_type_check_default_allows_none() -> None:
    check_stream_type_allowed(_limits(("rtsp",)), None)  # no raise


# ----------------------------------------------------------------------------
# Unit: check_execution_mode_allowed
# ----------------------------------------------------------------------------


def test_execution_mode_per_session_always_allowed() -> None:
    # per_session needs no entitlement, regardless of the gate.
    check_execution_mode_allowed(_limits(shared_inference_enabled=False), "per_session")
    check_execution_mode_allowed(_limits(shared_inference_enabled=True), "per_session")


def test_execution_mode_shared_inference_requires_entitlement() -> None:
    with pytest.raises(ForbiddenError) as exc:
        check_execution_mode_allowed(_limits(shared_inference_enabled=False), "shared_inference")
    assert exc.value.code == "shared_inference_not_enabled"


def test_execution_mode_shared_inference_allowed_when_entitled() -> None:
    check_execution_mode_allowed(
        _limits(shared_inference_enabled=True), "shared_inference"
    )  # no raise


def test_stream_type_check_rejects_unlisted_type() -> None:
    with pytest.raises(LimitExceeded) as exc:
        check_stream_type_allowed(_limits(("rtsp",)), "webcam")
    assert exc.value.code == "limit_exceeded.stream_type_not_allowed"
    assert exc.value.details["requested"] == "webcam"
    assert exc.value.details["allowed"] == ["rtsp"]


# ----------------------------------------------------------------------------
# Endpoint: POST /v1/cameras/register
# ----------------------------------------------------------------------------


class FakeConn:
    def __init__(self, allowed_stream_types: list[str]) -> None:
        self.allowed_stream_types = allowed_stream_types
        self.insert_args: tuple[Any, ...] | None = None
        self.update_args: tuple[Any, ...] | None = None
        self.update_sql: str = ""
        self.existing_camera_id: UUID | None = None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from agents" in query:
            return {"account_id": ACCOUNT_ID}
        if "from account_limits" in query:
            return {
                "max_cameras": 5,
                "max_concurrent_sessions": 5,
                "max_fps": 60,
                "max_width": 1920,
                "max_height": 1080,
                "max_bitrate_kbps": 8000,
                "allowed_codecs": ["h264"],
                "allowed_stream_types": self.allowed_stream_types,
                "monthly_minutes_quota": 600,
                "snapshot_storage_mb": 1024,
                "session_ttl_seconds": None,
                "shared_inference_enabled": False,
            }
        if "select id from cameras" in query:
            if self.existing_camera_id is not None:
                return {"id": self.existing_camera_id}
            return None
        if "insert into cameras" in query:
            self.insert_args = args
            return self._camera_row(source_type=args[5])
        if "update cameras" in query:
            self.update_sql = query
            self.update_args = args
            return self._camera_row(source_type=args[4])
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "count(*)" in query:
            return 0
        return None

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"

    def _camera_row(self, *, source_type: str | None) -> dict[str, Any]:
        return {
            "id": uuid4(),
            "account_id": ACCOUNT_ID,
            "agent_id": AGENT_ID,
            "name": "cam",
            "external_id": "webcam-0",
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


def _agent_client(conn: FakeConn) -> TestClient:
    app = create_app()
    auth = AgentAuth(account_id=ACCOUNT_ID, token_id=uuid4(), scopes=("agent",))
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_agent] = lambda: auth
    return TestClient(app)


def _register_body(source_type: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "agent_id": str(AGENT_ID),
        "name": "cam",
        "external_id": "webcam-0",
        "device_path": "webcam:0",
        "default_width": 1280,
        "default_height": 720,
        "default_fps": 30,
        "default_codec": "h264",
        "default_bitrate_kbps": 3000,
    }
    if source_type is not None:
        body["source_type"] = source_type
    return body


def test_register_camera_with_allowed_source_type_succeeds() -> None:
    conn = FakeConn(allowed_stream_types=["webcam", "rtsp"])
    r = _agent_client(conn).post("/v1/cameras/register", json=_register_body("webcam"))
    assert r.status_code == 200
    assert r.json()["source_type"] == "webcam"


def test_register_camera_with_disallowed_source_type_is_rejected() -> None:
    conn = FakeConn(allowed_stream_types=["rtsp"])
    r = _agent_client(conn).post("/v1/cameras/register", json=_register_body("webcam"))
    assert r.status_code == 403
    err = r.json()["error"]
    assert err["code"] == "limit_exceeded.stream_type_not_allowed"
    assert err["details"]["allowed"] == ["rtsp"]


def test_register_camera_without_source_type_is_default_allowed() -> None:
    conn = FakeConn(allowed_stream_types=["rtsp"])
    r = _agent_client(conn).post("/v1/cameras/register", json=_register_body(None))
    assert r.status_code == 200
    assert r.json()["source_type"] is None


def test_reregister_without_source_type_uses_coalesce() -> None:
    conn = FakeConn(allowed_stream_types=["webcam"])
    conn.existing_camera_id = uuid4()
    r = _agent_client(conn).post("/v1/cameras/register", json=_register_body(None))
    assert r.status_code == 200
    assert conn.update_args is not None
    # UPDATE binds: (id, account_id, name, device_path, source_type, ...)
    assert conn.update_args[4] is None  # None -> coalesce keeps the stored value
    assert "coalesce($5, source_type)" in conn.update_sql


# ----------------------------------------------------------------------------
# Endpoint: GET /v1/account/limits includes allowed_stream_types
# ----------------------------------------------------------------------------


class LimitsFakeConn:
    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        return {
            "account_id": ACCOUNT_ID,
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
            "shared_inference_enabled": False,
            "created_at": _NOW,
            "updated_at": _NOW,
        }


def test_account_limits_response_includes_allowed_stream_types() -> None:
    app = create_app()
    user = UserAuth(user_id=uuid4(), memberships={ACCOUNT_ID: "owner"}, aal="aal1", amr=())
    conn = LimitsFakeConn()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)  # type: ignore[arg-type]
    app.dependency_overrides[current_caller] = lambda: CallerAuth(agent=None, user=user)
    r = TestClient(app).get("/v1/account/limits")
    assert r.status_code == 200
    assert r.json()["allowed_stream_types"] == ["webcam", "rtsp"]
