from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, current_agent


class FakeConn:
    """Stubs the four DB calls in the snapshots handler.

    Order, by query substring:
      1. "from cameras where id"            -> camera row or None
      2. "from snapshots"                   -> used_bytes (int)
      3. "from account_limits"              -> { snapshot_storage_mb } or None
      4. "insert into snapshots ... returning" -> echoed row for the response
    """

    def __init__(
        self,
        *,
        camera_row: dict[str, Any] | None,
        used_bytes: int,
        limit_row: dict[str, Any] | None,
    ) -> None:
        self.camera_row = camera_row
        self.used_bytes = used_bytes
        self.limit_row = limit_row
        self.insert_args: tuple[Any, ...] | None = None

    def transaction(self) -> FakeTx:
        return FakeTx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return self.camera_row
        if "from account_limits" in query:
            return self.limit_row
        if "insert into snapshots" in query:
            self.insert_args = args
            return {
                "id": args[0],
                "camera_id": args[3],
                "session_id": args[4],
                "storage_key": args[5],
                "bytes": args[8],
                "width": args[6],
                "height": args[7],
                "captured_at": args[9],
                "created_at": datetime.now(tz=UTC),
            }
        return None

    async def fetchval(self, query: str, *_args: Any) -> Any:
        if "from snapshots" in query:
            return self.used_bytes
        if "to_regclass" in query:
            return False  # usage_events table not present in this unit test
        return None

    async def execute(self, _query: str, *_args: Any) -> str:
        return "OK"


class FakeTx:
    async def __aenter__(self) -> FakeTx:
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


_ACCOUNT = uuid4()
_AGENT = uuid4()
_CAMERA = uuid4()


def _agent_auth() -> AgentAuth:
    return AgentAuth(account_id=_ACCOUNT, token_id=uuid4(), scopes=("agent",))


def _client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_agent] = _agent_auth
    return TestClient(app)


def _form(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "camera_id": str(_CAMERA),
        "agent_id": str(_AGENT),
        "captured_at": datetime.now(tz=UTC).isoformat(),
    }
    base.update(overrides)
    return base


_JPEG_BYTES = b"\xff\xd8\xff" + b"x" * 1024  # ~1KB stub; we don't decode it


def test_upload_happy_path() -> None:
    conn = FakeConn(
        camera_row={"id": _CAMERA, "account_id": _ACCOUNT, "agent_id": _AGENT},
        used_bytes=0,
        limit_row={"snapshot_storage_mb": 1024},
    )
    client = _client(conn)
    with patch(
        "upstream_api.routers.snapshots.upload_bytes", new=AsyncMock(return_value=None)
    ) as mock_upload:
        r = client.post(
            "/v1/snapshots",
            data=_form(),
            files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["camera_id"] == str(_CAMERA)
    assert body["bytes"] == len(_JPEG_BYTES)
    # Storage was called exactly once with our bytes.
    mock_upload.assert_awaited_once()
    assert mock_upload.await_args is not None
    kwargs = mock_upload.await_args.kwargs
    assert kwargs["data"] == _JPEG_BYTES
    assert kwargs["content_type"] == "image/jpeg"


def test_upload_rejects_wrong_content_type() -> None:
    conn = FakeConn(
        camera_row={"id": _CAMERA, "account_id": _ACCOUNT, "agent_id": _AGENT},
        used_bytes=0,
        limit_row={"snapshot_storage_mb": 1024},
    )
    client = _client(conn)
    r = client.post(
        "/v1/snapshots",
        data=_form(),
        files={"file": ("frame.png", io.BytesIO(_JPEG_BYTES), "image/png")},
    )
    assert r.status_code == 415
    assert r.json()["error"]["code"] == "invalid_content_type"


def test_upload_rejects_oversized_file() -> None:
    conn = FakeConn(
        camera_row={"id": _CAMERA, "account_id": _ACCOUNT, "agent_id": _AGENT},
        used_bytes=0,
        limit_row={"snapshot_storage_mb": 1024},
    )
    client = _client(conn)
    # Default cap is 5 MB; send 6 MB.
    big = b"\xff\xd8\xff" + b"x" * (6 * 1024 * 1024)
    r = client.post(
        "/v1/snapshots",
        data=_form(),
        files={"file": ("frame.jpg", io.BytesIO(big), "image/jpeg")},
    )
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "snapshot_too_large"


def test_upload_rejects_quota_exceeded() -> None:
    # used_bytes alone already exceeds quota_bytes; even an empty new upload
    # would fail. We use 1 MB-worth of body for a realistic incoming size.
    conn = FakeConn(
        camera_row={"id": _CAMERA, "account_id": _ACCOUNT, "agent_id": _AGENT},
        used_bytes=2 * 1024 * 1024,  # 2 MB already used
        limit_row={"snapshot_storage_mb": 1},  # only 1 MB quota
    )
    client = _client(conn)
    with patch("upstream_api.routers.snapshots.upload_bytes", new=AsyncMock(return_value=None)):
        r = client.post(
            "/v1/snapshots",
            data=_form(),
            files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        )
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "snapshot_quota_exceeded"


def test_upload_rejects_foreign_camera() -> None:
    # Camera exists but belongs to a different agent under the same account.
    conn = FakeConn(
        camera_row={"id": _CAMERA, "account_id": _ACCOUNT, "agent_id": uuid4()},
        used_bytes=0,
        limit_row={"snapshot_storage_mb": 1024},
    )
    client = _client(conn)
    r = client.post(
        "/v1/snapshots",
        data=_form(),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "agent_account_mismatch"


def test_upload_rejects_missing_account_limits() -> None:
    conn = FakeConn(
        camera_row={"id": _CAMERA, "account_id": _ACCOUNT, "agent_id": _AGENT},
        used_bytes=0,
        limit_row=None,  # no account_limits row
    )
    client = _client(conn)
    r = client.post(
        "/v1/snapshots",
        data=_form(),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "account_limits_missing"


def test_upload_404_when_camera_missing() -> None:
    conn = FakeConn(
        camera_row=None,
        used_bytes=0,
        limit_row={"snapshot_storage_mb": 1024},
    )
    client = _client(conn)
    r = client.post(
        "/v1/snapshots",
        data=_form(camera_id=str(uuid4())),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
