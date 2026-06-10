"""Route tests for POST /v1/ingest/snapshots (webhook-secret authed).

Mirrors the FakeConn style of tests/test_snapshots.py, plus the
webhook-header pattern of the other ingest tests. `current_ingest` is NOT
overridden — the real dependency runs and is satisfied with the header
`X-Upstream-Webhook-Secret` (conftest sets UPSTREAM_MEDIAMTX_WEBHOOK_SECRET
and clears the get_settings cache per test).

Also includes two cheap regression tests for GET
/v1/cameras/{camera_id}/latest-frame since the refactor touches snapshots.py.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services import storage

# --- shared asyncpg fakes (same shape as tests/test_snapshots.py) -----------


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


class FakeConn:
    """Stubs the DB calls in ingest_upload_snapshot.

    fetchrow, by query substring:
      1. "from stream_sessions"            -> session row or None (load_session_state)
      2. "from account_limits"             -> { snapshot_storage_mb } or None
      3. "insert into snapshots ... returning" -> echoed row for the response
    fetchval:
      - "to_regclass"                      -> False (usage_events absent; record_event no-ops)
      - "from snapshots"                   -> used_bytes (int)
    """

    def __init__(
        self,
        *,
        session_row: dict[str, Any] | None,
        used_bytes: int = 0,
        limit_row: dict[str, Any] | None = None,
    ) -> None:
        self.session_row = session_row
        self.used_bytes = used_bytes
        self.limit_row: dict[str, Any] | None = (
            {"snapshot_storage_mb": 1024} if limit_row is None else limit_row
        )
        self.insert_args: tuple[Any, ...] | None = None

    def transaction(self) -> FakeTx:
        return FakeTx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions" in query:
            return self.session_row
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
        if "to_regclass" in query:
            return False  # usage_events table absent in this unit test
        if "from snapshots" in query:
            return self.used_bytes
        return None

    async def execute(self, _query: str, *_args: Any) -> str:
        return "OK"


def _session_row(*, state: str = "streaming") -> dict[str, Any]:
    """Shape returned by load_session_state (matches _SESSION_STATE_COLUMNS)."""
    return {
        "id": uuid4(),
        "account_id": uuid4(),
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "state": state,
        "expires_at": datetime.now(tz=UTC) + timedelta(hours=1),
        "session_token_jti": uuid4(),
        "streamid": "publish:sess_0123456789abcdef",
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _client(conn: Any, *, raise_server_exceptions: bool = True) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app, raise_server_exceptions=raise_server_exceptions)


def _webhook_headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()}


def _form(session_id: UUID, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "session_id": str(session_id),
        "captured_at": datetime.now(tz=UTC).isoformat(),
    }
    base.update(overrides)
    return base


_JPEG_BYTES = b"\xff\xd8\xff" + b"x" * 1024  # ~1KB stub; never decoded


# --- happy path -------------------------------------------------------------


def test_ingest_happy_path() -> None:
    session = _session_row(state="streaming")
    conn = FakeConn(session_row=session, used_bytes=0)
    client = _client(conn)
    with patch(
        "upstream_api.routers.snapshots.upload_bytes", new=AsyncMock(return_value=None)
    ) as mock_upload:
        r = client.post(
            "/v1/ingest/snapshots",
            data=_form(session["id"]),
            files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
            headers=_webhook_headers(),
        )
    assert r.status_code == 201, r.text
    body = r.json()
    # Identity comes from the session row, not the request.
    assert body["camera_id"] == str(session["camera_id"])
    assert body["session_id"] == str(session["id"])
    assert body["bytes"] == len(_JPEG_BYTES)
    assert body["storage_key"].startswith(f"{session['account_id']}/{session['camera_id']}/")
    mock_upload.assert_awaited_once()
    assert mock_upload.await_args is not None
    kwargs = mock_upload.await_args.kwargs
    assert kwargs["data"] == _JPEG_BYTES
    assert kwargs["content_type"] == "image/jpeg"
    # Insert used session-derived account/agent/camera.
    assert conn.insert_args is not None
    assert conn.insert_args[1] == session["account_id"]
    assert conn.insert_args[2] == session["agent_id"]
    assert conn.insert_args[3] == session["camera_id"]


# --- auth -------------------------------------------------------------------


def test_ingest_missing_secret_401() -> None:
    session = _session_row()
    client = _client(FakeConn(session_row=session))
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_ingest_wrong_secret_401() -> None:
    session = _session_row()
    client = _client(FakeConn(session_row=session))
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        headers={"X-Upstream-Webhook-Secret": "definitely-not-the-secret"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


# --- session state ----------------------------------------------------------


def test_ingest_unknown_session_404() -> None:
    conn = FakeConn(session_row=None)
    client = _client(conn)
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(uuid4()),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        headers=_webhook_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def _assert_state_rejected(state: str) -> None:
    session = _session_row(state=state)
    conn = FakeConn(session_row=session)
    client = _client(conn)
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        headers=_webhook_headers(),
    )
    assert r.status_code == 409, (state, r.text)
    err = r.json()["error"]
    assert err["code"] == "stream_state_invalid"
    assert err["details"]["current_state"] == state
    # Nothing persisted for a non-streaming session.
    assert conn.insert_args is None


def test_ingest_session_approved_409() -> None:
    _assert_state_rejected("approved")


def test_ingest_session_ended_409() -> None:
    _assert_state_rejected("ended")


def test_ingest_session_failed_409() -> None:
    _assert_state_rejected("failed")


def test_ingest_session_expired_409() -> None:
    _assert_state_rejected("expired")


# --- file validation --------------------------------------------------------


def test_ingest_rejects_non_jpeg_415() -> None:
    session = _session_row()
    client = _client(FakeConn(session_row=session))
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.png", io.BytesIO(_JPEG_BYTES), "image/png")},
        headers=_webhook_headers(),
    )
    assert r.status_code == 415
    assert r.json()["error"]["code"] == "invalid_content_type"


def test_ingest_rejects_empty_400() -> None:
    session = _session_row()
    client = _client(FakeConn(session_row=session))
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.jpg", io.BytesIO(b""), "image/jpeg")},
        headers=_webhook_headers(),
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "empty_upload"


def test_ingest_rejects_oversize_413() -> None:
    session = _session_row()
    client = _client(FakeConn(session_row=session))
    big = b"\xff\xd8\xff" + b"x" * (6 * 1024 * 1024)  # > 5 MB default cap
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.jpg", io.BytesIO(big), "image/jpeg")},
        headers=_webhook_headers(),
    )
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "snapshot_too_large"


# --- quota ------------------------------------------------------------------


def test_ingest_quota_exceeded_429() -> None:
    session = _session_row()
    conn = FakeConn(
        session_row=session,
        used_bytes=2 * 1024 * 1024,  # 2 MB already used
        limit_row={"snapshot_storage_mb": 1},  # 1 MB quota
    )
    client = _client(conn)
    with patch("upstream_api.routers.snapshots.upload_bytes", new=AsyncMock(return_value=None)):
        r = client.post(
            "/v1/ingest/snapshots",
            data=_form(session["id"]),
            files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
            headers=_webhook_headers(),
        )
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "snapshot_quota_exceeded"
    assert conn.insert_args is None


def test_ingest_account_limits_missing_404() -> None:
    session = _session_row()
    conn = FakeConn(session_row=session, limit_row={})  # sentinel; overridden below
    conn.limit_row = None  # no account_limits row
    client = _client(conn)
    r = client.post(
        "/v1/ingest/snapshots",
        data=_form(session["id"]),
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        headers=_webhook_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "account_limits_missing"


# --- storage failure --------------------------------------------------------


def test_ingest_storage_failure_no_insert() -> None:
    session = _session_row()
    conn = FakeConn(session_row=session)
    client = _client(conn, raise_server_exceptions=False)
    with patch(
        "upstream_api.routers.snapshots.upload_bytes",
        new=AsyncMock(side_effect=RuntimeError("storage down")),
    ):
        r = client.post(
            "/v1/ingest/snapshots",
            data=_form(session["id"]),
            files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
            headers=_webhook_headers(),
        )
    # Upload precedes the insert; a storage failure leaves no metadata row.
    assert r.status_code >= 500
    assert conn.insert_args is None


# --- tenant-spoof resistance ------------------------------------------------


def test_ingest_ignores_spoofed_identity_fields() -> None:
    session = _session_row(state="streaming")
    conn = FakeConn(session_row=session)
    client = _client(conn)
    spoof_camera = uuid4()
    spoof_account = uuid4()
    with patch("upstream_api.routers.snapshots.upload_bytes", new=AsyncMock(return_value=None)):
        r = client.post(
            "/v1/ingest/snapshots",
            data=_form(
                session["id"],
                camera_id=str(spoof_camera),
                agent_id=str(uuid4()),
                account_id=str(spoof_account),
                storage_key="evil/path/owned.jpg",
            ),
            files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
            headers=_webhook_headers(),
        )
    assert r.status_code == 201, r.text
    body = r.json()
    # Spoofed fields are ignored: identity + storage_key come from the session.
    assert body["camera_id"] == str(session["camera_id"])
    assert body["camera_id"] != str(spoof_camera)
    assert conn.insert_args is not None
    assert conn.insert_args[3] == session["camera_id"]
    assert conn.insert_args[5].startswith(f"{session['account_id']}/{session['camera_id']}/")
    assert "evil/path" not in conn.insert_args[5]


# --- regression: GET /v1/cameras/{camera_id}/latest-frame -------------------


_LF_ACCOUNT = uuid4()
_LF_CAMERA = uuid4()


class LatestFrameConn:
    """Stubs the two reads in get_latest_frame."""

    def __init__(self, *, snapshot_row: dict[str, Any] | None) -> None:
        self.snapshot_row = snapshot_row

    async def fetchrow(self, query: str, *_args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return {"account_id": _LF_ACCOUNT}
        if "order by captured_at desc" in query:
            return self.snapshot_row
        return None


def _lf_caller() -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=_LF_ACCOUNT, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )


def _lf_client(conn: Any) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = _lf_caller
    return TestClient(app)


def test_latest_frame_happy() -> None:
    snap = {
        "storage_key": f"{_LF_ACCOUNT}/{_LF_CAMERA}/20260518/{uuid4()}.jpg",
        "bytes": 4321,
        "width": 1280,
        "height": 720,
        "captured_at": datetime.now(tz=UTC),
        "created_at": datetime.now(tz=UTC),
    }
    client = _lf_client(LatestFrameConn(snapshot_row=snap))
    with patch(
        "upstream_api.routers.snapshots.create_signed_url",
        new=AsyncMock(return_value="https://signed.example/frame.jpg"),
    ):
        r = client.get(f"/v1/cameras/{_LF_CAMERA}/latest-frame")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["signed_url"] == "https://signed.example/frame.jpg"
    assert body["storage_key"] == snap["storage_key"]
    assert body["bytes"] == 4321


def test_latest_frame_not_found() -> None:
    client = _lf_client(LatestFrameConn(snapshot_row=None))
    r = client.get(f"/v1/cameras/{_LF_CAMERA}/latest-frame")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "snapshot_not_found"


# --- regression: real storage client path (unhashable-Settings bug) ---------


class _FakeSupabaseBucket:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes]] = []

    def upload(self, key: str, data: bytes, _opts: dict[str, str]) -> dict[str, Any]:
        self.uploads.append((key, data))
        return {}


class _FakeSupabaseClient:
    def __init__(self) -> None:
        bucket = _FakeSupabaseBucket()
        self.storage = type("S", (), {"from_": lambda _self, _n: bucket})()
        self._bucket = bucket


def test_ingest_happy_path_through_real_storage_client() -> None:
    """End-to-end: patch create_client (NOT upload_bytes) so the real
    _client(settings) path runs. Reproduces the deployed 500
    (TypeError: unhashable type: 'Settings') and proves it is fixed.
    """
    storage._client.cache_clear()
    session = _session_row(state="streaming")
    conn = FakeConn(session_row=session)
    client = _client(conn)
    fake = _FakeSupabaseClient()
    try:
        with patch("upstream_api.services.storage.create_client", return_value=fake):
            r = client.post(
                "/v1/ingest/snapshots",
                data=_form(session["id"]),
                files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
                headers=_webhook_headers(),
            )
    finally:
        storage._client.cache_clear()
    assert r.status_code == 201, r.text
    assert len(fake._bucket.uploads) == 1
    assert fake._bucket.uploads[0][1] == _JPEG_BYTES
