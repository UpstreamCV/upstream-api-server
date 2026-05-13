from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.session_jwt import SessionLimits, mint_session_token
from upstream_api.security.streamid import build_streamid


class FakeConn:
    def __init__(self, session_rows: dict[UUID, dict[str, Any]] | None = None) -> None:
        self.session_rows = session_rows or {}

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions" in query:
            return self.session_rows.get(args[0])
        return None


class AcquireCtx:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    """asyncpg-pool stand-in. `acquire()` returns an async-context FakeConn."""

    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _session_row(
    *, session_id: UUID, jti: UUID, state: str, expires_at: datetime
) -> dict[str, Any]:
    return {
        "id": session_id,
        "account_id": uuid4(),
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "state": state,
        "expires_at": expires_at,
        "session_token_jti": jti,
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _mint(jti: UUID | None = None) -> tuple[str, UUID]:
    settings = get_settings()
    session_id = uuid4()
    token, _claims = mint_session_token(
        secret=settings.session_jwt_secret.get_secret_value(),
        account_id=uuid4(),
        agent_id=uuid4(),
        camera_id=uuid4(),
        session_id=session_id,
        limits=SessionLimits(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=3000),
        ttl_seconds=settings.session_token_ttl_seconds,
        jti=jti,
    )
    return token, session_id


def _client(pool: FakePool) -> TestClient:
    # No `with` block on TestClient => skip lifespan (no real DB pool needed).
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: pool
    return TestClient(app)


_WEBHOOK = {"X-Upstream-Webhook-Secret": "test-mediamtx"}


def test_validate_stream_accepts_a_fresh_approved_session() -> None:
    jti = uuid4()
    token, session_id = _mint(jti=jti)
    row = _session_row(
        session_id=session_id,
        jti=jti,
        state="approved",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({session_id: row}))
    streamid = build_streamid(token)
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": streamid},
        headers=_WEBHOOK,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["accept"] is True
    assert body["session_id"] == str(session_id)
    assert body["expected"]["codec"] == "h264"
    assert body["expected"]["latency_mode"] == "balanced"


def test_validate_stream_rejects_malformed_streamid() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": "subscribe:abc123"},
        headers=_WEBHOOK,
    )
    assert r.status_code == 200
    assert r.json()["accept"] is False
    assert r.json()["reason_code"] == "streamid_malformed"


def test_validate_stream_rejects_jti_mismatch() -> None:
    # Mint with one jti but the DB row carries a different one — simulates a
    # leaked old JWT being replayed after the session was rotated.
    token, session_id = _mint(jti=uuid4())
    row = _session_row(
        session_id=session_id,
        jti=uuid4(),  # different
        state="approved",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({session_id: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": build_streamid(token)},
        headers=_WEBHOOK,
    )
    body = r.json()
    assert body["accept"] is False
    assert body["reason_code"] == "token_jti_mismatch"


def test_validate_stream_rejects_wrong_state() -> None:
    jti = uuid4()
    token, session_id = _mint(jti=jti)
    row = _session_row(
        session_id=session_id,
        jti=jti,
        state="ended",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({session_id: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": build_streamid(token)},
        headers=_WEBHOOK,
    )
    body = r.json()
    assert body["accept"] is False
    assert body["reason_code"] == "stream_state_invalid"


def test_validate_stream_rejects_expired_session() -> None:
    jti = uuid4()
    token, session_id = _mint(jti=jti)
    row = _session_row(
        session_id=session_id,
        jti=jti,
        state="approved",
        expires_at=datetime.now(tz=UTC) - timedelta(seconds=1),
    )
    pool = FakePool(FakeConn({session_id: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": build_streamid(token)},
        headers=_WEBHOOK,
    )
    body = r.json()
    assert body["accept"] is False
    assert body["reason_code"] == "session_expired"


def test_validate_stream_requires_webhook_secret() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post("/v1/ingest/validate-stream", json={"streamid": "publish:x"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
