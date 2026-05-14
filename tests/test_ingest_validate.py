from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.streamid import build_streamid, mint_stream_key


class FakeConn:
    def __init__(self, session_rows: dict[str, dict[str, Any]] | None = None) -> None:
        # Keyed by streamid (the column the new flow looks up by).
        self.session_rows = session_rows or {}

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions" in query and "where streamid" in query:
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


def _session_row(*, session_id: UUID, state: str, expires_at: datetime) -> dict[str, Any]:
    return {
        "id": session_id,
        "account_id": uuid4(),
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "state": state,
        "expires_at": expires_at,
        "session_token_jti": uuid4(),
        "streamid": "publish:sess_0123456789abcdef",
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _client(pool: FakePool) -> TestClient:
    # No `with` block on TestClient => skip lifespan (no real DB pool needed).
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: pool
    return TestClient(app)


def _webhook_headers() -> dict[str, str]:
    # Read the secret at call time. `conftest.py` uses `setdefault`, so CI envs
    # that set a real secret won't be overridden by the test default; we must
    # mirror whatever ``get_settings()`` resolved to.
    return {"X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()}


def test_validate_stream_accepts_a_fresh_approved_session() -> None:
    streamid = build_streamid(mint_stream_key())
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        state="approved",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({streamid: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": streamid},
        headers=_webhook_headers(),
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
        headers=_webhook_headers(),
    )
    assert r.status_code == 200
    assert r.json()["accept"] is False
    assert r.json()["reason_code"] == "streamid_malformed"


def test_validate_stream_rejects_unknown_streamid() -> None:
    # Well-formed streamid, but no session row matches — could be a stale
    # key from a long-since-ended session or a probe attempt.
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": build_streamid(mint_stream_key())},
        headers=_webhook_headers(),
    )
    body = r.json()
    assert body["accept"] is False
    assert body["reason_code"] == "session_not_found"


def test_validate_stream_rejects_wrong_state() -> None:
    streamid = build_streamid(mint_stream_key())
    row = _session_row(
        session_id=uuid4(),
        state="ended",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({streamid: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": streamid},
        headers=_webhook_headers(),
    )
    body = r.json()
    assert body["accept"] is False
    assert body["reason_code"] == "stream_state_invalid"


def test_validate_stream_rejects_expired_session() -> None:
    streamid = build_streamid(mint_stream_key())
    row = _session_row(
        session_id=uuid4(),
        state="approved",
        expires_at=datetime.now(tz=UTC) - timedelta(seconds=1),
    )
    pool = FakePool(FakeConn({streamid: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/validate-stream",
        json={"streamid": streamid},
        headers=_webhook_headers(),
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


# ----------------------------------------------------------------------------
# /v1/ingest/lookup-session — used by streaming-server lifecycle hooks to
# resolve a streamid to its session_id without coupling to validate-stream's
# state-gate semantics.
# ----------------------------------------------------------------------------


def test_lookup_session_returns_session_for_known_streamid() -> None:
    streamid = build_streamid(mint_stream_key())
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        # lookup must work regardless of state — including during streaming,
        # which is what runOnNotReady will see.
        state="streaming",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({streamid: row}))
    client = _client(pool)
    r = client.post(
        "/v1/ingest/lookup-session",
        json={"streamid": streamid},
        headers=_webhook_headers(),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["session_id"] == str(session_id)
    assert body["account_id"] == str(row["account_id"])
    assert body["agent_id"] == str(row["agent_id"])
    assert body["camera_id"] == str(row["camera_id"])
    assert body["state"] == "streaming"


def test_lookup_session_404_on_unknown_streamid() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(
        "/v1/ingest/lookup-session",
        json={"streamid": build_streamid(mint_stream_key())},
        headers=_webhook_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def test_lookup_session_requires_webhook_secret() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post("/v1/ingest/lookup-session", json={"streamid": "publish:x"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
