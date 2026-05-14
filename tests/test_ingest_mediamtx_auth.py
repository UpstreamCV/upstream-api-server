"""Tests for the M4.1 MediaMTX authHTTP deny gate.

The endpoint takes a path-segment secret because MediaMTX 1.9.3 cannot
send custom headers. MediaMTX inspects only the response status code
(20x = accept, otherwise reject), so the handler returns empty 200 / 401
bodies — these tests assert on status only.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.streamid import build_streamid, mint_stream_key

# ---------------------------------------------------------------------------
# Fixtures — duplicated from test_ingest_validate.py rather than lifted into
# conftest.py to keep this PR small. Lift later if a third test file needs them.
# ---------------------------------------------------------------------------


class FakeConn:
    def __init__(self, session_rows: dict[str, dict[str, Any]] | None = None) -> None:
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
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _client(pool: FakePool) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: pool
    return TestClient(app)


def _secret() -> str:
    return get_settings().mediamtx_webhook_secret.get_secret_value()


def _url(secret: str | None = None) -> str:
    return f"/v1/ingest/mediamtx-auth/{secret if secret is not None else _secret()}"


def _payload(
    *,
    action: str = "publish",
    protocol: str = "srt",
    path: str = "",
    query: str = "",
    ip: str = "203.0.113.42",
) -> dict[str, str]:
    return {
        "user": "",
        "password": "",
        "ip": ip,
        "action": action,
        "path": path,
        "protocol": protocol,
        "id": "deadbeef-dead-beef-dead-beefdeadbeef",
        "query": query,
    }


def _approved_pool(stream_key: str) -> FakePool:
    streamid = build_streamid(stream_key)
    row = _session_row(
        session_id=uuid4(),
        state="approved",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    return FakePool(FakeConn({streamid: row}))


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------


def test_correct_secret_and_valid_session_returns_200() -> None:
    key = mint_stream_key()
    pool = _approved_pool(key)
    client = _client(pool)
    r = client.post(_url(), json=_payload(path=key))
    assert r.status_code == 200
    # MediaMTX ignores the body; we return empty bytes.
    assert r.content == b""


def test_wrong_secret_returns_401() -> None:
    key = mint_stream_key()
    pool = _approved_pool(key)
    client = _client(pool)
    r = client.post(_url("not-the-right-secret"), json=_payload(path=key))
    assert r.status_code == 401
    assert r.content == b""


def test_action_not_publish_returns_401() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(_url(), json=_payload(action="read", path=mint_stream_key()))
    assert r.status_code == 401


def test_protocol_not_srt_returns_401() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(_url(), json=_payload(protocol="rtmp", path=mint_stream_key()))
    assert r.status_code == 401


def test_missing_path_and_query_returns_401() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(_url(), json=_payload(path="", query=""))
    assert r.status_code == 401


def test_path_only_valid_returns_200() -> None:
    # Same as the first happy-path test but asserts explicitly that
    # the `path` field alone (no `query`) suffices.
    key = mint_stream_key()
    pool = _approved_pool(key)
    client = _client(pool)
    r = client.post(_url(), json=_payload(path=key, query=""))
    assert r.status_code == 200


def test_query_streamid_publish_form_returns_200() -> None:
    key = mint_stream_key()
    pool = _approved_pool(key)
    client = _client(pool)
    r = client.post(
        _url(),
        json=_payload(path="", query=f"streamid=publish:{key}&latency=200"),
    )
    assert r.status_code == 200


def test_query_bare_publish_form_returns_200() -> None:
    key = mint_stream_key()
    pool = _approved_pool(key)
    client = _client(pool)
    r = client.post(_url(), json=_payload(path="", query=f"publish:{key}"))
    assert r.status_code == 200


def test_malformed_stream_key_returns_401() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(_url(), json=_payload(path="not-a-sess-key"))
    assert r.status_code == 401


def test_unknown_session_returns_401() -> None:
    # Well-formed key but no session row in the fake pool.
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(_url(), json=_payload(path=mint_stream_key()))
    assert r.status_code == 401


def test_expired_session_returns_401() -> None:
    key = mint_stream_key()
    streamid = build_streamid(key)
    row = _session_row(
        session_id=uuid4(),
        state="approved",
        expires_at=datetime.now(tz=UTC) - timedelta(seconds=1),
    )
    pool = FakePool(FakeConn({streamid: row}))
    client = _client(pool)
    r = client.post(_url(), json=_payload(path=key))
    assert r.status_code == 401


def test_wrong_state_session_returns_401() -> None:
    # `streaming` rather than `approved` — the session already moved past
    # the admit gate, so a re-publish should be denied (re-mint required).
    key = mint_stream_key()
    streamid = build_streamid(key)
    row = _session_row(
        session_id=uuid4(),
        state="streaming",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({streamid: row}))
    client = _client(pool)
    r = client.post(_url(), json=_payload(path=key))
    assert r.status_code == 401


def test_no_webhook_secret_header_required() -> None:
    # MediaMTX 1.9.3 cannot send custom headers — the endpoint MUST NOT
    # require X-Upstream-Webhook-Secret in addition to the path segment.
    # Explicitly assert by sending the correct path secret with no header.
    key = mint_stream_key()
    pool = _approved_pool(key)
    client = _client(pool)
    r = client.post(_url(), json=_payload(path=key))  # no headers
    assert r.status_code == 200
    # And the validate-stream endpoint, by contrast, still requires the header.
    r2 = client.post("/v1/ingest/validate-stream", json={"streamid": build_streamid(key)})
    assert r2.status_code == 401
