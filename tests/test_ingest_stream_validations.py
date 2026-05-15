"""Tests for the M5.1 observe/report endpoints.

GET  /v1/ingest/expected/{session_id} — state-agnostic expected lookup.
POST /v1/ingest/stream-validations    — report-only validation row insert
                                        that MUST NOT mutate session state
                                        or deprovision the MediaMTX path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool


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


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Stubs ``load_session_state`` lookups + records inserts/updates.

    The report-only path must INSERT into stream_validations and MUST
    NOT issue any UPDATE against stream_sessions. Tests assert on the
    bucket counts directly.
    """

    def __init__(self, sessions_by_id: dict[UUID, dict[str, Any]] | None = None) -> None:
        self.sessions_by_id = sessions_by_id or {}
        # Captured for state-immutability assertions.
        self.executed_updates: list[str] = []
        self.executed_inserts: list[tuple[str, tuple[Any, ...]]] = []
        self.executed_camera_updates: list[tuple[str, tuple[Any, ...]]] = []
        # Capture deprovision calls if anything ever routes through MediaMTX.
        # (Nothing should — present here purely to make a regression loud.)
        self.deprovision_calls: list[str] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions where id" in query:
            return self.sessions_by_id.get(args[0])
        if "from stream_sessions" in query and "where streamid" in query:
            return None
        if "update stream_sessions" in query:
            self.executed_updates.append(query)
            return None
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "select 1 from stream_sessions" in query:
            return 1 if args[0] in self.sessions_by_id else None
        if "insert into stream_validations" in query:
            self.executed_inserts.append((query, args))
            return uuid4()
        return None

    async def execute(self, query: str, *args: Any) -> str:
        if "update stream_sessions" in query:
            self.executed_updates.append(query)
        elif "update cameras" in query:
            self.executed_camera_updates.append((query, args))
        elif "insert into stream_validations" in query:
            self.executed_inserts.append((query, args))
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


def _client(pool: FakePool) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: pool
    return TestClient(app)


def _webhook_headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()}


# ---------------------------------------------------------------------------
# GET /v1/ingest/expected/{session_id}
# ---------------------------------------------------------------------------


def test_expected_returns_approved_config_for_approved_session() -> None:
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        state="approved",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({session_id: row}))
    client = _client(pool)
    r = client.get(f"/v1/ingest/expected/{session_id}", headers=_webhook_headers())
    assert r.status_code == 200
    body = r.json()
    assert body["session_id"] == str(session_id)
    assert body["account_id"] == str(row["account_id"])
    assert body["agent_id"] == str(row["agent_id"])
    assert body["camera_id"] == str(row["camera_id"])
    assert body["state"] == "approved"
    assert body["expected"] == {
        "codec": "h264",
        "width": 1280,
        "height": 720,
        "fps": 30,
        "bitrate_kbps": 3000,
        "latency_mode": "balanced",
    }


def test_expected_is_state_agnostic_streaming_returns_200() -> None:
    """M5.1-critical: hook reruns may hit /expected after the session is already streaming.

    Unlike validate-stream, this endpoint must not 4xx on non-approved
    states — the comparator just needs the approved limits.
    """
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        state="streaming",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    pool = FakePool(FakeConn({session_id: row}))
    client = _client(pool)
    r = client.get(f"/v1/ingest/expected/{session_id}", headers=_webhook_headers())
    assert r.status_code == 200
    assert r.json()["state"] == "streaming"
    assert r.json()["expected"]["codec"] == "h264"


def test_expected_404_on_unknown_session() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.get(f"/v1/ingest/expected/{uuid4()}", headers=_webhook_headers())
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def test_expected_requires_webhook_secret() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.get(f"/v1/ingest/expected/{uuid4()}")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_expected_rejects_wrong_webhook_secret() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.get(
        f"/v1/ingest/expected/{uuid4()}",
        headers={"X-Upstream-Webhook-Secret": "definitely-wrong"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


# ---------------------------------------------------------------------------
# POST /v1/ingest/stream-validations
# ---------------------------------------------------------------------------


def _report_body(*, session_id: UUID, passed: bool, mismatch_fields: list[str]) -> dict[str, Any]:
    return {
        "session_id": str(session_id),
        "passed": passed,
        "observed": {
            "codec": "h264",
            "width": 1920 if "width" in mismatch_fields else 1280,
            "height": 1080 if "height" in mismatch_fields else 720,
            "fps": 30,
            "bitrate_kbps": 0,
        },
        "expected": {
            "codec": "h264",
            "width": 1280,
            "height": 720,
            "fps": 30,
            "bitrate_kbps": 3000,
            "latency_mode": "balanced",
        },
        "mismatch_fields": mismatch_fields,
    }


def test_stream_validations_inserts_passed_row_and_leaves_state_intact() -> None:
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        state="streaming",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    conn = FakeConn({session_id: row})
    client = _client(FakePool(conn))
    r = client.post(
        "/v1/ingest/stream-validations",
        json=_report_body(session_id=session_id, passed=True, mismatch_fields=[]),
        headers=_webhook_headers(),
    )
    assert r.status_code == 200
    assert r.json()["session_id"] == str(session_id)
    assert r.json()["state"] == "streaming"
    # Exactly one validation row inserted.
    assert len(conn.executed_inserts) == 1
    # State immutability — no UPDATE against stream_sessions, no camera mirror.
    assert conn.executed_updates == []
    assert conn.executed_camera_updates == []
    # Defensive: nothing deprovisioned.
    assert conn.deprovision_calls == []


def test_stream_validations_inserts_failed_row_without_state_transition() -> None:
    """passed=false must STILL not flip the session to failed in M5.1."""
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        state="streaming",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    conn = FakeConn({session_id: row})
    client = _client(FakePool(conn))
    r = client.post(
        "/v1/ingest/stream-validations",
        json=_report_body(session_id=session_id, passed=False, mismatch_fields=["width", "height"]),
        headers=_webhook_headers(),
    )
    assert r.status_code == 200
    # State remains 'streaming' in the response and untouched in the DB.
    assert r.json()["state"] == "streaming"
    assert len(conn.executed_inserts) == 1
    _insert_query, insert_args = conn.executed_inserts[0]
    # mismatch_fields is the 5th positional arg to the insert.
    assert insert_args[4] == ["width", "height"]
    # passed=false is the 2nd positional arg.
    assert insert_args[1] is False
    assert conn.executed_updates == []
    assert conn.executed_camera_updates == []


def test_stream_validations_requires_webhook_secret() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(
        "/v1/ingest/stream-validations",
        json=_report_body(session_id=uuid4(), passed=True, mismatch_fields=[]),
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_stream_validations_404_on_unknown_session() -> None:
    pool = FakePool(FakeConn())
    client = _client(pool)
    r = client.post(
        "/v1/ingest/stream-validations",
        json=_report_body(session_id=uuid4(), passed=True, mismatch_fields=[]),
        headers=_webhook_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
