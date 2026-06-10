"""Router tests for /v1/admin/streams* — cross-account list, summary, detail."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import PlatformAdminAuth, current_platform_admin

_ADMIN = PlatformAdminAuth(user_id=uuid4(), email="admin@example.com", aal="aal1", amr=())


def _session_row(**overrides: Any) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    row: dict[str, Any] = {
        "id": uuid4(),
        "account_id": uuid4(),
        "account_name": "Acme",
        "account_slug": "acme",
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "camera_name": "dock-cam",
        "state": "streaming",
        "created_at": now,
        "started_at": now,
        "ended_at": None,
        "expires_at": now + timedelta(hours=4),
        "end_reason": None,
        "failure_code": None,
        "hb_status": "streaming",
        "hb_fps": 29.5,
        "hb_bitrate_kbps": 2400,
        "hb_reported_at": now,
        "latest_validation_passed": True,
        "processor_status": "processing",
        "total": 1,
    }
    row.update(overrides)
    return row


def _detail_extra() -> dict[str, Any]:
    return {
        "req_width": 1280,
        "req_height": 720,
        "req_fps": 30,
        "req_codec": "h264",
        "req_bitrate_kbps": 2500,
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 2500,
        "validated_width": 1280,
        "validated_height": 720,
        "validated_fps": 30,
        "validated_codec": "h264",
        "validated_bitrate_kbps": None,
    }


class FakeConn:
    def __init__(
        self,
        *,
        list_rows: list[dict[str, Any]] | None = None,
        detail_row: dict[str, Any] | None = None,
    ) -> None:
        self.list_rows = list_rows or []
        self.detail_row = detail_row
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.queries.append((query, args))
        if "from stream_sessions s" in query:
            return self.list_rows
        if "group by state" in query:
            return [{"state": "streaming", "count": 2}, {"state": "approved", "count": 1}]
        # heartbeats / validations / findings / processor_statuses sub-lists
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.queries.append((query, args))
        if "from stream_sessions s" in query:
            return self.detail_row
        if "started_24h" in query:
            return {
                "started_24h": 5,
                "validation_failures_24h": 1,
                "accounts_streaming": 2,
                "open_reliability_findings": 3,
            }
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


def _client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_platform_admin] = lambda: _ADMIN
    return TestClient(app)


def test_list_streams_includes_latest_heartbeat() -> None:
    client = _client(FakeConn(list_rows=[_session_row()]))
    r = client.get("/v1/admin/streams")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    item = body["items"][0]
    assert item["account_slug"] == "acme"
    assert item["latest_heartbeat"]["fps"] == 29.5
    assert item["processor_status"] == "processing"


def test_list_streams_without_heartbeat() -> None:
    row = _session_row(hb_status=None, hb_fps=None, hb_bitrate_kbps=None, hb_reported_at=None)
    client = _client(FakeConn(list_rows=[row]))
    r = client.get("/v1/admin/streams")
    assert r.status_code == 200
    assert r.json()["items"][0]["latest_heartbeat"] is None


def test_list_streams_rejects_unknown_state() -> None:
    client = _client(FakeConn())
    r = client.get("/v1/admin/streams?state=bogus")
    assert r.status_code == 422


def test_list_streams_passes_filters() -> None:
    conn = FakeConn(list_rows=[])
    client = _client(conn)
    account = uuid4()
    r = client.get(f"/v1/admin/streams?state=streaming&account_id={account}&limit=10")
    assert r.status_code == 200
    assert r.json() == {"items": [], "total": 0}
    _query, args = conn.queries[0]
    assert args[0] == "streaming"
    assert args[1] == account
    assert args[2] == 10


def test_streams_summary() -> None:
    client = _client(FakeConn())
    r = client.get("/v1/admin/streams/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["by_state"] == {"streaming": 2, "approved": 1}
    assert body["started_24h"] == 5
    assert body["validation_failures_24h"] == 1
    assert body["accounts_streaming"] == 2
    assert body["open_reliability_findings"] == 3


def test_stream_detail() -> None:
    row = {**_session_row(), **_detail_extra()}
    client = _client(FakeConn(detail_row=row))
    r = client.get(f"/v1/admin/streams/{row['id']}")
    assert r.status_code == 200
    body = r.json()
    assert body["session"]["state"] == "streaming"
    assert body["requested"]["fps"] == 30
    assert body["approved"]["codec"] == "h264"
    assert body["heartbeats"] == []


def test_stream_detail_404() -> None:
    client = _client(FakeConn(detail_row=None))
    r = client.get(f"/v1/admin/streams/{uuid4()}")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
