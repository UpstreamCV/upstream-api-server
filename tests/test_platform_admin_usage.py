"""Router tests for /v1/admin/usage* and /v1/admin/audit-log."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import PlatformAdminAuth, current_platform_admin

_ADMIN = PlatformAdminAuth(user_id=uuid4(), email="admin@example.com", aal="aal1", amr=())


class FakeConn:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.queries.append((query, args))
        return self.rows


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


def test_usage_defaults_to_30_days_and_day_bucket() -> None:
    conn = FakeConn()
    client = _client(conn)
    r = client.get("/v1/admin/usage")
    assert r.status_code == 200
    assert r.json() == {"buckets": []}
    _, args = conn.queries[0]
    assert args[0] == "day"
    start_at, end_at = args[1], args[2]
    assert (end_at - start_at).days == 30


def test_usage_returns_buckets() -> None:
    account = uuid4()
    rows = [
        {
            "bucket_start": datetime(2026, 6, 1, tzinfo=UTC),
            "account_id": account,
            "account_name": "Acme",
            "event_type": "stream_minutes",
            "unit": "minutes",
            "total_quantity": 120,
        }
    ]
    client = _client(FakeConn(rows))
    r = client.get("/v1/admin/usage?bucket=month")
    assert r.status_code == 200
    bucket = r.json()["buckets"][0]
    assert bucket["total_quantity"] == 120
    assert bucket["event_type"] == "stream_minutes"


def test_usage_rejects_bad_bucket() -> None:
    client = _client(FakeConn())
    r = client.get("/v1/admin/usage?bucket=hour")
    assert r.status_code == 422


def test_usage_rejects_inverted_range() -> None:
    client = _client(FakeConn())
    r = client.get(
        "/v1/admin/usage",
        params={"start": "2026-06-10T00:00:00Z", "end": "2026-06-01T00:00:00Z"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_range"


def test_usage_rejects_oversized_range() -> None:
    client = _client(FakeConn())
    r = client.get(
        "/v1/admin/usage",
        params={"start": "2024-01-01T00:00:00Z", "end": "2026-06-01T00:00:00Z"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "range_too_large"


def test_usage_summary_clamps_days() -> None:
    client = _client(FakeConn())
    assert client.get("/v1/admin/usage/summary?days=0").status_code == 422
    assert client.get("/v1/admin/usage/summary?days=400").status_code == 422
    r = client.get("/v1/admin/usage/summary?days=7")
    assert r.status_code == 200
    assert r.json() == {"days": 7, "rows": []}


def test_audit_log_list() -> None:
    entry = {
        "id": uuid4(),
        "admin_user_id": _ADMIN.user_id,
        "action": "account.plan.update",
        "target_type": "account",
        "target_id": uuid4(),
        "payload": '{"before": {"plan": "free"}, "after": {"plan": "pro"}}',
        "request_id": "req-1",
        "created_at": datetime.now(tz=UTC),
        "total": 1,
    }
    client = _client(FakeConn([entry]))
    r = client.get("/v1/admin/audit-log")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    # jsonb payload arriving as a string is decoded into an object.
    assert body["items"][0]["payload"]["after"]["plan"] == "pro"
