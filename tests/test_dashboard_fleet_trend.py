"""Tests for GET /v1/dashboard/fleet-trend and the summary count fixes.

The trend is derived live from `heartbeats` (no rollup table, no sampler), so
these fake the connection and assert the handler's bucket -> response mapping,
the legend/latest-point coupling, and account scoping. The SQL itself is
exercised against a real database by `scripts/smoke_local.py`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import (
    AgentAuth,
    CallerAuth,
    UserAuth,
    current_caller,
)

_BASE = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


class FakeConn:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        scalar: Any = 0,
        row: dict[str, Any] | None = None,
    ) -> None:
        self.rows = rows or []
        self.scalar = scalar
        self.row = row
        self.fetch_args: tuple[Any, ...] = ()

    async def fetch(self, _query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_args = args
        return self.rows

    async def fetchval(self, _query: str, *_args: Any) -> Any:
        return self.scalar

    async def fetchrow(self, _query: str, *_args: Any) -> dict[str, Any] | None:
        return self.row


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


def _client(conn: FakeConn, caller: CallerAuth) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


def _user(account: UUID) -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(user_id=uuid4(), memberships={account: "owner"}, aal="aal1", amr=()),
    )


def _agent(account: UUID) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )


def _bucket(
    offset_hours: int, *, critical: int = 0, review: int = 0, healthy: int = 0
) -> dict[str, Any]:
    start = _BASE + timedelta(hours=offset_hours)
    return {
        "bucket_start": start,
        "bucket_end": start + timedelta(hours=1),
        "critical": critical,
        "review": review,
        "healthy": healthy,
        "reporting_cameras": critical + review + healthy,
    }


def test_returns_hourly_points_and_echoes_range() -> None:
    account = uuid4()
    conn = FakeConn(
        rows=[_bucket(0, healthy=3), _bucket(1, healthy=2, critical=1)],
        scalar=5,
    )
    client = _client(conn, _user(account))

    r = client.get("/v1/dashboard/fleet-trend")

    assert r.status_code == 200
    body = r.json()
    assert body["range"] == "24h"
    assert body["bucket_size"] == "1h"
    assert len(body["points"]) == 2
    assert body["points"][0]["healthy"] == 3
    assert body["points"][1]["critical"] == 1


def test_legend_summary_reads_the_latest_bucket() -> None:
    # The legend must agree with the right-hand end of the plotted line: the old
    # Home tile derived its legend from a separate current-state query, which is
    # how it drifted out of sync with the chart above it.
    account = uuid4()
    conn = FakeConn(
        rows=[_bucket(0, healthy=9), _bucket(1, healthy=2, review=1, critical=4)],
        scalar=7,
    )
    client = _client(conn, _user(account))

    summary = client.get("/v1/dashboard/fleet-trend").json()["summary"]

    assert summary["point_count"] == 2
    assert summary["latest_healthy"] == 2
    assert summary["latest_review"] == 1
    assert summary["latest_critical"] == 4
    assert summary["total_cameras"] == 7
    assert summary["latest_bucket_start"].startswith("2026-07-16T13:00")


def test_empty_history_yields_zeroed_summary_not_an_error() -> None:
    account = uuid4()
    conn = FakeConn(rows=[], scalar=0)
    client = _client(conn, _user(account))

    r = client.get("/v1/dashboard/fleet-trend")

    assert r.status_code == 200
    body = r.json()
    assert body["points"] == []
    assert body["summary"]["latest_bucket_start"] is None
    assert body["summary"]["latest_healthy"] == 0


def test_range_7d_requests_168_hours() -> None:
    account = uuid4()
    conn = FakeConn(rows=[], scalar=0)
    client = _client(conn, _user(account))

    r = client.get("/v1/dashboard/fleet-trend?range=7d")

    assert r.status_code == 200
    assert r.json()["range"] == "7d"
    # (account_id, hours) — the window width is the handler's job, not the SQL's.
    assert conn.fetch_args[1] == 168


def test_unknown_range_is_rejected() -> None:
    account = uuid4()
    conn = FakeConn(rows=[], scalar=0)
    client = _client(conn, _user(account))

    assert client.get("/v1/dashboard/fleet-trend?range=90d").status_code == 422


def test_agent_token_is_scoped_to_its_own_account() -> None:
    account = uuid4()
    conn = FakeConn(rows=[], scalar=0)
    client = _client(conn, _agent(account))

    r = client.get(f"/v1/dashboard/fleet-trend?account_id={uuid4()}")

    assert r.status_code == 200
    # The agent's own account wins over the stray query param.
    assert conn.fetch_args[0] == account


def test_summary_online_cameras_is_cameras_not_agents() -> None:
    # Regression: online_cameras used to return the online-AGENT count, so a
    # 2-agent/40-camera account reported 2 online cameras on the headline tile.
    # offline_cameras used to be total - streaming, calling every idle-but-
    # connected camera offline; the pair must now sum to total_cameras.
    account = uuid4()
    conn = FakeConn(
        row={
            "total_cameras": 40,
            "streaming_cameras": 5,
            "idle_cameras": 33,
            "cameras_with_errors": 2,
            "online_cameras": 38,
            "online_agents": 2,
            "total_agents": 3,
            "active_streams": 5,
            "validation_errors_24h": 0,
        }
    )
    client = _client(conn, _user(account))

    body = client.get("/v1/dashboard/summary").json()

    assert body["online_cameras"] == 38
    assert body["offline_cameras"] == 2
    assert body["online_cameras"] + body["offline_cameras"] == body["total_cameras"]
