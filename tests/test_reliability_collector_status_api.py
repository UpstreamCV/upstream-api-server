"""Upstream Reliability M1c — collector-status read endpoint tests.

Scope enforcement, account-scoping, empty state, source grouping, heartbeat
extraction, and no-leak — against a hand-rolled FakeConn that dispatches on SQL
substrings (mirrors test_reliability_issues_api.py)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller

_NOW = datetime(2026, 6, 23, 18, 0, 0, tzinfo=UTC)
_URL = "/v1/reliability/collector-status"


class AcquireCtx:
    def __init__(self, conn: FakeStatusConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeStatusConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeStatusConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


class FakeStatusConn:
    """Dispatches the collector-status queries on SQL substrings.

    Defaults model an empty account; pass data to populate specific queries.
    """

    def __init__(
        self,
        *,
        last_received_at: datetime | None = None,
        recent_batch_count: int = 0,
        batches: list[dict[str, Any]] | None = None,
        recent_observation_count: int = 0,
        sources: list[dict[str, Any]] | None = None,
        active_source_count: int = 0,
        open_issue_count: int = 0,
        open_by_key: list[dict[str, Any]] | None = None,
        health_counts: list[dict[str, Any]] | None = None,
        heartbeat: dict[str, Any] | None = None,
    ) -> None:
        self._last_received_at = last_received_at
        self._recent_batch_count = recent_batch_count
        self._batches = batches or []
        self._recent_observation_count = recent_observation_count
        self._sources = sources or []
        self._active_source_count = active_source_count
        self._open_issue_count = open_issue_count
        self._open_by_key = open_by_key or []
        self._health_counts = health_counts or []
        self._heartbeat = heartbeat

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "order by received_at desc" in query:  # recent batches list
            return self._batches
        if "group by b.source" in query:  # sources
            return self._sources
        if "group by external_camera_key" in query:  # open issues by key
            return self._open_by_key
        if "group by health_status" in query:  # M2b source health rollup
            return self._health_counts
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "max(received_at)" in query:
            return {"last_received_at": self._last_received_at}
        if "from cv_sources" in query and "count(*)" in query:  # M2a active count
            return {"n": self._active_source_count}
        if "from cv_observation_batches" in query and "count(*)" in query:
            return {"n": self._recent_batch_count}
        if "from cv_observations" in query and "count(*)" in query:
            return {"n": self._recent_observation_count}
        if "from cv_issues" in query and "count(*)" in query:
            return {"n": self._open_issue_count}
        if "collector_heartbeat" in query and "order by o.observed_at desc" in query:
            return self._heartbeat
        return None


def _agent_caller(account_id: UUID, scopes: tuple[str, ...]) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=scopes), user=None
    )


def _user_caller(account_id: UUID, role: str = "owner") -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(user_id=uuid4(), memberships={account_id: role}, aal="aal1", amr=()),
    )


def _client(conn: FakeStatusConn, caller: CallerAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


# ---------------------------------------------------------------------------
# Auth.
# ---------------------------------------------------------------------------


def test_requires_read_scope() -> None:
    r = _client(FakeStatusConn(), _agent_caller(uuid4(), ("agent",))).get(_URL)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_token_with_read_scope_ok() -> None:
    r = _client(FakeStatusConn(), _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200


def test_jwt_member_can_read() -> None:
    account = uuid4()
    r = _client(FakeStatusConn(), _user_caller(account)).get(
        _URL, params={"account_id": str(account)}
    )
    assert r.status_code == 200


def test_unauthenticated_401() -> None:
    r = _client(FakeStatusConn(), None).get(_URL)
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Shape.
# ---------------------------------------------------------------------------


def test_empty_account_returns_empty_status() -> None:
    r = _client(FakeStatusConn(), _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["last_received_at"] is None
    assert body["recent_batch_count"] == 0
    assert body["recent_observation_count"] == 0
    assert body["active_source_count"] == 0
    assert body["open_issue_count"] == 0
    assert body["latest_collector_heartbeat"] is None
    assert body["sources"] == []
    assert body["recent_batches"] == []


def test_summarizes_batches_and_sources() -> None:
    batch_id = uuid4()
    conn = FakeStatusConn(
        last_received_at=_NOW,
        recent_batch_count=1,
        batches=[
            {
                "id": batch_id,
                "source": "edge-box-4",
                "external_camera_key": "dock-12",
                "observation_count": 120,
                "status": "completed",
                "received_at": _NOW,
            }
        ],
        recent_observation_count=8000,
        sources=[
            {
                "source": "edge-box-4",
                "external_camera_key": "dock-12",
                "last_seen_at": _NOW,
                "recent_observations": 1200,
                "recent_events": 80,
                "recent_detection_summaries": 112,
            }
        ],
        # M2a: active_source_count derives from the cv_sources registry (a
        # separate query), not from len(sources).
        active_source_count=1,
        open_issue_count=3,
        open_by_key=[{"external_camera_key": "dock-12", "n": 1}],
    )
    r = _client(conn, _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["recent_batch_count"] == 1
    assert body["recent_observation_count"] == 8000
    assert body["active_source_count"] == 1
    assert body["open_issue_count"] == 3
    assert body["recent_batches"][0]["id"] == str(batch_id)
    assert body["recent_batches"][0]["source"] == "edge-box-4"
    src = body["sources"][0]
    assert src["recent_events"] == 80
    assert src["recent_detection_summaries"] == 112
    assert src["open_issues"] == 1  # joined by external_camera_key


def test_latest_heartbeat_extracted_to_safe_fields() -> None:
    conn = FakeStatusConn(
        heartbeat={
            "observed_at": _NOW,
            "source": "edge-box-4",
            # Realistic agent payload plus an unexpected key that must be dropped.
            "payload": {
                "version": "1.2.3",
                "running": True,
                "cloud_reachable": True,
                "uploaded_batches": 42,
                "uploaded_observations": 8000,
                "spooled_batches": 0,
                "dropped_batches": 0,
                "last_upload_at": "2026-06-23T17:59:00+00:00",
                "last_error_code": None,
                "api_token": "ups_live_should_not_appear",
            },
        }
    )
    r = _client(conn, _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200, r.text
    hb = r.json()["latest_collector_heartbeat"]
    assert hb is not None
    assert hb["version"] == "1.2.3"
    assert hb["cloud_reachable"] is True
    assert hb["uploaded_batches"] == 42
    assert hb["source"] == "edge-box-4"
    # The unexpected/secret key is dropped by the schema mapping.
    assert "api_token" not in hb
    assert "ups_live_should_not_appear" not in r.text


def test_response_has_no_account_id() -> None:
    conn = FakeStatusConn(recent_observation_count=5)
    r = _client(conn, _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert "account_id" not in r.text


def test_empty_account_has_all_zero_health_counts() -> None:
    r = _client(FakeStatusConn(), _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    counts = r.json()["source_health_counts"]
    assert counts == {
        "healthy": 0,
        "delayed": 0,
        "silent": 0,
        "collector_offline": 0,
        "unknown": 0,
        "disabled": 0,
    }


def test_source_health_counts_rolled_up() -> None:
    conn = FakeStatusConn(
        health_counts=[
            {"health_status": "healthy", "n": 3},
            {"health_status": "silent", "n": 1},
            {"health_status": "collector_offline", "n": 1},
        ]
    )
    r = _client(conn, _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    counts = r.json()["source_health_counts"]
    assert counts["healthy"] == 3
    assert counts["silent"] == 1
    assert counts["collector_offline"] == 1
    assert counts["delayed"] == 0
