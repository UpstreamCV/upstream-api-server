"""Rules/Triggers M1 — route-level tests for the /v1/alerts inbox.

TestClient + fake asyncpg pool (no real DB), mirroring tests/test_rules_api.py.
The fake re-implements the list predicates and the read/resolve idempotency
(coalesce semantics); SQL-pinning tests keep it from drifting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import UserAuth, current_user
from upstream_api.services.alerts import _LIST_ALERTS_SQL, _MARK_READ_SQL, _RESOLVE_SQL

_NOW = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)

_RESPONSE_FIELDS = {
    "id",
    "camera_id",
    "rule_id",
    "observation_id",
    "workflow_run_id",
    "severity",
    "status",
    "title",
    "message",
    "payload",
    "created_at",
    "read_at",
    "resolved_at",
}


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    def __init__(self, alerts: list[dict[str, Any]] | None = None) -> None:
        self.alerts = alerts or []
        self.fetch_calls: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in row.items() if k != "account_id"}

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from alerts" in query
        self.fetch_calls.append(args)
        account_id, status, camera_id, limit = args
        result = [
            self._public(r)
            for r in self.alerts
            if r["account_id"] == account_id
            and (status is None or r["status"] == status)
            and (camera_id is None or r["camera_id"] == camera_id)
        ]
        result.sort(key=lambda r: r["created_at"], reverse=True)
        return result[:limit]

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "update alerts" in query
        account_id, alert_id = args
        for row in self.alerts:
            if row["account_id"] != account_id or row["id"] != alert_id:
                continue
            # Same coalesce semantics as the production SQL.
            if "resolved_at" in query:
                row["status"] = "resolved"
                row["resolved_at"] = row["resolved_at"] or _NOW
                row["read_at"] = row["read_at"] or _NOW
            else:
                row["read_at"] = row["read_at"] or _NOW
            return self._public(row)
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


def _user(account_id: UUID, role: str = "owner") -> UserAuth:
    return UserAuth(
        user_id=uuid4(),
        memberships={account_id: role},
        aal="aal1",
        amr=("password",),
    )


def _client(conn: FakeConn, user: UserAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if user is not None:
        app.dependency_overrides[current_user] = lambda: user
    return TestClient(app)


def _alert_row(
    *,
    account_id: UUID,
    camera_id: UUID | None = None,
    status: str = "open",
    severity: str = "warning",
    created_at: datetime = _NOW,
    read_at: datetime | None = None,
    resolved_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "camera_id": camera_id or uuid4(),
        "rule_id": uuid4(),
        "observation_id": uuid4(),
        "workflow_run_id": uuid4(),
        "severity": severity,
        "status": status,
        "title": "Front door",
        "message": "Line crossing detected on Front door.",
        "payload": {"line_id": "line-1"},
        "created_at": created_at,
        "read_at": read_at,
        "resolved_at": resolved_at,
    }


# ---------------------------------------------------------------------------
# List.
# ---------------------------------------------------------------------------


def test_list_alerts_scoped_to_account_newest_first() -> None:
    account, other = uuid4(), uuid4()
    conn = FakeConn(
        alerts=[
            _alert_row(account_id=account, created_at=_NOW - timedelta(minutes=5)),
            _alert_row(account_id=account, created_at=_NOW),
            _alert_row(account_id=other),
        ]
    )
    r = _client(conn, _user(account)).get("/v1/alerts", params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    alerts = r.json()["alerts"]
    assert len(alerts) == 2
    stamps = [a["created_at"] for a in alerts]
    assert stamps == sorted(stamps, reverse=True)


def test_list_alerts_status_filter() -> None:
    account = uuid4()
    conn = FakeConn(
        alerts=[
            _alert_row(account_id=account, status="open"),
            _alert_row(account_id=account, status="resolved", resolved_at=_NOW),
        ]
    )
    r = _client(conn, _user(account)).get(
        "/v1/alerts", params={"account_id": str(account), "status": "open"}
    )
    assert r.status_code == 200, r.text
    alerts = r.json()["alerts"]
    assert len(alerts) == 1
    assert alerts[0]["status"] == "open"


def test_list_alerts_invalid_status_422() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).get(
        "/v1/alerts", params={"account_id": str(account), "status": "snoozed"}
    )
    assert r.status_code == 422


def test_list_alerts_camera_filter_and_limit() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(
        alerts=[
            _alert_row(
                account_id=account,
                camera_id=camera,
                created_at=_NOW - timedelta(seconds=i),
            )
            for i in range(5)
        ]
        + [_alert_row(account_id=account)]
    )
    r = _client(conn, _user(account)).get(
        "/v1/alerts",
        params={"account_id": str(account), "camera_id": str(camera), "limit": 3},
    )
    assert r.status_code == 200, r.text
    alerts = r.json()["alerts"]
    assert len(alerts) == 3
    assert all(a["camera_id"] == str(camera) for a in alerts)


def test_list_alerts_default_limit_50() -> None:
    account = uuid4()
    conn = FakeConn()
    _client(conn, _user(account)).get("/v1/alerts", params={"account_id": str(account)})
    assert conn.fetch_calls[0][3] == 50


def test_list_alerts_response_shape_no_account_id() -> None:
    account = uuid4()
    conn = FakeConn(alerts=[_alert_row(account_id=account)])
    r = _client(conn, _user(account)).get("/v1/alerts", params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    alert = r.json()["alerts"][0]
    assert set(alert.keys()) == _RESPONSE_FIELDS
    assert "account_id" not in alert


def test_list_alerts_viewer_allowed() -> None:
    account = uuid4()
    conn = FakeConn(alerts=[_alert_row(account_id=account)])
    r = _client(conn, _user(account, role="viewer")).get(
        "/v1/alerts", params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text


def test_list_alerts_unauthenticated_401() -> None:
    conn = FakeConn()
    r = _client(conn, None).get("/v1/alerts")
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Mark read.
# ---------------------------------------------------------------------------


def test_mark_read_sets_read_at_once() -> None:
    account = uuid4()
    row = _alert_row(account_id=account)
    conn = FakeConn(alerts=[row])
    client = _client(conn, _user(account, role="viewer"))  # any member may read
    first = client.post(f"/v1/alerts/{row['id']}/read", params={"account_id": str(account)})
    assert first.status_code == 200, first.text
    stamp = first.json()["read_at"]
    assert stamp is not None
    second = client.post(f"/v1/alerts/{row['id']}/read", params={"account_id": str(account)})
    assert second.status_code == 200
    assert second.json()["read_at"] == stamp  # idempotent


def test_mark_read_unknown_alert_404() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).post(
        f"/v1/alerts/{uuid4()}/read", params={"account_id": str(account)}
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "alert_not_found"


def test_mark_read_cross_tenant_404() -> None:
    account, other = uuid4(), uuid4()
    row = _alert_row(account_id=other)
    conn = FakeConn(alerts=[row])
    r = _client(conn, _user(account)).post(
        f"/v1/alerts/{row['id']}/read", params={"account_id": str(account)}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Resolve.
# ---------------------------------------------------------------------------


def test_resolve_sets_status_resolved_and_read_at() -> None:
    account = uuid4()
    row = _alert_row(account_id=account)
    conn = FakeConn(alerts=[row])
    r = _client(conn, _user(account)).post(
        f"/v1/alerts/{row['id']}/resolve", params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "resolved"
    assert body["resolved_at"] is not None
    assert body["read_at"] is not None


def test_resolve_is_idempotent() -> None:
    account = uuid4()
    row = _alert_row(account_id=account)
    conn = FakeConn(alerts=[row])
    client = _client(conn, _user(account))
    first = client.post(f"/v1/alerts/{row['id']}/resolve", params={"account_id": str(account)})
    second = client.post(f"/v1/alerts/{row['id']}/resolve", params={"account_id": str(account)})
    assert second.status_code == 200
    assert second.json()["resolved_at"] == first.json()["resolved_at"]


def test_resolve_viewer_forbidden() -> None:
    account = uuid4()
    row = _alert_row(account_id=account)
    conn = FakeConn(alerts=[row])
    r = _client(conn, _user(account, role="viewer")).post(
        f"/v1/alerts/{row['id']}/resolve", params={"account_id": str(account)}
    )
    assert r.status_code == 403
    assert row["status"] == "open"


def test_resolve_unknown_alert_404() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).post(
        f"/v1/alerts/{uuid4()}/resolve", params={"account_id": str(account)}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pin the production SQL so the FakeConn simulation cannot drift.
# ---------------------------------------------------------------------------


def test_list_alerts_sql_pins_predicates() -> None:
    sql = _LIST_ALERTS_SQL
    assert "where account_id = $1" in sql
    assert "($2::text is null or status = $2)" in sql
    assert "($3::uuid is null or camera_id = $3)" in sql
    assert "order by created_at desc" in sql
    assert "limit $4" in sql
    # account_id must never be selected onto the read response.
    assert "select\n    id, camera_id" in sql


def test_mark_read_sql_pins_coalesce() -> None:
    sql = _MARK_READ_SQL
    assert "coalesce(read_at, now())" in sql
    assert "where account_id = $1" in sql


def test_resolve_sql_pins_transitions() -> None:
    sql = _RESOLVE_SQL
    assert "status      = 'resolved'" in sql
    assert "coalesce(resolved_at, now())" in sql
    assert "coalesce(read_at, now())" in sql
    assert "where account_id = $1" in sql
