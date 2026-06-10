"""Workflow Engine M2 — route-level tests for /v1/workflow-runs reads.

TestClient + fake asyncpg pool (no real DB), mirroring tests/test_alerts_api.py.
The fake re-implements the list predicates (status/camera/rule filters,
newest-first, limit) and the alert left-join; SQL-pinning tests keep it from
drifting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import UserAuth, current_user
from upstream_api.services.workflow_runs import _FETCH_RUN_SQL, _LIST_RUNS_SQL

_NOW = datetime(2026, 6, 13, 12, 0, 0, tzinfo=UTC)

_RESPONSE_FIELDS = {
    "id",
    "camera_id",
    "rule_id",
    "trigger_observation_id",
    "workflow_type",
    "status",
    "title",
    "summary",
    "trigger_type",
    "action_result",
    "evidence",
    "payload",
    "alert",
    "started_at",
    "ended_at",
    "created_at",
    "updated_at",
}


class FakeConn:
    """Mirrors the slice of asyncpg.Connection the runs router touches.

    Rows are stored in joined shape (run columns + alert_id/alert_status/
    alert_severity), matching what the LEFT JOIN SQL returns.
    """

    def __init__(self, runs: list[dict[str, Any]] | None = None) -> None:
        self.runs = runs or []
        self.fetch_calls: list[tuple[Any, ...]] = []

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in row.items() if k != "account_id"}

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from workflow_runs" in query
        self.fetch_calls.append(args)
        account_id, status, camera_id, rule_id, limit = args
        result = [
            self._public(r)
            for r in self.runs
            if r["account_id"] == account_id
            and (status is None or r["status"] == status)
            and (camera_id is None or r["camera_id"] == camera_id)
            and (rule_id is None or r["rule_id"] == rule_id)
        ]
        result.sort(key=lambda r: r["created_at"], reverse=True)
        return result[:limit]

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "from workflow_runs" in query
        account_id, run_id = args
        for row in self.runs:
            if row["account_id"] == account_id and row["id"] == run_id:
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


def _user(account_id: UUID, role: str = "viewer") -> UserAuth:
    """History is read-path — default to the least-privileged role to prove
    any member can read."""
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


def _run_row(
    *,
    account_id: UUID,
    camera_id: UUID | None = None,
    rule_id: UUID | None = None,
    status: str = "completed",
    action_result: str | None = "alert_created",
    with_alert: bool = True,
    created_at: datetime = _NOW,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "camera_id": camera_id or uuid4(),
        "rule_id": rule_id or uuid4(),
        "trigger_observation_id": uuid4(),
        "workflow_type": "simple_rule",
        "status": status,
        "title": "Front door",
        "summary": "Line crossing detected on Front door.",
        "trigger_type": "processor.line_crossing",
        "action_result": action_result,
        "evidence": {"line_id": "line-1"},
        "payload": {"rule": {"rule_type": "line_crossing"}},
        "started_at": created_at,
        "ended_at": created_at,
        "created_at": created_at,
        "updated_at": created_at,
        "alert_id": uuid4() if with_alert else None,
        "alert_status": "open" if with_alert else None,
        "alert_severity": "warning" if with_alert else None,
    }


# ---------------------------------------------------------------------------
# List.
# ---------------------------------------------------------------------------


def test_list_runs_scoped_to_account_newest_first() -> None:
    account, other = uuid4(), uuid4()
    conn = FakeConn(
        runs=[
            _run_row(account_id=account, created_at=_NOW - timedelta(minutes=5)),
            _run_row(account_id=account, created_at=_NOW),
            _run_row(account_id=other),
        ]
    )
    r = _client(conn, _user(account)).get("/v1/workflow-runs", params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    runs = r.json()["runs"]
    assert len(runs) == 2
    stamps = [x["created_at"] for x in runs]
    assert stamps == sorted(stamps, reverse=True)


def test_list_runs_status_filter() -> None:
    account = uuid4()
    conn = FakeConn(
        runs=[
            _run_row(account_id=account, status="completed"),
            _run_row(
                account_id=account,
                status="suppressed",
                action_result="alert_suppressed_cooldown",
                with_alert=False,
            ),
        ]
    )
    r = _client(conn, _user(account)).get(
        "/v1/workflow-runs",
        params={"account_id": str(account), "status": "suppressed"},
    )
    assert r.status_code == 200, r.text
    runs = r.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["status"] == "suppressed"
    assert runs[0]["action_result"] == "alert_suppressed_cooldown"
    assert runs[0]["alert"] is None


def test_list_runs_invalid_status_422() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).get(
        "/v1/workflow-runs",
        params={"account_id": str(account), "status": "snoozed"},
    )
    assert r.status_code == 422
    assert conn.fetch_calls == []


def test_list_runs_camera_and_rule_filters() -> None:
    account, camera, rule = uuid4(), uuid4(), uuid4()
    conn = FakeConn(
        runs=[
            _run_row(account_id=account, camera_id=camera, rule_id=rule),
            _run_row(account_id=account, camera_id=camera),
            _run_row(account_id=account),
        ]
    )
    r = _client(conn, _user(account)).get(
        "/v1/workflow-runs",
        params={
            "account_id": str(account),
            "camera_id": str(camera),
            "rule_id": str(rule),
        },
    )
    assert r.status_code == 200, r.text
    runs = r.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["camera_id"] == str(camera)
    assert runs[0]["rule_id"] == str(rule)


def test_list_runs_default_limit_50_and_bounds() -> None:
    account = uuid4()
    conn = FakeConn()
    client = _client(conn, _user(account))
    client.get("/v1/workflow-runs", params={"account_id": str(account)})
    assert conn.fetch_calls[0][4] == 50
    assert (
        client.get(
            "/v1/workflow-runs", params={"account_id": str(account), "limit": 201}
        ).status_code
        == 422
    )
    assert (
        client.get("/v1/workflow-runs", params={"account_id": str(account), "limit": 0}).status_code
        == 422
    )


def test_list_runs_response_shape_no_account_id() -> None:
    account = uuid4()
    conn = FakeConn(runs=[_run_row(account_id=account)])
    r = _client(conn, _user(account)).get("/v1/workflow-runs", params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    run = r.json()["runs"][0]
    assert set(run.keys()) == _RESPONSE_FIELDS
    assert "account_id" not in run
    assert set(run["alert"].keys()) == {"id", "status", "severity"}


def test_list_runs_unauthenticated_401() -> None:
    conn = FakeConn()
    r = _client(conn, None).get("/v1/workflow-runs")
    assert r.status_code == 401
    assert conn.fetch_calls == []


def test_list_runs_non_member_account_403() -> None:
    account, other = uuid4(), uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).get("/v1/workflow-runs", params={"account_id": str(other)})
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden_account"


# ---------------------------------------------------------------------------
# Get single.
# ---------------------------------------------------------------------------


def test_get_run_includes_evidence_and_alert() -> None:
    account = uuid4()
    row = _run_row(account_id=account)
    conn = FakeConn(runs=[row])
    r = _client(conn, _user(account)).get(
        f"/v1/workflow-runs/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["evidence"] == {"line_id": "line-1"}
    assert body["payload"]["rule"]["rule_type"] == "line_crossing"
    assert body["alert"]["id"] == str(row["alert_id"])


def test_get_run_unknown_404() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).get(
        f"/v1/workflow-runs/{uuid4()}", params={"account_id": str(account)}
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "workflow_run_not_found"


def test_get_run_cross_tenant_404() -> None:
    account, other = uuid4(), uuid4()
    row = _run_row(account_id=other)
    conn = FakeConn(runs=[row])
    r = _client(conn, _user(account)).get(
        f"/v1/workflow-runs/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pin the production SQL so the FakeConn simulation cannot drift.
# ---------------------------------------------------------------------------


def test_list_runs_sql_pins_predicates() -> None:
    sql = _LIST_RUNS_SQL
    assert "where r.account_id = $1" in sql
    assert "($2::text is null or r.status = $2)" in sql
    assert "($3::uuid is null or r.camera_id = $3)" in sql
    assert "($4::uuid is null or r.rule_id = $4)" in sql
    assert "order by r.created_at desc" in sql
    assert "limit $5" in sql
    assert "left join alerts a on a.workflow_run_id = r.id" in sql
    # account_id must never be selected onto the read response.
    assert "r.account_id," not in sql


def test_fetch_run_sql_pins_predicates() -> None:
    sql = _FETCH_RUN_SQL
    assert "where r.account_id = $1" in sql
    assert "and r.id = $2" in sql
    assert "left join alerts a on a.workflow_run_id = r.id" in sql
