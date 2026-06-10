"""Rules/Triggers M1 — route-level tests for /v1/rules CRUD.

Pattern mirrors tests/test_api_tokens.py + tests/test_observations.py:
TestClient + a fake asyncpg pool, no real DB. The fake re-implements the
documented predicates of the rules SQL so scoping, PATCH coalesce semantics
and delete-by-account are genuinely exercised; SQL-pinning tests keep the
simulation from drifting.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import UserAuth, current_user
from upstream_api.services.rules import (
    _ENABLED_RULES_FOR_CAMERA_SQL,
    _INSERT_ALERT_SQL,
    _LIST_RULES_SQL,
    _UPDATE_RULE_SQL,
)

_NOW = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fake pool/conn.
# ---------------------------------------------------------------------------


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Mirrors the slice of asyncpg.Connection the rules router touches.

    ``cameras`` maps camera_id -> owning account_id. ``rules`` holds full
    rule rows (with account_id, used only for scoping — reads never return
    it, matching the production SQL's column list).
    """

    def __init__(
        self,
        *,
        cameras: dict[UUID, UUID] | None = None,
        rules: list[dict[str, Any]] | None = None,
    ) -> None:
        self.cameras = cameras or {}
        self.rules = rules or []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in row.items() if k != "account_id"}

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            account = self.cameras.get(args[0])
            return None if account is None else {"account_id": account}
        if "insert into rules" in query:
            row = {
                "id": uuid4(),
                "account_id": args[0],
                "camera_id": args[1],
                "name": args[2],
                "rule_type": args[3],
                "enabled": args[4],
                "config": json.loads(args[5]),
                "severity": args[6],
                "cooldown_seconds": args[7],
                "created_by": args[8],
                "created_at": _NOW,
                "updated_at": _NOW,
            }
            self.rules.append(row)
            return self._public(row)
        if "update rules" in query:
            account_id, rule_id, name, enabled, config, severity, cooldown = args
            for row in self.rules:
                if row["account_id"] == account_id and row["id"] == rule_id:
                    # Same coalesce semantics as _UPDATE_RULE_SQL.
                    if name is not None:
                        row["name"] = name
                    if enabled is not None:
                        row["enabled"] = enabled
                    if config is not None:
                        row["config"] = json.loads(config)
                    if severity is not None:
                        row["severity"] = severity
                    if cooldown is not None:
                        row["cooldown_seconds"] = cooldown
                    return self._public(row)
            return None
        if "from rules" in query:
            account_id, rule_id = args
            for row in self.rules:
                if row["account_id"] == account_id and row["id"] == rule_id:
                    return self._public(row)
            return None
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from rules" in query
        account_id, camera_id = args
        result = [
            self._public(r)
            for r in self.rules
            if r["account_id"] == account_id and (camera_id is None or r["camera_id"] == camera_id)
        ]
        result.sort(key=lambda r: r["created_at"], reverse=True)
        return result

    async def execute(self, query: str, *args: Any) -> str:
        assert "delete from rules" in query
        account_id, rule_id = args
        before = len(self.rules)
        self.rules = [
            r for r in self.rules if not (r["account_id"] == account_id and r["id"] == rule_id)
        ]
        return f"DELETE {before - len(self.rules)}"


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


def _create_body(account_id: UUID, camera_id: UUID, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "account_id": str(account_id),
        "camera_id": str(camera_id),
        "name": "Lobby line",
        "rule_type": "line_crossing",
    }
    body.update(overrides)
    return body


def _rule_row(
    *,
    account_id: UUID,
    camera_id: UUID,
    rule_type: str = "line_crossing",
    config: dict[str, Any] | None = None,
    enabled: bool = True,
    created_at: datetime = _NOW,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "camera_id": camera_id,
        "name": "Rule",
        "rule_type": rule_type,
        "enabled": enabled,
        "config": config or {},
        "severity": "warning",
        "cooldown_seconds": 300,
        "created_by": uuid4(),
        "created_at": created_at,
        "updated_at": created_at,
    }


# ---------------------------------------------------------------------------
# Create.
# ---------------------------------------------------------------------------


def test_create_rule_happy_path() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, _user(account)).post(
        "/v1/rules",
        json=_create_body(account, camera, config={"line_id": "line-1"}),
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["name"] == "Lobby line"
    assert body["rule_type"] == "line_crossing"
    assert body["enabled"] is True
    assert body["severity"] == "warning"
    assert body["cooldown_seconds"] == 300
    assert body["config"] == {"line_id": "line-1"}
    assert "account_id" not in body


def test_create_rule_viewer_forbidden() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, _user(account, role="viewer")).post(
        "/v1/rules", json=_create_body(account, camera)
    )
    assert r.status_code == 403
    assert conn.rules == []


def test_create_rule_unknown_camera_404() -> None:
    account = uuid4()
    conn = FakeConn(cameras={})
    r = _client(conn, _user(account)).post("/v1/rules", json=_create_body(account, uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_create_rule_cross_tenant_camera_404() -> None:
    account, other_account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeConn(cameras={camera: other_account})
    r = _client(conn, _user(account)).post("/v1/rules", json=_create_body(account, camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.rules == []


def test_create_rule_unknown_type_422() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, _user(account)).post(
        "/v1/rules", json=_create_body(account, camera, rule_type="camera_offline")
    )
    assert r.status_code == 422


def test_create_rule_unknown_config_key_422() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, _user(account)).post(
        "/v1/rules",
        json=_create_body(account, camera, config={"roi_id": "roi-1"}),
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_rule_config"
    assert conn.rules == []


def test_create_rule_bad_min_total_count_422() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, _user(account)).post(
        "/v1/rules",
        json=_create_body(
            account,
            camera,
            rule_type="roi_occupancy",
            config={"min_total_count": 0},
        ),
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_rule_config"


def test_create_rule_cooldown_out_of_range_422() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, _user(account)).post(
        "/v1/rules", json=_create_body(account, camera, cooldown_seconds=86401)
    )
    assert r.status_code == 422


def test_create_rule_unauthenticated_401() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(cameras={camera: account})
    r = _client(conn, None).post("/v1/rules", json=_create_body(account, camera))
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# List / get.
# ---------------------------------------------------------------------------


def test_list_rules_scoped_to_account() -> None:
    account, other_account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeConn(
        rules=[
            _rule_row(account_id=account, camera_id=camera),
            _rule_row(account_id=other_account, camera_id=uuid4()),
        ]
    )
    r = _client(conn, _user(account)).get("/v1/rules", params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    assert len(r.json()["rules"]) == 1


def test_list_rules_camera_filter() -> None:
    account, cam_a, cam_b = uuid4(), uuid4(), uuid4()
    conn = FakeConn(
        rules=[
            _rule_row(account_id=account, camera_id=cam_a),
            _rule_row(account_id=account, camera_id=cam_b),
        ]
    )
    r = _client(conn, _user(account)).get(
        "/v1/rules", params={"account_id": str(account), "camera_id": str(cam_a)}
    )
    assert r.status_code == 200, r.text
    rules = r.json()["rules"]
    assert len(rules) == 1
    assert rules[0]["camera_id"] == str(cam_a)


def test_list_rules_viewer_allowed() -> None:
    account = uuid4()
    conn = FakeConn(rules=[_rule_row(account_id=account, camera_id=uuid4())])
    r = _client(conn, _user(account, role="viewer")).get(
        "/v1/rules", params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["rules"]) == 1


def test_get_rule_found_and_unknown() -> None:
    account = uuid4()
    row = _rule_row(account_id=account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    client = _client(conn, _user(account))
    ok = client.get(f"/v1/rules/{row['id']}", params={"account_id": str(account)})
    assert ok.status_code == 200, ok.text
    assert ok.json()["id"] == str(row["id"])
    missing = client.get(f"/v1/rules/{uuid4()}", params={"account_id": str(account)})
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "rule_not_found"


def test_get_rule_cross_tenant_404() -> None:
    account, other_account = uuid4(), uuid4()
    row = _rule_row(account_id=other_account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    r = _client(conn, _user(account)).get(
        f"/v1/rules/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Patch.
# ---------------------------------------------------------------------------


def test_patch_rule_partial_update() -> None:
    account = uuid4()
    row = _rule_row(account_id=account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    r = _client(conn, _user(account)).patch(
        f"/v1/rules/{row['id']}",
        params={"account_id": str(account)},
        json={"enabled": False, "severity": "critical"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["enabled"] is False
    assert body["severity"] == "critical"
    # Untouched fields keep their values (coalesce semantics).
    assert body["name"] == "Rule"
    assert body["cooldown_seconds"] == 300


def test_patch_rule_config_validated_against_stored_type() -> None:
    account = uuid4()
    row = _rule_row(account_id=account, camera_id=uuid4(), rule_type="line_crossing")
    conn = FakeConn(rules=[row])
    r = _client(conn, _user(account)).patch(
        f"/v1/rules/{row['id']}",
        params={"account_id": str(account)},
        json={"config": {"roi_id": "roi-1"}},  # roi_id invalid for line_crossing
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_rule_config"


def test_patch_rule_viewer_forbidden() -> None:
    account = uuid4()
    row = _rule_row(account_id=account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    r = _client(conn, _user(account, role="viewer")).patch(
        f"/v1/rules/{row['id']}",
        params={"account_id": str(account)},
        json={"enabled": False},
    )
    assert r.status_code == 403


def test_patch_unknown_rule_404() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user(account)).patch(
        f"/v1/rules/{uuid4()}",
        params={"account_id": str(account)},
        json={"enabled": False},
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Delete.
# ---------------------------------------------------------------------------


def test_delete_rule_204_then_404() -> None:
    account = uuid4()
    row = _rule_row(account_id=account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    client = _client(conn, _user(account))
    first = client.delete(f"/v1/rules/{row['id']}", params={"account_id": str(account)})
    assert first.status_code == 204
    assert conn.rules == []
    second = client.delete(f"/v1/rules/{row['id']}", params={"account_id": str(account)})
    assert second.status_code == 404


def test_delete_rule_cross_tenant_404() -> None:
    account, other_account = uuid4(), uuid4()
    row = _rule_row(account_id=other_account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    r = _client(conn, _user(account)).delete(
        f"/v1/rules/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 404
    assert len(conn.rules) == 1


def test_delete_rule_viewer_forbidden() -> None:
    account = uuid4()
    row = _rule_row(account_id=account, camera_id=uuid4())
    conn = FakeConn(rules=[row])
    r = _client(conn, _user(account, role="viewer")).delete(
        f"/v1/rules/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 403
    assert len(conn.rules) == 1


# ---------------------------------------------------------------------------
# Pin the production SQL so the FakeConn simulation cannot drift.
# ---------------------------------------------------------------------------


def test_list_rules_sql_pins_predicates() -> None:
    sql = _LIST_RULES_SQL
    assert "where account_id = $1" in sql
    assert "($2::uuid is null or camera_id = $2)" in sql
    assert "order by created_at desc" in sql
    # account_id must never be selected onto the read response.
    assert "select\n    id, camera_id" in sql


def test_update_rules_sql_pins_coalesce_semantics() -> None:
    sql = _UPDATE_RULE_SQL
    assert "coalesce($3, name)" in sql
    assert "coalesce($4, enabled)" in sql
    assert "coalesce($5::jsonb, config)" in sql
    assert "where account_id = $1" in sql


def test_enabled_rules_sql_pins_predicates() -> None:
    sql = _ENABLED_RULES_FOR_CAMERA_SQL
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
    assert "and rule_type = $3" in sql
    assert "and enabled" in sql


def test_insert_alert_sql_pins_cooldown_guard() -> None:
    sql = _INSERT_ALERT_SQL
    assert "where not exists" in sql
    assert "make_interval(secs => $10)" in sql
    assert "where rule_id = $3" in sql
    # M2: every alert carries the workflow run that created it.
    assert "workflow_run_id" in sql
