"""Upstream Reliability M2a — source registry read endpoint tests.

Scope enforcement, account-scoping, empty state, passthrough of the
DB-derived status_hint / open_issue_count, filter validation (422), limit cap,
and no-leak — against a hand-rolled FakeConn that dispatches on SQL substrings
(mirrors test_reliability_collector_status_api.py)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller

_NOW = datetime(2026, 6, 24, 18, 0, 0, tzinfo=UTC)
_URL = "/v1/reliability/sources"


def _source_row(
    *,
    source: str = "edge-box-4",
    external_camera_key: str | None = "dock-12",
    source_kind: str = "external_camera",
    status_hint: str = "active",
    health_status: str = "silent",
    open_issue_count: int = 0,
    last_seen_at: datetime = _NOW,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "source": source,
        "external_camera_key": external_camera_key,
        "display_name": external_camera_key or source,
        "source_kind": source_kind,
        "first_seen_at": _NOW,
        "last_seen_at": last_seen_at,
        "last_observation_at": last_seen_at,
        "last_event_at": last_seen_at,
        "last_detection_summary_at": None,
        "last_collector_heartbeat_at": None,
        "last_config_changed_at": None,
        "last_model_name": "detector-v1",
        "last_model_version": "2026-06-22",
        "observation_count_total": 1200,
        "event_count_total": 80,
        "detection_summary_count_total": 112,
        "collector_heartbeat_count_total": 0,
        "config_changed_count_total": 0,
        "open_issue_count": open_issue_count,
        "status_hint": status_hint,
        # M2b liveness/health (intervals NULL → router resolves to kind default).
        "health_status": health_status,
        "health_reason": "source_silent" if health_status == "silent" else None,
        "health_message": "No observations for 15 minutes." if health_status == "silent" else None,
        "liveness_enabled": True,
        "expected_interval_seconds": None,
        "liveness_grace_seconds": None,
        "last_liveness_checked_at": _NOW,
        "last_health_status_changed_at": _NOW,
        "liveness_silence_started_at": None,
        "last_liveness_issue_id": None,
    }


class AcquireCtx:
    def __init__(self, conn: FakeSourcesConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeSourcesConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeSourcesConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


class _Txn:
    async def __aenter__(self) -> _Txn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class FakeSourcesConn:
    """Returns the configured rows only for the account the service queries with
    (the router resolves it from the credential) — so an account with no rows
    sees an empty list, demonstrating account-scoping."""

    def __init__(
        self, account: UUID | None = None, rows: list[dict[str, Any]] | None = None
    ) -> None:
        self._account = account
        self._rows = rows or []
        self.patch_sql: str | None = None
        self.patch_args: tuple[Any, ...] | None = None

    def transaction(self) -> _Txn:
        return _Txn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from cv_sources" in query
        account_id = args[0]
        if self._account is not None and account_id == self._account:
            return self._rows
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "update cv_sources set" in query:  # PATCH dynamic UPDATE ... returning id
            self.patch_sql = query
            self.patch_args = args
            account_id, source_id = args[0], args[1]
            if self._account is not None and account_id == self._account:
                return {"id": source_id}
            return None
        if "with src as" in query:  # get_source_for_response (single-row CTE)
            account_id = args[0]
            if self._account is not None and account_id == self._account and self._rows:
                return self._rows[0]
            return None
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


def _client(conn: FakeSourcesConn, caller: CallerAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


# ---------------------------------------------------------------------------
# Auth.
# ---------------------------------------------------------------------------


def test_requires_read_scope() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("agent",))).get(_URL)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_token_with_read_scope_ok() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200


def test_jwt_member_can_read() -> None:
    account = uuid4()
    r = _client(FakeSourcesConn(), _user_caller(account)).get(
        _URL, params={"account_id": str(account)}
    )
    assert r.status_code == 200


def test_unauthenticated_401() -> None:
    r = _client(FakeSourcesConn(), None).get(_URL)
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Shape.
# ---------------------------------------------------------------------------


def test_empty_account_returns_empty_list() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200, r.text
    assert r.json() == {"sources": []}


def test_returns_registry_rows_with_derived_fields() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row(open_issue_count=1)])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(_URL)
    assert r.status_code == 200, r.text
    sources = r.json()["sources"]
    assert len(sources) == 1
    s = sources[0]
    assert s["source"] == "edge-box-4"
    assert s["external_camera_key"] == "dock-12"
    assert s["source_kind"] == "external_camera"
    assert s["status_hint"] == "active"
    assert s["open_issue_count"] == 1
    assert s["last_model_name"] == "detector-v1"
    assert s["event_count_total"] == 80
    assert s["detection_summary_count_total"] == 112


def test_order_is_preserved_from_db() -> None:
    account = uuid4()
    rows = [
        _source_row(external_camera_key="dock-12", last_seen_at=_NOW),
        _source_row(external_camera_key="dock-13", status_hint="recent"),
    ]
    conn = FakeSourcesConn(account=account, rows=rows)
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(_URL)
    keys = [s["external_camera_key"] for s in r.json()["sources"]]
    assert keys == ["dock-12", "dock-13"]


def test_collector_row_has_null_external_key() -> None:
    account = uuid4()
    conn = FakeSourcesConn(
        account=account,
        rows=[
            _source_row(
                external_camera_key=None, source_kind="collector", status_hint="active"
            )
        ],
    )
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(_URL)
    s = r.json()["sources"][0]
    assert s["external_camera_key"] is None
    assert s["source_kind"] == "collector"
    assert s["display_name"] == "edge-box-4"


# ---------------------------------------------------------------------------
# Filters / limits / isolation.
# ---------------------------------------------------------------------------


def test_bogus_source_kind_is_422() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("reliability:read",))).get(
        _URL, params={"source_kind": "camera"}
    )
    assert r.status_code == 422


def test_bogus_status_hint_is_422() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("reliability:read",))).get(
        _URL, params={"status_hint": "offline"}
    )
    assert r.status_code == 422


def test_limit_over_cap_is_422() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("reliability:read",))).get(
        _URL, params={"limit": "501"}
    )
    assert r.status_code == 422


def test_valid_filters_pass_through() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(
        _URL, params={"source_kind": "external_camera", "status_hint": "active", "limit": "50"}
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["sources"]) == 1


def test_cross_account_does_not_leak() -> None:
    # Rows belong to another account; the requesting agent's resolved account
    # has none, so the registry comes back empty.
    conn = FakeSourcesConn(account=uuid4(), rows=[_source_row()])
    r = _client(conn, _agent_caller(uuid4(), ("reliability:read",))).get(_URL)
    assert r.status_code == 200
    assert r.json() == {"sources": []}


def test_response_has_no_account_id() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(_URL)
    assert "account_id" not in r.text


# ---------------------------------------------------------------------------
# M2b — health fields, health_status filter, PATCH liveness.
# ---------------------------------------------------------------------------


def _patch_url(source_id: UUID) -> str:
    return f"/v1/reliability/sources/{source_id}/liveness"


def test_response_includes_health_fields_with_resolved_interval() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row(health_status="silent")])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(_URL)
    s = r.json()["sources"][0]
    assert s["health_status"] == "silent"
    assert s["liveness_enabled"] is True
    # stored intervals NULL → resolved to the external-source defaults.
    assert s["expected_interval_seconds"] == 600
    assert s["liveness_grace_seconds"] == 300


def test_bogus_health_status_is_422() -> None:
    r = _client(FakeSourcesConn(), _agent_caller(uuid4(), ("reliability:read",))).get(
        _URL, params={"health_status": "down"}
    )
    assert r.status_code == 422


def test_valid_health_status_filter_passes_through() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(
        _URL, params={"health_status": "silent"}
    )
    assert r.status_code == 200, r.text


def test_patch_requires_write_scope() -> None:
    sid = uuid4()
    conn = FakeSourcesConn()
    r = _client(conn, _agent_caller(uuid4(), ("reliability:read",))).patch(
        _patch_url(sid), json={"liveness_enabled": False}
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_patch_owner_can_update() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _user_caller(account, "owner")).patch(
        _patch_url(uuid4()),
        params={"account_id": str(account)},
        json={"expected_interval_seconds": 900},
    )
    assert r.status_code == 200, r.text
    assert "health_status" in r.json()


def test_patch_invalid_intervals_are_422() -> None:
    account = uuid4()
    caller = _agent_caller(account, ("reliability:write",))
    bad_expected = _client(FakeSourcesConn(account=account), caller).patch(
        _patch_url(uuid4()), json={"expected_interval_seconds": 0}
    )
    bad_grace = _client(FakeSourcesConn(account=account), caller).patch(
        _patch_url(uuid4()), json={"liveness_grace_seconds": -1}
    )
    assert bad_expected.status_code == 422
    assert bad_grace.status_code == 422


def test_patch_null_interval_resets_to_default() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _agent_caller(account, ("reliability:write",))).patch(
        _patch_url(uuid4()), json={"expected_interval_seconds": None}
    )
    assert r.status_code == 200, r.text
    assert conn.patch_sql is not None and "expected_interval_seconds =" in conn.patch_sql
    assert conn.patch_args is not None and conn.patch_args[2] is None  # stored NULL = default


def test_patch_absent_field_is_unchanged() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _agent_caller(account, ("reliability:write",))).patch(
        _patch_url(uuid4()), json={"liveness_enabled": True}
    )
    assert r.status_code == 200, r.text
    # expected_interval_seconds was absent → not in the UPDATE.
    assert conn.patch_sql is not None and "expected_interval_seconds" not in conn.patch_sql


def test_patch_disable_sets_health_disabled() -> None:
    account = uuid4()
    conn = FakeSourcesConn(account=account, rows=[_source_row()])
    r = _client(conn, _agent_caller(account, ("reliability:write",))).patch(
        _patch_url(uuid4()), json={"liveness_enabled": False}
    )
    assert r.status_code == 200, r.text
    assert conn.patch_sql is not None and "health_status =" in conn.patch_sql
    assert conn.patch_args is not None and "disabled" in conn.patch_args


def test_patch_cross_account_is_404() -> None:
    conn = FakeSourcesConn(account=uuid4(), rows=[_source_row()])
    r = _client(conn, _agent_caller(uuid4(), ("reliability:write",))).patch(
        _patch_url(uuid4()), json={"liveness_enabled": False}
    )
    assert r.status_code == 404
