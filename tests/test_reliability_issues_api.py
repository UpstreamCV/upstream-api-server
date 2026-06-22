"""Upstream Reliability M1a — issues read + resolve API tests.

Scope/role enforcement, account-scoped listing (no cross-account leak),
cross-account 404, and the resolve lifecycle (owner/admin or reliability:write,
idempotent, canonical-event emission) — against hand-rolled fakes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller

_NOW = datetime(2026, 6, 22, 18, 0, 0, tzinfo=UTC)
_BASE = "/v1/reliability/issues"


class FakeTxn:
    async def __aenter__(self) -> FakeTxn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


def _issue(account: UUID, *, status: str = "open", issue_id: UUID | None = None) -> dict[str, Any]:
    return {
        "__account": account,
        "id": issue_id or uuid4(),
        "camera_id": None,
        "source_batch_id": None,
        "external_camera_key": "dock-12",
        "issue_type": "event_count_drop",
        "category": "count",
        "severity": "warning",
        "status": status,
        "title": "Event rate dropped",
        "message": "msg",
        "likely_cause": None,
        "recommended_action": None,
        "evidence": {},
        "dedupe_key": "external:dock-12:event_count_drop:forklift",
        "detector_version": "reliability.observations.v0.1",
        "source_observation_ids": None,
        "first_seen_at": _NOW,
        "last_seen_at": _NOW,
        "resolved_at": None,
        "resolved_reason": None,
        "created_at": _NOW,
        "updated_at": _NOW,
    }


class FakeIssuesConn:
    def __init__(self, issues: list[dict[str, Any]]) -> None:
        self.issues = issues
        self.events: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTxn:
        return FakeTxn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from cv_issues" in query and "order by" in query:  # list
            account, status, limit = args
            rows = [
                i
                for i in self.issues
                if i["__account"] == account and (status is None or i["status"] == status)
            ]
            return rows[:limit]
        if "from event_destinations" in query:
            return []
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "insert into events" in query:
            self.events.append(args)
            return {"id": uuid4(), "seq": len(self.events), "created_at": _NOW}
        if "update cv_issues" in query:  # resolve by id
            account, issue_id, reason = args
            for i in self.issues:
                if i["id"] == issue_id and i["__account"] == account and i["status"] == "open":
                    i["status"] = "resolved"
                    i["resolved_reason"] = reason
                    return i
            return None
        if "from cv_issues" in query:  # get-by-id or select-for-update
            account, issue_id = args
            for i in self.issues:
                if i["id"] == issue_id and i["__account"] == account:
                    return i
            return None
        return None


class AcquireCtx:
    def __init__(self, conn: FakeIssuesConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeIssuesConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeIssuesConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _agent_caller(account_id: UUID, scopes: tuple[str, ...]) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=scopes), user=None
    )


def _user_caller(account_id: UUID, role: str = "owner") -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(user_id=uuid4(), memberships={account_id: role}, aal="aal1", amr=()),
    )


def _client(conn: FakeIssuesConn, caller: CallerAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


# ---------------------------------------------------------------------------
# List.
# ---------------------------------------------------------------------------


def test_list_requires_read_scope() -> None:
    account = uuid4()
    r = _client(FakeIssuesConn([]), _agent_caller(account, ("agent",))).get(_BASE)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_list_returns_account_scoped_issues() -> None:
    account, other = uuid4(), uuid4()
    conn = FakeIssuesConn([_issue(account), _issue(account), _issue(other)])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(_BASE)
    assert r.status_code == 200, r.text
    issues = r.json()["issues"]
    assert len(issues) == 2  # the `other` account's issue never leaks


def test_list_status_filter() -> None:
    account = uuid4()
    conn = FakeIssuesConn([_issue(account, status="open"), _issue(account, status="resolved")])
    r = _client(conn, _agent_caller(account, ("reliability:read",))).get(
        _BASE, params={"status": "resolved"}
    )
    issues = r.json()["issues"]
    assert len(issues) == 1
    assert issues[0]["status"] == "resolved"


def test_list_bogus_status_422() -> None:
    account = uuid4()
    r = _client(FakeIssuesConn([]), _agent_caller(account, ("reliability:read",))).get(
        _BASE, params={"status": "nonsense"}
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Get one.
# ---------------------------------------------------------------------------


def test_get_issue_found() -> None:
    account = uuid4()
    issue = _issue(account)
    r = _client(FakeIssuesConn([issue]), _agent_caller(account, ("reliability:read",))).get(
        f"{_BASE}/{issue['id']}"
    )
    assert r.status_code == 200, r.text
    assert r.json()["id"] == str(issue["id"])


def test_get_issue_cross_account_404() -> None:
    account, other = uuid4(), uuid4()
    issue = _issue(other)
    r = _client(FakeIssuesConn([issue]), _agent_caller(account, ("reliability:read",))).get(
        f"{_BASE}/{issue['id']}"
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "issue_not_found"


def test_get_unknown_issue_404() -> None:
    account = uuid4()
    r = _client(FakeIssuesConn([]), _agent_caller(account, ("reliability:read",))).get(
        f"{_BASE}/{uuid4()}"
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Resolve.
# ---------------------------------------------------------------------------


def test_resolve_requires_write_scope_for_tokens() -> None:
    account = uuid4()
    issue = _issue(account)
    r = _client(FakeIssuesConn([issue]), _agent_caller(account, ("reliability:read",))).post(
        f"{_BASE}/{issue['id']}/resolve"
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_resolve_token_with_write_scope_succeeds() -> None:
    account = uuid4()
    issue = _issue(account)
    conn = FakeIssuesConn([issue])
    r = _client(conn, _agent_caller(account, ("reliability:write",))).post(
        f"{_BASE}/{issue['id']}/resolve", json={"resolved_reason": "handled"}
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "resolved"
    assert len(conn.events) == 1  # reliability.issue_resolved emitted


def test_resolve_jwt_non_owner_403() -> None:
    account = uuid4()
    issue = _issue(account)
    r = _client(FakeIssuesConn([issue]), _user_caller(account, role="viewer")).post(
        f"{_BASE}/{issue['id']}/resolve"
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.role"


def test_resolve_jwt_owner_succeeds() -> None:
    account = uuid4()
    issue = _issue(account)
    r = _client(FakeIssuesConn([issue]), _user_caller(account, role="owner")).post(
        f"{_BASE}/{issue['id']}/resolve"
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "resolved"


def test_resolve_unknown_issue_404() -> None:
    account = uuid4()
    r = _client(FakeIssuesConn([]), _agent_caller(account, ("reliability:write",))).post(
        f"{_BASE}/{uuid4()}/resolve"
    )
    assert r.status_code == 404


def test_resolve_already_resolved_is_idempotent_no_event() -> None:
    account = uuid4()
    issue = _issue(account, status="resolved")
    conn = FakeIssuesConn([issue])
    r = _client(conn, _agent_caller(account, ("reliability:write",))).post(
        f"{_BASE}/{issue['id']}/resolve"
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "resolved"
    assert conn.events == []  # no second event for an already-resolved issue
