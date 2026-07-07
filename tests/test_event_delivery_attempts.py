"""Module 6 M6.4 — delivery-attempt enqueue, claim shape, and read API tests.

Matching is pure-Python (testable directly); enqueue/claim/list are exercised
against fakes; claim + migration are pinned by SQL/file assertions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.schemas.events import CanonicalEvent
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller
from upstream_api.services.event_delivery_attempts import (
    destination_matches_event,
    enqueue_delivery_attempts_for_event,
)

_NOW = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)


def _event(account: UUID, **overrides: Any) -> CanonicalEvent:
    base: dict[str, Any] = {
        "id": uuid4(),
        "seq": 1,
        "event_type": "alert.created",
        "schema_version": 1,
        "account_id": account,
        "camera_id": None,
        "session_id": None,
        "source": "rule",
        "workflow_id": None,
        "dedupe_key": "k1",
        "occurred_at": _NOW,
        "created_at": _NOW,
        "payload": {},
        "evidence": {},
    }
    base.update(overrides)
    return CanonicalEvent(**base)


# ---------------------------------------------------------------------------
# Pure matching.
# ---------------------------------------------------------------------------


def test_empty_filters_match_all() -> None:
    e = _event(uuid4(), event_type="alert.created", camera_id=uuid4(), workflow_id=uuid4())
    assert destination_matches_event(
        event_type_filters=[], camera_id_filters=[], workflow_id_filters=[], event=e
    )


def test_event_type_filter() -> None:
    e = _event(uuid4(), event_type="alert.created")
    assert destination_matches_event(
        event_type_filters=["alert.created"],
        camera_id_filters=[],
        workflow_id_filters=[],
        event=e,
    )
    assert not destination_matches_event(
        event_type_filters=["workflow.failed"],
        camera_id_filters=[],
        workflow_id_filters=[],
        event=e,
    )


def test_camera_filter() -> None:
    cam = uuid4()
    e = _event(uuid4(), camera_id=cam)
    assert destination_matches_event(
        event_type_filters=[], camera_id_filters=[cam], workflow_id_filters=[], event=e
    )
    assert not destination_matches_event(
        event_type_filters=[], camera_id_filters=[uuid4()], workflow_id_filters=[], event=e
    )
    # An event with no camera never matches a camera-filtered destination.
    e_no_cam = _event(uuid4(), camera_id=None)
    assert not destination_matches_event(
        event_type_filters=[], camera_id_filters=[cam], workflow_id_filters=[], event=e_no_cam
    )


def test_workflow_filter() -> None:
    wf = uuid4()
    e = _event(uuid4(), workflow_id=wf)
    assert destination_matches_event(
        event_type_filters=[], camera_id_filters=[], workflow_id_filters=[wf], event=e
    )
    assert not destination_matches_event(
        event_type_filters=[], camera_id_filters=[], workflow_id_filters=[uuid4()], event=e
    )


# ---------------------------------------------------------------------------
# Enqueue against a fake conn.
# ---------------------------------------------------------------------------


class _Txn:
    async def __aenter__(self) -> _Txn:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeEnqueueConn:
    """Returns seeded enabled-webhook destinations and records attempt inserts,
    modeling the unique (event_id, destination_id) ON CONFLICT guard."""

    def __init__(self, destinations: list[dict[str, Any]]) -> None:
        self.destinations = destinations
        self.inserts: list[tuple[UUID, UUID]] = []
        self._seen: set[tuple[UUID, UUID]] = set()

    def transaction(self) -> _Txn:
        return _Txn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from event_destinations" in query
        return self.destinations

    async def fetchval(self, query: str, *args: Any) -> Any:
        assert "insert into event_delivery_attempts" in query
        _account_id, event_id, dest_id, _max = args
        if (event_id, dest_id) in self._seen:
            return None
        self._seen.add((event_id, dest_id))
        self.inserts.append((event_id, dest_id))
        return uuid4()


def _dest(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": uuid4(),
        "event_type_filters": [],
        "camera_id_filters": [],
        "workflow_id_filters": [],
    }
    base.update(overrides)
    return base


async def test_enqueue_matches_enabled_destination() -> None:
    account = uuid4()
    conn = FakeEnqueueConn([_dest()])
    n = await enqueue_delivery_attempts_for_event(conn, event=_event(account))
    assert n == 1
    assert len(conn.inserts) == 1


async def test_enqueue_respects_event_type_filter() -> None:
    account = uuid4()
    conn = FakeEnqueueConn([_dest(event_type_filters=["workflow.failed"])])
    n = await enqueue_delivery_attempts_for_event(
        conn, event=_event(account, event_type="alert.created")
    )
    assert n == 0
    assert conn.inserts == []


async def test_enqueue_respects_camera_filter() -> None:
    account, cam = uuid4(), uuid4()
    conn = FakeEnqueueConn([_dest(camera_id_filters=[cam])])
    matched = await enqueue_delivery_attempts_for_event(conn, event=_event(account, camera_id=cam))
    assert matched == 1
    conn2 = FakeEnqueueConn([_dest(camera_id_filters=[uuid4()])])
    missed = await enqueue_delivery_attempts_for_event(conn2, event=_event(account, camera_id=cam))
    assert missed == 0


async def test_enqueue_multiple_destinations() -> None:
    account = uuid4()
    conn = FakeEnqueueConn([_dest(), _dest(), _dest(event_type_filters=["workflow.failed"])])
    # Two match (empty filters), one filtered out.
    n = await enqueue_delivery_attempts_for_event(conn, event=_event(account))
    assert n == 2


async def test_enqueue_dedupes_on_event_destination() -> None:
    account = uuid4()
    d = _dest()
    conn = FakeEnqueueConn([d])
    event = _event(account)
    first = await enqueue_delivery_attempts_for_event(conn, event=event)
    second = await enqueue_delivery_attempts_for_event(conn, event=event)
    assert first == 1
    assert second == 0  # ON CONFLICT (event_id, destination_id) DO NOTHING
    assert len(conn.inserts) == 1


async def test_enqueue_failure_is_best_effort() -> None:
    class RaisingConn:
        def transaction(self) -> _Txn:
            return _Txn()

        async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
            raise RuntimeError("relation event_destinations does not exist")

    n = await enqueue_delivery_attempts_for_event(RaisingConn(), event=_event(uuid4()))
    assert n == 0  # swallowed; never raises into the event-insert path


# ---------------------------------------------------------------------------
# Claim SQL pin (multi-worker safety).
# ---------------------------------------------------------------------------


def test_claim_sql_uses_skip_locked_and_due_predicates() -> None:
    from upstream_api.services.event_delivery_attempts import _CLAIM_SQL

    sql = _CLAIM_SQL.lower()
    assert "for update skip locked" in sql
    assert "status in ('pending', 'failed')" in sql
    assert "next_attempt_at <= now()" in sql
    assert "attempt_count < max_attempts" in sql
    assert "order by next_attempt_at asc, created_at asc" in sql
    assert "set\n       status = 'delivering'" in sql or "status = 'delivering'" in sql
    assert "limit $1" in sql


# ---------------------------------------------------------------------------
# Read API.
# ---------------------------------------------------------------------------

_FLAT = "/v1/event-delivery-attempts"


class FakeReadConn:
    def __init__(self, attempts: list[dict[str, Any]]) -> None:
        self.attempts = attempts

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from event_delivery_attempts" in query
        account_id, event_id, destination_id, status, before, limit = args
        rows = [
            a
            for a in self.attempts
            if a["account_id"] == account_id
            and (event_id is None or a["event_id"] == event_id)
            and (destination_id is None or a["destination_id"] == destination_id)
            and (status is None or a["status"] == status)
            and (before is None or a["created_at"] < before)
        ]
        rows.sort(key=lambda a: a["created_at"], reverse=True)
        return rows[:limit]


class AcquireCtx:
    def __init__(self, conn: FakeReadConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeReadConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeReadConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _user_caller(account_id: UUID) -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(user_id=uuid4(), memberships={account_id: "owner"}, aal="aal1", amr=()),
    )


def _agent_caller(account_id: UUID, scopes: tuple[str, ...]) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=scopes), user=None
    )


def _client(conn: FakeReadConn, caller: CallerAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


def _attempt(account: UUID, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": uuid4(),
        "account_id": account,
        "event_id": uuid4(),
        "destination_id": uuid4(),
        "status": "delivered",
        "attempt_count": 1,
        "max_attempts": 5,
        "next_attempt_at": _NOW,
        "started_at": _NOW,
        "completed_at": _NOW,
        "response_status_code": 200,
        "response_body_preview": "ok",
        "error_message": None,
        "last_attempt_at": _NOW,
        "created_at": _NOW,
        "updated_at": _NOW,
    }
    base.update(overrides)
    return base


def test_member_lists_attempts() -> None:
    account = uuid4()
    conn = FakeReadConn([_attempt(account)])
    r = _client(conn, _user_caller(account)).get(_FLAT, params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    assert len(r.json()["items"]) == 1


def test_list_account_isolation() -> None:
    account_a, account_b = uuid4(), uuid4()
    conn = FakeReadConn([_attempt(account_a), _attempt(account_b)])
    r = _client(conn, _user_caller(account_a)).get(_FLAT, params={"account_id": str(account_a)})
    assert len(r.json()["items"]) == 1


def test_list_status_filter() -> None:
    account = uuid4()
    conn = FakeReadConn([_attempt(account, status="delivered"), _attempt(account, status="dead")])
    r = _client(conn, _user_caller(account)).get(
        _FLAT, params={"account_id": str(account), "status": "dead"}
    )
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "dead"


def test_per_event_attempts_and_cross_account_empty() -> None:
    account_a, account_b = uuid4(), uuid4()
    ev = uuid4()
    conn = FakeReadConn([_attempt(account_b, event_id=ev)])
    # Member of A asking for B's event id -> account filter yields empty (no leak).
    r = _client(conn, _user_caller(account_a)).get(
        f"/v1/events/{ev}/delivery-attempts", params={"account_id": str(account_a)}
    )
    assert r.status_code == 200
    assert r.json()["items"] == []


def test_per_destination_attempts() -> None:
    account, dest = uuid4(), uuid4()
    conn = FakeReadConn([_attempt(account, destination_id=dest), _attempt(account)])
    r = _client(conn, _user_caller(account)).get(
        f"/v1/event-destinations/{dest}/delivery-attempts", params={"account_id": str(account)}
    )
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["destination_id"] == str(dest)


def test_token_with_read_scope_can_list() -> None:
    account = uuid4()
    conn = FakeReadConn([_attempt(account)])
    for scope in ("events:read", "destinations:read"):
        r = _client(conn, _agent_caller(account, (scope,))).get(_FLAT)
        assert r.status_code == 200, scope


def test_token_without_read_scope_403() -> None:
    account = uuid4()
    r = _client(FakeReadConn([]), _agent_caller(account, ("agent",))).get(_FLAT)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_list_unauthenticated_401() -> None:
    r = _client(FakeReadConn([]), None).get(_FLAT)
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Migration shape pin.
# ---------------------------------------------------------------------------


def test_migration_shape_pin() -> None:
    root = Path(__file__).resolve().parents[1]
    sql = (
        (root / "supabase" / "migrations" / "20260621120000_event_delivery_attempts.sql")
        .read_text()
        .lower()
    )
    assert "create table event_delivery_attempts" in sql
    assert "status in ('pending', 'delivering', 'delivered', 'failed', 'dead')" in sql
    assert "unique (event_id, destination_id)" in sql
    assert "event_delivery_attempts_due_idx" in sql
    assert "where status in ('pending', 'failed')" in sql
    assert "event_delivery_attempts_account_created_idx" in sql
    assert "event_delivery_attempts_event_idx" in sql
    assert "event_delivery_attempts_destination_created_idx" in sql
    assert "enable row level security" in sql
    assert "event_delivery_attempts_member_select" in sql
    assert "grant select on public.event_delivery_attempts to authenticated" in sql
    # Exactly one policy — member select; no client insert/update/delete.
    assert sql.count("create policy") == 1
    assert "for select" in sql
