"""Module 6 M6.2 — route-level tests for the Event REST API.

TestClient + fake asyncpg pool (no real DB), mirroring tests/test_alerts_api.py
and tests/test_workflow_runs_api.py. The fake re-implements the list filter +
keyset predicates; SQL-pin tests keep it from drifting from the real SQL.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.errors import ForbiddenError
from upstream_api.security.api_tokens import ALLOWED_SCOPES
from upstream_api.security.deps import (
    AgentAuth,
    CallerAuth,
    UserAuth,
    current_caller,
    require_token_scope,
)

_NOW = datetime(2026, 6, 14, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fake pool / conn.
# ---------------------------------------------------------------------------


class FakeConn:
    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self.events = events or []
        self.fetch_calls: list[tuple[Any, ...]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from events" in query
        self.fetch_calls.append(args)
        (
            account_id,
            camera_id,
            event_type,
            source,
            rule_id,
            workflow_id,
            workflow_run_id,
            session_id,
            start_time,
            end_time,
            before_seq,
            after_seq,
            has_evidence,
            limit,
        ) = args
        out = []
        for e in self.events:
            if e["account_id"] != account_id:
                continue
            if camera_id is not None and e["camera_id"] != camera_id:
                continue
            if event_type is not None and e["event_type"] != event_type:
                continue
            if source is not None and e["source"] != source:
                continue
            if rule_id is not None and e["rule_id"] != rule_id:
                continue
            if workflow_id is not None and e["workflow_id"] != workflow_id:
                continue
            if workflow_run_id is not None and e["workflow_run_id"] != workflow_run_id:
                continue
            if session_id is not None and e["session_id"] != session_id:
                continue
            if start_time is not None and e["occurred_at"] < start_time:
                continue
            if end_time is not None and not (e["occurred_at"] < end_time):
                continue
            if before_seq is not None and not (e["seq"] < before_seq):
                continue
            if after_seq is not None and not (e["seq"] > after_seq):
                continue
            if has_evidence is not None and (len(e["evidence"]) > 0) != has_evidence:
                continue
            out.append(e)
        out.sort(key=lambda e: e["seq"], reverse=True)
        return out[:limit]

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "from events" in query
        account_id, event_id = args
        for e in self.events:
            if e["account_id"] == account_id and e["id"] == event_id:
                return e
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


def _user_caller(account_id: UUID, role: str = "owner") -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(
            user_id=uuid4(), memberships={account_id: role}, aal="aal1", amr=("password",)
        ),
    )


def _agent_caller(account_id: UUID, scopes: tuple[str, ...] = ("events:read",)) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=scopes), user=None
    )


def _client(conn: FakeConn, caller: CallerAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


def _event_row(
    *,
    account_id: UUID,
    seq: int,
    event_type: str = "alert.created",
    source: str = "rule",
    camera_id: UUID | None = None,
    rule_id: UUID | None = None,
    workflow_id: UUID | None = None,
    workflow_run_id: UUID | None = None,
    session_id: UUID | None = None,
    source_alert_id: UUID | None = None,
    source_observation_id: UUID | None = None,
    occurred_at: datetime | None = None,
    payload: dict[str, Any] | None = None,
    evidence: dict[str, Any] | None = None,
    reliability_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "seq": seq,
        "account_id": account_id,
        "camera_id": camera_id,
        "session_id": session_id,
        "event_type": event_type,
        "schema_version": 1,
        "source": source,
        "source_observation_id": source_observation_id,
        "workflow_run_id": workflow_run_id,
        "source_alert_id": source_alert_id,
        "source_finding_id": None,
        "rule_id": rule_id,
        "workflow_id": workflow_id,
        "dedupe_key": f"workflow_run:{workflow_run_id or seq}:completed:alert_created",
        "occurred_at": occurred_at or _NOW,
        "created_at": occurred_at or _NOW,
        "payload": payload if payload is not None else {},
        "evidence": evidence if evidence is not None else {},
        "reliability_context": reliability_context,
    }


_PATH = "/v1/events"


# ---------------------------------------------------------------------------
# List.
# ---------------------------------------------------------------------------


def test_list_returns_account_events_newest_first() -> None:
    account = uuid4()
    conn = FakeConn(
        [
            _event_row(account_id=account, seq=1),
            _event_row(account_id=account, seq=3),
            _event_row(account_id=account, seq=2),
        ]
    )
    r = _client(conn, _user_caller(account)).get(_PATH, params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    body = r.json()
    assert [e["seq"] for e in body["items"]] == [3, 2, 1]
    assert body["next_before_seq"] is None  # page not full


def test_list_account_isolation() -> None:
    account_a, account_b = uuid4(), uuid4()
    conn = FakeConn(
        [
            _event_row(account_id=account_a, seq=1),
            _event_row(account_id=account_b, seq=2),
        ]
    )
    r = _client(conn, _user_caller(account_a)).get(_PATH, params={"account_id": str(account_a)})
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["account_id"] == str(account_a)


def test_list_empty_account_returns_empty() -> None:
    account = uuid4()
    r = _client(FakeConn([]), _user_caller(account)).get(
        _PATH, params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"items": [], "next_before_seq": None}


def test_list_limit_and_before_seq_pagination() -> None:
    account = uuid4()
    conn = FakeConn([_event_row(account_id=account, seq=s) for s in (1, 2, 3, 4, 5)])
    client = _client(conn, _user_caller(account))
    # First page: newest two, cursor points at the oldest returned.
    r1 = client.get(_PATH, params={"account_id": str(account), "limit": 2})
    assert r1.status_code == 200, r1.text
    page1 = r1.json()
    assert [e["seq"] for e in page1["items"]] == [5, 4]
    assert page1["next_before_seq"] == 4
    # Second page via before_seq: strictly older, no overlap with page 1.
    r2 = client.get(
        _PATH, params={"account_id": str(account), "limit": 2, "before_seq": 4}
    )
    page2 = r2.json()
    assert [e["seq"] for e in page2["items"]] == [3, 2]
    assert page2["next_before_seq"] == 2
    # Final page: not full -> cursor null, no duplicates across pages.
    r3 = client.get(
        _PATH, params={"account_id": str(account), "limit": 2, "before_seq": 2}
    )
    page3 = r3.json()
    assert [e["seq"] for e in page3["items"]] == [1]
    assert page3["next_before_seq"] is None
    seen = [e["seq"] for e in page1["items"] + page2["items"] + page3["items"]]
    assert seen == [5, 4, 3, 2, 1]  # stable, no dupes


def test_list_after_seq_returns_newer_only() -> None:
    account = uuid4()
    conn = FakeConn([_event_row(account_id=account, seq=s) for s in (1, 2, 3)])
    r = _client(conn, _user_caller(account)).get(
        _PATH, params={"account_id": str(account), "after_seq": 1}
    )
    assert r.status_code == 200, r.text
    assert [e["seq"] for e in r.json()["items"]] == [3, 2]


def test_list_filters() -> None:
    account = uuid4()
    cam = uuid4()
    rule = uuid4()
    run = uuid4()
    conn = FakeConn(
        [
            _event_row(account_id=account, seq=1, camera_id=cam, rule_id=rule, workflow_run_id=run),
            _event_row(account_id=account, seq=2, event_type="workflow.cooldown_suppressed"),
            _event_row(account_id=account, seq=3, source="timer"),
        ]
    )
    client = _client(conn, _user_caller(account))
    base = {"account_id": str(account)}

    def seqs(**extra: str) -> list[int]:
        return [e["seq"] for e in client.get(_PATH, params={**base, **extra}).json()["items"]]

    assert seqs(camera_id=str(cam)) == [1]
    assert seqs(event_type="workflow.cooldown_suppressed") == [2]
    assert seqs(source="timer") == [3]
    assert seqs(rule_id=str(rule)) == [1]
    assert seqs(workflow_run_id=str(run)) == [1]


def test_list_time_range_filter() -> None:
    account = uuid4()
    conn = FakeConn(
        [
            _event_row(account_id=account, seq=1, occurred_at=_NOW - timedelta(hours=2)),
            _event_row(account_id=account, seq=2, occurred_at=_NOW - timedelta(hours=1)),
            _event_row(account_id=account, seq=3, occurred_at=_NOW),
        ]
    )
    r = _client(conn, _user_caller(account)).get(
        _PATH,
        params={
            "account_id": str(account),
            "start_time": (_NOW - timedelta(hours=1, minutes=30)).isoformat(),
            "end_time": _NOW.isoformat(),  # end is exclusive
        },
    )
    assert r.status_code == 200, r.text
    assert [e["seq"] for e in r.json()["items"]] == [2]


def test_list_has_evidence_filter() -> None:
    account = uuid4()
    conn = FakeConn(
        [
            _event_row(account_id=account, seq=1, evidence={}),
            _event_row(account_id=account, seq=2, evidence={"zone_id": "z-1"}),
        ]
    )
    client = _client(conn, _user_caller(account))
    base = {"account_id": str(account)}

    def seqs(**extra: str) -> list[int]:
        return [e["seq"] for e in client.get(_PATH, params={**base, **extra}).json()["items"]]

    assert seqs(has_evidence="true") == [2]
    assert seqs(has_evidence="false") == [1]


def test_list_returns_full_payload_evidence_reliability() -> None:
    account = uuid4()
    conn = FakeConn(
        [
            _event_row(
                account_id=account,
                seq=1,
                payload={"status": "completed", "action_result": "alert_created"},
                evidence={"zone_id": "z-1", "track_id": "7"},
                reliability_context={"detector_version": "reliability.v0.6"},
            )
        ]
    )
    r = _client(conn, _user_caller(account)).get(_PATH, params={"account_id": str(account)})
    item = r.json()["items"][0]
    assert item["payload"] == {"status": "completed", "action_result": "alert_created"}
    assert item["evidence"] == {"zone_id": "z-1", "track_id": "7"}
    assert item["reliability_context"] == {"detector_version": "reliability.v0.6"}
    assert item["schema_version"] == 1


def test_list_invalid_limit_422() -> None:
    account = uuid4()
    r = _client(FakeConn([]), _user_caller(account)).get(
        _PATH, params={"account_id": str(account), "limit": 0}
    )
    assert r.status_code == 422
    r2 = _client(FakeConn([]), _user_caller(account)).get(
        _PATH, params={"account_id": str(account), "limit": 9999}
    )
    assert r2.status_code == 422


def test_list_unauthenticated_401() -> None:
    # No caller override + no Authorization header -> current_caller raises 401.
    r = _client(FakeConn([]), None).get(_PATH)
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Get by id.
# ---------------------------------------------------------------------------


def test_get_event_by_id() -> None:
    account = uuid4()
    row = _event_row(account_id=account, seq=1)
    conn = FakeConn([row])
    r = _client(conn, _user_caller(account)).get(
        f"{_PATH}/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text
    assert r.json()["id"] == str(row["id"])
    assert r.json()["seq"] == 1


def test_get_unknown_404() -> None:
    account = uuid4()
    r = _client(FakeConn([]), _user_caller(account)).get(
        f"{_PATH}/{uuid4()}", params={"account_id": str(account)}
    )
    assert r.status_code == 404


def test_get_cross_account_404() -> None:
    account_a, account_b = uuid4(), uuid4()
    row_b = _event_row(account_id=account_b, seq=1)
    conn = FakeConn([row_b])
    # Member of A querying B's event id (scoped to A) -> 404, no existence leak.
    r = _client(conn, _user_caller(account_a)).get(
        f"{_PATH}/{row_b['id']}", params={"account_id": str(account_a)}
    )
    assert r.status_code == 404


def test_get_invalid_uuid_422() -> None:
    account = uuid4()
    r = _client(FakeConn([]), _user_caller(account)).get(
        f"{_PATH}/not-a-uuid", params={"account_id": str(account)}
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Auth / scope groundwork.
# ---------------------------------------------------------------------------


def test_agent_token_with_events_read_can_list() -> None:
    account = uuid4()
    conn = FakeConn([_event_row(account_id=account, seq=1)])
    # Agent token resolves to its own account, ignoring any hint.
    r = _client(conn, _agent_caller(account, ("events:read",))).get(_PATH)
    assert r.status_code == 200, r.text
    assert len(r.json()["items"]) == 1


def test_agent_token_without_events_read_403_list() -> None:
    account = uuid4()
    conn = FakeConn([_event_row(account_id=account, seq=1)])
    r = _client(conn, _agent_caller(account, ("agent",))).get(_PATH)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_agent_token_with_events_read_can_get() -> None:
    account = uuid4()
    row = _event_row(account_id=account, seq=1)
    conn = FakeConn([row])
    r = _client(conn, _agent_caller(account, ("events:read",))).get(f"{_PATH}/{row['id']}")
    assert r.status_code == 200, r.text


def test_agent_token_without_events_read_403_get() -> None:
    account = uuid4()
    row = _event_row(account_id=account, seq=1)
    conn = FakeConn([row])
    r = _client(conn, _agent_caller(account, ("agent",))).get(f"{_PATH}/{row['id']}")
    assert r.status_code == 403


def test_events_read_is_a_mintable_scope() -> None:
    # The mint-time gate (routers/api_tokens.py uses set(scopes) - ALLOWED_SCOPES).
    assert "events:read" in ALLOWED_SCOPES
    assert {"events:read"} - ALLOWED_SCOPES == set()
    assert {"bogus"} - ALLOWED_SCOPES == {"bogus"}  # unknown still rejected
    assert "agent" in ALLOWED_SCOPES  # agent scope unchanged


def test_require_token_scope_user_caller_bypasses() -> None:
    # Dashboard users are gated by membership, not scopes.
    require_token_scope(_user_caller(uuid4()), "events:read")  # no raise


def test_require_token_scope_agent_with_scope_ok() -> None:
    require_token_scope(_agent_caller(uuid4(), ("events:read",)), "events:read")  # no raise


def test_require_token_scope_agent_missing_raises() -> None:
    with pytest.raises(ForbiddenError) as exc:
        require_token_scope(_agent_caller(uuid4(), ("agent",)), "events:read")
    assert exc.value.code == "forbidden.scope"


# ---------------------------------------------------------------------------
# SQL pins.
# ---------------------------------------------------------------------------


def test_list_sql_pins_predicates() -> None:
    from upstream_api.services.events import _LIST_EVENTS_SQL

    sql = _LIST_EVENTS_SQL.lower()
    assert "from events" in sql
    assert "where account_id = $1" in sql
    assert "order by seq desc" in sql
    assert "seq < $11" in sql  # before_seq
    assert "seq > $12" in sql  # after_seq
    assert "evidence <> '{}'::jsonb" in sql  # has_evidence


def test_get_sql_pins_predicates() -> None:
    from upstream_api.services.events import _GET_EVENT_SQL

    sql = _GET_EVENT_SQL.lower()
    assert "from events" in sql
    assert "where account_id = $1" in sql
    assert "id = $2" in sql
