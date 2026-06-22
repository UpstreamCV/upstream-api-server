"""Upstream Reliability M1a — service-layer tests.

Covers the batch ingest service (insert + idempotent replay), the issue
lifecycle service (open/refresh/resolve), and the canonical-event emit seam
(``reliability.issue_opened`` / ``reliability.issue_resolved``), all against
hand-rolled fakes that dispatch on SQL substrings.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from upstream_api.schemas.reliability_observations import ReliabilityObservationItem
from upstream_api.services.events import (
    emit_reliability_issue_event,
    reliability_issue_dedupe_key,
    reliability_issue_event_type,
)
from upstream_api.services.reliability_issues import (
    resolve_issue_by_id,
    upsert_open_issue,
)
from upstream_api.services.reliability_observation_ingest import record_observation_batch

_NOW = datetime(2026, 6, 22, 18, 0, 0, tzinfo=UTC)


class FakeTxn:
    async def __aenter__(self) -> FakeTxn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


# ---------------------------------------------------------------------------
# record_observation_batch.
# ---------------------------------------------------------------------------


class FakeBatchConn:
    def __init__(self, existing: dict[str, Any] | None = None) -> None:
        self.existing = existing
        self.batch_id = uuid4()
        self.observations: list[tuple[Any, ...]] | None = None
        self.finalized: tuple[Any, ...] | None = None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "insert into cv_observation_batches" in query:
            return None if self.existing is not None else self.batch_id
        return None

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "select id, observation_count" in query:
            return self.existing
        return None

    async def executemany(self, query: str, args_list: list[tuple[Any, ...]]) -> None:
        assert "insert into cv_observations" in query
        self.observations = args_list

    async def execute(self, query: str, *args: Any) -> str:
        if "update cv_observation_batches" in query:
            self.finalized = args
        return "OK"


def _obs() -> list[ReliabilityObservationItem]:
    return [
        ReliabilityObservationItem(
            type="event_observed",
            external_camera_key="dock-12",
            observed_at=_NOW,
            payload={"event_name": "line_crossing"},
        ),
        ReliabilityObservationItem(
            type="detection_summary",
            external_camera_key="dock-12",
            observed_at=_NOW,
            payload={"counts_by_class": {"forklift": 2}},
        ),
    ]


async def test_record_batch_inserts_header_and_observations() -> None:
    conn = FakeBatchConn()
    result = await record_observation_batch(
        conn,
        account_id=uuid4(),
        client_batch_id="batch_1",
        source="edge-box-4",
        observations=_obs(),
    )
    assert result.inserted is True
    assert result.accepted == 2
    assert result.batch_id == conn.batch_id
    assert conn.observations is not None and len(conn.observations) == 2
    assert conn.finalized is not None and conn.finalized[1] == 2  # observation_count


async def test_record_batch_replay_is_idempotent() -> None:
    original = uuid4()
    conn = FakeBatchConn(existing={"id": original, "observation_count": 5})
    result = await record_observation_batch(
        conn,
        account_id=uuid4(),
        client_batch_id="batch_1",
        source="edge-box-4",
        observations=_obs(),
    )
    assert result.inserted is False
    assert result.accepted == 5  # prior count, not len(observations)
    assert result.batch_id == original
    assert conn.observations is None  # no re-insert
    assert conn.finalized is None


# ---------------------------------------------------------------------------
# emit_reliability_issue_event.
# ---------------------------------------------------------------------------


class FakeEmitConn:
    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []
        self._seen: set[tuple[Any, str]] = set()

    def transaction(self) -> FakeTxn:
        return FakeTxn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from event_destinations" in query
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "insert into events" in query
        account_id, dedupe_key = args[0], args[12]
        if (account_id, dedupe_key) in self._seen:
            return None  # ON CONFLICT DO NOTHING
        self._seen.add((account_id, dedupe_key))
        self.events.append(args)
        return {"id": uuid4(), "seq": len(self.events), "created_at": _NOW}


def test_event_type_and_dedupe_key_helpers() -> None:
    iid = uuid4()
    assert reliability_issue_event_type("opened") == "reliability.issue_opened"
    assert reliability_issue_event_type("resolved") == "reliability.issue_resolved"
    assert reliability_issue_dedupe_key(iid, "opened") == f"reliability_issue:{iid}:opened"
    # opened vs resolved keys must differ or the second emit gets swallowed.
    assert reliability_issue_dedupe_key(iid, "opened") != reliability_issue_dedupe_key(
        iid, "resolved"
    )


async def test_emit_opened_event_shape() -> None:
    conn = FakeEmitConn()
    issue_id, account = uuid4(), uuid4()
    row = await emit_reliability_issue_event(
        conn,
        issue_id=issue_id,
        kind="opened",
        account_id=account,
        occurred_at=_NOW,
        payload={"issue_type": "event_count_drop"},
    )
    assert row is not None
    args = conn.events[0]
    assert args[3] == "reliability.issue_opened"  # event_type
    assert args[5] == "reliability"  # source
    assert args[9] is None  # source_finding_id NOT reused for the issue id
    assert args[12] == f"reliability_issue:{issue_id}:opened"  # dedupe_key
    payload = json.loads(args[14])
    assert payload["issue_id"] == str(issue_id)
    assert payload["issue_type"] == "event_count_drop"


async def test_emit_resolved_event_shape() -> None:
    conn = FakeEmitConn()
    issue_id = uuid4()
    await emit_reliability_issue_event(
        conn, issue_id=issue_id, kind="resolved", account_id=uuid4(), occurred_at=_NOW
    )
    args = conn.events[0]
    assert args[3] == "reliability.issue_resolved"
    assert args[12] == f"reliability_issue:{issue_id}:resolved"


async def test_emit_is_best_effort() -> None:
    class RaisingConn(FakeEmitConn):
        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            raise RuntimeError('relation "events" does not exist')

    row = await emit_reliability_issue_event(
        RaisingConn(), issue_id=uuid4(), kind="opened", account_id=uuid4(), occurred_at=_NOW
    )
    assert row is None  # swallowed, never raises


async def test_emit_dedupes_on_repeat() -> None:
    conn = FakeEmitConn()
    issue_id, account = uuid4(), uuid4()
    first = await emit_reliability_issue_event(
        conn, issue_id=issue_id, kind="opened", account_id=account, occurred_at=_NOW
    )
    second = await emit_reliability_issue_event(
        conn, issue_id=issue_id, kind="opened", account_id=account, occurred_at=_NOW
    )
    assert first is not None
    assert second is None
    assert len(conn.events) == 1


# ---------------------------------------------------------------------------
# upsert_open_issue / resolve_issue_by_id (issue lifecycle service).
# ---------------------------------------------------------------------------


def _insert_row(args: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "camera_id": args[1],
        "source_batch_id": args[2],
        "external_camera_key": args[3],
        "issue_type": args[4],
        "category": args[5],
        "severity": args[6],
        "status": "open",
        "title": args[7],
        "message": args[8],
        "likely_cause": args[9],
        "recommended_action": args[10],
        "evidence": args[11],
        "dedupe_key": args[12],
        "detector_version": args[13],
        "source_observation_ids": args[14],
        "first_seen_at": _NOW,
        "last_seen_at": _NOW,
        "resolved_at": None,
        "resolved_reason": None,
        "created_at": _NOW,
        "updated_at": _NOW,
    }


def _full_issue(*, status: str, issue_id: UUID) -> dict[str, Any]:
    return {
        "id": issue_id,
        "camera_id": None,
        "source_batch_id": None,
        "external_camera_key": "dock-12",
        "issue_type": "event_count_drop",
        "category": "count",
        "severity": "warning",
        "status": status,
        "title": "t",
        "message": "m",
        "likely_cause": None,
        "recommended_action": None,
        "evidence": {},
        "dedupe_key": "external:dock-12:event_count_drop:x",
        "detector_version": "reliability.observations.v0.1",
        "source_observation_ids": None,
        "first_seen_at": _NOW,
        "last_seen_at": _NOW,
        "resolved_at": None,
        "resolved_reason": None,
        "created_at": _NOW,
        "updated_at": _NOW,
    }


class FakeIssueConn:
    def __init__(
        self,
        *,
        open_issue: dict[str, Any] | None = None,
        issue_by_id: dict[str, Any] | None = None,
    ) -> None:
        self._open_issue = open_issue
        self._issue_by_id = issue_by_id
        self.events: list[tuple[Any, ...]] = []
        self.inserts: list[tuple[Any, ...]] = []
        self.updates: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTxn:
        return FakeTxn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from event_destinations" in query:
            return []
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "insert into events" in query:
            self.events.append(args)
            return {"id": uuid4(), "seq": len(self.events), "created_at": _NOW}
        if "insert into cv_issues" in query:
            self.inserts.append(args)
            return _insert_row(args)
        if "update cv_issues" in query:
            if "= 'resolved'" in query and "id = $2" in query:  # resolve by id
                if self._issue_by_id is None:
                    return None
                row = dict(self._issue_by_id)
                row["status"] = "resolved"
                row["resolved_reason"] = args[2]
                return row
            self.updates.append(args)  # refresh (UPDATE open)
            row = dict(self._open_issue or {})
            row.setdefault("id", uuid4())
            return _insert_row(
                (
                    args[1],
                    args[2],
                    args[3],
                    args[4],
                    "x",
                    "count",
                    args[5],
                    args[6],
                    args[7],
                    args[8],
                    args[9],
                    args[10],
                    "k",
                    args[11],
                    args[12],
                )
            )
        if "from cv_issues" in query and "for update" in query:
            if "dedupe_key = $2" in query:
                return self._open_issue
            return self._issue_by_id
        return None


def _upsert_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "account_id": uuid4(),
        "dedupe_key": "external:dock-12:event_count_drop:forklift",
        "issue_type": "event_count_drop",
        "category": "count",
        "severity": "warning",
        "title": "Event rate dropped",
        "message": "msg",
        "likely_cause": None,
        "recommended_action": None,
        "evidence": {"baseline_total": 12},
        "detector_version": "reliability.observations.v0.1",
        "camera_id": None,
        "external_camera_key": "dock-12",
        "source_batch_id": None,
        "source_observation_ids": None,
        "occurred_at": _NOW,
    }
    base.update(over)
    return base


async def test_upsert_open_issue_inserts_and_emits() -> None:
    conn = FakeIssueConn(open_issue=None)
    row, opened = await upsert_open_issue(conn, **_upsert_kwargs())
    assert opened is True
    assert len(conn.inserts) == 1
    assert len(conn.events) == 1  # reliability.issue_opened emitted on new open
    assert conn.events[0][3] == "reliability.issue_opened"
    assert row["status"] == "open"


async def test_upsert_open_issue_refresh_does_not_emit() -> None:
    conn = FakeIssueConn(open_issue={"id": uuid4()})
    _row, opened = await upsert_open_issue(conn, **_upsert_kwargs())
    assert opened is False
    assert conn.inserts == []  # refreshed in place, not inserted
    assert len(conn.updates) == 1
    assert conn.events == []  # a refresh fires no event


async def test_resolve_issue_by_id_resolves_and_emits() -> None:
    issue_id = uuid4()
    conn = FakeIssueConn(issue_by_id=_full_issue(status="open", issue_id=issue_id))
    row = await resolve_issue_by_id(
        conn, account_id=uuid4(), issue_id=issue_id, reason="resolved_by_user", occurred_at=_NOW
    )
    assert row is not None
    assert row["status"] == "resolved"
    assert len(conn.events) == 1
    assert conn.events[0][3] == "reliability.issue_resolved"


async def test_resolve_already_resolved_is_idempotent() -> None:
    issue_id = uuid4()
    conn = FakeIssueConn(issue_by_id=_full_issue(status="resolved", issue_id=issue_id))
    row = await resolve_issue_by_id(
        conn, account_id=uuid4(), issue_id=issue_id, reason="again", occurred_at=_NOW
    )
    assert row is not None
    assert row["status"] == "resolved"
    assert conn.events == []  # no second event


async def test_resolve_missing_issue_returns_none() -> None:
    conn = FakeIssueConn(issue_by_id=None)
    row = await resolve_issue_by_id(
        conn, account_id=uuid4(), issue_id=uuid4(), reason="x", occurred_at=_NOW
    )
    assert row is None
    assert conn.events == []
