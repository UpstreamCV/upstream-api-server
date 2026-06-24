"""Upstream Reliability M2b — source-liveness sweep tests.

Drives the real sweep (which drives the real upsert_open_issue /
resolve_open_issues_by_dedupe / event emit) against a FakeConn that models
cv_sources + cv_issues + the events insert (mirrors the M1a issues fakes).
SQL-level effects that the fake can't execute (the last_health_status_changed_at
CASE) are not asserted here — those are pinned structurally + covered by mypy.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from upstream_api.services.reliability_source_liveness import run_source_liveness_sweep

_NOW = datetime(2026, 6, 25, 12, 0, 0, tzinfo=UTC)
_EARLY = datetime(2000, 1, 1, tzinfo=UTC)


def _source(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": uuid4(),
        "account_id": uuid4(),
        "source": "edge-box-4",
        "external_camera_key": "dock-12",
        "source_kind": "external_camera",
        "liveness_enabled": True,
        "expected_interval_seconds": 600,
        "liveness_grace_seconds": 300,
        "last_seen_at": _NOW - timedelta(seconds=60),
        "last_collector_heartbeat_at": None,
        "health_status": "healthy",
        "last_liveness_issue_id": None,
        "liveness_silence_started_at": None,
        "last_liveness_checked_at": None,
    }
    base.update(over)
    return base


class _Txn:
    async def __aenter__(self) -> _Txn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class FakeSweepConn:
    def __init__(self, sources: list[dict[str, Any]]) -> None:
        self.sources = {s["id"]: dict(s) for s in sources}
        self.issues: dict[str, dict[str, Any]] = {}  # dedupe_key -> issue
        self.events: list[tuple[Any, ...]] = []
        self.health_writes: dict[UUID, dict[str, Any]] = {}
        self._issue_seq = 0

    def transaction(self) -> _Txn:
        return _Txn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "for update skip locked" in query:  # claim
            limit, now = args
            enabled = [s for s in self.sources.values() if s["liveness_enabled"]]
            enabled.sort(key=lambda s: s["last_liveness_checked_at"] or _EARLY)
            claimed = enabled[:limit]
            for s in claimed:
                s["last_liveness_checked_at"] = now
            return [{"id": s["id"]} for s in claimed]
        if "update cv_issues" in query and "any($2::text[])" in query:  # resolve-by-dedupe
            _account, dedupe_keys, _reason = args
            out = []
            for key in dedupe_keys:
                issue = self.issues.get(key)
                if issue is not None and issue["status"] == "open":
                    issue["status"] = "resolved"
                    out.append(issue)
            return out
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cv_sources" in query and "for update" in query:  # select one source
            return self.sources.get(args[0])
        if "from cv_issues" in query and "for update" in query:  # select open by dedupe
            _account, dedupe = args
            issue = self.issues.get(dedupe)
            return {"id": issue["id"]} if issue and issue["status"] == "open" else None
        if "insert into cv_issues" in query:
            self._issue_seq += 1
            issue_id = uuid4()
            account, camera_id, _sb, _key, issue_type = args[0], args[1], args[2], args[3], args[4]
            dedupe = args[12]
            self.issues[dedupe] = {
                "id": issue_id,
                "camera_id": camera_id,
                "account_id": account,
                "issue_type": issue_type,
                "status": "open",
            }
            return {"id": issue_id, "camera_id": camera_id}
        if "update cv_issues" in query:  # refresh open (UPDATE ... where id = $1)
            return {"id": args[0], "camera_id": args[2]}
        if "insert into events" in query:
            self.events.append(args)
            return {"id": uuid4(), "seq": len(self.events), "created_at": _NOW}
        return None

    async def execute(self, query: str, *args: Any) -> str:
        if "update cv_sources" in query:  # _UPDATE_HEALTH_SQL
            (source_id, status, reason, message, issue_id, silence, _now) = args
            self.health_writes[source_id] = {
                "health_status": status,
                "health_reason": reason,
                "health_message": message,
                "last_liveness_issue_id": issue_id,
                "liveness_silence_started_at": silence,
            }
            self.sources[source_id].update(
                health_status=status,
                last_liveness_issue_id=issue_id,
                liveness_silence_started_at=silence,
            )
        return "OK"

    @property
    def event_types(self) -> list[str]:
        return [a[3] for a in self.events]


class _Acquire:
    def __init__(self, conn: FakeSweepConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeSweepConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeSweepConn) -> None:
        self.conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


async def _sweep(conn: FakeSweepConn, *, limit: int = 500) -> Any:
    return await run_source_liveness_sweep(FakePool(conn), now=_NOW, limit=limit)  # type: ignore[arg-type]


async def test_silent_external_opens_source_silent_issue() -> None:
    src = _source(last_seen_at=_NOW - timedelta(seconds=1000))
    conn = FakeSweepConn([src])
    stats = await _sweep(conn)
    assert stats.opened == 1
    dedupe = f"source_liveness:{src['id']}:source_silent"
    assert conn.issues[dedupe]["status"] == "open"
    assert conn.issues[dedupe]["issue_type"] == "source_silent"
    assert conn.issues[dedupe]["account_id"] == src["account_id"]
    assert "reliability.issue_opened" in conn.event_types
    write = conn.health_writes[src["id"]]
    assert write["health_status"] == "silent"
    assert write["last_liveness_issue_id"] == conn.issues[dedupe]["id"]
    assert write["liveness_silence_started_at"] is not None


async def test_offline_collector_opens_collector_offline_issue() -> None:
    src = _source(
        source_kind="collector",
        external_camera_key=None,
        expected_interval_seconds=180,
        liveness_grace_seconds=60,
        last_seen_at=_NOW - timedelta(seconds=10),
        last_collector_heartbeat_at=_NOW - timedelta(seconds=600),
    )
    conn = FakeSweepConn([src])
    stats = await _sweep(conn)
    assert stats.opened == 1
    dedupe = f"source_liveness:{src['id']}:collector_offline"
    assert conn.issues[dedupe]["status"] == "open"
    assert conn.health_writes[src["id"]]["health_status"] == "collector_offline"


async def test_delayed_and_healthy_open_no_issue() -> None:
    delayed = _source(id=uuid4(), last_seen_at=_NOW - timedelta(seconds=700))
    healthy = _source(id=uuid4(), last_seen_at=_NOW - timedelta(seconds=30))
    conn = FakeSweepConn([delayed, healthy])
    stats = await _sweep(conn)
    assert stats.opened == 0
    assert conn.issues == {}
    assert conn.health_writes[delayed["id"]]["health_status"] == "delayed"
    assert conn.health_writes[healthy["id"]]["health_status"] == "healthy"


async def test_repeat_sweep_refreshes_not_duplicates() -> None:
    src = _source(last_seen_at=_NOW - timedelta(seconds=1000))
    conn = FakeSweepConn([src])
    first = await _sweep(conn)
    second = await _sweep(conn)
    assert first.opened == 1
    assert second.opened == 0  # refresh, no new open
    assert len(conn.issues) == 1
    # liveness_silence_started_at set once, unchanged on refresh.
    assert conn.event_types.count("reliability.issue_opened") == 1


async def test_recovered_source_resolves_issue() -> None:
    src = _source(last_seen_at=_NOW - timedelta(seconds=1000))
    conn = FakeSweepConn([src])
    await _sweep(conn)  # opens
    dedupe = f"source_liveness:{src['id']}:source_silent"
    assert conn.issues[dedupe]["status"] == "open"
    # Data resumes: bump last_seen to recent, sweep again.
    conn.sources[src["id"]]["last_seen_at"] = _NOW - timedelta(seconds=10)
    stats = await _sweep(conn)
    assert stats.resolved == 1
    assert conn.issues[dedupe]["status"] == "resolved"
    assert "reliability.issue_resolved" in conn.event_types
    write = conn.health_writes[src["id"]]
    assert write["health_status"] == "healthy"
    assert write["last_liveness_issue_id"] is None
    assert write["liveness_silence_started_at"] is None


async def test_disabled_sources_are_never_claimed() -> None:
    disabled = _source(
        id=uuid4(), liveness_enabled=False, last_seen_at=_NOW - timedelta(seconds=9999)
    )
    active = _source(id=uuid4(), last_seen_at=_NOW - timedelta(seconds=30))
    conn = FakeSweepConn([disabled, active])
    stats = await _sweep(conn)
    assert stats.evaluated == 1  # only the enabled source
    assert disabled["id"] not in conn.health_writes
    assert active["id"] in conn.health_writes


async def test_limit_is_respected() -> None:
    sources = [_source(id=uuid4()) for _ in range(3)]
    conn = FakeSweepConn(sources)
    stats = await _sweep(conn, limit=2)
    assert stats.evaluated == 2


async def test_cross_account_issue_keeps_source_account() -> None:
    a = _source(id=uuid4(), account_id=uuid4(), last_seen_at=_NOW - timedelta(seconds=1000))
    b = _source(id=uuid4(), account_id=uuid4(), last_seen_at=_NOW - timedelta(seconds=1000))
    conn = FakeSweepConn([a, b])
    await _sweep(conn)
    assert conn.issues[f"source_liveness:{a['id']}:source_silent"]["account_id"] == a["account_id"]
    assert conn.issues[f"source_liveness:{b['id']}:source_silent"]["account_id"] == b["account_id"]
