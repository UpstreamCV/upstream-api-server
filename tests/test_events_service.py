"""Module 6 M6.1 — canonical event ledger service tests.

Four layers:

1. ``workflow_run_event_type`` / ``workflow_run_dedupe_key`` — pure mapping.
2. ``insert_event`` — service-level with a fake conn (insert, dedupe, defaults,
   sanitization, required-field validation).
3. Atomicity — an event staged inside a transaction does not survive a
   rollback (it rides the caller's transaction; ``insert_event`` opens none).
4. Migration shape pin — the table/indexes/RLS/grant the ledger relies on.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from upstream_api.services.events import (
    insert_event,
    workflow_run_dedupe_key,
    workflow_run_event_type,
)
from upstream_api.services.workflow_runs import finalize_workflow_run

_NOW = datetime(2026, 6, 14, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Pure mapping: (status, action_result) -> event_type.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "action_result", "expected"),
    [
        # The two combinations M6.1's finalize seam actually produces today.
        ("completed", "alert_created", "alert.created"),
        ("suppressed", "alert_suppressed_cooldown", "workflow.cooldown_suppressed"),
        # Defined for future producers routing through the same seam.
        ("completed", "no_action", "workflow.succeeded"),
        ("completed", None, "workflow.succeeded"),
        ("failed", "sequence_timeout", "workflow.timed_out"),
        ("failed", "error", "workflow.failed"),
        ("active", None, "workflow.started"),
        # Anything unrecognized still yields a non-empty, non-dropping type.
        ("weird", "huh", "workflow.finalized"),
    ],
)
def test_workflow_run_event_type(
    status: str, action_result: str | None, expected: str
) -> None:
    assert workflow_run_event_type(status, action_result) == expected


def test_dedupe_key_is_stable_and_distinguishes_outcomes() -> None:
    run_id = uuid4()
    key = workflow_run_dedupe_key(run_id, "completed", "alert_created")
    # Stable for the same finalization.
    assert key == workflow_run_dedupe_key(run_id, "completed", "alert_created")
    assert key == f"workflow_run:{run_id}:completed:alert_created"
    # Different outcomes -> different keys (a suppressed re-fire is its own row).
    assert key != workflow_run_dedupe_key(run_id, "suppressed", "alert_suppressed_cooldown")
    # None action_result renders explicitly, never as "None"/empty.
    assert workflow_run_dedupe_key(run_id, "active", None).endswith(":active:none")


# ---------------------------------------------------------------------------
# insert_event — fake conn modeling ON CONFLICT (account_id, dedupe_key).
# ---------------------------------------------------------------------------


class FakeEventConn:
    """Backs the events insert, modeling the unique (account_id, dedupe_key)
    constraint: a repeated key returns no row (ON CONFLICT DO NOTHING)."""

    def __init__(self) -> None:
        self.inserts: list[tuple[Any, ...]] = []
        self._seen: set[tuple[Any, str]] = set()
        self._seq = 0

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "insert into events" in query
        account_id, dedupe_key = args[0], args[12]
        if (account_id, dedupe_key) in self._seen:
            return None
        self._seen.add((account_id, dedupe_key))
        self.inserts.append(args)
        self._seq += 1
        return {
            "id": uuid4(),
            "seq": self._seq,
            "account_id": account_id,
            "camera_id": args[1],
            "session_id": args[2],
            "event_type": args[3],
            "schema_version": args[4],
            "source": args[5],
            "source_observation_id": args[6],
            "workflow_run_id": args[7],
            "source_alert_id": args[8],
            "source_finding_id": args[9],
            "rule_id": args[10],
            "workflow_id": args[11],
            "dedupe_key": dedupe_key,
            "occurred_at": args[13],
            "created_at": _NOW,
            "payload": json.loads(args[14]),
            "evidence": json.loads(args[15]),
            "reliability_context": json.loads(args[16]) if args[16] is not None else None,
        }


async def test_insert_event_inserts_a_valid_row() -> None:
    conn = FakeEventConn()
    account, camera, run = uuid4(), uuid4(), uuid4()
    row = await insert_event(
        conn,
        account_id=account,
        event_type="alert.created",
        source="rule",
        dedupe_key=f"workflow_run:{run}:completed:alert_created",
        occurred_at=_NOW,
        camera_id=camera,
        workflow_run_id=run,
        payload={"status": "completed"},
        evidence={"line_id": "line-1"},
    )
    assert row is not None
    assert row["event_type"] == "alert.created"
    assert row["account_id"] == account
    assert row["camera_id"] == camera
    assert row["workflow_run_id"] == run
    assert row["schema_version"] == 1
    assert row["payload"] == {"status": "completed"}
    assert row["evidence"] == {"line_id": "line-1"}
    assert row["reliability_context"] is None


async def test_duplicate_dedupe_key_does_not_insert_second_event() -> None:
    conn = FakeEventConn()
    account, run = uuid4(), uuid4()
    key = f"workflow_run:{run}:completed:alert_created"
    first = await insert_event(
        conn, account_id=account, event_type="alert.created", source="rule",
        dedupe_key=key, occurred_at=_NOW,
    )
    second = await insert_event(
        conn, account_id=account, event_type="alert.created", source="rule",
        dedupe_key=key, occurred_at=_NOW,
    )
    assert first is not None
    assert second is None  # ON CONFLICT DO NOTHING -> no row returned
    assert len(conn.inserts) == 1


async def test_same_dedupe_key_different_account_is_not_a_conflict() -> None:
    conn = FakeEventConn()
    run = uuid4()
    key = f"workflow_run:{run}:completed:alert_created"
    a = await insert_event(
        conn, account_id=uuid4(), event_type="alert.created", source="rule",
        dedupe_key=key, occurred_at=_NOW,
    )
    b = await insert_event(
        conn, account_id=uuid4(), event_type="alert.created", source="rule",
        dedupe_key=key, occurred_at=_NOW,
    )
    assert a is not None and b is not None
    assert len(conn.inserts) == 2


async def test_payload_and_evidence_default_to_empty_objects() -> None:
    conn = FakeEventConn()
    await insert_event(
        conn, account_id=uuid4(), event_type="alert.created", source="rule",
        dedupe_key="k1", occurred_at=_NOW,
    )
    args = conn.inserts[0]
    assert args[14] == "{}"  # payload
    assert args[15] == "{}"  # evidence
    assert args[16] is None  # reliability_context


async def test_secret_keys_are_stripped_from_payload_and_evidence() -> None:
    conn = FakeEventConn()
    row = await insert_event(
        conn, account_id=uuid4(), event_type="alert.created", source="rule",
        dedupe_key="k1", occurred_at=_NOW,
        payload={"api_token": "ups_secret", "zone_id": "z-1"},
        evidence={"password": "hunter2", "track_id": "7"},
    )
    assert row is not None
    assert "api_token" not in row["payload"]
    assert row["payload"]["zone_id"] == "z-1"
    assert "password" not in row["evidence"]
    assert row["evidence"]["track_id"] == "7"


@pytest.mark.parametrize("bad", [{"event_type": ""}, {"source": ""}, {"dedupe_key": ""}])
async def test_blank_required_fields_raise_value_error(bad: dict[str, str]) -> None:
    conn = FakeEventConn()
    kwargs: dict[str, Any] = {
        "account_id": uuid4(),
        "event_type": "alert.created",
        "source": "rule",
        "dedupe_key": "k1",
        "occurred_at": _NOW,
    }
    kwargs.update(bad)
    with pytest.raises(ValueError):
        await insert_event(conn, **kwargs)


async def test_missing_required_kwarg_raises_type_error() -> None:
    conn = FakeEventConn()
    with pytest.raises(TypeError):
        # account_id omitted entirely.
        await insert_event(  # type: ignore[call-arg]
            conn, event_type="alert.created", source="rule", dedupe_key="k1", occurred_at=_NOW
        )


# ---------------------------------------------------------------------------
# Atomicity: an event staged inside a transaction does not survive a rollback.
# ---------------------------------------------------------------------------


class _Txn:
    """Models a savepoint: on rollback, only rows staged SINCE this savepoint
    began are discarded (so a nested savepoint — e.g. the M6.4 enqueue — never
    nukes the parent's staged rows); only the top-level commit flushes."""

    def __init__(self, conn: TxnConn) -> None:
        self.conn = conn
        self._mark = 0

    async def __aenter__(self) -> _Txn:
        self._mark = len(self.conn.staged)
        self.conn.depth += 1
        return self

    async def __aexit__(self, exc_type: Any, *_rest: Any) -> bool:
        self.conn.depth -= 1
        if exc_type is not None:
            del self.conn.staged[self._mark :]  # rollback to savepoint
        elif self.conn.depth == 0:
            self.conn.committed.extend(self.conn.staged)
            self.conn.staged.clear()
        return False


class TxnConn:
    """Models that ``insert_event`` writes into — and only into — the caller's
    transaction: staged rows commit on a clean exit and vanish on rollback.
    ``insert_event`` itself opens no transaction, so the outer ``transaction()``
    is the only thing that can commit its row."""

    def __init__(self) -> None:
        self.staged: list[tuple[Any, ...]] = []
        self.committed: list[tuple[Any, ...]] = []
        self.depth = 0

    def transaction(self) -> _Txn:
        return _Txn(self)

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        # M6.4 enqueue looks up webhook destinations — none in this fake.
        assert "from event_destinations" in query
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any]:
        assert "insert into events" in query
        self.staged.append(args)
        return {"id": uuid4(), "seq": len(self.committed) + len(self.staged), "event_type": args[3]}


async def test_event_is_rolled_back_with_the_outer_transaction() -> None:
    conn = TxnConn()
    with pytest.raises(RuntimeError):
        async with conn.transaction():
            await insert_event(
                conn, account_id=uuid4(), event_type="alert.created", source="rule",
                dedupe_key="k1", occurred_at=_NOW,
            )
            assert conn.committed == []  # not committed yet
            raise RuntimeError("producer failed after the event insert")
    # The rollback dropped the staged event — it does not remain.
    assert conn.committed == []
    assert conn.staged == []


async def test_event_commits_with_a_clean_transaction() -> None:
    conn = TxnConn()
    async with conn.transaction():
        await insert_event(
            conn, account_id=uuid4(), event_type="alert.created", source="rule",
            dedupe_key="k1", occurred_at=_NOW,
        )
    assert len(conn.committed) == 1


# ---------------------------------------------------------------------------
# finalize_workflow_run -> canonical event (the M6.1 emit seam).
# ---------------------------------------------------------------------------


class _NoopTxn:
    """A savepoint that commits on clean exit and propagates (never suppresses)
    on error — like asyncpg's transaction, minus persistence modeling."""

    async def __aenter__(self) -> _NoopTxn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class FakeFinalizeConn:
    """Backs finalize_workflow_run: the run UPDATE plus the canonical-event
    insert, modeling ON CONFLICT (account_id, dedupe_key)."""

    def __init__(self) -> None:
        self.run_updates: list[tuple[Any, ...]] = []
        self.event_inserts: list[tuple[Any, ...]] = []
        self._seen: set[tuple[Any, str]] = set()

    def transaction(self) -> _NoopTxn:
        return _NoopTxn()

    async def execute(self, query: str, *args: Any) -> str:
        assert "update workflow_runs" in query
        self.run_updates.append(args)
        return "UPDATE 1"

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "insert into events" in query
        account_id, dedupe_key = args[0], args[12]
        if (account_id, dedupe_key) in self._seen:
            return None
        self._seen.add((account_id, dedupe_key))
        self.event_inserts.append(args)
        return {"id": uuid4(), "seq": len(self.event_inserts)}


async def test_finalize_emits_one_event_with_references() -> None:
    conn = FakeFinalizeConn()
    account, camera, run, obs, alert = uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    await finalize_workflow_run(
        conn,
        workflow_run_id=run,
        status="completed",
        action_result="alert_created",
        account_id=account,
        occurred_at=_NOW,
        camera_id=camera,
        source="rule",
        rule_id=uuid4(),
        source_observation_id=obs,
        source_alert_id=alert,
        trigger_type="processor.zone_entered",
        evidence={"zone_id": "z-1"},
        payload={"rule": {"rule_type": "zone_entered"}},
    )
    assert len(conn.run_updates) == 1  # the run is still finalized
    assert len(conn.event_inserts) == 1  # exactly one event
    args = conn.event_inserts[0]
    assert args[3] == "alert.created"  # event_type
    assert args[6] == obs  # source_observation_id
    assert args[7] == run  # workflow_run_id
    assert args[8] == alert  # source_alert_id


async def test_finalize_twice_does_not_duplicate_event() -> None:
    conn = FakeFinalizeConn()
    kwargs: dict[str, Any] = {
        "workflow_run_id": uuid4(),
        "status": "completed",
        "action_result": "alert_created",
        "account_id": uuid4(),
        "occurred_at": _NOW,
        "source": "rule",
    }
    await finalize_workflow_run(conn, **kwargs)
    await finalize_workflow_run(conn, **kwargs)
    assert len(conn.run_updates) == 2  # the UPDATE is idempotent, runs twice
    assert len(conn.event_inserts) == 1  # but the event is deduped


async def test_finalize_without_event_context_emits_nothing() -> None:
    """Backward-compatible: omitting account_id/occurred_at finalizes the run
    exactly as before and emits no event."""
    conn = FakeFinalizeConn()
    await finalize_workflow_run(
        conn, workflow_run_id=uuid4(), status="completed", action_result="alert_created"
    )
    assert len(conn.run_updates) == 1
    assert conn.event_inserts == []


async def test_finalize_suppressed_emits_cooldown_event() -> None:
    conn = FakeFinalizeConn()
    await finalize_workflow_run(
        conn,
        workflow_run_id=uuid4(),
        status="suppressed",
        action_result="alert_suppressed_cooldown",
        account_id=uuid4(),
        occurred_at=_NOW,
        source="rule",
    )
    assert len(conn.event_inserts) == 1
    assert conn.event_inserts[0][3] == "workflow.cooldown_suppressed"


async def test_event_emission_failure_never_breaks_finalization() -> None:
    """Best-effort: if the ledger insert raises (e.g. the table isn't migrated
    yet), the run is still finalized and no exception escapes."""

    class RaisingConn(FakeFinalizeConn):
        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            raise RuntimeError("relation \"events\" does not exist")

    conn = RaisingConn()
    await finalize_workflow_run(
        conn,
        workflow_run_id=uuid4(),
        status="completed",
        action_result="alert_created",
        account_id=uuid4(),
        occurred_at=_NOW,
        source="rule",
    )
    assert len(conn.run_updates) == 1  # finalized despite the ledger error
    assert conn.event_inserts == []


# ---------------------------------------------------------------------------
# Migration shape pin.
# ---------------------------------------------------------------------------


def test_migration_file_pins_ledger_shape() -> None:
    root = Path(__file__).resolve().parents[1]
    migration = root / "supabase" / "migrations" / "20260619120000_events_ledger.sql"
    sql = migration.read_text().lower()
    assert "create table events" in sql
    # Monotonic cursor + the exactly-once dedupe guard.
    assert "seq" in sql
    assert "bigserial not null" in sql
    assert "unique index events_account_seq_idx" in sql
    assert "on events(account_id, seq)" in sql
    assert "unique index events_account_dedupe_idx" in sql
    assert "on events(account_id, dedupe_key)" in sql
    # Filtered-read + workflow-run lookup indexes.
    assert "events_account_type_seq_idx" in sql
    assert "events_camera_seq_idx" in sql
    assert "events_workflow_run_idx" in sql
    # RLS member-select + grant follow the dashboard-table convention; no
    # insert/update/delete policy (service-role writes only).
    assert "enable row level security" in sql
    assert "events_member_select" in sql
    assert "grant select on public.events to authenticated" in sql
    assert "for select" in sql
    assert "for insert" not in sql
    assert "for update" not in sql
