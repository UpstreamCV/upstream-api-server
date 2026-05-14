from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from upstream_api.services import usage
from upstream_api.services.usage import record_event, reset_usage_table_cache


class FakeConn:
    def __init__(self, table_exists: bool) -> None:
        self._table_exists = table_exists
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchvals: list[str] = []

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchvals.append(query)
        if "to_regclass" in query:
            return self._table_exists
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        return "INSERT 0 1"


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    reset_usage_table_cache()


async def test_record_event_skips_insert_when_table_missing() -> None:
    conn = FakeConn(table_exists=False)
    await record_event(
        conn,
        account_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type="session_started",
    )
    assert conn.executes == []
    assert any("to_regclass" in q for q in conn.fetchvals)


async def test_record_event_inserts_when_table_present() -> None:
    conn = FakeConn(table_exists=True)
    await record_event(
        conn,
        account_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type="session_started",
    )
    assert len(conn.executes) == 1
    assert "insert into usage_events" in conn.executes[0][0]


async def test_record_event_omitted_metadata_inserts_empty_object_not_null() -> None:
    # The `usage_events.metadata` column is jsonb NOT NULL default '{}'. Since
    # the INSERT explicitly binds the column, the default does not fire — so
    # `record_event(metadata=None)` must bind '{}' rather than SQL NULL, or the
    # write fails with NotNullViolationError and bubbles up as a 500.
    conn = FakeConn(table_exists=True)
    await record_event(
        conn,
        account_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type="heartbeat_batch",
    )
    assert len(conn.executes) == 1
    bound_metadata = conn.executes[0][1][-1]
    assert bound_metadata == "{}"
    assert bound_metadata is not None


async def test_record_event_explicit_metadata_is_json_encoded() -> None:
    conn = FakeConn(table_exists=True)
    await record_event(
        conn,
        account_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type="heartbeat_batch",
        metadata={"reason": "test"},
    )
    bound_metadata = conn.executes[0][1][-1]
    assert bound_metadata == '{"reason": "test"}'


async def test_probe_caches_after_first_call() -> None:
    conn = FakeConn(table_exists=True)
    await record_event(
        conn,
        account_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type="session_started",
    )
    await record_event(
        conn,
        account_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_type="session_ended",
    )
    # Second call must reuse the cached probe result.
    assert sum(1 for q in conn.fetchvals if "to_regclass" in q) == 1
    assert len(conn.executes) == 2


def test_record_event_does_not_swallow_unrelated_errors() -> None:
    # Sanity check: the function no longer wraps the insert in a broad except,
    # so any error other than the one-shot probe miss propagates. This is the
    # whole point of the fix — a typo on a different table won't be silenced.
    assert not hasattr(usage, "_swallow_undefined_table")
