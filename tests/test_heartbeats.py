from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from upstream_api.errors import ForbiddenError
from upstream_api.schemas.heartbeats import Heartbeat
from upstream_api.services.heartbeats import record_batch


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Records call sites; lets a test choose which camera/session IDs are 'owned'."""

    def __init__(
        self,
        owned_camera_ids: set[UUID] | None = None,
        owned_session_ids: set[UUID] | None = None,
    ) -> None:
        self.owned_camera_ids = owned_camera_ids or set()
        self.owned_session_ids = owned_session_ids or set()
        self.executemany_args: list[tuple[Any, ...]] = []
        self.executes: list[str] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from cameras" in query:
            ids = args[0]
            return [{"id": i} for i in ids if i in self.owned_camera_ids]
        if "from stream_sessions" in query:
            ids = args[0]
            return [{"id": i} for i in ids if i in self.owned_session_ids]
        return []

    async def executemany(self, query: str, args: list[tuple[Any, ...]]) -> None:
        self.executemany_args.append((query, args))

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append(query)
        return "OK"


def _hb(*, camera_id: UUID | None = None, session_id: UUID | None = None) -> Heartbeat:
    return Heartbeat(
        camera_id=camera_id,
        session_id=session_id,
        status="streaming",
        fps=30.0,
        bitrate_kbps=3000,
        width=1280,
        height=720,
        codec="h264",
        agent_version="0.1.0",
        reported_at=datetime.now(tz=UTC),
    )


async def test_record_batch_inserts_when_all_resources_owned() -> None:
    cam = uuid4()
    sess = uuid4()
    conn = FakeConn(owned_camera_ids={cam}, owned_session_ids={sess})
    items = [_hb(camera_id=cam, session_id=sess), _hb(camera_id=cam)]
    result = await record_batch(
        conn,
        account_id=uuid4(),
        agent_id=uuid4(),
        items=items,
    )
    assert result.accepted == 2
    # one executemany for the inserts, then agents.last_seen_at update, then cameras.updated_at
    assert len(conn.executemany_args) == 1
    assert any("update agents" in q for q in conn.executes)
    assert any("update cameras" in q for q in conn.executes)


async def test_record_batch_rejects_unknown_camera() -> None:
    owned_cam = uuid4()
    foreign_cam = uuid4()
    conn = FakeConn(owned_camera_ids={owned_cam})
    items = [_hb(camera_id=owned_cam), _hb(camera_id=foreign_cam)]
    with pytest.raises(ForbiddenError) as exc:
        await record_batch(
            conn,
            account_id=uuid4(),
            agent_id=uuid4(),
            items=items,
        )
    assert exc.value.code == "forbidden_resource"
    assert "missing_camera_ids" in exc.value.details


async def test_record_batch_rejects_unknown_session() -> None:
    cam = uuid4()
    foreign_sess = uuid4()
    conn = FakeConn(owned_camera_ids={cam}, owned_session_ids=set())
    items = [_hb(camera_id=cam, session_id=foreign_sess)]
    with pytest.raises(ForbiddenError) as exc:
        await record_batch(
            conn,
            account_id=uuid4(),
            agent_id=uuid4(),
            items=items,
        )
    assert exc.value.code == "forbidden_resource"
    assert "missing_session_ids" in exc.value.details


async def test_record_batch_allows_heartbeats_without_camera_or_session() -> None:
    conn = FakeConn()
    items = [_hb(), _hb(), _hb()]
    result = await record_batch(
        conn,
        account_id=uuid4(),
        agent_id=uuid4(),
        items=items,
    )
    assert result.accepted == 3
    assert result.distinct_camera_ids == ()
