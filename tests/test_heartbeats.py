from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.errors import ForbiddenError
from upstream_api.schemas.heartbeats import Heartbeat
from upstream_api.security.deps import AgentAuth, current_agent
from upstream_api.services import usage as usage_service
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


# ----------------------------------------------------------------------------
# End-to-end POST /v1/heartbeats tests.
#
# Exercises:
#   - the agent_id ownership check on the router
#   - the usage_events insert path (metadata=None must bind '{}', not NULL)
# ----------------------------------------------------------------------------


class _RouterFakeConn:
    """End-to-end fake for POST /v1/heartbeats.

    Captures the bound args of every ``execute`` call so tests can assert that
    no heartbeat insert was issued on the unauthorized path, and that the
    usage_events insert never binds NULL metadata.
    """

    def __init__(
        self,
        *,
        agent_owner_account_id: UUID | None,
        usage_table_exists: bool = True,
    ) -> None:
        self._agent_owner_account_id = agent_owner_account_id
        self._usage_table_exists = usage_table_exists
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrows: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchval(self, query: str, *_args: Any) -> Any:
        if "to_regclass" in query:
            return self._usage_table_exists
        return None

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrows.append((query, args))
        if "from agents" in query:
            if self._agent_owner_account_id is None:
                return None
            return {"account_id": self._agent_owner_account_id}
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetches.append((query, args))
        return []

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        return "OK"

    async def executemany(self, query: str, args: list[tuple[Any, ...]]) -> None:
        self.executemany_calls.append((query, args))

    @property
    def inserted_heartbeat_rows(self) -> int:
        total = 0
        for query, args in self.executemany_calls:
            if "insert into heartbeats" in query:
                total += len(args)
        return total

    @property
    def usage_event_inserts(self) -> list[tuple[Any, ...]]:
        return [args for query, args in self.executes if "insert into usage_events" in query]


class _AcquireCtx:
    def __init__(self, conn: _RouterFakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RouterFakeConn:
        return self._conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _RouterFakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


def _heartbeat_payload(camera_id: UUID | None = None) -> dict[str, Any]:
    return {
        "heartbeats": [
            {
                "camera_id": str(camera_id) if camera_id is not None else None,
                "session_id": None,
                "status": "online",
                "fps": 30.0,
                "bitrate_kbps": 3000,
                "width": 1280,
                "height": 720,
                "codec": "h264",
                "agent_version": "0.1.0",
                "reported_at": datetime.now(tz=UTC).isoformat(),
            }
        ]
    }


def _build_client(
    conn: _RouterFakeConn,
    *,
    account_id: UUID,
) -> TestClient:
    app = create_app()
    agent_auth = AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",))
    app.dependency_overrides[get_pool] = lambda: _FakePool(conn)
    app.dependency_overrides[current_agent] = lambda: agent_auth
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_usage_probe_cache() -> None:
    usage_service.reset_usage_table_cache()


def test_submit_heartbeats_returns_200_for_valid_owned_agent() -> None:
    account_id = uuid4()
    agent_id = uuid4()
    conn = _RouterFakeConn(agent_owner_account_id=account_id)
    client = _build_client(conn, account_id=account_id)

    response = client.post(
        f"/v1/heartbeats?agent_id={agent_id}",
        json=_heartbeat_payload(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 1
    assert "server_time" in body

    # The heartbeat batch must have been inserted, exactly one row.
    assert conn.inserted_heartbeat_rows == 1

    # The usage_events insert must bind '{}' for metadata, never NULL —
    # otherwise the production DB raises NotNullViolationError and the request
    # fails with 500.
    assert len(conn.usage_event_inserts) == 1
    bound_args = conn.usage_event_inserts[0]
    bound_metadata = bound_args[-1]
    assert bound_metadata is not None
    assert bound_metadata == "{}"


def test_submit_heartbeats_returns_404_when_agent_unknown() -> None:
    account_id = uuid4()
    agent_id = uuid4()
    # Agent does not exist at all.
    conn = _RouterFakeConn(agent_owner_account_id=None)
    client = _build_client(conn, account_id=account_id)

    response = client.post(
        f"/v1/heartbeats?agent_id={agent_id}",
        json=_heartbeat_payload(),
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "agent_not_found"
    # No heartbeat row should have been inserted on the unauthorized path.
    assert conn.inserted_heartbeat_rows == 0
    assert conn.usage_event_inserts == []


def test_submit_heartbeats_returns_404_when_agent_belongs_to_another_account() -> None:
    caller_account = uuid4()
    other_account = uuid4()
    agent_id = uuid4()
    # Agent exists but is owned by a different account.
    conn = _RouterFakeConn(agent_owner_account_id=other_account)
    client = _build_client(conn, account_id=caller_account)

    response = client.post(
        f"/v1/heartbeats?agent_id={agent_id}",
        json=_heartbeat_payload(),
    )

    # 404, not 403: the response shape must be identical to "unknown agent"
    # so cross-account existence is not leaked.
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "agent_not_found"
    assert conn.inserted_heartbeat_rows == 0
    assert conn.usage_event_inserts == []
