"""P6-D — route-level tests for ``GET /v1/cameras/{id}/processor-status/latest``.

The read endpoint that exposes the latest ``processor_statuses`` row (including
the ``payload.applied_geometry`` lineage block stamped by the P6-C analytics
worker). Pattern mirrors ``tests/test_perception_snapshots.py``'s read side:
TestClient + fake asyncpg pool, no real DB.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services.processor import _LATEST_PROCESSOR_STATUS_SQL

# ---------------------------------------------------------------------------
# Fake pool plumbing.
# ---------------------------------------------------------------------------


class AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    async def __aenter__(self) -> Any:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


class FakeStatusConn:
    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        status_row: dict[str, Any] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.status_row = status_row
        # Records the (account_id, camera_id, stream_session_id) args the
        # router passed to ``fetch_latest_processor_status``.
        self.status_queries: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras where id" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}
        if "from processor_statuses" in query:
            self.status_queries.append(args)
            return self.status_row
        return None


# ---------------------------------------------------------------------------
# Client builders.
# ---------------------------------------------------------------------------


def _caller_client(conn: Any, *, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def _caller_client_no_auth(conn: Any) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Row builders.
# ---------------------------------------------------------------------------


def _applied_geometry(*, revision: int = 9) -> dict[str, Any]:
    return {
        "source": "operator_saved",
        "revision": revision,
        "line_count": 2,
        "roi_count": 1,
    }


def _status_row(
    *,
    camera_id: UUID,
    payload: dict[str, Any] | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "camera_id": camera_id,
        "stream_session_id": uuid4(),
        "processor_id": "proc-1",
        "runtime": "deepstream",
        "package_id": "deepstream.analytics_probe.v0",
        "status": "processing",
        "fps_input": 30.0,
        "fps_processed": 29.8,
        "last_frame_at": now,
        "last_observation_at": now,
        "error_code": error_code,
        "error_message": None,
        "payload": payload if payload is not None else {"applied_geometry": _applied_geometry()},
        "created_at": now,
        "updated_at": now,
    }


_PATH = "/v1/cameras/{camera_id}/processor-status/latest"


# ===========================================================================
# Happy path.
# ===========================================================================


def test_get_latest_status_returns_full_row() -> None:
    account, camera = uuid4(), uuid4()
    row = _status_row(camera_id=camera)
    conn = FakeStatusConn(camera_account_id=account, status_row=row)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    status = r.json()["status"]
    # Account id is intentionally NOT echoed back (caller is already
    # account-scoped); pin that here so a future regression that adds it
    # back is caught.
    assert "account_id" not in status
    assert set(status.keys()) == {
        "id",
        "camera_id",
        "stream_session_id",
        "processor_id",
        "runtime",
        "package_id",
        "status",
        "fps_input",
        "fps_processed",
        "last_frame_at",
        "last_observation_at",
        "error_code",
        "error_message",
        "payload",
        "created_at",
        "updated_at",
    }
    assert status["camera_id"] == str(camera)


def test_applied_geometry_payload_flows_through() -> None:
    """The whole point of P6-D: the ``payload.applied_geometry`` lineage block
    P6-C stamps must reach the read response unchanged."""
    account, camera = uuid4(), uuid4()
    payload = {
        "applied_geometry": _applied_geometry(revision=11),
        "extra_runtime_counter": 42,
    }
    conn = FakeStatusConn(
        camera_account_id=account,
        status_row=_status_row(camera_id=camera, payload=payload),
    )
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()["status"]
    assert body["payload"] == payload
    assert body["payload"]["applied_geometry"]["source"] == "operator_saved"
    assert body["payload"]["applied_geometry"]["revision"] == 11


def test_error_code_round_trips() -> None:
    """Degraded-geometry status (``error_code=geometry_invalid``) must reach
    the caller — that's how a dashboard chip / verification script learns the
    processor fell back to probe defaults."""
    account, camera = uuid4(), uuid4()
    conn = FakeStatusConn(
        camera_account_id=account,
        status_row=_status_row(camera_id=camera, error_code="geometry_invalid"),
    )
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json()["status"]["error_code"] == "geometry_invalid"


# ===========================================================================
# Query scoping.
# ===========================================================================


def test_query_passes_account_and_camera() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeStatusConn(camera_account_id=account, status_row=_status_row(camera_id=camera))
    _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    # ``fetch_latest_processor_status`` is called with (account, camera, None).
    assert conn.status_queries == [(account, camera, None)]


def test_stream_session_id_query_param_narrows() -> None:
    account, camera, session = uuid4(), uuid4(), uuid4()
    conn = FakeStatusConn(camera_account_id=account, status_row=_status_row(camera_id=camera))
    _caller_client(conn, account_id=account).get(
        _PATH.format(camera_id=camera) + f"?stream_session_id={session}"
    )
    assert conn.status_queries == [(account, camera, session)]


# ===========================================================================
# Negative paths.
# ===========================================================================


def test_unknown_camera_returns_camera_not_found() -> None:
    """The camera-ownership check trumps the status check — an unknown camera
    must look identical to a known-but-other-account camera, so a probe can't
    enumerate camera IDs across tenants."""
    account = uuid4()
    conn = FakeStatusConn(camera_account_id=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    # Status query never runs when ownership fails.
    assert conn.status_queries == []


def test_cross_account_camera_returns_camera_not_found() -> None:
    caller_account, camera_owner = uuid4(), uuid4()
    conn = FakeStatusConn(camera_account_id=camera_owner)
    r = _caller_client(conn, account_id=caller_account).get(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.status_queries == []


def test_no_status_row_returns_processor_status_not_found() -> None:
    """No ``processor_statuses`` row for this camera (camera exists, processor
    has never reported) → 404 ``processor_status_not_found``. Mirrors the
    ``perception_snapshot_not_found`` convention; no synthetic default."""
    account, camera = uuid4(), uuid4()
    conn = FakeStatusConn(camera_account_id=account, status_row=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "processor_status_not_found"
    # The query ran (camera exists), it just returned nothing.
    assert conn.status_queries == [(account, camera, None)]


def test_unauthenticated_returns_401() -> None:
    conn = FakeStatusConn(camera_account_id=uuid4())
    r = _caller_client_no_auth(conn).get(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


# ===========================================================================
# Pin the production SQL — same regression-guard pattern as observations/
# perception read paths.
# ===========================================================================


def test_latest_processor_status_sql_pins_predicates() -> None:
    sql = _LATEST_PROCESSOR_STATUS_SQL
    assert "from processor_statuses" in sql
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
    # The optional stream_session_id filter must be a tri-state predicate so a
    # null arg matches every row (the unnarrowed case).
    assert "($3::uuid is null or stream_session_id = $3)" in sql
    # Picks the freshest row.
    assert "order by updated_at desc" in sql
    assert "limit 1" in sql
    # The ``account_id`` column is filtered against but never selected onto
    # the response (caller is already account-scoped).
    selected = sql.split("from processor_statuses")[0]
    assert "account_id" not in selected
