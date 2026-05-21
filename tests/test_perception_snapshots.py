"""P5-C — route-level tests for the latest perception snapshot endpoints.

Two endpoints, two auth surfaces:
- ``POST /v1/internal/processor/perception-snapshots`` — ``CurrentProcessor``
  (shared webhook secret); upserts one latest snapshot per camera.
- ``GET /v1/cameras/{camera_id}/perception/latest`` — ``CurrentCaller``
  (dashboard/agent); reads it back.

Pattern mirrors tests/test_processor.py (write side) and
tests/test_observations.py (read side): TestClient + a fake asyncpg pool,
no real DB.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services.processor import (
    _LATEST_PERCEPTION_SNAPSHOT_SQL,
    _UPSERT_PERCEPTION_SNAPSHOT_SQL,
)

_STREAMID = "publish:sess_0123456789abcdef"


# ---------------------------------------------------------------------------
# Shared fake pool plumbing.
# ---------------------------------------------------------------------------


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


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


# ---------------------------------------------------------------------------
# Write side — backs load_session_state + the perception-snapshot upsert.
# ---------------------------------------------------------------------------


class FakeSnapshotConn:
    def __init__(self, session_row: dict[str, Any] | None) -> None:
        self._session_row = session_row
        self.snapshot_upserts: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions where id" in query:
            return self._session_row
        if "insert into latest_perception_snapshots" in query:
            self.snapshot_upserts.append(args)
            now = datetime.now(tz=UTC)
            # args: account_id, camera_id, stream_session_id, processor_id,
            #       package_id, snapshot(json str), observed_at
            return {
                "id": uuid4(),
                "camera_id": args[1],
                "stream_session_id": args[2],
                "processor_id": args[3],
                "package_id": args[4],
                "snapshot": args[5],
                "observed_at": args[6],
                "updated_at": now,
            }
        return None


# ---------------------------------------------------------------------------
# Read side — camera-ownership check + latest-snapshot fetch.
# ---------------------------------------------------------------------------


class FakeReadConn:
    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        snapshot_row: dict[str, Any] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.snapshot_row = snapshot_row
        self.snapshot_queries: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras where id" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}
        if "from latest_perception_snapshots" in query:
            self.snapshot_queries.append(args)
            return self.snapshot_row
        return None


# ---------------------------------------------------------------------------
# Client builders.
# ---------------------------------------------------------------------------


def _processor_client(conn: Any) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


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


def _headers() -> dict[str, str]:
    secret = get_settings().processor_webhook_secret.get_secret_value()
    return {"X-Upstream-Webhook-Secret": secret}


def _session_row(*, session_id: UUID, account_id: UUID, camera_id: UUID) -> dict[str, Any]:
    return {
        "id": session_id,
        "account_id": account_id,
        "agent_id": uuid4(),
        "camera_id": camera_id,
        "state": "streaming",
        "expires_at": datetime.now(tz=UTC) + timedelta(hours=1),
        "session_token_jti": uuid4(),
        "streamid": _STREAMID,
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _snapshot_body(
    session_id: UUID,
    *,
    snapshot: dict[str, Any] | None = None,
    omit_observed_at: bool = False,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "stream_session_id": str(session_id),
        "processor_id": "proc-1",
        "package_id": "deepstream.analytics_probe.v0",
        "observed_at": datetime.now(tz=UTC).isoformat(),
        "snapshot": snapshot
        if snapshot is not None
        else {"frame": {"width": 1280, "height": 720}, "objects": []},
    }
    if omit_observed_at:
        del body["observed_at"]
    return body


def _snapshot_row(*, camera_id: UUID, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "camera_id": camera_id,
        "stream_session_id": uuid4(),
        "processor_id": "proc-1",
        "package_id": "deepstream.analytics_probe.v0",
        "snapshot": snapshot if snapshot is not None else {"objects": []},
        "observed_at": now,
        "updated_at": now,
    }


_PATH = "/v1/cameras/{camera_id}/perception/latest"
_INGEST = "/v1/internal/processor/perception-snapshots"


# ===========================================================================
# Write — POST /v1/internal/processor/perception-snapshots
# ===========================================================================


def test_perception_snapshot_write_succeeds_and_derives_tenant() -> None:
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSnapshotConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _processor_client(conn).post(_INGEST, json=_snapshot_body(session_id), headers=_headers())
    assert r.status_code == 200, r.text
    assert len(conn.snapshot_upserts) == 1
    args = conn.snapshot_upserts[0]
    # $1 account_id, $2 camera_id derived from the session row, not the body.
    assert args[0] == account_id
    assert args[1] == camera_id


def test_perception_snapshot_response_shape() -> None:
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSnapshotConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _processor_client(conn).post(_INGEST, json=_snapshot_body(session_id), headers=_headers())
    assert r.status_code == 200, r.text
    snap = r.json()["snapshot"]
    assert set(snap.keys()) == {
        "id",
        "camera_id",
        "stream_session_id",
        "processor_id",
        "package_id",
        "snapshot",
        "observed_at",
        "updated_at",
    }
    assert "account_id" not in snap
    assert snap["camera_id"] == str(camera_id)


def test_perception_snapshot_unknown_session_rejected() -> None:
    conn = FakeSnapshotConn(None)
    r = _processor_client(conn).post(_INGEST, json=_snapshot_body(uuid4()), headers=_headers())
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
    assert conn.snapshot_upserts == []


def test_perception_snapshot_requires_processor_secret() -> None:
    conn = FakeSnapshotConn(None)
    r = _processor_client(conn).post(_INGEST, json=_snapshot_body(uuid4()))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
    assert conn.snapshot_upserts == []


def test_perception_snapshot_rejects_wrong_secret() -> None:
    conn = FakeSnapshotConn(None)
    r = _processor_client(conn).post(
        _INGEST,
        json=_snapshot_body(uuid4()),
        headers={"X-Upstream-Webhook-Secret": "definitely-wrong"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_perception_snapshot_ingest_secret_does_not_authenticate() -> None:
    """The mediamtx ingest webhook secret must not authenticate a processor route."""
    conn = FakeSnapshotConn(None)
    r = _processor_client(conn).post(
        _INGEST,
        json=_snapshot_body(uuid4()),
        headers={
            "X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()
        },
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_perception_snapshot_missing_observed_at_rejected() -> None:
    session_id = uuid4()
    conn = FakeSnapshotConn(
        _session_row(session_id=session_id, account_id=uuid4(), camera_id=uuid4())
    )
    r = _processor_client(conn).post(
        _INGEST,
        json=_snapshot_body(session_id, omit_observed_at=True),
        headers=_headers(),
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


def test_perception_snapshot_arbitrary_snapshot_dict_round_trips() -> None:
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    rich = {
        "frame": {"width": 1280, "height": 720, "frame_num": 99, "fps": 30.0},
        "objects": [
            {
                "track_id": "0",
                "class_id": 2,
                "label": "person",
                "confidence": 0.82,
                "bbox": {"x": 410, "y": 120, "w": 180, "h": 520},
                "roi_labels": ["entry-roi"],
            }
        ],
        "geometry": {"rois": [{"id": "entry-roi", "points": [{"x": 64, "y": 36}]}]},
    }
    conn = FakeSnapshotConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _processor_client(conn).post(
        _INGEST, json=_snapshot_body(session_id, snapshot=rich), headers=_headers()
    )
    assert r.status_code == 200, r.text
    assert r.json()["snapshot"]["snapshot"] == rich
    # And the upsert received it as a json string ($6).
    assert json.loads(conn.snapshot_upserts[0][5]) == rich


def test_perception_snapshot_request_account_id_field_is_ignored() -> None:
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSnapshotConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    body = _snapshot_body(session_id)
    body["account_id"] = str(uuid4())  # spoof attempt — must be ignored
    body["camera_id"] = str(uuid4())
    r = _processor_client(conn).post(_INGEST, json=body, headers=_headers())
    assert r.status_code == 200, r.text
    args = conn.snapshot_upserts[0]
    assert args[0] == account_id  # session-derived, not the spoofed value
    assert args[1] == camera_id


# ===========================================================================
# Read — GET /v1/cameras/{camera_id}/perception/latest
# ===========================================================================


def test_get_latest_snapshot_owned_camera() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReadConn(camera_account_id=account, snapshot_row=_snapshot_row(camera_id=camera))
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json()["snapshot"]["camera_id"] == str(camera)


def test_get_latest_snapshot_missing_returns_404() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReadConn(camera_account_id=account, snapshot_row=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "perception_snapshot_not_found"


def test_get_latest_snapshot_unknown_camera_returns_404() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReadConn(camera_account_id=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.snapshot_queries == []


def test_get_latest_snapshot_cross_tenant_returns_404() -> None:
    caller_account, other_account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeReadConn(
        camera_account_id=other_account, snapshot_row=_snapshot_row(camera_id=camera)
    )
    r = _caller_client(conn, account_id=caller_account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.snapshot_queries == []


def test_get_latest_snapshot_unauthenticated_returns_401() -> None:
    conn = FakeReadConn(camera_account_id=uuid4())
    r = _caller_client_no_auth(conn).get(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


def test_get_latest_snapshot_query_scoped_to_caller_account() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReadConn(camera_account_id=account, snapshot_row=_snapshot_row(camera_id=camera))
    _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert conn.snapshot_queries[0] == (account, camera)


def test_get_latest_snapshot_omits_account_id() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReadConn(camera_account_id=account, snapshot_row=_snapshot_row(camera_id=camera))
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert "account_id" not in r.json()["snapshot"]


def test_get_latest_snapshot_payload_intact() -> None:
    account, camera = uuid4(), uuid4()
    rich = {
        "objects": [{"track_id": "7", "label": "person"}],
        "geometry": {"lines": [{"id": "entry-line"}]},
    }
    conn = FakeReadConn(
        camera_account_id=account,
        snapshot_row=_snapshot_row(camera_id=camera, snapshot=rich),
    )
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json()["snapshot"]["snapshot"] == rich


# ===========================================================================
# Pin the production SQL.
# ===========================================================================


def test_upsert_perception_snapshot_sql_pins_conflict_target() -> None:
    sql = _UPSERT_PERCEPTION_SNAPSHOT_SQL
    assert "insert into latest_perception_snapshots" in sql
    assert "on conflict (camera_id) do update set" in sql
    assert "returning" in sql


def test_latest_perception_snapshot_sql_pins_predicates() -> None:
    sql = _LATEST_PERCEPTION_SNAPSHOT_SQL
    assert "from latest_perception_snapshots" in sql
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
