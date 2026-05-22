"""P6-A — route-level tests for the camera geometry config endpoints.

Two endpoints, one auth surface (``CurrentCaller`` — dashboard/agent):
- ``GET  /v1/cameras/{camera_id}/geometry-config`` — read saved or default.
- ``PUT  /v1/cameras/{camera_id}/geometry-config`` — full-replace upsert.

Pattern mirrors tests/test_perception_snapshots.py: TestClient + a fake
asyncpg pool, no real DB. ``revision`` is the DB row revision (0 = unsaved
default, 1 = first save, +1 per save); ``config.version`` is the unrelated
schema version (always 1).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services.geometry import (
    _SELECT_GEOMETRY_CONFIG_SQL,
    _UPSERT_GEOMETRY_CONFIG_SQL,
)

_PATH = "/v1/cameras/{camera_id}/geometry-config"
_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260522120000_camera_geometry_configs.sql"
)


# ---------------------------------------------------------------------------
# Fake pool plumbing.
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


class FakeGeometryConn:
    """Backs the camera-ownership check + the geometry select / upsert.

    ``camera_account_id=None`` simulates a camera that does not exist.
    ``geometry_row`` is the currently-stored config row (``None`` = never
    saved); the upsert simulation re-implements the revision bump.
    """

    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        geometry_row: dict[str, Any] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.geometry_row = geometry_row
        self.select_queries: list[tuple[Any, ...]] = []
        self.upserts: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras where id" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}
        if "insert into camera_geometry_configs" in query:
            self.upserts.append(args)
            now = datetime.now(tz=UTC)
            prior = self.geometry_row
            # args: $1 account_id, $2 camera_id, $3 config(json str).
            row = {
                "id": prior["id"] if prior is not None else uuid4(),
                "camera_id": args[1],
                "revision": (prior["revision"] + 1) if prior is not None else 1,
                "config": args[2],
                "created_at": prior["created_at"] if prior is not None else now,
                "updated_at": now,
            }
            self.geometry_row = row
            return row
        if "from camera_geometry_configs" in query:
            self.select_queries.append(args)
            return self.geometry_row
        return None


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
# Config fixtures / helpers.
# ---------------------------------------------------------------------------


def _config(**overrides: Any) -> dict[str, Any]:
    """A valid geometry config dict; override individual top-level keys."""
    base: dict[str, Any] = {
        "version": 1,
        "frame": {"width": 1280, "height": 720, "coordinate_space": "normalized"},
        "rois": [
            {
                "id": "entry-roi",
                "label": "Entry ROI",
                "type": "polygon",
                "points": [
                    {"x": 0.05, "y": 0.05},
                    {"x": 0.95, "y": 0.05},
                    {"x": 0.95, "y": 0.95},
                    {"x": 0.05, "y": 0.95},
                ],
                "enabled": True,
            }
        ],
        "lines": [
            {
                "id": "entry-line",
                "label": "Entry Line",
                "points": [{"x": 0.25, "y": 0.5}, {"x": 0.75, "y": 0.5}],
                "direction": "any",
                "enabled": True,
            }
        ],
    }
    base.update(overrides)
    return base


def _roi(idx: int) -> dict[str, Any]:
    return {
        "id": f"roi-{idx}",
        "points": [{"x": 0.1, "y": 0.1}, {"x": 0.2, "y": 0.1}, {"x": 0.2, "y": 0.2}],
    }


def _line(idx: int) -> dict[str, Any]:
    return {"id": f"line-{idx}", "points": [{"x": 0.1, "y": 0.5}, {"x": 0.9, "y": 0.5}]}


def _stored_row(*, camera_id: UUID, revision: int = 1) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "camera_id": camera_id,
        "revision": revision,
        "config": _config(),
        "created_at": now,
        "updated_at": now,
    }


# ===========================================================================
# GET — default fallback + saved read.
# ===========================================================================


def test_get_returns_default_when_no_saved_config() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    gc = r.json()["geometry_config"]
    assert gc["is_default"] is True
    assert gc["revision"] == 0
    assert gc["id"] is None
    assert gc["config"]["version"] == 1


def test_get_default_contains_probe_roi_and_line() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    cfg = r.json()["geometry_config"]["config"]
    assert [roi["id"] for roi in cfg["rois"]] == ["entry-roi"]
    assert [line["id"] for line in cfg["lines"]] == ["entry-line"]


def test_get_returns_saved_config() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(
        camera_account_id=account, geometry_row=_stored_row(camera_id=camera, revision=4)
    )
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    gc = r.json()["geometry_config"]
    assert gc["is_default"] is False
    assert gc["revision"] == 4
    assert gc["config"] == _config()


def test_get_query_scoped_to_caller_account_and_camera() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=_stored_row(camera_id=camera))
    _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert conn.select_queries[0] == (account, camera)


def test_get_unknown_camera_returns_404() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.select_queries == []


def test_get_cross_tenant_camera_returns_404() -> None:
    caller_account, other_account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeGeometryConn(
        camera_account_id=other_account, geometry_row=_stored_row(camera_id=camera)
    )
    r = _caller_client(conn, account_id=caller_account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.select_queries == []


def test_get_unauthenticated_returns_401() -> None:
    conn = FakeGeometryConn(camera_account_id=uuid4())
    r = _caller_client_no_auth(conn).get(_PATH.format(camera_id=uuid4()))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


# ===========================================================================
# PUT — upsert + revision bump.
# ===========================================================================


def test_put_inserts_new_config() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=None)
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 200, r.text
    gc = r.json()["geometry_config"]
    assert gc["is_default"] is False
    assert gc["revision"] == 1
    assert len(conn.upserts) == 1


def test_put_response_round_trips_config() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=None)
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 200, r.text
    assert r.json()["geometry_config"]["config"] == _config()


def test_put_updates_existing_and_bumps_revision() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(
        camera_account_id=account, geometry_row=_stored_row(camera_id=camera, revision=4)
    )
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 200, r.text
    assert r.json()["geometry_config"]["revision"] == 5


def test_put_bumps_revision_across_repeated_saves() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=None)
    client = _caller_client(conn, account_id=account)
    first = client.put(_PATH.format(camera_id=camera), json={"config": _config()})
    second = client.put(_PATH.format(camera_id=camera), json={"config": _config()})
    assert first.json()["geometry_config"]["revision"] == 1
    assert second.json()["geometry_config"]["revision"] == 2


def test_put_scoped_to_caller_account_ignores_body_tenant_fields() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account, geometry_row=None)
    body = {
        "config": _config(),
        "account_id": str(uuid4()),  # spoof attempt — must be ignored
        "camera_id": str(uuid4()),
    }
    r = _caller_client(conn, account_id=account).put(_PATH.format(camera_id=camera), json=body)
    assert r.status_code == 200, r.text
    args = conn.upserts[0]
    # $1 account_id derived from the caller, $2 camera_id from the path.
    assert args[0] == account
    assert args[1] == camera


def test_put_unknown_camera_returns_404() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=None)
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.upserts == []


def test_put_cross_tenant_camera_returns_404() -> None:
    caller_account, other_account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=other_account)
    r = _caller_client(conn, account_id=caller_account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.upserts == []


def test_put_unauthenticated_returns_401() -> None:
    conn = FakeGeometryConn(camera_account_id=uuid4())
    r = _caller_client_no_auth(conn).put(
        _PATH.format(camera_id=uuid4()), json={"config": _config()}
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


# ===========================================================================
# Validation — bad config bodies rejected at the schema layer (422).
# ===========================================================================


def _put_bad(conn: FakeGeometryConn, account: UUID, camera: UUID, config: dict[str, Any]) -> Any:
    return _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": config}
    )


def test_put_rejects_wrong_schema_version() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(version=2))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"
    assert conn.upserts == []


def test_put_rejects_non_normalized_coordinate_space() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    r = _put_bad(
        conn,
        account,
        camera,
        _config(frame={"width": 1280, "height": 720, "coordinate_space": "pixel"}),
    )
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_point_outside_unit_range() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    bad_roi = {
        "id": "roi-a",
        "points": [{"x": 1.5, "y": 0.1}, {"x": 0.2, "y": 0.1}, {"x": 0.2, "y": 0.2}],
    }
    r = _put_bad(conn, account, camera, _config(rois=[bad_roi]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_duplicate_roi_ids() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(rois=[_roi(1), {**_roi(2), "id": "roi-1"}]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_duplicate_line_ids() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(lines=[_line(1), {**_line(2), "id": "line-1"}]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_roi_with_too_few_points() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    two_pt = {"id": "roi-a", "points": [{"x": 0.1, "y": 0.1}, {"x": 0.2, "y": 0.2}]}
    r = _put_bad(conn, account, camera, _config(rois=[two_pt]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_line_with_wrong_point_count() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    three_pt = {
        "id": "line-a",
        "points": [{"x": 0.1, "y": 0.5}, {"x": 0.5, "y": 0.5}, {"x": 0.9, "y": 0.5}],
    }
    r = _put_bad(conn, account, camera, _config(lines=[three_pt]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_invalid_slug_id() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    bad_id = {**_roi(1), "id": "Entry ROI"}  # spaces + caps fail the slug pattern
    r = _put_bad(conn, account, camera, _config(rois=[bad_id]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_too_many_rois() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(rois=[_roi(i) for i in range(21)]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_too_many_lines() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(lines=[_line(i) for i in range(21)]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_style_fields() -> None:
    """Geometry config is processing geometry only — a presentation field
    (color/CSS/stroke) is a 422, never silently stored."""
    account, camera = uuid4(), uuid4()
    conn = FakeGeometryConn(camera_account_id=account)
    styled_roi = {**_roi(1), "color": "#ff0000", "fill_opacity": 0.3}
    r = _put_bad(conn, account, camera, _config(rois=[styled_roi]))
    assert r.status_code == 422
    assert conn.upserts == []


# ===========================================================================
# Migration + production SQL pinning.
# ===========================================================================


def test_migration_file_pins_structure() -> None:
    sql = _MIGRATION.read_text(encoding="utf-8")
    assert "create table camera_geometry_configs" in sql
    assert "unique index camera_geometry_configs_camera_idx" in sql
    assert "camera_geometry_configs_set_updated_at" in sql
    assert "execute function set_updated_at()" in sql
    assert "enable row level security" in sql
    assert "policy camera_geometry_configs_member_select" in sql


def test_upsert_geometry_config_sql_pins_conflict_and_revision_bump() -> None:
    sql = _UPSERT_GEOMETRY_CONFIG_SQL
    assert "insert into camera_geometry_configs" in sql
    assert "on conflict (camera_id) do update set" in sql
    assert "camera_geometry_configs.revision + 1" in sql
    assert "returning" in sql


def test_select_geometry_config_sql_pins_predicates() -> None:
    sql = _SELECT_GEOMETRY_CONFIG_SQL
    assert "from camera_geometry_configs" in sql
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
