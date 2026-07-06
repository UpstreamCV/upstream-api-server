"""M4 — route-level tests for the per-camera processing-config endpoints.

Two endpoints, one auth surface (``CurrentCaller`` — dashboard/agent):
- ``GET  /v1/cameras/{camera_id}/processing-config`` — read saved or default.
- ``PUT  /v1/cameras/{camera_id}/processing-config`` — registry-validated
  full-replace upsert.

Pattern mirrors tests/test_camera_geometry_config.py: TestClient + a fake
asyncpg pool, no real DB. ``revision`` is the DB row revision (0 = unsaved
default, 1 = first save, +1 per save); ``config.version`` is the unrelated
schema version (always 1).

Registry validation (unknown / disabled model, unsupported class / tracker)
runs in the ``CameraProcessingConfigUpdateRequest`` ``@model_validator`` and
surfaces as a Pydantic request-body 422; structural bounds (confidence,
fps, extra/style fields) are plain field validation, also 422. Every 422
must leave the DB untouched (``conn.upserts == []``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services.models import MODEL_REGISTRY

_PATH = "/v1/cameras/{camera_id}/processing-config"


# ---------------------------------------------------------------------------
# Fake pool plumbing (cloned from tests/test_camera_geometry_config.py).
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


class FakeProcessingConn:
    """Backs the camera-ownership check + the processing-config select / upsert.

    ``camera_account_id=None`` simulates a camera that does not exist.
    ``row`` is the currently-stored config row (``None`` = never saved); the
    upsert simulation re-implements the revision bump. The RETURNING /
    SELECT column set includes ``account_id`` (unlike geometry), matching the
    production SQL.
    """

    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        row: dict[str, Any] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.row = row
        self.select_queries: list[tuple[Any, ...]] = []
        self.upserts: list[tuple[Any, ...]] = []
        # Package-assignment v1: records the cameras.metadata package stamp
        # (and answers the audit writer's best-effort probes).
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        return "UPDATE 1"

    async def fetchval(self, query: str, *args: Any) -> Any:
        # account_audit's to_regclass probe — report the table missing so
        # the best-effort audit write no-ops in these unit tests.
        return None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras where id" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id, "agent_id": None}
        if "insert into camera_processing_configs" in query:
            self.upserts.append(args)
            now = datetime.now(tz=UTC)
            prior = self.row
            # args: $1 account_id, $2 camera_id, $3 config(json str).
            row = {
                "id": prior["id"] if prior is not None else uuid4(),
                "account_id": args[0],
                "camera_id": args[1],
                "revision": (prior["revision"] + 1) if prior is not None else 1,
                "config": args[2],
                "created_at": prior["created_at"] if prior is not None else now,
                "updated_at": now,
            }
            self.row = row
            return row
        if "from camera_processing_configs" in query:
            self.select_queries.append(args)
            return self.row
        return None


def _caller_client(conn: Any, *, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


# ---------------------------------------------------------------------------
# Config fixtures / helpers.
# ---------------------------------------------------------------------------


def _config(**overrides: Any) -> dict[str, Any]:
    """A valid processing config dict; override individual top-level keys.

    Default references the platform default model ``trafficcamnet_v1`` with
    an empty ``selected_classes`` ([] means "all supported classes").
    """
    base: dict[str, Any] = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": [],
        "tracker": None,
        "confidence_threshold": None,
        "processing_fps": None,
    }
    base.update(overrides)
    return base


def _stored_row(*, account_id: UUID, camera_id: UUID, revision: int = 1) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "account_id": account_id,
        "camera_id": camera_id,
        "revision": revision,
        "config": _config(),
        "created_at": now,
        "updated_at": now,
    }


def _put_bad(conn: FakeProcessingConn, account: UUID, camera: UUID, config: dict[str, Any]) -> Any:
    return _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": config}
    )


# ===========================================================================
# (3) GET — default fallback when no saved row.
# ===========================================================================


def test_get_returns_default_when_no_saved_config() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _caller_client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    pc = r.json()["processing_config"]
    assert pc["is_default"] is True
    assert pc["revision"] == 0
    assert pc["id"] is None
    assert pc["config"]["model_key"] == "trafficcamnet_v1"


# ===========================================================================
# (4)/(5) PUT — upsert + revision bump.
# ===========================================================================


def test_put_valid_inserts_revision_1_and_one_upsert() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 200, r.text
    pc = r.json()["processing_config"]
    assert pc["is_default"] is False
    assert pc["revision"] == 1
    assert len(conn.upserts) == 1


def test_put_with_seeded_row_bumps_revision() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(
        camera_account_id=account,
        row=_stored_row(account_id=account, camera_id=camera, revision=4),
    )
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config()}
    )
    assert r.status_code == 200, r.text
    assert r.json()["processing_config"]["revision"] == 5


# ===========================================================================
# (6) PUT unknown model_key → 422, no upsert.
# ===========================================================================


def test_put_unknown_model_key_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(model_key="nope_v9"))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"
    assert conn.upserts == []


# ===========================================================================
# (7) PUT disabled model_key → 422, no upsert.
#
# ``get_model`` reads the module-level ``MODEL_REGISTRY`` on every call and
# the schema validator calls it fresh per request, so monkeypatching the
# registry to a tuple whose entry is ``enabled=False`` is sufficient.
# ===========================================================================


def test_put_disabled_model_key_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    disabled = MODEL_REGISTRY[0].model_copy(update={"enabled": False})
    monkeypatch.setattr(
        "upstream_api.services.models.MODEL_REGISTRY",
        (disabled,),
    )
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(model_key=disabled.model_key))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"
    assert conn.upserts == []


# ===========================================================================
# (8)/(9)/(10) PUT selected_classes validation against the model registry.
# ===========================================================================


def test_put_unsupported_class_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    # "unicorn" is not in trafficcamnet_v1.supported_classes.
    r = _put_bad(conn, account, camera, _config(selected_classes=["unicorn"]))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_empty_selected_classes_accepted() -> None:
    # [] means "all supported classes" — always valid for an enabled model.
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config(selected_classes=[])}
    )
    assert r.status_code == 200, r.text
    assert r.json()["processing_config"]["revision"] == 1


def test_put_supported_class_accepted() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera), json={"config": _config(selected_classes=["person"])}
    )
    assert r.status_code == 200, r.text
    assert r.json()["processing_config"]["config"]["selected_classes"] == ["person"]


# ===========================================================================
# (11) PUT unsupported tracker → 422, no upsert.
# ===========================================================================


def test_put_unsupported_tracker_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    # trafficcamnet_v1 supports only "iou".
    r = _put_bad(conn, account, camera, _config(tracker="bytetrack"))
    assert r.status_code == 422
    assert conn.upserts == []


# ===========================================================================
# (12) PUT confidence_threshold out of [0, 1] → 422, no upsert.
# ===========================================================================


def test_put_confidence_threshold_above_one_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(confidence_threshold=1.5))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_confidence_threshold_below_zero_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(confidence_threshold=-0.1))
    assert r.status_code == 422
    assert conn.upserts == []


# ===========================================================================
# (13) PUT processing_fps out of [1, 30] → 422, no upsert.
# ===========================================================================


def test_put_processing_fps_zero_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(processing_fps=0))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_processing_fps_too_high_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(processing_fps=99))
    assert r.status_code == 422
    assert conn.upserts == []


# ===========================================================================
# (14) PUT with styling / extra fields → 422 (extra="forbid"), no upsert.
#
# Processing config is facts/config only; presentation stays frontend-owned.
# A stray style field is a hard 422, never silently stored.
# ===========================================================================


def test_put_rejects_styling_color_field() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(color="#ff0000"))
    assert r.status_code == 422
    assert conn.upserts == []


def test_put_rejects_styling_overlay_style_field() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account)
    r = _put_bad(conn, account, camera, _config(overlay_style={"stroke": 2}))
    assert r.status_code == 422
    assert conn.upserts == []


# --- package assignment v1: model choice stamps the camera's package ---------


@pytest.fixture(autouse=True)
def _reset_audit_cache() -> Any:
    """The audit writer caches its to_regclass probe module-globally; our
    fake answers None (table missing), so reset around each test to avoid
    leaking that verdict into other files."""
    from upstream_api.services.account_audit import reset_audit_table_cache

    reset_audit_table_cache()
    yield
    reset_audit_table_cache()


def _package_stamps(conn: FakeProcessingConn) -> list[tuple[str, tuple[Any, ...]]]:
    return [(q, a) for q, a in conn.executes if "jsonb_set" in q]


def _put_model(conn: FakeProcessingConn, account: UUID, camera: UUID, model_key: str) -> Any:
    return _caller_client(conn, account_id=account).put(
        _PATH.format(camera_id=camera),
        json={"config": _config(model_key=model_key)},
    )


def test_put_yolox_model_stamps_yolox_package() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _put_model(conn, account, camera, "yolox_s_v1")
    assert r.status_code == 200
    stamps = _package_stamps(conn)
    assert len(stamps) == 1
    query, args = stamps[0]
    assert "update cameras" in query
    assert "'{package_id}'" in query
    assert args == (camera, "deepstream.yolox_analytics_probe.v0")


def test_put_trafficcamnet_model_stamps_analytics_package() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _put_model(conn, account, camera, "trafficcamnet_v1")
    assert r.status_code == 200
    assert _package_stamps(conn)[0][1] == (camera, "deepstream.analytics_probe.v0")


def test_put_mock_model_stamps_mock_package() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    r = _put_model(conn, account, camera, "mock_detect_v0")
    assert r.status_code == 200
    assert _package_stamps(conn)[0][1] == (camera, "mock.detect.v0")


def test_metadata_stamp_is_surgical_jsonb_set() -> None:
    """The stamp must set ONE key — pinned by asserting the SQL uses
    jsonb_set over coalesce(metadata,'{}'), never a whole-column overwrite."""
    account, camera = uuid4(), uuid4()
    conn = FakeProcessingConn(camera_account_id=account, row=None)
    _put_model(conn, account, camera, "yolox_s_v1")
    query = _package_stamps(conn)[0][0]
    assert "coalesce(metadata, '{}'::jsonb)" in query
    assert "set metadata = jsonb_set(" in query


def test_models_catalog_exposes_package_ids() -> None:
    from upstream_api.services.models import get_model, list_models

    by_key = {m.model_key: m for m in list_models()}
    assert by_key["yolox_s_v1"].package_id == "deepstream.yolox_analytics_probe.v0"
    assert by_key["yolox_s_v1"].enabled is True
    assert "person" in by_key["yolox_s_v1"].supported_classes
    assert by_key["trafficcamnet_v1"].package_id == "deepstream.analytics_probe.v0"
    assert by_key["mock_detect_v0"].package_id == "mock.detect.v0"
    assert get_model("yolox_s_v1") is not None
