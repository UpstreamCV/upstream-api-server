"""P5-G — route-level tests for the camera package request/apply endpoints.

``GET/PUT /v1/cameras/{id}/processing-config/package`` and
``POST .../processing-config/apply``. Requested config is stored in
``cameras.metadata``; applied is derived from the latest processor status;
state compares them. Fake asyncpg pool, no real DB (mirrors
tests/test_camera_processing_config.py).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller

_GET = "/v1/cameras/{camera_id}/processing-config/package"
_APPLY = "/v1/cameras/{camera_id}/processing-config/apply"
_IOU = "deepstream.yolox_analytics_probe.v0"
_NVDCF = "deepstream.yolox_analytics_nvdcf_probe.v0"
# P5-K
_VEHICLE = "deepstream.trafficcamnet_vehicle_probe.v0"
_LINE = "deepstream.yolox_people_line_crossing_probe.v0"
_YOLO11S = "deepstream.yolo11s_people_probe.v0"


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


class FakePkgConn:
    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        metadata: dict[str, Any] | None = None,
        latest_status: dict[str, Any] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.latest_status = latest_status
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        if "processing_config_requested" in query:
            # store_requested_package: $1 camera_id, $2 package_id, $3 requested json
            self.metadata["package_id"] = args[1]
            self.metadata["processing_config_requested"] = json.loads(args[2])
        elif "{package_id}" in query:
            # apply re-stamp: $1 camera_id, $2 package_id
            self.metadata["package_id"] = args[1]
        return "UPDATE 1"

    async def fetchval(self, query: str, *args: Any) -> Any:
        return None  # audit table probe -> treat as missing (best-effort no-op)

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "select account_id from cameras" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}
        if "select metadata from cameras" in query:
            return {"metadata": self.metadata}
        if "from processor_statuses" in query:
            return self.latest_status
        return None


def _client(conn: Any, *, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def _requested_meta(package_id: str = _IOU, revision: int = 1) -> dict[str, Any]:
    return {
        "package_id": package_id,
        "processing_config_requested": {
            "package_id": package_id,
            "settings": {"confidence_threshold": 0.4, "target_fps": None, "selected_classes": []},
            "revision": revision,
            "updated_at": datetime.now(UTC).isoformat(),
            "updated_by": "user:abc",
        },
    }


def _status(
    package_id: str, *, status: str = "processing", error_code: str | None = None
) -> dict[str, Any]:
    return {
        "package_id": package_id,
        "status": status,
        "error_code": error_code,
        "payload": {
            "model_id": "yolox_s_v1",
            "tracker_profile_id": "deepstream_iou_fast_v1",
            "analytics_profile_id": "deepstream_nvdsanalytics_probe_v1",
        },
        "updated_at": datetime.now(UTC),
    }


# --- auth + ownership ---------------------------------------------------------


def test_package_config_requires_auth() -> None:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(FakePkgConn(camera_account_id=uuid4()))
    r = TestClient(app).get(_GET.format(camera_id=uuid4()))
    assert r.status_code == 401, r.text


def test_get_unknown_camera_404() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=None)
    r = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_get_cross_account_404() -> None:
    conn = FakePkgConn(camera_account_id=uuid4())  # owned by someone else
    r = _client(conn, account_id=uuid4()).get(_GET.format(camera_id=uuid4()))
    assert r.status_code == 404


# --- GET state machine --------------------------------------------------------


def test_get_unset_when_no_request() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4()))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["requested"] is None
    assert body["state"] == "unset"


def test_get_pending_apply_when_no_status() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata=_requested_meta(), latest_status=None)
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["requested"]["package_id"] == _IOU
    assert body["state"] == "pending_apply"


def test_get_active_when_applied_matches() -> None:
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct, metadata=_requested_meta(), latest_status=_status(_IOU)
    )
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["state"] == "active"
    assert body["applied"]["package_id"] == _IOU
    assert body["applied"]["model_id"] == "yolox_s_v1"


def test_get_pending_restart_when_applied_differs() -> None:
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct,
        metadata=_requested_meta(_IOU),
        latest_status=_status("deepstream.analytics_probe.v0"),  # old package still running
    )
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["state"] == "pending_restart"


def test_get_failed_config_state() -> None:
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct,
        metadata=_requested_meta(_IOU),
        latest_status=_status(_IOU, status="failed", error_code="package_resolution"),
    )
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["state"] == "failed_config"


def test_get_experimental_selected_state_and_warning() -> None:
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct,
        metadata=_requested_meta(_NVDCF),
        latest_status=_status(_NVDCF),
    )
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["state"] == "experimental_selected"
    assert any("experimental" in w for w in body["warnings"])


# --- PUT validation -----------------------------------------------------------


def test_put_valid_request() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).put(
        _GET.format(camera_id=uuid4()),
        json={"package_id": _IOU, "settings": {"confidence_threshold": 0.5}},
    )
    assert r.status_code == 200, r.text
    assert r.json()["requested"]["package_id"] == _IOU
    # metadata.package_id stamped for the processor.
    assert conn.metadata["package_id"] == _IOU


def test_put_unknown_package_422() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).put(
        _GET.format(camera_id=uuid4()), json={"package_id": "nope.v0"}
    )
    assert r.status_code == 422
    assert conn.executes == []  # no write on validation failure


def test_put_unsupported_class_422() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).put(
        _GET.format(camera_id=uuid4()),
        json={"package_id": _IOU, "settings": {"selected_classes": ["car"]}},  # IOU = person only
    )
    assert r.status_code == 422


def test_put_unsupported_setting_422() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    # decode_probe allows no overrides -> setting confidence_threshold is rejected.
    r = _client(conn, account_id=acct).put(
        _GET.format(camera_id=uuid4()),
        json={
            "package_id": "deepstream.decode_probe.v0",
            "settings": {"confidence_threshold": 0.5},
        },
    )
    assert r.status_code == 422


def test_put_out_of_range_value_422() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).put(
        _GET.format(camera_id=uuid4()),
        json={"package_id": _IOU, "settings": {"confidence_threshold": 1.5}},
    )
    assert r.status_code == 422


def test_put_experimental_needs_opt_in() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    client = _client(conn, account_id=acct)
    # Without opt-in -> rejected.
    r = client.put(_GET.format(camera_id=uuid4()), json={"package_id": _NVDCF})
    assert r.status_code == 422
    # With explicit opt-in -> allowed, and the state is experimental.
    r2 = client.put(
        _GET.format(camera_id=uuid4()),
        json={"package_id": _NVDCF, "allow_experimental": True},
    )
    assert r2.status_code == 200, r2.text
    assert r2.json()["state"] in ("pending_apply", "experimental_selected")


def test_put_p5k_experimental_packages_need_opt_in() -> None:
    # P5-K: the line-crossing, vehicle, and per-new-model packages are all
    # experimental and require the same explicit opt-in as NvDCF.
    for pid in (_LINE, _VEHICLE, _YOLO11S):
        acct = uuid4()
        conn = FakePkgConn(camera_account_id=acct, metadata={})
        client = _client(conn, account_id=acct)
        assert (
            client.put(_GET.format(camera_id=uuid4()), json={"package_id": pid}).status_code == 422
        )
        r = client.put(
            _GET.format(camera_id=uuid4()),
            json={"package_id": pid, "allow_experimental": True},
        )
        assert r.status_code == 200, r.text


def test_put_vehicle_package_class_validation() -> None:
    # The vehicle package supports car/bicycle (not person).
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    client = _client(conn, account_id=acct)
    ok = client.put(
        _GET.format(camera_id=uuid4()),
        json={
            "package_id": _VEHICLE,
            "allow_experimental": True,
            "settings": {"selected_classes": ["car"]},
        },
    )
    assert ok.status_code == 200, ok.text
    bad = client.put(
        _GET.format(camera_id=uuid4()),
        json={
            "package_id": _VEHICLE,
            "allow_experimental": True,
            "settings": {"selected_classes": ["person"]},
        },  # not a vehicle class
    )
    assert bad.status_code == 422


# --- POST apply ---------------------------------------------------------------


def test_apply_without_request_409() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).post(_APPLY.format(camera_id=uuid4()))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "no_requested_config"


def test_apply_marks_pending_and_restamps() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata=_requested_meta(_IOU), latest_status=None)
    r = _client(conn, account_id=acct).post(_APPLY.format(camera_id=uuid4()))
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "pending_apply"
    assert conn.metadata["package_id"] == _IOU


# --- P5-G audit regressions ---------------------------------------------------


def test_failed_non_package_resolution_is_not_active() -> None:
    """A non-package_resolution failure of the requested package must NOT show
    as 'active' (the false-healthy bug)."""
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct,
        metadata=_requested_meta(_IOU),
        latest_status=_status(_IOU, status="failed", error_code="geometry_invalid"),
    )
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["state"] == "failed_config"
    assert any("geometry_invalid" in w for w in body["warnings"])


def test_failed_config_with_empty_payload() -> None:
    """A real package_resolution failure is posted by the supervisor with an
    EMPTY payload — applied fields are None but the state is still failed_config."""
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct,
        metadata=_requested_meta(_IOU),
        latest_status={
            "package_id": _IOU,
            "status": "failed",
            "error_code": "package_resolution",
            "payload": {},
            "updated_at": datetime.now(UTC),
        },
    )
    body = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4())).json()
    assert body["state"] == "failed_config"
    assert body["applied"]["model_id"] is None
    assert body["applied"]["status"] == "failed"


def test_put_arbitrary_class_on_empty_supported_rejected() -> None:
    acct = uuid4()
    conn = FakePkgConn(camera_account_id=acct, metadata={})
    r = _client(conn, account_id=acct).put(
        _GET.format(camera_id=uuid4()),
        json={
            "package_id": "deepstream.metadata_probe.v0",
            "settings": {"selected_classes": ["unicorn"]},
        },
    )
    assert r.status_code == 422
    assert conn.executes == []


def test_put_cross_account_404_no_write() -> None:
    conn = FakePkgConn(camera_account_id=uuid4(), metadata={})  # owned by someone else
    r = _client(conn, account_id=uuid4()).put(
        _GET.format(camera_id=uuid4()), json={"package_id": _IOU}
    )
    assert r.status_code == 404
    assert conn.executes == []  # no metadata mutation on a non-owned camera


def test_apply_cross_account_404() -> None:
    conn = FakePkgConn(camera_account_id=uuid4(), metadata=_requested_meta())
    r = _client(conn, account_id=uuid4()).post(_APPLY.format(camera_id=uuid4()))
    assert r.status_code == 404


def test_put_requires_auth() -> None:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(FakePkgConn(camera_account_id=uuid4()))
    r = TestClient(app).put(_GET.format(camera_id=uuid4()), json={"package_id": _IOU})
    assert r.status_code == 401


def test_get_tolerates_drifted_stored_settings() -> None:
    """A stored requested config with a stray settings key / bad revision must
    not 500 the GET."""
    acct = uuid4()
    meta = {
        "package_id": _IOU,
        "processing_config_requested": {
            "package_id": _IOU,
            "settings": {"confidence_threshold": 0.4, "stray_future_field": 99},
            "revision": "not-an-int",
            "updated_at": datetime.now(UTC).isoformat(),
        },
    }
    conn = FakePkgConn(camera_account_id=acct, metadata=meta)
    r = _client(conn, account_id=acct).get(_GET.format(camera_id=uuid4()))
    assert r.status_code == 200, r.text
    assert r.json()["requested"]["revision"] == 1  # coerced fallback


def test_apply_when_already_active() -> None:
    acct = uuid4()
    conn = FakePkgConn(
        camera_account_id=acct, metadata=_requested_meta(_IOU), latest_status=_status(_IOU)
    )
    r = _client(conn, account_id=acct).post(_APPLY.format(camera_id=uuid4()))
    assert r.status_code == 200
    assert r.json()["state"] == "active"
    assert conn.metadata["package_id"] == _IOU
