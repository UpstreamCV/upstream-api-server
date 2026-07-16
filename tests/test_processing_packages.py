"""P5-F — route-level tests for the processing-package catalog endpoints.

``GET /v1/processing/packages`` (list + grouping) and
``GET /v1/processing/packages/{package_id}`` (detail). Authenticated
(``CurrentCaller``), platform-global static data, no DB. Mirrors
tests/test_models.py: override ``get_pool`` with a ``FakePool`` and
``current_caller`` with a ``CallerAuth`` (except the 401 test, which leaves the
real auth dependency in place).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller

_NVDCF = "deepstream.yolox_analytics_nvdcf_probe.v0"
_IOU = "deepstream.yolox_analytics_probe.v0"
# P5-K additions used across tests.
_VEHICLE = "deepstream.trafficcamnet_vehicle_probe.v0"
_LINE = "deepstream.yolox_people_line_crossing_probe.v0"
_OCC = "deepstream.yolox_people_occupancy_probe.v0"
_YOLO11S = "deepstream.yolo11s_people_probe.v0"  # AGPL / first_party=false
_NANO = "deepstream.yolox_nano_people_probe.v0"  # permissive / first_party=true
_COUNTING = "overhead_people_counter.v1"  # live CrowdCount counting package

_EXPECTED_FIELDS = {
    "package_id",
    "display_name",
    "description",
    "category",
    "status",
    "production_ready",
    "test_mode_safe",
    "runtime",
    "model_id",
    "tracker_profile_id",
    "analytics_profile_id",
    "supported_classes",
    "supported_outputs",
    "supported_shapes",
    "supported_triggers",
    "default_settings",
    "allowed_overrides",
    "compute_tier",
    "pricing_tier",
    "observation_volume_risk",
    "license_class",
    "first_party",
    "warnings",
    "limitations",
    "best_for",
    "not_recommended_for",
    "requires_geometry",
    "requires_restart",
    "readiness_notes",
    "related_packages",
    "model_metrics",
}


class _AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    async def __aenter__(self) -> Any:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class _FakePool:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self.conn)


def _caller_client(*, account_id: UUID | None = None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: _FakePool(None)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id or uuid4(), token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def _unauth_client() -> TestClient:
    """Real auth dependency in place (pool overridden so the dep graph
    resolves); no Authorization header => 401."""
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: _FakePool(None)
    return TestClient(app)


def _first_party_only_client() -> TestClient:
    """A caller client with the P5-K catalog gate ON (production posture)."""
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: _FakePool(None)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=uuid4(), token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    gated = get_settings().model_copy(update={"catalog_first_party_only": True})
    app.dependency_overrides[get_settings] = lambda: gated
    return TestClient(app)


# --- auth ---------------------------------------------------------------------


def test_packages_requires_auth() -> None:
    r = _unauth_client().get("/v1/processing/packages")
    assert r.status_code == 401, r.text


# --- list ---------------------------------------------------------------------


def test_list_packages_shape() -> None:
    r = _caller_client().get("/v1/processing/packages")
    assert r.status_code == 200, r.text
    body = r.json()
    assert {"packages", "categories", "warnings"} <= body.keys()
    assert isinstance(body["packages"], list) and body["packages"]
    ids = {p["package_id"] for p in body["packages"]}
    assert _IOU in ids and _NVDCF in ids
    # Grouping + catalog-level warning for dashboard cards.
    cat_ids = {c["id"] for c in body["categories"]}
    assert "people_analytics" in cat_ids
    assert any("next stream session" in w for w in body["warnings"])


def test_package_schema_is_stable() -> None:
    r = _caller_client().get("/v1/processing/packages")
    pkg = next(p for p in r.json()["packages"] if p["package_id"] == _IOU)
    assert set(pkg.keys()) == _EXPECTED_FIELDS


def test_analytics_package_carries_outputs_and_geometry() -> None:
    r = _caller_client().get("/v1/processing/packages")
    by_id = {p["package_id"]: p for p in r.json()["packages"]}
    iou = by_id[_IOU]
    assert "processor.zone_exited" in iou["supported_outputs"]
    assert "processor.line_crossing_object" in iou["supported_outputs"]
    assert iou["requires_geometry"] is True
    assert iou["requires_restart"] is True
    assert iou["supported_classes"] == ["person"]


# --- status honesty -----------------------------------------------------------


def test_iou_package_is_probe_nvdcf_is_experimental() -> None:
    r = _caller_client().get("/v1/processing/packages")
    by_id = {p["package_id"]: p for p in r.json()["packages"]}
    assert by_id[_IOU]["status"] == "probe"
    nvdcf = by_id[_NVDCF]
    assert nvdcf["status"] == "experimental"
    # Experimental is visible but clearly marked and never recommended.
    assert nvdcf["production_ready"] is False
    assert any("Experimental" in w for w in nvdcf["warnings"])


def test_no_package_is_dishonestly_production_ready() -> None:
    r = _caller_client().get("/v1/processing/packages")
    for p in r.json()["packages"]:
        if p["production_ready"]:
            assert p["status"] == "supported"  # honesty gate


# --- detail -------------------------------------------------------------------


def test_get_package_detail() -> None:
    r = _caller_client().get(f"/v1/processing/packages/{_NVDCF}")
    assert r.status_code == 200, r.text
    pkg = r.json()
    assert pkg["package_id"] == _NVDCF
    assert pkg["tracker_profile_id"] == "deepstream_nvdcf_balanced_v1"
    assert pkg["readiness_notes"]  # detail carries promotion/readiness notes
    assert _IOU in pkg["related_packages"]
    assert set(pkg.keys()) == _EXPECTED_FIELDS


def test_get_unknown_package_404() -> None:
    r = _caller_client().get("/v1/processing/packages/does.not.exist.v0")
    assert r.status_code == 404, r.text
    assert r.json()["error"]["code"] == "package_not_found"


# --- filters ------------------------------------------------------------------


def test_filter_by_category_people_analytics() -> None:
    r = _caller_client().get("/v1/processing/packages", params={"category": "people_analytics"})
    pkgs = r.json()["packages"]
    assert pkgs
    assert all(p["category"] == "people_analytics" for p in pkgs)
    ids = {p["package_id"] for p in pkgs}
    assert {_IOU, _NVDCF} <= ids


def test_filter_by_status_and_runtime() -> None:
    probe = _caller_client().get("/v1/processing/packages", params={"status": "probe"}).json()
    assert all(p["status"] == "probe" for p in probe["packages"])
    assert _NVDCF not in {p["package_id"] for p in probe["packages"]}  # experimental excluded
    ds = _caller_client().get("/v1/processing/packages", params={"runtime": "deepstream"}).json()
    assert all(p["runtime"] == "deepstream" for p in ds["packages"])
    assert "mock.detect.v0" not in {p["package_id"] for p in ds["packages"]}


def test_filter_by_test_mode_safe() -> None:
    r = _caller_client().get("/v1/processing/packages", params={"test_mode_safe": "true"})
    pkgs = r.json()["packages"]
    assert pkgs and all(p["test_mode_safe"] is True for p in pkgs)


# --- P5-K: catalog expansion --------------------------------------------------


def test_vehicle_analytics_category_present() -> None:
    body = _caller_client().get("/v1/processing/packages").json()
    cat_ids = {c["id"] for c in body["categories"]}
    assert "vehicle_analytics" in cat_ids
    veh = (
        _caller_client()
        .get("/v1/processing/packages", params={"category": "vehicle_analytics"})
        .json()
    )
    ids = {p["package_id"] for p in veh["packages"]}
    assert _VEHICLE in ids
    assert all(p["category"] == "vehicle_analytics" for p in veh["packages"])


def test_p5k_new_experimental_packages_are_honest() -> None:
    by_id = {
        p["package_id"]: p
        for p in _caller_client().get("/v1/processing/packages").json()["packages"]
    }
    for pid in (_VEHICLE, _LINE, _YOLO11S):
        pkg = by_id[pid]
        assert pkg["status"] == "experimental"
        assert pkg["production_ready"] is False
        assert pkg["warnings"]  # clearly marked
    # The 5 YOLOX-S variants that reuse the verified pipeline stay probe.
    assert by_id[_OCC]["status"] == "probe"


def test_p5k_provenance_and_byom_marking() -> None:
    by_id = {
        p["package_id"]: p
        for p in _caller_client().get("/v1/processing/packages").json()["packages"]
    }
    # Permissive YOLOX size: first-party.
    assert by_id[_NANO]["license_class"] == "permissive"
    assert by_id[_NANO]["first_party"] is True
    # Ultralytics (AGPL): restrictive + BYOM (not first-party).
    assert by_id[_YOLO11S]["license_class"] == "restrictive"
    assert by_id[_YOLO11S]["first_party"] is False
    assert any("BYOM" in w for w in by_id[_YOLO11S]["warnings"])
    # TAO PeopleNet: covered by a commercial agreement, first-party.
    pn = by_id["deepstream.peoplenet_people_probe.v0"]
    assert pn["license_class"] == "commercial_agreement" and pn["first_party"] is True


def test_people_counting_category_has_a_display_name() -> None:
    body = _caller_client().get("/v1/processing/packages").json()
    by_cat = {c["id"]: c for c in body["categories"]}
    assert by_cat["people_counting"]["display_name"] == "People Counting"


def test_counting_package_does_not_advertise_observations_it_never_emits() -> None:
    pkg = _caller_client().get(f"/v1/processing/packages/{_COUNTING}").json()
    # Live + verified in prod, but counts post to crowdcount-server — the worker
    # writes no Upstream observations, so none may be advertised.
    assert pkg["status"] == "supported" and pkg["production_ready"] is True
    assert pkg["supported_outputs"] == []
    assert pkg["readiness_notes"]
    # The bundled-package picker keys off a non-empty supported_triggers.
    assert pkg["supported_triggers"] == ["line_crossing"]


def test_p5l_model_metrics_on_packages() -> None:
    by_id = {
        p["package_id"]: p
        for p in _caller_client().get("/v1/processing/packages").json()["packages"]
    }
    # YOLOX-S package carries published COCO mAP + params, never Upstream-measured.
    iou = by_id[_IOU]["model_metrics"]
    assert iou is not None
    assert iou["coco_map50_95"] == 40.5
    assert iou["params_m"] == 9.0
    assert iou["upstream_measured"] is False
    assert iou["metric_source"]
    # The TrafficCamNet (people) package: NVIDIA TAO model → no COCO mAP, vendor KPI label.
    tcn = by_id["deepstream.analytics_probe.v0"]["model_metrics"]
    assert tcn is not None and tcn["coco_map50_95"] is None
    assert tcn["vendor_kpi_label"] and "not comparable" in (tcn["vendor_kpi_note"] or "")
    # Diagnostic decode probe has no backing model → no metrics.
    assert by_id["deepstream.decode_probe.v0"]["model_metrics"] is None


def test_p5l_metrics_hidden_with_gated_packages() -> None:
    # The gate hides BYOM packages (and thus their model_metrics) in production.
    gated = {
        p["package_id"]
        for p in _first_party_only_client().get("/v1/processing/packages").json()["packages"]
    }
    assert _YOLO11S not in gated  # AGPL model's package (and its metrics) not exposed


def test_first_party_only_gate_hides_agpl_from_list() -> None:
    # Default (dev): AGPL packages are visible (with a BYOM badge).
    default_ids = {
        p["package_id"] for p in _caller_client().get("/v1/processing/packages").json()["packages"]
    }
    assert _YOLO11S in default_ids
    # Gate ON (go-live): AGPL/non-first-party packages are hidden; permissive +
    # the recommended IOU package stay.
    gated_ids = {
        p["package_id"]
        for p in _first_party_only_client().get("/v1/processing/packages").json()["packages"]
    }
    assert _YOLO11S not in gated_ids
    assert "deepstream.yolov8n_people_probe.v0" not in gated_ids
    assert _NANO in gated_ids and _IOU in gated_ids and _VEHICLE in gated_ids


def test_first_party_only_gate_404s_agpl_detail() -> None:
    # Visible by default; 404 when the gate is on.
    assert _caller_client().get(f"/v1/processing/packages/{_YOLO11S}").status_code == 200
    r = _first_party_only_client().get(f"/v1/processing/packages/{_YOLO11S}")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "package_not_found"
    # A permissive package is unaffected by the gate.
    assert _first_party_only_client().get(f"/v1/processing/packages/{_NANO}").status_code == 200
