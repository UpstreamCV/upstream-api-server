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
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller

_NVDCF = "deepstream.yolox_analytics_nvdcf_probe.v0"
_IOU = "deepstream.yolox_analytics_probe.v0"

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
    "warnings",
    "limitations",
    "best_for",
    "not_recommended_for",
    "requires_geometry",
    "requires_restart",
    "readiness_notes",
    "related_packages",
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
    assert any("Catalog only" in w for w in body["warnings"])


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
