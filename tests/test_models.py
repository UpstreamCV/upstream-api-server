"""M2 — route-level tests for the static model-registry catalog endpoint.

One endpoint, one auth surface (``CurrentCaller`` — dashboard/agent):
- ``GET /v1/models`` — the platform's static detection-model catalog.

The catalog is in-process static data (``services/models.py``) with no DB
access, but the route still runs through ``create_app`` + the auth
dependency, so we mirror the geometry tests: override ``get_pool`` with a
``FakePool`` (never touched here) and ``current_caller`` with a
``CallerAuth``.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller

# ---------------------------------------------------------------------------
# Fake pool plumbing (cloned from tests/test_camera_geometry_config.py). The
# /v1/models route needs no DB, but the override keeps create_app's pool
# dependency satisfied without opening a real connection.
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


def _caller_client(*, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(None)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


# ===========================================================================
# GET /v1/models
# ===========================================================================


def test_get_models_returns_registry_entries() -> None:
    r = _caller_client(account_id=uuid4()).get("/v1/models")
    assert r.status_code == 200, r.text
    models = r.json()["models"]
    assert isinstance(models, list)
    assert len(models) >= 1
    keys = {m["model_key"] for m in models}
    assert "trafficcamnet_v1" in keys
    assert "mock_detect_v0" in keys


def test_get_models_entries_carry_supported_classes_and_trackers() -> None:
    r = _caller_client(account_id=uuid4()).get("/v1/models")
    assert r.status_code == 200, r.text
    models = r.json()["models"]
    for m in models:
        assert isinstance(m["supported_classes"], list)
        assert isinstance(m["supported_trackers"], list)
    by_key = {m["model_key"]: m for m in models}
    traffic = by_key["trafficcamnet_v1"]
    assert "person" in traffic["supported_classes"]
    assert "iou" in traffic["supported_trackers"]
