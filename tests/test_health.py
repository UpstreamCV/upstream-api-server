from __future__ import annotations

from fastapi.testclient import TestClient

from upstream_api.app import create_app


def test_healthz_returns_ok() -> None:
    # No `with` block => skip lifespan (no DB pool init), since /healthz doesn't need DB.
    client = TestClient(create_app())
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_request_id_round_trip() -> None:
    client = TestClient(create_app())
    response = client.get("/healthz", headers={"X-Request-ID": "test-rid-1"})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "test-rid-1"
