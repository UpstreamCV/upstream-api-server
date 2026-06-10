"""Tests for /v1/admin/system/health and the admin_health service probes."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.schemas.platform_admin import DatabaseHealth, ProcessorsHealth
from upstream_api.security.deps import PlatformAdminAuth, current_platform_admin
from upstream_api.services.admin_health import check_streaming_server
from upstream_api.services.limits import check_account_active

_ADMIN = PlatformAdminAuth(user_id=uuid4(), email="admin@example.com", aal="aal1", amr=())


# ---------------------------------------------------------------------------
# check_streaming_server (httpx.MockTransport — no real network)
# ---------------------------------------------------------------------------


def _client_returning(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_streaming_server_ok_with_path_count() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v3/paths/list"
        return httpx.Response(200, json={"itemCount": 3, "items": [{}, {}, {}]})

    async with _client_returning(handler) as client:
        health = await check_streaming_server(client, base_url="http://mtx:9997")
    assert health.status == "ok"
    assert health.active_paths == 3


async def test_streaming_server_unreachable_degrades() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with _client_returning(handler) as client:
        health = await check_streaming_server(client, base_url="http://mtx:9997")
    assert health.status == "unreachable"
    assert health.error is not None


async def test_streaming_server_http_error_degrades() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    async with _client_returning(handler) as client:
        health = await check_streaming_server(client, base_url="http://mtx:9997")
    assert health.status == "error"
    assert "500" in (health.error or "")


# ---------------------------------------------------------------------------
# GET /v1/admin/system/health (probes monkeypatched at the router module)
# ---------------------------------------------------------------------------


class FakePool:
    pass


def _health_client(monkeypatch: pytest.MonkeyPatch, *, db_ok: bool) -> TestClient:
    import upstream_api.routers.platform_admin.system as system_module

    async def fake_check_database(_pool: Any) -> DatabaseHealth:
        if db_ok:
            return DatabaseHealth(status="ok", latency_ms=1.2)
        return DatabaseHealth(status="error", error="down")

    async def fake_check_streaming_server(_client: Any, *, base_url: str) -> Any:
        from upstream_api.schemas.platform_admin import StreamingServerHealth

        return StreamingServerHealth(status="unreachable", error="refused")

    async def fake_processor_rollup(_pool: Any) -> ProcessorsHealth:
        return ProcessorsHealth(active=2, stale=1, failed=0)

    monkeypatch.setattr(system_module, "check_database", fake_check_database)
    monkeypatch.setattr(system_module, "check_streaming_server", fake_check_streaming_server)
    monkeypatch.setattr(system_module, "processor_rollup", fake_processor_rollup)

    app = create_app()
    app.state.http_client = object()  # lifespan doesn't run under TestClient
    app.dependency_overrides[get_pool] = lambda: FakePool()
    app.dependency_overrides[current_platform_admin] = lambda: _ADMIN
    return TestClient(app)


def test_health_rollup_renders_degraded_components(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _health_client(monkeypatch, db_ok=True)
    r = client.get("/v1/admin/system/health")
    assert r.status_code == 200
    body = r.json()
    assert body["api"]["status"] == "ok"
    assert body["database"]["status"] == "ok"
    assert body["streaming_server"]["status"] == "unreachable"
    assert body["processors"] == {
        "active": 2,
        "stale": 1,
        "failed": 0,
        "latest_update_at": None,
    }


def test_health_rollup_skips_processor_query_when_db_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _health_client(monkeypatch, db_ok=False)
    r = client.get("/v1/admin/system/health")
    assert r.status_code == 200
    body = r.json()
    assert body["database"]["status"] == "error"
    assert body["processors"]["active"] == 0


# ---------------------------------------------------------------------------
# Suspension enforcement (services.limits.check_account_active)
# ---------------------------------------------------------------------------


class StatusConn:
    def __init__(self, status: str | None) -> None:
        self.status = status

    async def fetchval(self, _query: str, *_args: Any) -> str | None:
        return self.status


async def test_suspended_account_cannot_start_sessions() -> None:
    from upstream_api.errors import ForbiddenError

    with pytest.raises(ForbiddenError) as exc:
        await check_account_active(StatusConn("suspended"), uuid4())
    assert exc.value.code == "account_suspended"


async def test_missing_account_is_404() -> None:
    from upstream_api.errors import NotFoundError

    with pytest.raises(NotFoundError) as exc:
        await check_account_active(StatusConn(None), uuid4())
    assert exc.value.code == "account_not_found"


async def test_active_account_passes() -> None:
    await check_account_active(StatusConn("active"), uuid4())
