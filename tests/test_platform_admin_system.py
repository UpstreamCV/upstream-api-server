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
    app.dependency_overrides[get_pool] = FakePool
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


# ---------------------------------------------------------------------------
# GET /v1/admin/system/timeseries
# ---------------------------------------------------------------------------


class TimeseriesConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.args: tuple[Any, ...] = ()

    async def fetch(self, _query: str, *args: Any) -> list[dict[str, Any]]:
        self.args = args
        return self.rows


class TimeseriesAcquire:
    def __init__(self, conn: TimeseriesConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> TimeseriesConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class TimeseriesPool:
    def __init__(self, conn: TimeseriesConn) -> None:
        self.conn = conn

    def acquire(self) -> TimeseriesAcquire:
        return TimeseriesAcquire(self.conn)


def _ts_client(conn: TimeseriesConn) -> TestClient:
    app = create_app()
    app.state.http_client = object()
    app.dependency_overrides[get_pool] = lambda: TimeseriesPool(conn)
    app.dependency_overrides[current_platform_admin] = lambda: _ADMIN
    return TestClient(app)


def test_timeseries_returns_zero_filled_points() -> None:
    from datetime import UTC, datetime

    conn = TimeseriesConn(
        [
            {
                "bucket": datetime(2026, 6, 11, 10, tzinfo=UTC),
                "sessions_started": 0,
                "validation_failures": 0,
                "streaming_minutes": 0.0,
                "avg_fps": None,
                "alerts_fired": 0,
                "workflow_runs": 0,
                "objects_detected": 0,
                "avg_detection_confidence": None,
            },
            {
                "bucket": datetime(2026, 6, 11, 11, tzinfo=UTC),
                "sessions_started": 3,
                "validation_failures": 1,
                "streaming_minutes": 42.5,
                "avg_fps": 29.97,
                "alerts_fired": 4,
                "workflow_runs": 7,
                "objects_detected": 152,
                "avg_detection_confidence": 0.87654,
            },
        ]
    )
    client = _ts_client(conn)
    r = client.get("/v1/admin/system/timeseries?hours=2")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["hours"] == 2
    assert conn.args == (2,)
    assert body["points"][0]["avg_fps"] is None  # no heartbeats != 0 fps
    assert body["points"][0]["avg_detection_confidence"] is None  # no metrics != 0
    assert body["points"][1]["sessions_started"] == 3
    assert body["points"][1]["streaming_minutes"] == 42.5
    assert body["points"][1]["avg_fps"] == 30.0  # rounded to 1dp
    assert body["points"][1]["alerts_fired"] == 4
    assert body["points"][1]["workflow_runs"] == 7
    assert body["points"][1]["objects_detected"] == 152
    assert body["points"][1]["avg_detection_confidence"] == 0.877  # rounded to 3dp


def test_timeseries_clamps_hours() -> None:
    client = _ts_client(TimeseriesConn([]))
    assert client.get("/v1/admin/system/timeseries?hours=0").status_code == 422
    assert client.get("/v1/admin/system/timeseries?hours=200").status_code == 422


# ---------------------------------------------------------------------------
# GET /v1/admin/system/health/history + the sampler service
# ---------------------------------------------------------------------------


def test_health_history_returns_samples() -> None:
    from datetime import UTC, datetime

    conn = TimeseriesConn(
        [
            {
                "sampled_at": datetime(2026, 6, 11, 12, 0, tzinfo=UTC),
                "db_ok": True,
                "db_latency_ms": 1.4,
                "mediamtx_status": "ok",
                "mediamtx_active_paths": 2,
                "processors_active": 1,
                "processors_stale": 0,
                "processors_failed": 0,
                "active_sessions": 2,
            }
        ]
    )
    client = _ts_client(conn)
    r = client.get("/v1/admin/system/health/history?minutes=30")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["minutes"] == 30
    assert body["interval_seconds"] == 60  # settings default
    assert body["samples"][0]["db_latency_ms"] == 1.4
    assert conn.args == (30,)


def test_health_history_clamps_minutes() -> None:
    client = _ts_client(TimeseriesConn([]))
    assert client.get("/v1/admin/system/health/history?minutes=1").status_code == 422
    assert client.get("/v1/admin/system/health/history?minutes=2000").status_code == 422


async def test_record_sample_inserts_row(monkeypatch: pytest.MonkeyPatch) -> None:
    import upstream_api.services.health_sampler as sampler_module
    from upstream_api.config import get_settings
    from upstream_api.schemas.platform_admin import (
        DatabaseHealth as DbHealth,
    )
    from upstream_api.schemas.platform_admin import (
        ProcessorsHealth as ProcHealth,
    )
    from upstream_api.schemas.platform_admin import (
        StreamingServerHealth as StreamHealth,
    )
    from upstream_api.services.health_sampler import record_sample

    async def fake_check_database(_pool: Any) -> DbHealth:
        return DbHealth(status="ok", latency_ms=2.5)

    async def fake_check_streaming_server(_client: Any, *, base_url: str) -> StreamHealth:
        return StreamHealth(status="unreachable", error="refused")

    async def fake_processor_rollup(_pool: Any) -> ProcHealth:
        return ProcHealth(active=1, stale=0, failed=0)

    monkeypatch.setattr(sampler_module, "check_database", fake_check_database)
    monkeypatch.setattr(sampler_module, "check_streaming_server", fake_check_streaming_server)
    monkeypatch.setattr(sampler_module, "processor_rollup", fake_processor_rollup)

    class SamplerConn:
        def __init__(self) -> None:
            self.executes: list[tuple[str, tuple[Any, ...]]] = []

        async def fetchval(self, _query: str, *args: Any) -> int:
            return 3  # active sessions

        async def execute(self, query: str, *args: Any) -> str:
            self.executes.append((query, args))
            return "OK"

    class SamplerAcquire:
        def __init__(self, conn: SamplerConn) -> None:
            self.conn = conn

        async def __aenter__(self) -> SamplerConn:
            return self.conn

        async def __aexit__(self, *_exc: Any) -> None:
            return None

    class SamplerPool:
        def __init__(self) -> None:
            self.conn = SamplerConn()

        def acquire(self) -> SamplerAcquire:
            return SamplerAcquire(self.conn)

    pool = SamplerPool()
    await record_sample(pool, object(), get_settings())  # type: ignore[arg-type]
    inserts = [a for q, a in pool.conn.executes if "insert into system_health_samples" in q]
    assert len(inserts) == 1
    # (db_ok, latency, mediamtx_status, paths, active, stale, failed, sessions)
    assert inserts[0] == (True, 2.5, "unreachable", None, 1, 0, 0, 3)


# ---------------------------------------------------------------------------
# GET /v1/admin/system/metrics — control-plane counter snapshot
#
# The Prometheus registry is module-level and shared across the whole test
# process, so assertions are delta-based: record, snapshot, compare deltas.
# ---------------------------------------------------------------------------


def _metrics_client() -> TestClient:
    app = create_app()
    app.state.http_client = object()
    app.dependency_overrides[get_pool] = FakePool
    app.dependency_overrides[current_platform_admin] = lambda: _ADMIN
    return TestClient(app)


def test_system_metrics_reflects_recorded_counters() -> None:
    from upstream_api.observability.metrics import (
        record_auth,
        record_ingest,
        record_transition,
        snapshot,
    )

    before = snapshot()
    record_ingest("stream_started", "ok")
    record_ingest("stream_started", "server_error")
    record_auth("allow")
    record_auth("deny")
    record_transition("approved", "streaming")
    record_transition("streaming", "failed")

    client = _metrics_client()
    r = client.get("/v1/admin/system/metrics")
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["ingest_webhooks"]["ok"] - before["ingest_webhooks"]["ok"] == 1
    assert (
        body["ingest_webhooks"]["server_error"] - before["ingest_webhooks"]["server_error"] == 1
    )
    assert body["stream_auth"]["allowed"] - before["stream_auth"]["allowed"] == 1
    assert body["stream_auth"]["denied"] - before["stream_auth"]["denied"] == 1
    assert body["session_transitions_total"] - before["session_transitions_total"] == 2
    assert body["sessions_failed_total"] - before["sessions_failed_total"] == 1
    assert body["process_uptime_seconds"] > 0
    # Under TestClient there is no app.state.db_pool; gauges keep their last value.
    assert body["db_pool"]["size"] >= 0


def test_system_metrics_time_to_streaming_quantiles() -> None:
    from upstream_api.observability.metrics import observe_time_to_streaming, snapshot

    before = snapshot()
    for seconds in (1.0, 1.5, 2.0, 3.0, 6.0):
        observe_time_to_streaming(seconds)
    after = snapshot()

    tts = after["time_to_streaming"]
    assert tts["count"] - before["time_to_streaming"]["count"] == 5
    # With observations recorded, both quantile estimates exist and order.
    assert tts["p50_seconds"] is not None
    assert tts["p95_seconds"] is not None
    assert tts["p50_seconds"] <= tts["p95_seconds"]


def test_histogram_quantile_estimation() -> None:
    from upstream_api.observability.metrics import _histogram_quantile

    inf = float("inf")
    # 10 observations: cumulative 4 by le=1, 8 by le=2, 10 by le=4.
    buckets = [(1.0, 4.0), (2.0, 8.0), (4.0, 10.0), (inf, 10.0)]
    p50 = _histogram_quantile(buckets, 0.50)
    assert p50 is not None and 1.0 <= p50 <= 2.0
    p95 = _histogram_quantile(buckets, 0.95)
    assert p95 is not None and 2.0 <= p95 <= 4.0
    # Empty histogram → no estimate, not a crash.
    assert _histogram_quantile([], 0.5) is None
    assert _histogram_quantile([(1.0, 0.0), (inf, 0.0)], 0.5) is None
    # Quantile landing in +Inf returns the last finite bound (a floor).
    assert _histogram_quantile([(1.0, 0.0), (inf, 10.0)], 0.5) == 1.0


def test_system_metrics_requires_platform_admin() -> None:
    app = create_app()
    app.state.http_client = object()
    app.dependency_overrides[get_pool] = FakePool
    client = TestClient(app)
    r = client.get("/v1/admin/system/metrics")
    assert r.status_code == 401
