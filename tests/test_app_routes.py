from __future__ import annotations

from typing import Any

from upstream_api.app import create_app


def test_expected_routes_are_mounted() -> None:
    app = create_app()
    paths = {f"{r.path}:{','.join(sorted(r.methods or set()))}" for r in app.routes}  # type: ignore[attr-defined]
    expected = {
        "/:GET",
        "/healthz:GET",
        "/readyz:GET",
        "/v1/agents/register:POST",
        "/v1/agents:GET",
        "/v1/agents/{agent_id}:GET",
        "/v1/cameras/register:POST",
        "/v1/cameras:GET",
        "/v1/cameras/{camera_id}:GET",
        "/v1/stream-sessions/start:POST",
        "/v1/stream-sessions/{session_id}/stop:POST",
        "/v1/stream-sessions/{session_id}:GET",
        "/v1/stream-sessions:GET",
        "/v1/api-tokens:GET",
        "/v1/api-tokens:POST",
        "/v1/api-tokens/{token_id}:DELETE",
        "/v1/heartbeats:POST",
        "/v1/agents/{agent_id}/heartbeats:GET",
        "/v1/cameras/{camera_id}/heartbeats:GET",
        "/v1/snapshots:POST",
        "/v1/cameras/{camera_id}/latest-frame:GET",
        "/v1/stream-sessions/{session_id}/validations:GET",
        "/v1/me:GET",
        "/v1/account:GET",
        "/v1/account/limits:GET",
        "/v1/dashboard/summary:GET",
        "/v1/ingest/validate-stream:POST",
        "/v1/ingest/stream-started:POST",
        "/v1/ingest/stream-ended:POST",
        "/v1/ingest/validation-failed:POST",
        "/v1/ingest/lookup-session:POST",
    }
    missing = expected - paths
    assert not missing, f"Missing routes: {sorted(missing)}"


def test_cors_middleware_uses_explicit_methods_and_headers() -> None:
    app = create_app()
    cors: Any = next(
        m for m in app.user_middleware if getattr(m.cls, "__name__", "") == "CORSMiddleware"
    )
    methods = cors.kwargs["allow_methods"]
    headers = cors.kwargs["allow_headers"]
    assert "*" not in methods
    assert "*" not in headers
    assert set(methods) == {"GET", "POST", "DELETE"}
    assert "Authorization" in headers
