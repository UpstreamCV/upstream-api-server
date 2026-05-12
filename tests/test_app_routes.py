from __future__ import annotations

from upstream_api.app import create_app


def test_expected_routes_are_mounted() -> None:
    app = create_app()
    paths = {f"{r.path}:{','.join(sorted(r.methods or set()))}" for r in app.routes}  # type: ignore[attr-defined]
    expected = {
        "/healthz:GET",
        "/readyz:GET",
        "/v1/agents/register:POST",
        "/v1/cameras/register:POST",
        "/v1/stream-sessions/start:POST",
        "/v1/stream-sessions/{session_id}/stop:POST",
        "/v1/stream-sessions/{session_id}:GET",
        "/v1/api-tokens:POST",
        "/v1/api-tokens/{token_id}:DELETE",
    }
    missing = expected - paths
    assert not missing, f"Missing routes: {sorted(missing)}"
