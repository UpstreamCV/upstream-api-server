"""Phase 4.5 endpoint smoke tests.

Verifies the dashboard-facing GETs added in `Phase 4.5` are wired up with
the right auth dep and the right path-param validation. These are
no-lifespan tests (no DB) — we exercise the request handler up to the
auth dep and confirm the error envelope shape. End-to-end coverage
with a real DB lives in `scripts/smoke_local.py`.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool

# Endpoints with no path parameters.
NO_PARAM_ENDPOINTS = [
    "/v1/me",
    "/v1/account",
    "/v1/account/limits",
    "/v1/api-tokens",
    "/v1/dashboard/summary",
]

# (path_template, param_name) pairs for endpoints with a UUID path segment.
WITH_UUID_ENDPOINTS = [
    ("/v1/agents/{u}/heartbeats", "agent_id"),
    ("/v1/cameras/{u}/heartbeats", "camera_id"),
    ("/v1/cameras/{u}/latest-frame", "camera_id"),
    ("/v1/stream-sessions/{u}/validations", "session_id"),
]

_VALID_UUID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture(name="client")
def _client() -> TestClient:
    # Skip the lifespan (no real DB). FastAPI resolves the `PoolDep` for
    # the auth dep before the request body runs, so we override get_pool
    # with a no-op stand-in — the auth dep rejects on bad/missing headers
    # before ever touching the pool.
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: None
    return TestClient(app)


# ---------------------------------------------------------------------------
# Missing Authorization header → 401 `missing_credentials`
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", NO_PARAM_ENDPOINTS)
def test_no_param_endpoint_requires_auth(client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 401
    body = response.json()
    assert body["error"]["code"] == "missing_credentials"


@pytest.mark.parametrize("path_template,_param", WITH_UUID_ENDPOINTS)
def test_uuid_param_endpoint_requires_auth(
    client: TestClient, path_template: str, _param: str
) -> None:
    path = path_template.format(u=_VALID_UUID)
    response = client.get(path)
    assert response.status_code == 401
    body = response.json()
    assert body["error"]["code"] == "missing_credentials"


# ---------------------------------------------------------------------------
# Non-Bearer Authorization header → 401 `invalid_auth_scheme`
# ---------------------------------------------------------------------------


def _all_endpoints() -> list[str]:
    return NO_PARAM_ENDPOINTS + [
        template.format(u=_VALID_UUID) for template, _ in WITH_UUID_ENDPOINTS
    ]


@pytest.mark.parametrize("path", _all_endpoints())
def test_endpoint_rejects_non_bearer_scheme(client: TestClient, path: str) -> None:
    response = client.get(path, headers={"Authorization": "Basic foo"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_auth_scheme"


# ---------------------------------------------------------------------------
# Malformed Bearer payload → 401 (invalid_user_token for /v1/me + caller routes)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", _all_endpoints())
def test_endpoint_rejects_garbage_bearer(client: TestClient, path: str) -> None:
    response = client.get(path, headers={"Authorization": "Bearer not.a.jwt"})
    assert response.status_code == 401
    # `current_user` rejects Supabase JWT verification with this code.
    # `current_caller` dispatches to `current_user` for non-ups_ tokens,
    # so both kinds of endpoints land here.
    code = response.json()["error"]["code"]
    assert code in {"invalid_user_token", "invalid_token"}, code


# ---------------------------------------------------------------------------
# /v1/api-tokens — agent token (ups_*) is the wrong auth class
# ---------------------------------------------------------------------------


def test_api_tokens_list_rejects_agent_token(client: TestClient) -> None:
    """`GET /v1/api-tokens` is admin-user only; agent tokens never have a role.

    The handler uses `CurrentUser` directly, so an `ups_*` token tries the
    Supabase JWT path and fails with `invalid_user_token`.
    """
    response = client.get(
        "/v1/api-tokens",
        headers={"Authorization": "Bearer ups_test_AB12CDEF_FAKESECRET22CHARS00"},
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_user_token"
