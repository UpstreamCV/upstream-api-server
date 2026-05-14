"""Tests for the MediaMTX path-provisioning HTTP client.

Uses `respx` to mock MediaMTX. Covers the four shapes that matter for
production: idempotent PATCH success, PATCH 4xx, DELETE success, DELETE
404 (treated as success), and transport-error → typed exception.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from upstream_api.services.mediamtx_provisioning import (
    MediaMTXProvisioningFailure,
    deprovision_path,
    provision_path,
)

_BASE = "http://mediamtx.test:9997"
_PATH = "sess_0123456789abcdef"
_PASS = "test-passphrase-of-sufficient-length"


@pytest.fixture
def http() -> httpx.AsyncClient:
    return httpx.AsyncClient()


@respx.mock
async def test_provision_path_sends_publisher_source_and_passphrase(
    http: httpx.AsyncClient,
) -> None:
    route = respx.patch(f"{_BASE}/v3/config/paths/patch/{_PATH}").mock(
        return_value=httpx.Response(200, json={})
    )
    await provision_path(http, base_url=_BASE, path=_PATH, passphrase=_PASS)
    assert route.called
    body = route.calls.last.request.content
    # Body is JSON; keep this loose so a future MediaMTX bump that demands
    # extra fields in the patch payload doesn't false-fail the test.
    text = body.decode()
    assert '"source": "publisher"' in text or '"source":"publisher"' in text
    assert _PASS in text
    # Lifecycle hooks must travel with the per-session path config since
    # the catch-all `all_others` block (the previous holder of these
    # defaults) is removed in the Phase 2 deploy step.
    assert "/hooks/on_ready.sh" in text
    assert "/hooks/on_not_ready.sh" in text


@respx.mock
async def test_provision_path_trailing_slash_in_base_url_is_normalized(
    http: httpx.AsyncClient,
) -> None:
    route = respx.patch(f"{_BASE}/v3/config/paths/patch/{_PATH}").mock(
        return_value=httpx.Response(200, json={})
    )
    await provision_path(http, base_url=f"{_BASE}/", path=_PATH, passphrase=_PASS)
    assert route.called


@respx.mock
async def test_provision_path_raises_typed_on_4xx(http: httpx.AsyncClient) -> None:
    respx.patch(f"{_BASE}/v3/config/paths/patch/{_PATH}").mock(
        return_value=httpx.Response(
            400, json={"error": "passphrase must be 10-79 characters"}
        )
    )
    with pytest.raises(MediaMTXProvisioningFailure) as ei:
        await provision_path(http, base_url=_BASE, path=_PATH, passphrase=_PASS)
    assert ei.value.reason == "status"
    assert ei.value.status == 400


@respx.mock
async def test_provision_path_raises_typed_on_5xx(http: httpx.AsyncClient) -> None:
    respx.patch(f"{_BASE}/v3/config/paths/patch/{_PATH}").mock(
        return_value=httpx.Response(503, text="MediaMTX unhealthy")
    )
    with pytest.raises(MediaMTXProvisioningFailure) as ei:
        await provision_path(http, base_url=_BASE, path=_PATH, passphrase=_PASS)
    assert ei.value.reason == "status"
    assert ei.value.status == 503


@respx.mock
async def test_provision_path_raises_typed_on_transport_error(
    http: httpx.AsyncClient,
) -> None:
    respx.patch(f"{_BASE}/v3/config/paths/patch/{_PATH}").mock(
        side_effect=httpx.ConnectError("no route to host")
    )
    with pytest.raises(MediaMTXProvisioningFailure) as ei:
        await provision_path(http, base_url=_BASE, path=_PATH, passphrase=_PASS)
    assert ei.value.reason == "transport"


@respx.mock
async def test_deprovision_path_success(http: httpx.AsyncClient) -> None:
    route = respx.delete(f"{_BASE}/v3/config/paths/delete/{_PATH}").mock(
        return_value=httpx.Response(200)
    )
    await deprovision_path(http, base_url=_BASE, path=_PATH)
    assert route.called


@respx.mock
async def test_deprovision_path_404_is_success(http: httpx.AsyncClient) -> None:
    """A 404 means MediaMTX restarted between provision and deprovision —
    the path is already gone, which is the desired terminal state."""
    respx.delete(f"{_BASE}/v3/config/paths/delete/{_PATH}").mock(
        return_value=httpx.Response(404, json={"error": "path not found"})
    )
    # Should not raise.
    await deprovision_path(http, base_url=_BASE, path=_PATH)


@respx.mock
async def test_deprovision_path_raises_on_other_4xx(http: httpx.AsyncClient) -> None:
    respx.delete(f"{_BASE}/v3/config/paths/delete/{_PATH}").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )
    with pytest.raises(MediaMTXProvisioningFailure) as ei:
        await deprovision_path(http, base_url=_BASE, path=_PATH)
    assert ei.value.reason == "status"
    assert ei.value.status == 403


@respx.mock
async def test_deprovision_path_raises_on_transport_error(
    http: httpx.AsyncClient,
) -> None:
    respx.delete(f"{_BASE}/v3/config/paths/delete/{_PATH}").mock(
        side_effect=httpx.ConnectTimeout("timeout")
    )
    with pytest.raises(MediaMTXProvisioningFailure) as ei:
        await deprovision_path(http, base_url=_BASE, path=_PATH)
    assert ei.value.reason == "transport"
