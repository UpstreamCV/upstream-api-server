"""MediaMTX path-management API client for per-session dynamic paths.

Phase 2 of the SRT-side security work: each `stream_sessions` row gets a
matching MediaMTX path created via the HTTP API at session start, with
its own random `srtPublishPassphrase`. The path is deleted at session
end. Two function shapes — `provision_path` and `deprovision_path` —
both async, both raise typed exceptions on transport failure.

Rationale for using the HTTP API instead of static YAML:
- We need per-session passphrases. Static `pathDefaults.srtPublishPassphrase`
  is a single key across all paths; dynamic per-path config is the only way
  to rotate per session.
- MediaMTX's `paths/patch` is idempotent (creates if missing, updates if
  present), so a retry-on-error / re-provision-after-restart loop doesn't
  need to disambiguate "does it exist" first.

Why no URL token:
- The api-server reaches MediaMTX's HTTP API over the public internet
  (different physical hosts), but the streaming-server's `authHTTPExclude`
  carves out `{action: api, ip: <api-ip>/32}` so MediaMTX accepts the
  api-server's calls without the authHTTP callback firing. Additionally,
  the host-level firewall on the streaming-server only allows TCP 9997
  from the api-server's Elastic IP. Two layers of network-gate; no need
  to add a shared-secret URL parameter the api-server would have to
  carry around.

Restart recovery is not in this module — see plan FU-7. After a MediaMTX
restart, in-flight sessions break until the agent retries `/start`,
which calls `provision_path` again. A periodic reconciler would close
the gap; deferred.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass(frozen=True, slots=True)
class MediaMTXProvisioningFailure(Exception):
    """Raised when the MediaMTX HTTP API call fails.

    Distinguishes between transport-level failures (`reason='transport'`,
    no response) and 4xx/5xx responses we got but didn't like
    (`reason='status'`, with the upstream status code).

    `start_session` catches this, marks the session `failed` with
    `failure_code='mediamtx_provision_failed'`, and surfaces a 502 to
    the agent. The agent retries `/start`, which provisions afresh.
    """

    reason: str  # 'transport' | 'status'
    status: int | None = None
    body: str | None = None


def _hook_paths() -> dict[str, object]:
    """Lifecycle hook config that every per-session path needs.

    Lifted to a helper so the values are documented in one place and so
    `provision_path` doesn't have to inline the full dict each call.

    The streaming-server side ships these scripts inside the MediaMTX
    container at `/hooks/` (mounted via `docker-compose.yml`). Removing
    the catch-all `all_others` block from `mediamtx.yml` (Phase 2 deploy
    step) leaves these per-session paths as the only places the hooks
    are defined; they must therefore be set explicitly here.
    """
    return {
        "runOnReady": "/hooks/on_ready.sh",
        "runOnReadyRestart": False,
        "runOnNotReady": "/hooks/on_not_ready.sh",
    }


async def provision_path(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    path: str,
    passphrase: str,
) -> None:
    """Create or update the MediaMTX path with a session-specific passphrase.

    MediaMTX 1.9.3's path-management API is split: `POST add/{name}` only
    creates (400 on existing), `PATCH patch/{name}` only updates (404 on
    missing). Neither is independently idempotent. We try POST first
    (the common case — each `sess_<hex>` is unique per session, so the
    path doesn't yet exist) and fall back to PATCH on a 400 conflict
    (the retry case — api-server retried after a prior failure, or a
    previous deprovision failed leaving an orphan path).

    Args:
        client: shared `httpx.AsyncClient` from the app lifespan (the same
            pool used by the WHEP proxy; one connection per host).
        base_url: e.g. `http://ingest.upstreamcv.com:19997`. No trailing slash.
        path: the streamid suffix without the `publish:` prefix, e.g.
            `sess_2a88f3405d513c05`. Must already match MediaMTX's path
            naming rules (alphanumeric + `_`). Caller is responsible.
        passphrase: 10-79 char libsrt passphrase. Must be the same string
            the api-server returns to the agent in the SRT URL.

    Raises:
        MediaMTXProvisioningFailure: transport error or non-2xx (after the
            POST→PATCH fallback has been exhausted).
    """
    base = base_url.rstrip("/")
    body: dict[str, object] = {
        "source": "publisher",
        "srtPublishPassphrase": passphrase,
        **_hook_paths(),
    }

    add_url = f"{base}/v3/config/paths/add/{path}"
    try:
        response = await client.post(add_url, json=body)
    except httpx.HTTPError as exc:
        raise MediaMTXProvisioningFailure(reason="transport", body=str(exc)) from exc

    if 200 <= response.status_code < 300:
        return

    # MediaMTX 1.9.3 returns 400 with body `{"error":"path already exists"}`
    # when add is attempted on an existing path. Fall back to PATCH which
    # updates in place. We don't parse the body — any 400 from add could
    # plausibly be "already exists" given that we control the path name and
    # have already shape-validated the payload.
    if response.status_code != 400:
        raise MediaMTXProvisioningFailure(
            reason="status",
            status=response.status_code,
            body=response.text[:512],
        )

    patch_url = f"{base}/v3/config/paths/patch/{path}"
    try:
        response = await client.patch(patch_url, json=body)
    except httpx.HTTPError as exc:
        raise MediaMTXProvisioningFailure(reason="transport", body=str(exc)) from exc

    if not 200 <= response.status_code < 300:
        raise MediaMTXProvisioningFailure(
            reason="status",
            status=response.status_code,
            body=response.text[:512],
        )


async def deprovision_path(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    path: str,
) -> None:
    """Delete the per-session MediaMTX path.

    Tolerates 404 — MediaMTX may have restarted (dropping its in-memory
    paths) between provisioning and now, in which case there's nothing
    to delete and that's success. Any other non-2xx raises.

    Raises:
        MediaMTXProvisioningFailure: transport error or non-2xx (excluding
            404, which is silently treated as success).
    """
    url = f"{base_url.rstrip('/')}/v3/config/paths/delete/{path}"
    try:
        response = await client.delete(url)
    except httpx.HTTPError as exc:
        raise MediaMTXProvisioningFailure(reason="transport", body=str(exc)) from exc

    if response.status_code == 404:
        return
    if not 200 <= response.status_code < 300:
        raise MediaMTXProvisioningFailure(
            reason="status",
            status=response.status_code,
            body=response.text[:512],
        )
