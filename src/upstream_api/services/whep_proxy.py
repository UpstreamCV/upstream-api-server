"""WHEP signaling proxy to MediaMTX.

The api-server is the auth chokepoint for the dashboard's WebRTC playback.
The browser POSTs an SDP offer to the api-server (under the
``/v1/stream-sessions/{id}/whep`` route), the api-server checks that the
caller has access to the session, then forwards the offer to MediaMTX and
returns the SDP answer.

UDP/ICE media flows browser ↔ MediaMTX directly; only the HTTP signaling
makes the api-server hop.

Statelessness: the api-server reconstructs the MediaMTX URL from
``session.streamid`` + a ``resource_id`` path param on DELETE. No mapping
table is stored. If the api-server restarts mid-session the ICE timeout
(~30 s) reclaims orphaned readers on MediaMTX.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import httpx

# MediaMTX 1.9.3 returns `Location: /<path>/whep/<uuid>` on a successful
# POST (verified against a running upstream-streaming-server on 2026-05-14;
# the older `/whepsession/<uuid>` format from earlier MediaMTX versions is
# kept here as a fallback so we don't break older deployments). We extract
# the trailing resource id. The allowed character set must match
# `_RESOURCE_ID_RE` in routers/stream_sessions.py — anything we accept here
# must also pass DELETE validation, otherwise POST succeeds and DELETE 400s.
_LOCATION_RE = re.compile(r"/whep(?:session)?/([A-Za-z0-9_~-]{1,128})/?$")


class UpstreamMediaMTXUnavailable(Exception):
    """Raised when MediaMTX cannot be reached or times out.

    The router translates this to ``502 upstream_unavailable``. Distinct from
    a MediaMTX-returned error status (those flow through ``WhepProxyResult``).
    """


@dataclass(frozen=True, slots=True)
class WhepProxyResult:
    status: int
    body: bytes
    content_type: str | None
    # Resource id parsed out of MediaMTX's Location header, if present. None
    # if the upstream call failed, returned a non-2xx, or omitted Location.
    resource_id: str | None


def parse_mediamtx_location(location: str | None) -> str | None:
    """Pull the resource id out of MediaMTX's ``Location`` header.

    Returns ``None`` for missing/empty/malformed values. The router treats a
    successful (2xx) MediaMTX response without a parseable Location as a
    protocol failure (502 to the caller) — MediaMTX has consistently emitted
    Location on success since 1.0.
    """
    if not location:
        return None
    match = _LOCATION_RE.search(location.strip())
    if match is None:
        return None
    return match.group(1)


async def proxy_whep_offer(
    client: httpx.AsyncClient,
    *,
    mediamtx_base_url: str,
    path: str,
    sdp_offer: bytes,
) -> WhepProxyResult:
    """Forward an SDP offer to MediaMTX's WHEP endpoint and return its reply."""
    url = f"{mediamtx_base_url.rstrip('/')}/{path}/whep"
    try:
        response = await client.post(
            url,
            content=sdp_offer,
            headers={"Content-Type": "application/sdp"},
        )
    except httpx.HTTPError as exc:
        raise UpstreamMediaMTXUnavailable(str(exc)) from exc

    location = response.headers.get("Location")
    return WhepProxyResult(
        status=response.status_code,
        body=response.content,
        content_type=response.headers.get("Content-Type"),
        resource_id=parse_mediamtx_location(location) if response.is_success else None,
    )


async def proxy_whep_delete(
    client: httpx.AsyncClient,
    *,
    mediamtx_base_url: str,
    path: str,
    resource_id: str,
) -> int:
    """Tear down a WHEP session at MediaMTX. Returns the upstream status code.

    Caller treats this as best-effort: a 404 from MediaMTX is still success
    from the dashboard's perspective (session already gone). Transport
    failures bubble as :class:`UpstreamMediaMTXUnavailable`.
    """
    # MediaMTX 1.9.3 exposes the session endpoint at `/<path>/whep/<id>`;
    # earlier MediaMTX versions used `/whepsession/<id>`. The Location header
    # MediaMTX returns from POST is the canonical URL, but we reconstruct
    # statelessly here from the resource_id. If we ever support a deploy
    # against pre-1.9 MediaMTX, gate this on a settings flag — for now we
    # only target 1.9.x.
    url = f"{mediamtx_base_url.rstrip('/')}/{path}/whep/{resource_id}"
    try:
        response = await client.delete(url)
    except httpx.HTTPError as exc:
        raise UpstreamMediaMTXUnavailable(str(exc)) from exc
    return response.status_code
