"""Shared httpx.AsyncClient bound to the FastAPI app lifespan.

Outbound calls (e.g. WHEP proxy to MediaMTX) reuse a single connection-pooled
client created at startup and closed on shutdown. Per-request clients would
re-handshake TCP/TLS each time, which matters under any sustained load.
"""

from __future__ import annotations

from typing import Annotated

import httpx
from fastapi import Depends, Request


def get_http_client(request: Request) -> httpx.AsyncClient:
    """Return the app-scoped httpx client created in lifespan."""
    client = getattr(request.app.state, "http_client", None)
    if client is None:
        # In tests that bypass lifespan, fall back to a transient client so
        # the dep doesn't crash. The lifespan path is the production hot path.
        client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)
        request.app.state.http_client = client
    return client


_DEFAULT_TIMEOUT = httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0)


def build_http_client() -> httpx.AsyncClient:
    """Construct the lifespan-managed client. Called from app.py."""
    return httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)


HttpClientDep = Annotated[httpx.AsyncClient, Depends(get_http_client)]
