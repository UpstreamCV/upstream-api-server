"""Supabase Storage upload helpers.

The supabase-py client is sync; we wrap each call in ``asyncio.to_thread`` so
a slow upload doesn't block the event loop.

The client is cached per ``(supabase_url, service_role_key)`` pair — **not**
per ``Settings`` object. A pydantic ``Settings`` instance is unhashable, so an
``@lru_cache``d helper that took ``Settings`` raised
``TypeError: unhashable type: 'Settings'`` on the first real upload (the unit
tests never caught it because they patch ``upload_bytes`` and never construct
a client). Keying on the two primitive strings is hashable and still yields
one shared client per worker (the URL/key are process-stable).
"""

from __future__ import annotations

import asyncio
from functools import lru_cache

from supabase import Client, create_client

from upstream_api.config import Settings
from upstream_api.observability.logging import get_logger

_logger = get_logger("storage")


@lru_cache(maxsize=2)
def _client(supabase_url: str, service_role_key: str) -> Client:
    """Cached Supabase client. Args are primitives so lru_cache can hash them."""
    return create_client(supabase_url, service_role_key)


def _client_for(settings: Settings) -> Client:
    return _client(
        settings.supabase_url,
        settings.supabase_service_role_key.get_secret_value(),
    )


async def upload_bytes(
    *,
    settings: Settings,
    bucket: str,
    storage_key: str,
    data: bytes,
    content_type: str,
) -> None:
    """Upload ``data`` to ``bucket/storage_key``.

    On failure logs a non-secret structured warning (``bucket`` /
    ``storage_key`` already carry account/camera ids that appear elsewhere in
    logs; the service-role key and ``data`` are never logged) and re-raises so
    the caller's transaction rolls back and the API returns ``internal_error``.
    """
    client = _client_for(settings)
    try:
        await asyncio.to_thread(
            client.storage.from_(bucket).upload,
            storage_key,
            data,
            {"content-type": content_type, "upsert": "true"},
        )
    except Exception as exc:
        _logger.warning(
            "storage_upload_failed",
            bucket=bucket,
            storage_key=storage_key,
            error=type(exc).__name__,
        )
        raise


async def create_signed_url(
    *,
    settings: Settings,
    bucket: str,
    storage_key: str,
    expires_in: int = 60,
) -> str:
    """Return a time-limited signed URL for ``bucket/storage_key``.

    Used by ``GET /v1/cameras/{id}/latest-frame`` so dashboards can render a
    JPEG without holding the Supabase service-role key client-side.
    """
    client = _client_for(settings)
    try:
        response = await asyncio.to_thread(
            client.storage.from_(bucket).create_signed_url,
            storage_key,
            expires_in,
        )
    except Exception as exc:
        _logger.warning(
            "storage_signed_url_failed",
            bucket=bucket,
            storage_key=storage_key,
            error=type(exc).__name__,
        )
        raise
    # supabase-py returns dict with key 'signedURL' (camelCase from the JS-style API).
    signed_url = response.get("signedURL") if isinstance(response, dict) else None
    if not signed_url:
        raise RuntimeError(f"Supabase did not return a signed URL for {storage_key}")
    return signed_url
