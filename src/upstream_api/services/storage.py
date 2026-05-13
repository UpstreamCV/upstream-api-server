"""Supabase Storage upload helpers.

The supabase-py client is sync; we wrap each call in `asyncio.to_thread` so a
slow upload doesn't block the event loop. The client is constructed lazily and
cached per-settings instance — the lru_cache on get_settings already ensures
one shared client across requests in the same worker.
"""

from __future__ import annotations

import asyncio
from functools import lru_cache

from supabase import Client, create_client

from upstream_api.config import Settings


@lru_cache(maxsize=1)
def _client(settings: Settings) -> Client:
    return create_client(
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
    """Upload `data` to `bucket/storage_key`.

    Raises whatever the Supabase client raises on failure (HTTP error, quota,
    etc.) — the caller is expected to translate to an ApiError.
    """
    client = _client(settings)
    await asyncio.to_thread(
        client.storage.from_(bucket).upload,
        storage_key,
        data,
        {"content-type": content_type, "upsert": "true"},
    )


async def create_signed_url(
    *,
    settings: Settings,
    bucket: str,
    storage_key: str,
    expires_in: int = 60,
) -> str:
    """Return a time-limited signed URL for `bucket/storage_key`.

    Used by `GET /v1/cameras/{id}/latest-frame` so dashboards can render a
    JPEG without holding the Supabase service-role key client-side.
    """
    client = _client(settings)
    response = await asyncio.to_thread(
        client.storage.from_(bucket).create_signed_url,
        storage_key,
        expires_in,
    )
    # supabase-py returns dict with key 'signedURL' (camelCase from the JS-style API).
    signed_url = response.get("signedURL") if isinstance(response, dict) else None
    if not signed_url:
        raise RuntimeError(f"Supabase did not return a signed URL for {storage_key}")
    return signed_url
