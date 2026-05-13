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
