"""Per-snapshot brightness with a small in-memory TTL cache.

One reason this module exists separately from ``image_analysis``: brightness
is computed during a reliability summary read, which can fire on every
dashboard poll. Decoding the same JPEG twice in five minutes is wasted work.
Cache by ``snapshot_id`` (not ``camera_id``) so a freshly-captured frame
always gets a fresh decode and we never serve a brightness reading that's
older than the snapshot it refers to.

Mirrors the TTLCache pattern in ``security/deps._membership_cache``:
process-local, ``cachetools.TTLCache`` already a dep, modest sizing.
``maxsize=1024`` accommodates a few hundred cameras polled at a normal
cadence without flagging anything as old; ``ttl=300s`` means a stale entry
turns over within 5 minutes if a camera goes silent.

Failures (download error, decode failure) are intentionally **not** cached.
A transient blip should self-heal on the next read, not be locked in for
5 minutes.
"""

from __future__ import annotations

from uuid import UUID

from cachetools import TTLCache

from upstream_api.config import Settings
from upstream_api.observability.logging import get_logger
from upstream_api.services.image_analysis import (
    ImageBrightness,
    compute_image_brightness,
)
from upstream_api.services.storage import download_bytes

_logger = get_logger("snapshot_brightness")

# Same shape as security/deps._membership_cache. Process-local; gunicorn runs
# 2 workers in prod (see Dockerfile), so the cache helps ~50% of the time on a
# round-robin balancer. A larger maxsize is pointless — a busy account has
# tens of cameras, not thousands of distinct active snapshots in a 5-min window.
_brightness_cache: TTLCache[UUID, ImageBrightness] = TTLCache(maxsize=1024, ttl=300)


def _clear_cache_for_tests() -> None:
    """Reset the brightness cache. Tests only — production callers never call
    this."""
    _brightness_cache.clear()


async def get_brightness_for_snapshot(
    *,
    settings: Settings,
    snapshot_id: UUID,
    storage_key: str,
) -> ImageBrightness | None:
    """Cached brightness reading for a single snapshot.

    Returns ``None`` (and does NOT cache the absence) when the JPEG can't be
    downloaded or can't be decoded. Returning ``None`` is the evaluator's
    "skip this rule" signal — uncached so the next read tries again.
    """
    cached = _brightness_cache.get(snapshot_id)
    if cached is not None:
        return cached

    try:
        image_bytes = await download_bytes(
            settings=settings,
            bucket=settings.supabase_storage_bucket,
            storage_key=storage_key,
        )
    except Exception as exc:
        # download_bytes already logged the failure; re-log here only at debug
        # level (silent in prod's INFO) so the brightness module owns a
        # consistent "skipping this snapshot" trail without doubling the
        # warning volume.
        _logger.debug(
            "snapshot_brightness_download_skipped",
            snapshot_id=str(snapshot_id),
            error=type(exc).__name__,
        )
        return None

    brightness = compute_image_brightness(image_bytes)
    if brightness is None:
        _logger.debug(
            "snapshot_brightness_decode_skipped",
            snapshot_id=str(snapshot_id),
        )
        return None

    _brightness_cache[snapshot_id] = brightness
    return brightness
