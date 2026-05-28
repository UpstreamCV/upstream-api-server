"""Per-snapshot brightness cache — behavior tests.

Pins the cache contract:

- first call decodes (loader called); result is cached
- second call with same snapshot_id is served from cache (loader not called)
- download failure returns None and is NOT cached (next call retries)
- decode failure returns None and is NOT cached (next call retries)

The async loaders are patched at the import site inside the module so the
test never touches real Supabase Storage or Pillow.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from upstream_api.config import Settings, get_settings
from upstream_api.services import snapshot_brightness as sb
from upstream_api.services.image_analysis import ImageBrightness


@pytest.fixture(autouse=True)
def _clean_cache() -> None:
    sb._clear_cache_for_tests()


def _settings() -> Settings:
    return get_settings()


class _DownloadStub:
    """Counts calls and returns the configured payload (or raises)."""

    def __init__(self, payload: bytes | Exception) -> None:
        self.payload = payload
        self.calls = 0

    async def __call__(self, *, settings: Settings, bucket: str, storage_key: str) -> bytes:
        self.calls += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _DecodeStub:
    def __init__(self, payload: ImageBrightness | None) -> None:
        self.payload = payload
        self.calls = 0

    def __call__(self, image_bytes: bytes) -> ImageBrightness | None:
        self.calls += 1
        return self.payload


async def test_first_call_decodes_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    download = _DownloadStub(payload=b"jpeg-bytes")
    decode = _DecodeStub(ImageBrightness(mean=42.0, width=640, height=480))
    monkeypatch.setattr(sb, "download_bytes", download)
    monkeypatch.setattr(sb, "compute_image_brightness", decode)

    sid = uuid4()
    first = await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="acct/cam/d/k.jpg"
    )
    assert first == ImageBrightness(mean=42.0, width=640, height=480)
    assert download.calls == 1
    assert decode.calls == 1


async def test_second_call_returns_cached_value(monkeypatch: pytest.MonkeyPatch) -> None:
    download = _DownloadStub(payload=b"jpeg-bytes")
    decode = _DecodeStub(ImageBrightness(mean=42.0, width=640, height=480))
    monkeypatch.setattr(sb, "download_bytes", download)
    monkeypatch.setattr(sb, "compute_image_brightness", decode)

    sid = uuid4()
    await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="acct/cam/d/k.jpg"
    )
    cached = await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="acct/cam/d/k.jpg"
    )
    assert cached is not None
    assert cached.mean == 42.0
    # The second call must NOT have decoded or downloaded again.
    assert download.calls == 1
    assert decode.calls == 1


async def test_distinct_snapshots_decode_independently(monkeypatch: pytest.MonkeyPatch) -> None:
    """A new snapshot id always pays the decode — the cache must not collapse
    distinct snapshots onto each other."""
    download = _DownloadStub(payload=b"jpeg-bytes")
    decode = _DecodeStub(ImageBrightness(mean=50.0, width=1, height=1))
    monkeypatch.setattr(sb, "download_bytes", download)
    monkeypatch.setattr(sb, "compute_image_brightness", decode)

    await sb.get_brightness_for_snapshot(settings=_settings(), snapshot_id=uuid4(), storage_key="a")
    await sb.get_brightness_for_snapshot(settings=_settings(), snapshot_id=uuid4(), storage_key="b")
    assert download.calls == 2
    assert decode.calls == 2


async def test_download_failure_returns_none_and_does_not_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download = _DownloadStub(payload=RuntimeError("network blip"))
    decode = _DecodeStub(ImageBrightness(mean=999.0, width=1, height=1))  # unused
    monkeypatch.setattr(sb, "download_bytes", download)
    monkeypatch.setattr(sb, "compute_image_brightness", decode)

    sid = uuid4()
    first = await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="k"
    )
    assert first is None
    # Now succeed on the retry — the failure must NOT have been cached.
    download.payload = b"jpeg-bytes"
    second = await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="k"
    )
    assert second is not None
    assert second.mean == 999.0
    assert download.calls == 2
    assert decode.calls == 1  # only the successful path decoded


async def test_decode_failure_returns_none_and_does_not_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download = _DownloadStub(payload=b"garbage-bytes")
    decode = _DecodeStub(None)
    monkeypatch.setattr(sb, "download_bytes", download)
    monkeypatch.setattr(sb, "compute_image_brightness", decode)

    sid = uuid4()
    first = await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="k"
    )
    assert first is None
    # Subsequent successful decode is allowed to populate the cache.
    decode.payload = ImageBrightness(mean=10.0, width=1, height=1)
    second = await sb.get_brightness_for_snapshot(
        settings=_settings(), snapshot_id=sid, storage_key="k"
    )
    assert second is not None
    assert download.calls == 2  # second call did re-download
    assert decode.calls == 2
