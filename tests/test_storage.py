"""Regression tests for services/storage.py.

The live M6.2-delta failure: ``@lru_cache`` on a helper that took a pydantic
``Settings`` raised ``TypeError: unhashable type: 'Settings'`` on the first
real upload. The existing snapshot tests patch ``upload_bytes`` so they never
construct a client and never hit it. These tests call ``upload_bytes`` /
``create_signed_url`` with a real ``Settings`` (only ``create_client`` is
patched, to avoid a network call) so the unhashable-Settings path is
exercised end to end.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from upstream_api.config import get_settings
from upstream_api.services import storage


class _FakeBucket:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes, dict[str, str]]] = []

    def upload(self, key: str, data: bytes, opts: dict[str, str]) -> dict[str, Any]:
        self.uploads.append((key, data, opts))
        return {}

    def create_signed_url(self, key: str, expires_in: int) -> dict[str, str]:
        return {"signedURL": f"https://signed.example/{key}?exp={expires_in}"}


class _FakeStorage:
    def __init__(self, bucket: _FakeBucket) -> None:
        self._bucket = bucket

    def from_(self, _name: str) -> _FakeBucket:
        return self._bucket


class _FakeClient:
    def __init__(self) -> None:
        self.storage = _FakeStorage(_FakeBucket())


@pytest.fixture(autouse=True)
def _clear_client_cache() -> Any:
    # _client is lru_cached and keyed on (url, key) which are constant across
    # tests; clear it so each test's patched create_client is actually used.
    storage._client.cache_clear()
    yield
    storage._client.cache_clear()


async def test_upload_bytes_accepts_settings_object() -> None:
    settings = get_settings()
    fake = _FakeClient()
    with patch("upstream_api.services.storage.create_client", return_value=fake) as mk_create:
        # Before the fix this raised: TypeError: unhashable type: 'Settings'.
        await storage.upload_bytes(
            settings=settings,
            bucket="snapshots",
            storage_key="acct/cam/20260518/a.jpg",
            data=b"\xff\xd8\xffaaaa",
            content_type="image/jpeg",
        )
        await storage.upload_bytes(
            settings=settings,
            bucket="snapshots",
            storage_key="acct/cam/20260518/b.jpg",
            data=b"\xff\xd8\xffbbbb",
            content_type="image/jpeg",
        )
    # One client built and reused (lru_cache keyed on primitive url/key).
    assert mk_create.call_count == 1
    assert len(fake.storage.from_("snapshots").uploads) == 2


async def test_create_signed_url_accepts_settings_object() -> None:
    settings = get_settings()
    fake = _FakeClient()
    with patch("upstream_api.services.storage.create_client", return_value=fake):
        url = await storage.create_signed_url(
            settings=settings,
            bucket="snapshots",
            storage_key="acct/cam/20260518/a.jpg",
        )
    assert url.startswith("https://signed.example/")


async def test_upload_bytes_reraises_and_logs_without_secret() -> None:
    settings = get_settings()

    class _BoomBucket(_FakeBucket):
        def upload(self, key: str, data: bytes, opts: dict[str, str]) -> dict[str, Any]:
            raise RuntimeError("supabase 500")

    class _BoomClient:
        def __init__(self) -> None:
            self.storage = _FakeStorage(_BoomBucket())

    with (
        patch("upstream_api.services.storage.create_client", return_value=_BoomClient()),
        pytest.raises(RuntimeError, match="supabase 500"),
    ):
        await storage.upload_bytes(
            settings=settings,
            bucket="snapshots",
            storage_key="acct/cam/20260518/a.jpg",
            data=b"\xff\xd8\xff",
            content_type="image/jpeg",
        )
