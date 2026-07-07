"""Appliance-class sessions: per-account session TTL override.

`account_limits.session_ttl_seconds` (NULL = platform default) must drive
both the session row's `expires_at` and the minted JWT's expiry, and a
missing override must fall back to `Settings.session_token_ttl_seconds`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from upstream_api.config import get_settings
from upstream_api.services import usage
from upstream_api.services.limits import RequestedStream, effective_session_ttl_seconds
from upstream_api.services.session_lifecycle import start_session

_THIRTY_DAYS = 30 * 24 * 3600


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    def __init__(self, *, account_id: Any, ttl_override: int | None) -> None:
        self.account_id = account_id
        self.agent_id = uuid4()
        self.ttl_override = ttl_override
        self.insert_args: tuple[Any, ...] | None = None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return {
                "id": args[0],
                "account_id": self.account_id,
                "agent_id": self.agent_id,
                "source_type": "webcam",
            }
        if "from account_limits" in query:
            return {
                "max_cameras": 10,
                "max_concurrent_sessions": 5,
                "max_fps": 60,
                "max_width": 4096,
                "max_height": 2160,
                "max_bitrate_kbps": 12000,
                "allowed_codecs": ["h264"],
                "allowed_stream_types": ["webcam", "rtsp"],
                "monthly_minutes_quota": 600,
                "snapshot_storage_mb": 1024,
                "session_ttl_seconds": self.ttl_override,
                "shared_inference_enabled": False,
            }
        if "insert into stream_sessions" in query:
            self.insert_args = args
            return {
                "id": args[0],
                "state": "approved",
                "expires_at": args[19],
                "created_at": datetime.now(tz=UTC),
                "started_at": None,
                "ended_at": None,
            }
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "status from accounts" in query:
            return "active"
        if "select session_ttl_seconds from account_limits" in query:
            return self.ttl_override
        if "count(*)" in query:
            return 0
        if "to_regclass" in query:
            return False
        if "exists" in query.lower():
            return False
        return None

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"


@pytest.fixture(autouse=True)
def _reset_usage_cache() -> None:
    usage.reset_usage_table_cache()


def _requested() -> RequestedStream:
    return RequestedStream(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=2500)


async def _start(conn: FakeConn) -> Any:
    return await start_session(
        conn,
        settings=get_settings(),
        account_id=conn.account_id,
        camera_id=uuid4(),
        requested=_requested(),
    )


async def test_account_ttl_override_drives_expiry_and_jwt() -> None:
    conn = FakeConn(account_id=uuid4(), ttl_override=_THIRTY_DAYS)
    before = datetime.now(tz=UTC)
    result = await _start(conn)
    after = datetime.now(tz=UTC)

    expected_low = before + timedelta(seconds=_THIRTY_DAYS)
    expected_high = after + timedelta(seconds=_THIRTY_DAYS)
    # Row expiry (insert arg $20).
    assert conn.insert_args is not None
    row_expires: datetime = conn.insert_args[19]
    assert expected_low <= row_expires <= expected_high
    # JWT expiry mirrors the row.
    assert expected_low <= result.claims.expires_at <= expected_high


async def test_null_override_falls_back_to_platform_default() -> None:
    settings = get_settings()
    conn = FakeConn(account_id=uuid4(), ttl_override=None)
    before = datetime.now(tz=UTC)
    result = await _start(conn)
    after = datetime.now(tz=UTC)

    default = timedelta(seconds=settings.session_token_ttl_seconds)
    assert before + default <= result.claims.expires_at <= after + default


async def test_effective_ttl_helper_prefers_override() -> None:
    conn = FakeConn(account_id=uuid4(), ttl_override=_THIRTY_DAYS)
    ttl = await effective_session_ttl_seconds(
        conn,
        account_id=conn.account_id,
        default_ttl_seconds=14400,
    )
    assert ttl == _THIRTY_DAYS

    conn_default = FakeConn(account_id=uuid4(), ttl_override=None)
    ttl = await effective_session_ttl_seconds(
        conn_default,
        account_id=conn_default.account_id,
        default_ttl_seconds=14400,
    )
    assert ttl == 14400
