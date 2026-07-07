from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import asyncpg
import pytest

from upstream_api.config import get_settings
from upstream_api.errors import ConflictError
from upstream_api.services import usage
from upstream_api.services.limits import LimitsSnapshot, RequestedStream
from upstream_api.services.session_lifecycle import start_session


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Records SQL calls and lets a test inject an exception on the INSERT."""

    def __init__(
        self,
        camera_account_id: UUID,
        camera_agent_id: UUID,
        insert_raises: type[BaseException] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.camera_agent_id = camera_agent_id
        self.insert_raises = insert_raises
        self.executed: list[str] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return {
                "id": args[0],
                "account_id": self.camera_account_id,
                "agent_id": self.camera_agent_id,
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
                "session_ttl_seconds": None,
                "shared_inference_enabled": False,
            }
        if "insert into stream_sessions" in query:
            if self.insert_raises is not None:
                raise self.insert_raises("duplicate")
            return {
                "id": args[0],
                "state": "approved",
                "expires_at": None,
                "created_at": datetime.now(tz=UTC),
                "started_at": None,
                "ended_at": None,
            }
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "status from accounts" in query:
            return "active"
        if "count(*)" in query:
            return 0
        if "to_regclass" in query:
            return False
        if "exists" in query.lower():
            return False
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append(query)
        return "OK"


@pytest.fixture(autouse=True)
def _reset_usage_cache() -> None:
    usage.reset_usage_table_cache()


async def test_start_session_translates_unique_violation_to_conflict() -> None:
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(
        camera_account_id=account_id,
        camera_agent_id=uuid4(),
        insert_raises=asyncpg.UniqueViolationError,
    )
    requested = RequestedStream(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=2500)

    with (
        patch(
            "upstream_api.services.session_lifecycle.mint_session_token",
            return_value=("dummy-token", _DummyClaims()),
        ),
        pytest.raises(ConflictError) as exc_info,
    ):
        await start_session(
            conn,
            settings=get_settings(),
            account_id=account_id,
            camera_id=camera_id,
            requested=requested,
        )
    assert exc_info.value.code == "camera_already_streaming"


async def test_start_session_srt_url_uses_settings_host_and_port() -> None:
    """srt_url reads host/port from Settings — no hardcoded value in _build_srt_url."""
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(camera_account_id=account_id, camera_agent_id=uuid4())
    requested = RequestedStream(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=2500)

    # Override both with values that are clearly not the default so a regression
    # to a hardcoded host or port can't be masked by happening to match.
    settings = get_settings().model_copy(
        update={"srt_ingest_host": "127.0.0.1", "srt_ingest_port": 9999}
    )
    with patch(
        "upstream_api.services.session_lifecycle.mint_session_token",
        return_value=("dummy-token", _DummyClaims()),
    ):
        result = await start_session(
            conn,
            settings=settings,
            account_id=account_id,
            camera_id=camera_id,
            requested=requested,
        )
    expected_prefix = "srt://127.0.0.1:9999?streamid=publish:sess_"
    assert result.srt_url.startswith(expected_prefix), result.srt_url
    assert "ingest.upstreamcv.com" not in result.srt_url
    assert "ingest.upstream.tv" not in result.srt_url
    # Phase 2 / FU-6: per-session AES passphrase must be present in the URL
    # and surfaced on the StartSessionResult so the router can PATCH MediaMTX
    # with the same value. URL-encoded; pbkeylen=32 selects AES-256.
    assert result.srt_publish_passphrase, "passphrase must be populated"
    assert 10 <= len(result.srt_publish_passphrase) <= 79  # libsrt range
    assert "passphrase=" in result.srt_url
    assert "pbkeylen=32" in result.srt_url


async def test_start_session_takes_advisory_lock_per_account() -> None:
    """The advisory lock is taken in check_session_start before the count(*)."""
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(camera_account_id=account_id, camera_agent_id=uuid4())
    requested = RequestedStream(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=2500)

    with patch(
        "upstream_api.services.session_lifecycle.mint_session_token",
        return_value=("dummy-token", _DummyClaims()),
    ):
        await start_session(
            conn,
            settings=get_settings(),
            account_id=account_id,
            camera_id=camera_id,
            requested=requested,
        )
    assert any("pg_advisory_xact_lock" in q for q in conn.executed), (
        "check_session_start must take a per-account advisory lock"
    )


class _DummyClaims:
    """Stub for SessionTokenClaims; only ``expires_at`` is read by callers."""

    expires_at = None
    jti = None
    account_id = None
    agent_id = None
    camera_id = None
    session_id = None
    limits = LimitsSnapshot(width=0, height=0, fps=0, codec="h264", bitrate_kbps=0)
