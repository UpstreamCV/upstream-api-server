from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from upstream_api.errors import ConflictError, NotFoundError
from upstream_api.services.stream_validation import (
    mark_ended,
    mark_failed,
    mark_streaming,
)


class FakeConn:
    """Stubs `update ... returning` + `insert into stream_validations` calls.

    Captures every ``execute`` call so tests can assert on both the
    ``stream_validations`` insert and the ``cameras.status`` mirror update.
    """

    def __init__(
        self,
        *,
        update_returns: dict[str, Any] | None,
        current_state: str | None = "approved",
    ) -> None:
        self.update_returns = update_returns
        self.current_state = current_state
        self.executed_inserts: list[tuple[str, tuple[Any, ...]]] = []
        self.executed_camera_updates: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "update stream_sessions" in query:
            return self.update_returns
        if "from stream_sessions" in query:
            # Fallback probe for "row missing vs wrong state" disambiguation.
            if self.current_state is None:
                return None
            return {
                "id": args[0],
                "account_id": uuid4(),
                "agent_id": uuid4(),
                "camera_id": uuid4(),
                "state": self.current_state,
                "expires_at": datetime.now(tz=UTC),
                "session_token_jti": uuid4(),
                "streamid": "publish:sess_0123456789abcdef",
                "lim_width": 1280,
                "lim_height": 720,
                "lim_fps": 30,
                "lim_codec": "h264",
                "lim_bitrate_kbps": 3000,
                "lim_latency_mode": "balanced",
            }
        return None

    async def execute(self, query: str, *args: Any) -> str:
        if "insert into stream_validations" in query:
            self.executed_inserts.append((query, args))
        elif "update cameras" in query:
            self.executed_camera_updates.append((query, args))
        return "OK"


async def test_mark_streaming_transitions_approved_to_streaming() -> None:
    session_id = uuid4()
    conn = FakeConn(
        update_returns={
            "state": "streaming",
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    observed = {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 2980}
    state = await mark_streaming(
        conn,
        session_id=session_id,
        observed=observed,
        started_at=datetime.now(tz=UTC),
    )
    assert state == "streaming"
    # Validation row should be written with passed=true.
    assert len(conn.executed_inserts) == 1
    assert "true" in conn.executed_inserts[0][0].lower()
    # cameras.status should be mirrored to 'streaming'.
    assert len(conn.executed_camera_updates) == 1
    cam_q, cam_args = conn.executed_camera_updates[0]
    assert "update cameras" in cam_q
    assert cam_args[0] == session_id
    assert cam_args[1] == "streaming"


async def test_mark_streaming_idempotent_on_streaming_reconnect() -> None:
    """SRT publisher reconnect fires runOnReady on a session already streaming.

    The hook should still nudge cameras.status to 'streaming' (in case it
    drifted), but must NOT write a duplicate stream_validations row.
    """
    session_id = uuid4()
    # Update returns nothing because state is already 'streaming', not 'approved'.
    # Probe finds the session in 'streaming' state.
    conn = FakeConn(update_returns=None, current_state="streaming")
    observed = {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 2980}
    state = await mark_streaming(
        conn,
        session_id=session_id,
        observed=observed,
        started_at=datetime.now(tz=UTC),
    )
    assert state == "streaming"
    # No new validation row.
    assert conn.executed_inserts == []
    # cameras.status still nudged to 'streaming'.
    assert len(conn.executed_camera_updates) == 1
    assert conn.executed_camera_updates[0][1][1] == "streaming"


async def test_mark_streaming_rejects_when_not_approved() -> None:
    conn = FakeConn(update_returns=None, current_state="ended")
    with pytest.raises(ConflictError) as exc:
        await mark_streaming(
            conn,
            session_id=uuid4(),
            observed={
                "codec": "h264",
                "width": 1280,
                "height": 720,
                "fps": 30,
                "bitrate_kbps": 3000,
            },
            started_at=datetime.now(tz=UTC),
        )
    assert exc.value.code == "stream_state_invalid"
    assert exc.value.details["current_state"] == "ended"
    # Camera status should NOT be touched on a wrong-state rejection.
    assert conn.executed_camera_updates == []


async def test_mark_streaming_404_when_session_missing() -> None:
    conn = FakeConn(update_returns=None, current_state=None)
    with pytest.raises(NotFoundError) as exc:
        await mark_streaming(
            conn,
            session_id=uuid4(),
            observed={
                "codec": "h264",
                "width": 1280,
                "height": 720,
                "fps": 30,
                "bitrate_kbps": 3000,
            },
            started_at=datetime.now(tz=UTC),
        )
    assert exc.value.code == "session_not_found"


async def test_mark_ended_idempotent_on_terminal_state() -> None:
    # No row returned by UPDATE (because state was already 'ended'),
    # but a probe finds it — we should return that state, not raise.
    conn = FakeConn(update_returns=None, current_state="ended")
    state, duration = await mark_ended(
        conn,
        session_id=uuid4(),
        ended_at=datetime.now(tz=UTC),
        reason="client_disconnect",
    )
    assert state == "ended"
    assert duration is None
    # No camera mirror on idempotent terminal — it was already updated when
    # the session originally ended.
    assert conn.executed_camera_updates == []


async def test_mark_ended_computes_duration_from_started_at() -> None:
    started = datetime.now(tz=UTC) - timedelta(minutes=5)
    ended = datetime.now(tz=UTC)
    session_id = uuid4()
    conn = FakeConn(
        update_returns={
            "state": "ended",
            "started_at": started,
            "ended_at": ended,
        }
    )
    state, duration = await mark_ended(
        conn,
        session_id=session_id,
        ended_at=ended,
        reason="client_disconnect",
    )
    assert state == "ended"
    assert duration is not None
    # ~300 seconds; allow a small wall-clock window for the two now() calls.
    assert 295 <= duration <= 305
    # cameras.status should have been mirrored to 'idle'.
    assert len(conn.executed_camera_updates) == 1
    cam_q, cam_args = conn.executed_camera_updates[0]
    assert "update cameras" in cam_q
    assert cam_args[0] == session_id
    assert cam_args[1] == "idle"


async def test_mark_failed_writes_validation_row_with_mismatch() -> None:
    session_id = uuid4()
    conn = FakeConn(
        update_returns={
            "state": "failed",
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    state = await mark_failed(
        conn,
        session_id=session_id,
        observed={"codec": "h265", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 3000},
        mismatch_fields=["codec"],
        failure_code="codec_mismatch",
        failure_message="expected h264, observed h265",
    )
    assert state == "failed"
    assert len(conn.executed_inserts) == 1
    insert_query, insert_args = conn.executed_inserts[0]
    assert "false" in insert_query.lower()
    # mismatch_fields was the 4th positional arg to the insert.
    assert insert_args[3] == ["codec"]
    # cameras.status should have been mirrored to 'error'.
    assert len(conn.executed_camera_updates) == 1
    assert conn.executed_camera_updates[0][1] == (session_id, "error")
