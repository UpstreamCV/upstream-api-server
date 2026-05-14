"""State-transition helpers for the four MediaMTX ingest webhooks.

Each transition is encoded as a single SQL statement so the DB is the only
source of truth for session state. Callers must hold a transaction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.errors import ConflictError, NotFoundError


@dataclass(frozen=True, slots=True)
class SessionStateRow:
    id: UUID
    account_id: UUID
    agent_id: UUID
    camera_id: UUID
    state: str
    expires_at: datetime
    session_token_jti: UUID
    lim_width: int
    lim_height: int
    lim_fps: int
    lim_codec: str
    lim_bitrate_kbps: int
    lim_latency_mode: str


_SESSION_STATE_COLUMNS = (
    "id, account_id, agent_id, camera_id, state, expires_at, session_token_jti, "
    "lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps, lim_latency_mode"
)


def _session_state_row(row: dict[str, Any]) -> SessionStateRow:
    return SessionStateRow(
        id=row["id"],
        account_id=row["account_id"],
        agent_id=row["agent_id"],
        camera_id=row["camera_id"],
        state=row["state"],
        expires_at=row["expires_at"],
        session_token_jti=row["session_token_jti"],
        lim_width=row["lim_width"],
        lim_height=row["lim_height"],
        lim_fps=row["lim_fps"],
        lim_codec=row["lim_codec"],
        lim_bitrate_kbps=row["lim_bitrate_kbps"],
        lim_latency_mode=row["lim_latency_mode"],
    )


async def load_session_state(conn: asyncpg.Connection, session_id: UUID) -> SessionStateRow | None:
    row = await conn.fetchrow(
        f"select {_SESSION_STATE_COLUMNS} from stream_sessions where id = $1",
        session_id,
    )
    if row is None:
        return None
    return _session_state_row(row)


async def load_session_by_streamid(
    conn: asyncpg.Connection, streamid: str
) -> SessionStateRow | None:
    row = await conn.fetchrow(
        f"select {_SESSION_STATE_COLUMNS} from stream_sessions where streamid = $1",
        streamid,
    )
    if row is None:
        return None
    return _session_state_row(row)


async def mark_streaming(
    conn: asyncpg.Connection,
    *,
    session_id: UUID,
    observed: dict[str, Any],
    started_at: datetime,
) -> str:
    """Transition ``approved`` -> ``streaming`` and write the ffprobe snapshot.

    Inserts a passing ``stream_validations`` row alongside the transition so
    the history of "we accepted this stream" is auditable. SRT publisher
    reconnects can fire the streaming server's ``runOnReady`` hook on a
    session already in ``streaming``; that case is idempotent — no duplicate
    validation row is written, but ``cameras.status`` is still nudged to
    ``streaming`` in case it drifted. Both the session row and the parent
    ``cameras.status`` are kept in lockstep so the dashboard cameras list
    reflects live state.
    """
    row = await conn.fetchrow(
        """
        update stream_sessions
           set state = 'streaming',
               started_at = $2,
               validated_width = $3,
               validated_height = $4,
               validated_fps = $5,
               validated_codec = $6,
               validated_bitrate_kbps = $7
         where id = $1
           and state = 'approved'
         returning state, lim_width, lim_height, lim_fps, lim_codec,
                   lim_bitrate_kbps, lim_latency_mode
        """,
        session_id,
        started_at,
        observed["width"],
        observed["height"],
        int(observed["fps"]),
        observed["codec"],
        observed["bitrate_kbps"],
    )
    if row is None:
        # Disambiguate: missing row vs. already-streaming reconnect vs. wrong state.
        existing = await load_session_state(conn, session_id)
        if existing is None:
            raise NotFoundError("Session not found.", code="session_not_found")
        if existing.state == "streaming":
            await _sync_camera_status(conn, session_id, "streaming")
            return existing.state
        raise ConflictError(
            f"Session is in state '{existing.state}', expected 'approved'.",
            code="stream_state_invalid",
            details={"current_state": existing.state, "expected": "approved"},
        )

    await _sync_camera_status(conn, session_id, "streaming")

    expected = {
        "codec": row["lim_codec"],
        "width": row["lim_width"],
        "height": row["lim_height"],
        "fps": row["lim_fps"],
        "bitrate_kbps": row["lim_bitrate_kbps"],
        "latency_mode": row["lim_latency_mode"],
    }
    await conn.execute(
        """
        insert into stream_validations (session_id, passed, observed, expected, mismatch_fields)
        values ($1, true, $2::jsonb, $3::jsonb, '{}')
        """,
        session_id,
        json.dumps(observed),
        json.dumps(expected),
    )
    return str(row["state"])


async def mark_ended(
    conn: asyncpg.Connection,
    *,
    session_id: UUID,
    ended_at: datetime,
    reason: str,
) -> tuple[str, float | None]:
    """Transition any non-terminal state -> ``ended``. Idempotent.

    Mirrors the transition onto ``cameras.status = 'idle'`` so the dashboard
    cameras list returns to "Idle" once the stream stops.
    """
    row = await conn.fetchrow(
        """
        update stream_sessions
           set state = 'ended',
               ended_at = $2,
               end_reason = $3
         where id = $1
           and state in ('approved', 'streaming')
         returning state, started_at, ended_at
        """,
        session_id,
        ended_at,
        reason,
    )
    if row is None:
        # Either the session does not exist or it's already terminal.
        existing = await load_session_state(conn, session_id)
        if existing is None:
            raise NotFoundError("Session not found.", code="session_not_found")
        return existing.state, None

    await _sync_camera_status(conn, session_id, "idle")

    duration: float | None = None
    if row["started_at"] is not None and row["ended_at"] is not None:
        duration = (row["ended_at"] - row["started_at"]).total_seconds()
    return str(row["state"]), duration


async def mark_failed(
    conn: asyncpg.Connection,
    *,
    session_id: UUID,
    observed: dict[str, Any] | None,
    mismatch_fields: list[str],
    failure_code: str,
    failure_message: str,
) -> str:
    """Transition any non-terminal state -> ``failed``. Writes a failing validation.

    Mirrors the transition onto ``cameras.status = 'error'`` so the dashboard
    cameras list surfaces the failure.
    """
    row = await conn.fetchrow(
        """
        update stream_sessions
           set state = 'failed',
               ended_at = now(),
               failure_code = $2,
               failure_message = $3
         where id = $1
           and state in ('approved', 'streaming')
         returning state, lim_width, lim_height, lim_fps, lim_codec,
                   lim_bitrate_kbps, lim_latency_mode
        """,
        session_id,
        failure_code,
        failure_message,
    )
    if row is None:
        await _explain_unexpected_state(conn, session_id, expected="approved|streaming")

    await _sync_camera_status(conn, session_id, "error")

    expected = {
        "codec": row["lim_codec"],
        "width": row["lim_width"],
        "height": row["lim_height"],
        "fps": row["lim_fps"],
        "bitrate_kbps": row["lim_bitrate_kbps"],
        "latency_mode": row["lim_latency_mode"],
    }
    await conn.execute(
        """
        insert into stream_validations (session_id, passed, observed, expected, mismatch_fields)
        values ($1, false, $2::jsonb, $3::jsonb, $4)
        """,
        session_id,
        json.dumps(observed) if observed is not None else json.dumps({}),
        json.dumps(expected),
        mismatch_fields,
    )
    return str(row["state"])


async def _sync_camera_status(conn: asyncpg.Connection, session_id: UUID, status: str) -> None:
    """Mirror a session-state transition onto ``cameras.status``.

    The dashboard's cameras list reads ``cameras.status`` directly (not
    ``stream_sessions.state``), so this keeps the list view in lockstep
    with stream lifecycle. Uses a subquery so the camera_id is resolved
    in the same transaction snapshot as the caller's stream_sessions write.
    """
    await conn.execute(
        """
        update cameras
           set status = $2
         where id = (select camera_id from stream_sessions where id = $1)
        """,
        session_id,
        status,
    )


async def _explain_unexpected_state(
    conn: asyncpg.Connection, session_id: UUID, *, expected: str
) -> None:
    """Translate a no-op UPDATE into a precise error.

    The update RETURNING is empty in two cases: the row doesn't exist, or
    it's in the wrong state. Distinguishing them produces a useful response
    code instead of a generic 404.
    """
    existing = await load_session_state(conn, session_id)
    if existing is None:
        raise NotFoundError("Session not found.", code="session_not_found")
    raise ConflictError(
        f"Session is in state '{existing.state}', expected {expected}.",
        code="stream_state_invalid",
        details={"current_state": existing.state, "expected": expected},
    )
