"""Stream session state machine.

States: ``approved`` -> ``streaming`` -> ``ended`` | ``expired`` | ``failed``.
Transitions enforced here; each writes through asyncpg in a single statement so
the DB is always the source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg

from upstream_api.config import Settings
from upstream_api.errors import ConflictError, NotFoundError
from upstream_api.security.session_jwt import (
    SessionLimits,
    SessionTokenClaims,
    mint_session_token,
)
from upstream_api.security.streamid import build_streamid, mint_stream_key
from upstream_api.services.limits import LimitsSnapshot, RequestedStream, check_session_start
from upstream_api.services.usage import record_event


@dataclass(frozen=True, slots=True)
class StreamSessionRow:
    id: UUID
    account_id: UUID
    agent_id: UUID
    camera_id: UUID
    state: str
    srt_url: str
    streamid: str
    expires_at: datetime
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    requested: RequestedStream
    approved: LimitsSnapshot


@dataclass(frozen=True, slots=True)
class StartSessionResult:
    session: StreamSessionRow
    session_token: str
    claims: SessionTokenClaims
    srt_url: str


# SRT latency in milliseconds per mode. Lower = lower glass-to-glass delay,
# higher = better tolerance for jitter / packet loss on weaker links.
_LATENCY_MS_BY_MODE = {
    "low": 80,
    "balanced": 200,
    "stable": 1500,
}


def _build_srt_url(
    streamid: str,
    *,
    ingest_host: str,
    port: int,
    latency_mode: str = "balanced",
) -> str:
    # URL-safe streamid; SRT carries the streamid as a UTF-8 string. Host/port
    # come from `Settings.srt_ingest_host` / `.srt_ingest_port` so dev and prod
    # can point at different streaming servers.
    latency_ms = _LATENCY_MS_BY_MODE.get(latency_mode, _LATENCY_MS_BY_MODE["balanced"])
    return f"srt://{ingest_host}:{port}?streamid={streamid}&latency={latency_ms}"


async def start_session(
    conn: asyncpg.Connection,
    *,
    settings: Settings,
    account_id: UUID,
    camera_id: UUID,
    requested: RequestedStream,
) -> StartSessionResult:
    """Create a new ``approved`` session row and mint its session token.

    Caller must own a transaction (use ``async with conn.transaction(): ...``).
    """
    camera = await conn.fetchrow(
        """
        select id, account_id, agent_id from cameras
         where id = $1 and status <> 'disabled'
        """,
        camera_id,
    )
    if camera is None or camera["account_id"] != account_id:
        raise NotFoundError("Camera not found.", code="camera_not_found")
    agent_id: UUID = camera["agent_id"]

    # Concurrent-session check on the same camera.
    busy = await conn.fetchval(
        """
        select exists(
          select 1 from stream_sessions
           where camera_id = $1 and state in ('approved', 'streaming')
        )
        """,
        camera_id,
    )
    if busy:
        raise ConflictError(
            "Camera already has an active stream session.",
            code="camera_already_streaming",
        )

    approved = await check_session_start(conn, account_id=account_id, requested=requested)

    session_id = uuid4()
    jti = uuid4()
    expires_at = datetime.now(tz=UTC) + timedelta(seconds=settings.session_token_ttl_seconds)

    session_token, claims = mint_session_token(
        secret=settings.session_jwt_secret.get_secret_value(),
        account_id=account_id,
        agent_id=agent_id,
        camera_id=camera_id,
        session_id=session_id,
        limits=SessionLimits(
            width=approved.width,
            height=approved.height,
            fps=approved.fps,
            codec=approved.codec,
            bitrate_kbps=approved.bitrate_kbps,
        ),
        ttl_seconds=settings.session_token_ttl_seconds,
        jti=jti,
    )

    # Short opaque key in the SRT streamid; the signed JWT is still returned
    # to the agent as `session_token` for any non-ingest use, but libsrt's
    # streamid is capped at ~512 bytes so we never put the JWT in the URL.
    streamid = build_streamid(mint_stream_key())
    srt_url = _build_srt_url(
        streamid,
        ingest_host=settings.srt_ingest_host,
        port=settings.srt_ingest_port,
        latency_mode=approved.latency_mode,
    )

    try:
        row = await conn.fetchrow(
            """
            insert into stream_sessions (
              id, account_id, agent_id, camera_id, state,
              req_width, req_height, req_fps, req_codec, req_bitrate_kbps, req_latency_mode,
              lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps, lim_latency_mode,
              streamid, srt_url, session_token_jti, expires_at
            ) values (
              $1, $2, $3, $4, 'approved',
              $5, $6, $7, $8, $9, $10,
              $11, $12, $13, $14, $15, $16,
              $17, $18, $19, $20
            )
            returning id, state, expires_at, created_at, started_at, ended_at
            """,
            session_id,
            account_id,
            agent_id,
            camera_id,
            requested.width,
            requested.height,
            requested.fps,
            requested.codec,
            requested.bitrate_kbps,
            requested.latency_mode,
            approved.width,
            approved.height,
            approved.fps,
            approved.codec,
            approved.bitrate_kbps,
            approved.latency_mode,
            streamid,
            srt_url,
            jti,
            expires_at,
        )
    except asyncpg.UniqueViolationError as exc:
        # Hit by the partial unique index `stream_sessions_one_active_per_camera_idx`
        # when a concurrent /start raced past the pre-check above.
        raise ConflictError(
            "Camera already has an active stream session.",
            code="camera_already_streaming",
        ) from exc
    assert row is not None  # insert ... returning always returns a row

    await record_event(
        conn,
        account_id=account_id,
        event_type="session_started",
        session_id=session_id,
        unit="session",
        metadata={"camera_id": str(camera_id)},
    )

    session = StreamSessionRow(
        id=row["id"],
        account_id=account_id,
        agent_id=agent_id,
        camera_id=camera_id,
        state=row["state"],
        srt_url=srt_url,
        streamid=streamid,
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        requested=requested,
        approved=approved,
    )
    return StartSessionResult(
        session=session,
        session_token=session_token,
        claims=claims,
        srt_url=srt_url,
    )


async def stop_session(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    session_id: UUID,
    reason: str = "agent_requested",
) -> StreamSessionRow:
    """Idempotent stop: ``approved|streaming`` -> ``ended``. No-op on terminal."""
    row = await conn.fetchrow(
        """
        select id, account_id, agent_id, camera_id, state, srt_url, streamid,
               expires_at, created_at, started_at, ended_at,
               req_width, req_height, req_fps, req_codec, req_bitrate_kbps, req_latency_mode,
               lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps, lim_latency_mode
          from stream_sessions
         where id = $1
        """,
        session_id,
    )
    if row is None or row["account_id"] != account_id:
        raise NotFoundError("Session not found.", code="session_not_found")

    if row["state"] in ("approved", "streaming"):
        updated = await conn.fetchrow(
            """
            update stream_sessions
               set state = 'ended',
                   ended_at = now(),
                   end_reason = $2
             where id = $1
             returning state, ended_at
            """,
            session_id,
            reason,
        )
        assert updated is not None
        state = updated["state"]
        ended_at = updated["ended_at"]

        # Mirror to cameras.status so the cameras list view returns to "idle".
        # The streaming-server's runOnNotReady hook only fires after MediaMTX
        # actually had the stream attached, so an agent-driven /stop (or our
        # own stale-session cleanup) bypasses that mirror unless we do it here.
        await conn.execute(
            """
            update cameras
               set status = 'idle'
             where id = (select camera_id from stream_sessions where id = $1)
            """,
            session_id,
        )

        duration_seconds: float | None = None
        if row["started_at"] is not None and ended_at is not None:
            duration_seconds = (ended_at - row["started_at"]).total_seconds()

        await record_event(
            conn,
            account_id=account_id,
            event_type="session_ended",
            session_id=session_id,
            quantity=duration_seconds or 0.0,
            unit="seconds",
            metadata={"reason": reason},
        )
    else:
        state = row["state"]
        ended_at = row["ended_at"]

    return StreamSessionRow(
        id=row["id"],
        account_id=row["account_id"],
        agent_id=row["agent_id"],
        camera_id=row["camera_id"],
        state=state,
        srt_url=row["srt_url"],
        streamid=row["streamid"],
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        ended_at=ended_at,
        requested=RequestedStream(
            width=row["req_width"],
            height=row["req_height"],
            fps=row["req_fps"],
            codec=row["req_codec"],
            bitrate_kbps=row["req_bitrate_kbps"],
            latency_mode=row["req_latency_mode"],
        ),
        approved=LimitsSnapshot(
            width=row["lim_width"],
            height=row["lim_height"],
            fps=row["lim_fps"],
            codec=row["lim_codec"],
            bitrate_kbps=row["lim_bitrate_kbps"],
            latency_mode=row["lim_latency_mode"],
        ),
    )
