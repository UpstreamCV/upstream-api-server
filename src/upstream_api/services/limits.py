"""Account-limit enforcement.

These functions are pure-ish: they take a connection and an account_id and
return either a populated :class:`LimitsSnapshot` or raise :class:`LimitExceeded`.

Limits are snapshotted onto the session row at approval time so a mid-stream
limit change doesn't break the contract of an in-flight session.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from upstream_api.errors import LimitExceeded, NotFoundError


@dataclass(frozen=True, slots=True)
class AccountLimits:
    max_cameras: int
    max_concurrent_sessions: int
    max_fps: int
    max_width: int
    max_height: int
    max_bitrate_kbps: int
    allowed_codecs: frozenset[str]
    monthly_minutes_quota: int
    snapshot_storage_mb: int


@dataclass(frozen=True, slots=True)
class RequestedStream:
    width: int
    height: int
    fps: int
    codec: str
    bitrate_kbps: int


@dataclass(frozen=True, slots=True)
class LimitsSnapshot:
    """What we approved this session for; copied onto the session row."""

    width: int
    height: int
    fps: int
    codec: str
    bitrate_kbps: int


async def load_account_limits(conn: asyncpg.Connection, account_id: UUID) -> AccountLimits:
    row = await conn.fetchrow(
        """
        select max_cameras, max_concurrent_sessions, max_fps, max_width, max_height,
               max_bitrate_kbps, allowed_codecs, monthly_minutes_quota, snapshot_storage_mb
          from account_limits
         where account_id = $1
        """,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            "Account limits not configured.",
            code="account_limits_missing",
            details={"account_id": str(account_id)},
        )
    return AccountLimits(
        max_cameras=row["max_cameras"],
        max_concurrent_sessions=row["max_concurrent_sessions"],
        max_fps=row["max_fps"],
        max_width=row["max_width"],
        max_height=row["max_height"],
        max_bitrate_kbps=row["max_bitrate_kbps"],
        allowed_codecs=frozenset(row["allowed_codecs"]),
        monthly_minutes_quota=row["monthly_minutes_quota"],
        snapshot_storage_mb=row["snapshot_storage_mb"],
    )


def check_camera_defaults(limits: AccountLimits, defaults: RequestedStream) -> None:
    """Reject obviously-out-of-bounds camera defaults at registration time."""
    if defaults.codec.lower() not in {c.lower() for c in limits.allowed_codecs}:
        raise LimitExceeded(
            f"Codec '{defaults.codec}' is not allowed for this account.",
            code="limit_exceeded.codec_not_allowed",
            details={"allowed": sorted(limits.allowed_codecs)},
        )
    if defaults.fps > limits.max_fps:
        raise LimitExceeded(
            f"Requested fps {defaults.fps} exceeds limit {limits.max_fps}.",
            code="limit_exceeded.fps_too_high",
            details={"requested": defaults.fps, "max": limits.max_fps},
        )
    if defaults.width > limits.max_width or defaults.height > limits.max_height:
        raise LimitExceeded(
            f"Requested resolution {defaults.width}x{defaults.height} exceeds "
            f"limit {limits.max_width}x{limits.max_height}.",
            code="limit_exceeded.resolution_too_high",
            details={
                "requested": f"{defaults.width}x{defaults.height}",
                "max": f"{limits.max_width}x{limits.max_height}",
            },
        )
    if defaults.bitrate_kbps > limits.max_bitrate_kbps:
        raise LimitExceeded(
            f"Requested bitrate {defaults.bitrate_kbps}kbps exceeds limit "
            f"{limits.max_bitrate_kbps}kbps.",
            code="limit_exceeded.bitrate_too_high",
            details={"requested": defaults.bitrate_kbps, "max": limits.max_bitrate_kbps},
        )


async def check_session_start(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    requested: RequestedStream,
) -> LimitsSnapshot:
    """Validate a stream-session start request and return the approved snapshot.

    Raises :class:`LimitExceeded` on any failure with a stable error code.
    """
    limits = await load_account_limits(conn, account_id)

    check_camera_defaults(limits, requested)

    # Serialize concurrent start_session calls for the same account so the
    # count(*) below and the eventual insert observe a consistent view of
    # active sessions. Released automatically at transaction commit/rollback;
    # caller must hold a transaction (start_session does).
    await conn.execute(
        "select pg_advisory_xact_lock(hashtextextended($1::text, 0))",
        str(account_id),
    )

    active_count = await conn.fetchval(
        """
        select count(*)::int
          from stream_sessions
         where account_id = $1 and state in ('approved', 'streaming')
        """,
        account_id,
    )
    if active_count >= limits.max_concurrent_sessions:
        raise LimitExceeded(
            f"Concurrent session limit reached: "
            f"{active_count} of {limits.max_concurrent_sessions} active.",
            code="limit_exceeded.max_concurrent_sessions",
            details={"active": active_count, "max": limits.max_concurrent_sessions},
        )

    return LimitsSnapshot(
        width=requested.width,
        height=requested.height,
        fps=requested.fps,
        codec=requested.codec,
        bitrate_kbps=requested.bitrate_kbps,
    )


async def check_camera_slot_available(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    is_new_camera: bool,
) -> None:
    """When registering a NEW camera (vs upserting an existing one), enforce max_cameras."""
    if not is_new_camera:
        return
    limits = await load_account_limits(conn, account_id)
    count = await conn.fetchval(
        """
        select count(*)::int from cameras
         where account_id = $1 and status <> 'disabled'
        """,
        account_id,
    )
    if count >= limits.max_cameras:
        raise LimitExceeded(
            f"Camera slot limit reached: {count} of {limits.max_cameras}.",
            code="limit_exceeded.max_cameras",
            details={"in_use": count, "max": limits.max_cameras},
        )
