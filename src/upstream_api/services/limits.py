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

from upstream_api.errors import ForbiddenError, LimitExceeded, NotFoundError
from upstream_api.services.usage import current_usage_period


@dataclass(frozen=True, slots=True)
class AccountLimits:
    max_cameras: int
    max_concurrent_sessions: int
    max_fps: int
    max_width: int
    max_height: int
    max_bitrate_kbps: int
    allowed_codecs: frozenset[str]
    allowed_stream_types: frozenset[str]
    monthly_minutes_quota: int
    snapshot_storage_mb: int
    # Appliance-class override: per-account session TTL in seconds. None means
    # "use the platform default" (Settings.session_token_ttl_seconds). Large
    # values (e.g. 30 days) turn every session the account starts into an
    # appliance-class session for 24/7 start-once edge clients.
    session_ttl_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class RequestedStream:
    width: int
    height: int
    fps: int
    codec: str
    bitrate_kbps: int
    latency_mode: str = "balanced"


@dataclass(frozen=True, slots=True)
class LimitsSnapshot:
    """What we approved this session for; copied onto the session row."""

    width: int
    height: int
    fps: int
    codec: str
    bitrate_kbps: int
    latency_mode: str = "balanced"


async def check_account_active(conn: asyncpg.Connection, account_id: UUID) -> None:
    """Reject session starts for suspended accounts.

    `accounts.status` is set from the admin panel (PATCH /v1/admin/accounts/
    {id}); without this gate the suspend toggle would be cosmetic. In-flight
    sessions are not killed — suspension stops NEW sessions only.
    """
    status = await conn.fetchval("select status from accounts where id = $1", account_id)
    if status is None:
        raise NotFoundError(
            "Account not found.",
            code="account_not_found",
            details={"account_id": str(account_id)},
        )
    if status != "active":
        raise ForbiddenError(
            "Account is suspended.",
            code="account_suspended",
            details={"account_id": str(account_id), "status": status},
        )


async def load_account_limits(conn: asyncpg.Connection, account_id: UUID) -> AccountLimits:
    row = await conn.fetchrow(
        """
        select max_cameras, max_concurrent_sessions, max_fps, max_width, max_height,
               max_bitrate_kbps, allowed_codecs, allowed_stream_types,
               monthly_minutes_quota, snapshot_storage_mb, session_ttl_seconds
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
        allowed_stream_types=frozenset(row["allowed_stream_types"]),
        monthly_minutes_quota=row["monthly_minutes_quota"],
        snapshot_storage_mb=row["snapshot_storage_mb"],
        session_ttl_seconds=row["session_ttl_seconds"],
    )


def check_stream_type_allowed(limits: AccountLimits, source_type: str | None) -> None:
    """Reject cameras whose source type isn't in the account's allow-list.

    ``source_type is None`` is default-allowed: cameras registered by
    pre-smart-flow agents never declared a type.
    """
    if source_type is None:
        return
    if source_type.lower() not in {t.lower() for t in limits.allowed_stream_types}:
        raise LimitExceeded(
            f"Stream type '{source_type}' is not allowed for this account.",
            code="limit_exceeded.stream_type_not_allowed",
            details={
                "requested": source_type,
                "allowed": sorted(limits.allowed_stream_types),
            },
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
    camera_source_type: str | None = None,
) -> LimitsSnapshot:
    """Validate a stream-session start request and return the approved snapshot.

    Raises :class:`LimitExceeded` on any failure with a stable error code.
    """
    await check_account_active(conn, account_id)
    limits = await load_account_limits(conn, account_id)

    check_camera_defaults(limits, requested)
    check_stream_type_allowed(limits, camera_source_type)
    await check_monthly_minutes_quota(conn, account_id=account_id, limits=limits)

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
        latency_mode=requested.latency_mode,
    )


async def effective_session_ttl_seconds(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    default_ttl_seconds: int,
) -> int:
    """Resolve the session TTL for one account.

    Appliance-class accounts carry a ``session_ttl_seconds`` override on
    ``account_limits``; everyone else gets the platform default. A missing
    limits row falls back to the default too — ``load_account_limits`` has
    already 404'd earlier in the session-start path if the row is required.
    """
    override = await conn.fetchval(
        "select session_ttl_seconds from account_limits where account_id = $1",
        account_id,
    )
    return int(override) if override is not None else default_ttl_seconds


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


async def check_monthly_minutes_quota(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    limits: AccountLimits,
) -> None:
    """Reject session start once COMPLETED stream minutes hit the monthly quota.

    Module 7 M1 — the quota existed since day one but was never enforced.
    Minutes come from usage_events ``session_ended`` only (the same source
    the usage summary and agent bootstrap report), seconds // 60. Two
    documented softnesses, both acceptable for a coarse cost-control cap:

    - Currently-active sessions contribute nothing until they end, so an
      account can start one final session near the cap and overshoot by
      that session's length.
    - A quota of 0 means no streaming minutes are allowed at all — any
      session start is rejected — consistent with how max_cameras=0 would
      behave. There is no 0-means-unlimited convention in this codebase.
    """
    period_start, period_end = current_usage_period()
    used_seconds = await conn.fetchval(
        """
        select coalesce(sum(quantity), 0)::bigint
          from usage_events
         where account_id = $1
           and event_type = 'session_ended'
           and occurred_at >= $2
           and occurred_at < $3
        """,
        account_id,
        period_start,
        period_end,
    )
    used_minutes = int(used_seconds or 0) // 60
    if used_minutes >= limits.monthly_minutes_quota:
        raise LimitExceeded(
            f"Monthly streaming minutes quota reached: "
            f"{used_minutes} of {limits.monthly_minutes_quota} minutes used.",
            code="limit_exceeded.monthly_minutes_quota",
            details={
                "limit": limits.monthly_minutes_quota,
                "used": used_minutes,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
            },
        )
