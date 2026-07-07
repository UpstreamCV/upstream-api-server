"""Session maintenance: expiry sweeper (FU-5) + MediaMTX restart reconciler (FU-7).

One periodic tick, three phases, per design doc 04-restart-and-recovery
(upstream-streaming-server/docs/design/04-restart-and-recovery.md §2-§3):

1. **Expiry sweep** — ``approved``/``streaming`` sessions past ``expires_at``
   transition to ``expired``. Every read path (token auth, processor
   eligibility, validate-stream, mediamtx-auth) already treats
   ``expires_at < now()`` as dead, so this changes no admission decision —
   it turns today's silent eligibility drop into a monitored state change,
   mirrors ``cameras.status`` back to idle, records usage, and deprovisions
   the MediaMTX path (which per LG-5 evicts any zombie publisher so the
   agent reconnects into a fresh session).
2. **Staleness sweep** — ``streaming`` sessions with no session-scoped
   heartbeat for ``sweeper_stale_minutes`` are flagged (report mode, the
   default) or swept to ``ended``/``swept_stale`` with
   ``ended_at = last_heartbeat`` — never bill dead air.
3. **Path reconcile** — MediaMTX keeps API-provisioned paths in memory only;
   a restart (or a ``mediamtx.yml`` file deploy, per LG-6) drops them all.
   Diff ``/v3/config/paths/list`` against live sessions and re-provision
   what's missing with the stored per-session passphrase. Extra ``sess_*``
   paths (path exists, no live session) are logged + gauged, never deleted
   here — deletion stays under the report-only → enforce gate.

Sweep writes run in ONE transaction guarded by ``pg_try_advisory_xact_lock``
so the tick is single-flight across api-server replicas; a replica that
loses the lock skips the whole tick (the reconcile phase is idempotent but
skipping it too avoids duplicate MediaMTX traffic). Metrics and MediaMTX
calls happen strictly AFTER commit — same posture as the ingest webhooks
(a rollback must not count a transition or evict a publisher).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import UUID

import asyncpg
import httpx

from upstream_api.config import Settings
from upstream_api.observability.logging import get_logger
from upstream_api.observability.metrics import (
    record_path_drift,
    record_sweeper_action,
    record_transition,
)
from upstream_api.security.streamid import extract_stream_key
from upstream_api.services.account_audit import try_record_account_audit_log
from upstream_api.services.mediamtx_provisioning import (
    MediaMTXProvisioningFailure,
    deprovision_path,
    provision_path,
)
from upstream_api.services.usage import record_event

_log = get_logger("session_maintenance")

# Constant advisory-lock label for the single-flight tick guard. Same
# hashtextextended idiom as the per-account lock in services/limits.py, but
# with a fixed label so all replicas contend on one lock.
_LOCK_LABEL = "upstream:session_maintenance"

# MediaMTX paths-list page size. 500 comfortably covers the validated
# capacity ceiling (35 streams) and the 100-counter fleet target in one
# request while staying honest about pagination for anything bigger.
_PATHS_PAGE_SIZE = 500

# How many ids/paths to include in a single log line before truncating —
# a fleet-wide event (mass expiry, MediaMTX restart) shouldn't emit a
# megabyte log record.
_LOG_SAMPLE = 20

# Ceiling on MediaMTX mutations (adds + deletes) per tick. A synchronized
# fleet-wide event against a sick MediaMTX (10s read timeout per call)
# must not stall the loop for tens of minutes; whatever is deferred is
# logged and picked up on later ticks (terminal-path cleanup makes missed
# deletes self-healing, so deferral is safe, not silent loss).
_MAX_MUTATIONS_PER_TICK = 100


@dataclass
class MaintenanceStats:
    """One tick's outcomes; the internal run endpoint returns these."""

    skipped: bool = False  # another replica held the advisory lock
    expired_approved: int = 0
    expired_streaming: int = 0
    would_expire: int = 0  # expiry clause, report mode
    stale_reported: int = 0  # staleness clause, report mode
    stale_swept: int = 0  # staleness clause, enforce mode
    paths_reprovisioned: int = 0
    paths_extra: int = 0
    paths_cleaned: int = 0  # extra sess_* paths of TERMINAL sessions, deleted
    provision_errors: int = 0
    deprovision_errors: int = 0
    deferred_mutations: int = 0  # per-tick MediaMTX mutation cap hit
    reconcile_skipped: bool = False  # MediaMTX paths list unreachable

    @property
    def any_action(self) -> bool:
        return bool(
            self.expired_approved
            or self.expired_streaming
            or self.would_expire
            or self.stale_reported
            or self.stale_swept
            or self.paths_reprovisioned
            or self.paths_extra
            or self.paths_cleaned
            or self.provision_errors
            or self.deprovision_errors
            or self.deferred_mutations
        )


@dataclass(frozen=True, slots=True)
class _SweptSession:
    """What the post-commit phase needs to know about one swept session."""

    id: UUID
    stream_key: str | None  # None when the streamid is malformed
    from_state: str
    to_state: str


def _stream_key_or_none(streamid: str) -> str | None:
    """The session row's streamid format is owned by this server; tolerate a
    malformed one (skip MediaMTX teardown) rather than aborting the sweep."""
    try:
        return extract_stream_key(streamid)
    except ValueError:
        return None


async def _sweep_expired(
    conn: asyncpg.Connection,
    *,
    mode: str,
    stats: MaintenanceStats,
) -> list[_SweptSession]:
    """Expiry clause: overdue ``approved``/``streaming`` -> ``expired``."""
    if mode == "report":
        rows = await conn.fetch(
            """
            select id, state from stream_sessions
             where state in ('approved', 'streaming')
               and expires_at < now()
            """
        )
        stats.would_expire = len(rows)
        if rows:
            _log.info(
                "sweeper_would_expire",
                count=len(rows),
                session_ids=[str(r["id"]) for r in rows[:_LOG_SAMPLE]],
            )
        return []

    # ended_at = expires_at, NOT now(): the session's contract ended at its
    # TTL. If the sweeper was down for hours, billing started_at→now() would
    # record the whole dead gap as streamed minutes — enough to exhaust a
    # monthly quota and block the account's next /start.
    rows = await conn.fetch(
        """
        update stream_sessions
           set state = 'expired',
               ended_at = expires_at,
               end_reason = 'ttl_expired'
         where state in ('approved', 'streaming')
           and expires_at < now()
        returning id, account_id, camera_id, streamid, started_at, ended_at,
                  (select max(h.reported_at) from heartbeats h
                    where h.session_id = stream_sessions.id) as last_beat
        """
    )
    swept: list[_SweptSession] = []
    for row in rows:
        # `started_at` is written exactly once, on the approved→streaming
        # edge, so its presence is the authoritative "was streaming" marker
        # (same rule as stream_validation.mark_ended).
        was_streaming = row["started_at"] is not None
        if was_streaming:
            stats.expired_streaming += 1
        else:
            stats.expired_approved += 1

        # Mirror to cameras.status so the dashboard returns to "Idle". Safe
        # unconditionally: the one-active-session-per-camera unique index
        # guarantees no other live session holds this camera.
        await conn.execute(
            "update cameras set status = 'idle' where id = $1",
            row["camera_id"],
        )

        # Bill actual streamed time, not wall-clock session lifetime. A stream
        # that died mid-TTL but was never swept (staleness runs in report mode
        # by default, so nothing ends it early) would otherwise be billed all
        # the way to expires_at — days of dead air on a multi-day appliance TTL,
        # enough to exhaust a quota on its own. Clamp the billed end to the last
        # session-scoped heartbeat, mirroring _sweep_stale's "never bill dead
        # air" rule. No heartbeat at all -> fall back to ended_at (unchanged
        # behavior; this is the never-streamed edge, since appliances heartbeat
        # every ~15s). ended_at itself stays expires_at: the session's
        # administrative end is its TTL; only the billed quantity is clamped.
        duration = 0.0
        if was_streaming and row["ended_at"] is not None:
            billed_end = row["ended_at"]
            if row["last_beat"] is not None and row["last_beat"] < billed_end:
                billed_end = row["last_beat"]
            duration = max(0.0, (billed_end - row["started_at"]).total_seconds())
        await record_event(
            conn,
            account_id=row["account_id"],
            event_type="session_ended",
            session_id=row["id"],
            quantity=duration,
            unit="seconds",
            metadata={"reason": "ttl_expired"},
        )
        await try_record_account_audit_log(
            conn,
            account_id=row["account_id"],
            actor_type="system",
            event_type="stream_session.expired",
            resource_type="stream_session",
            resource_id=str(row["id"]),
            summary="Stream session expired at TTL and was swept.",
            metadata={"camera_id": str(row["camera_id"])},
        )
        swept.append(
            _SweptSession(
                id=row["id"],
                stream_key=_stream_key_or_none(row["streamid"]),
                from_state="streaming" if was_streaming else "approved",
                to_state="expired",
            )
        )
    return swept


async def _sweep_stale(
    conn: asyncpg.Connection,
    *,
    mode: str,
    stale_minutes: int,
    stats: MaintenanceStats,
) -> list[_SweptSession]:
    """Staleness clause: ``streaming`` with no session heartbeat for N min.

    ``last_beat`` falls back to ``started_at`` for sessions that never got a
    session-scoped heartbeat — an agent fleet that doesn't tag heartbeats
    with session_id would otherwise be swept the moment enforcement turns
    on. Report-only is the default posture (config.sweeper_stale_mode).
    """
    rows = await conn.fetch(
        """
        select s.id, s.account_id, s.camera_id, s.streamid, s.started_at,
               coalesce(hb.last_beat, s.started_at, s.created_at) as last_beat
          from stream_sessions s
          left join lateral (
            select max(reported_at) as last_beat
              from heartbeats h
             where h.session_id = s.id
          ) hb on true
         where s.state = 'streaming'
           and s.expires_at > now()  -- overdue ones belong to the expiry clause
           and coalesce(hb.last_beat, s.started_at, s.created_at)
               < now() - make_interval(mins => $1)
        """,
        stale_minutes,
    )
    if not rows:
        return []

    if mode == "report":
        stats.stale_reported = len(rows)
        _log.info(
            "sweeper_would_sweep_stale",
            count=len(rows),
            stale_minutes=stale_minutes,
            session_ids=[str(r["id"]) for r in rows[:_LOG_SAMPLE]],
        )
        return []

    swept: list[_SweptSession] = []
    for row in rows:
        # ended_at = last heartbeat, NOT now(): never bill the dead air
        # between the real death and the sweep (design doc 04 §2).
        #
        # The guarded UPDATE re-checks BOTH the state (raced webhook
        # transition) and the staleness predicate itself: under READ
        # COMMITTED a heartbeat committed between the candidate SELECT and
        # this statement is visible here, so a session that recovered
        # mid-tick (partition heals, heartbeat flood lands) is NOT swept —
        # without the re-check we'd end a live stream and then evict its
        # publisher via the post-commit path delete.
        updated = await conn.fetchrow(
            """
            update stream_sessions
               set state = 'ended',
                   ended_at = $2,
                   end_reason = 'swept_stale'
             where id = $1 and state = 'streaming'
               and not exists (
                 select 1 from heartbeats h
                  where h.session_id = $1
                    and h.reported_at >= now() - make_interval(mins => $3)
               )
            returning id, started_at, ended_at
            """,
            row["id"],
            row["last_beat"],
            stale_minutes,
        )
        if updated is None:  # raced a webhook transition or a recovery beat
            continue
        stats.stale_swept += 1
        await conn.execute(
            "update cameras set status = 'idle' where id = $1",
            row["camera_id"],
        )
        duration = 0.0
        if updated["started_at"] is not None and updated["ended_at"] is not None:
            duration = max(0.0, (updated["ended_at"] - updated["started_at"]).total_seconds())
        await record_event(
            conn,
            account_id=row["account_id"],
            event_type="session_ended",
            session_id=row["id"],
            quantity=duration,
            unit="seconds",
            metadata={"reason": "swept_stale"},
        )
        await try_record_account_audit_log(
            conn,
            account_id=row["account_id"],
            actor_type="system",
            event_type="stream_session.swept_stale",
            resource_type="stream_session",
            resource_id=str(row["id"]),
            summary="Stream session swept: no heartbeat within the staleness window.",
            metadata={
                "camera_id": str(row["camera_id"]),
                "stale_minutes": stale_minutes,
            },
        )
        swept.append(
            _SweptSession(
                id=row["id"],
                stream_key=_stream_key_or_none(row["streamid"]),
                from_state="streaming",
                to_state="ended",
            )
        )
    return swept


async def _list_mediamtx_paths(
    http: httpx.AsyncClient,
    *,
    base_url: str,
) -> set[str] | None:
    """All configured MediaMTX path names, or None if the API is unreachable.

    Paginates ``/v3/config/paths/list`` (MediaMTX caps a page at its own
    maximum regardless of ``itemsPerPage``, so trust ``pageCount``).
    """
    base = base_url.rstrip("/")
    names: set[str] = set()
    page = 0
    while True:
        try:
            response = await http.get(
                f"{base}/v3/config/paths/list",
                params={"itemsPerPage": _PATHS_PAGE_SIZE, "page": page},
            )
        except httpx.HTTPError as exc:
            _log.warning("reconciler_paths_list_failed", error=type(exc).__name__)
            return None
        if not 200 <= response.status_code < 300:
            _log.warning("reconciler_paths_list_failed", status=response.status_code)
            return None
        try:
            data = response.json()
        except ValueError:
            _log.warning("reconciler_paths_list_failed", error="invalid_json")
            return None
        for item in data.get("items") or []:
            name = item.get("name")
            if name:
                names.add(name)
        page_count = int(data.get("pageCount") or 1)
        page += 1
        if page >= page_count:
            return names


async def _reconcile_paths(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
    stats: MaintenanceStats,
    *,
    mutations_used: int = 0,
) -> None:
    """FU-7: re-provision MediaMTX paths for live sessions after a restart.

    ``mutations_used`` is how much of the per-tick MediaMTX mutation budget
    the sweeper's teardown phase already spent.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select streamid, srt_publish_passphrase
              from stream_sessions
             where state in ('approved', 'streaming')
               and expires_at > now()
            """
        )
    active: dict[str, str] = {}
    for row in rows:
        key = _stream_key_or_none(row["streamid"])
        if key is None:
            continue
        # Pre-passphrase-migration rows can carry NULL; nothing to restore.
        if row["srt_publish_passphrase"] is None:
            continue
        active[key] = row["srt_publish_passphrase"]

    present = await _list_mediamtx_paths(http, base_url=settings.mediamtx_api_base_url)
    if present is None:
        stats.reconcile_skipped = True
        return

    missing = sorted(k for k in active if k not in present)
    budget = max(0, _MAX_MUTATIONS_PER_TICK - mutations_used)
    if len(missing) > budget:
        stats.deferred_mutations += len(missing) - budget
    for key in missing[:budget]:
        try:
            await provision_path(
                http,
                base_url=settings.mediamtx_api_base_url,
                path=key,
                passphrase=active[key],
            )
        except MediaMTXProvisioningFailure as exc:
            stats.provision_errors += 1
            _log.warning(
                "reconciler_provision_failed",
                path=key,
                reason=exc.reason,
                status=exc.status,
            )
            continue
        # TOCTOU guard: the session may have ended (webhook committed +
        # deprovisioned its path) between our DB snapshot and the provision
        # call above — in which case we just resurrected a path nothing
        # will ever clean up. Re-check liveness and compensate. This is
        # NOT the gated extra-path deletion: we are deleting a path we
        # ourselves created moments ago for a session now known terminal.
        async with pool.acquire() as conn:
            still_live = await conn.fetchval(
                """
                select exists(
                  select 1 from stream_sessions
                   where streamid = $1
                     and state in ('approved', 'streaming')
                     and expires_at > now()
                )
                """,
                f"publish:{key}",
            )
        if still_live:
            stats.paths_reprovisioned += 1
        else:
            try:
                await deprovision_path(http, base_url=settings.mediamtx_api_base_url, path=key)
                _log.info("reconciler_compensated_dead_session", path=key)
            except MediaMTXProvisioningFailure:
                # Terminal-path cleanup below (next ticks) self-heals this.
                stats.deprovision_errors += 1
    if missing:
        _log.info(
            "reconciler_reprovisioned",
            count=stats.paths_reprovisioned,
            paths=missing[:_LOG_SAMPLE],
        )

    # Extra sess_* paths: configured in MediaMTX, no live session behind
    # them. Only our own namespace — the shared box also hosts non-session
    # paths that are none of our business. Two sub-cases:
    #   - the path's streamid matches a TERMINAL session row → provably
    #     garbage (terminal states are final; new sessions mint fresh
    #     keys). Delete it — this is also the retry path for a sweeper
    #     deprovision that failed (LG-5 eviction) and for any resurrection
    #     the compensating delete above missed.
    #   - no matching session row at all → log + gauge only; blind
    #     deletion stays behind the report-only → enforce gate
    #     (design doc 04 §3 step 4).
    extra = sorted(p for p in present if p.startswith("sess_") and p not in active)
    stats.paths_extra = len(extra)
    orphans: list[str] = []
    if extra:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select streamid from stream_sessions
                 where streamid = any($1::text[])
                   and state in ('ended', 'expired', 'failed')
                """,
                [f"publish:{p}" for p in extra],
            )
        terminal = {extract_stream_key(r["streamid"]) for r in rows}
        cleanup_budget = max(0, _MAX_MUTATIONS_PER_TICK - mutations_used - len(missing[:budget]))
        to_clean = sorted(terminal)
        if len(to_clean) > cleanup_budget:
            stats.deferred_mutations += len(to_clean) - cleanup_budget
        for key in to_clean[:cleanup_budget]:
            try:
                await deprovision_path(http, base_url=settings.mediamtx_api_base_url, path=key)
                stats.paths_cleaned += 1
            except MediaMTXProvisioningFailure as exc:
                stats.deprovision_errors += 1
                _log.warning(
                    "reconciler_cleanup_failed",
                    path=key,
                    reason=exc.reason,
                    status=exc.status,
                )
        orphans = [p for p in extra if p not in terminal]
        if to_clean:
            _log.info(
                "reconciler_cleaned_terminal_paths",
                count=stats.paths_cleaned,
                paths=to_clean[:_LOG_SAMPLE],
            )
        if orphans:
            _log.info("reconciler_extra_paths", count=len(orphans), paths=orphans[:_LOG_SAMPLE])

    record_path_drift(missing=len(missing), extra=len(orphans))


async def run_session_maintenance(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
) -> MaintenanceStats:
    """One maintenance tick: sweep (single transaction) then reconcile.

    Callable from the in-process loop, the internal run endpoint, or
    external cron — overlapping invocations are safe (the advisory lock
    makes the sweep single-flight; the reconcile phase is idempotent).
    """
    stats = MaintenanceStats()
    swept: list[_SweptSession] = []

    async with pool.acquire() as conn, conn.transaction():
        locked = await conn.fetchval(
            "select pg_try_advisory_xact_lock(hashtextextended($1::text, 0))",
            _LOCK_LABEL,
        )
        if not locked:
            stats.skipped = True
            return stats
        swept += await _sweep_expired(conn, mode=settings.sweeper_expiry_mode, stats=stats)
        if settings.sweeper_stale_mode != "off":
            swept += await _sweep_stale(
                conn,
                mode=settings.sweeper_stale_mode,
                stale_minutes=settings.sweeper_stale_minutes,
                stats=stats,
            )

    # After COMMIT — a rollback must not count transitions or touch MediaMTX.
    for session in swept:
        record_transition(session.from_state, session.to_state)
    record_sweeper_action("expired_approved", stats.expired_approved)
    record_sweeper_action("expired_streaming", stats.expired_streaming)
    record_sweeper_action("would_expire", stats.would_expire)
    record_sweeper_action("would_sweep_stale", stats.stale_reported)
    record_sweeper_action("swept_stale", stats.stale_swept)

    # Deprovision swept sessions' paths. Best-effort, and per LG-5 the path
    # delete IS the publisher eviction — a zombie publisher on an expired
    # session gets cut here and reconnects into a fresh /start. Capped per
    # tick; anything deferred is already terminal in the DB, so the
    # reconciler's terminal-path cleanup picks it up on later ticks.
    mutations_used = 0
    teardowns = [s for s in swept if s.stream_key is not None]
    if len(teardowns) > _MAX_MUTATIONS_PER_TICK:
        stats.deferred_mutations += len(teardowns) - _MAX_MUTATIONS_PER_TICK
        _log.warning(
            "sweeper_teardowns_deferred",
            deferred=len(teardowns) - _MAX_MUTATIONS_PER_TICK,
        )
    for session in teardowns[:_MAX_MUTATIONS_PER_TICK]:
        mutations_used += 1  # a failed call still spends wall-clock budget
        try:
            await deprovision_path(
                http,
                base_url=settings.mediamtx_api_base_url,
                path=session.stream_key,  # type: ignore[arg-type]
            )
        except MediaMTXProvisioningFailure as exc:
            stats.deprovision_errors += 1
            _log.warning(
                "sweeper_deprovision_failed",
                session_id=str(session.id),
                reason=exc.reason,
                status=exc.status,
            )

    await _reconcile_paths(pool, http, settings, stats, mutations_used=mutations_used)
    return stats


async def session_maintenance_loop(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
    interval_seconds: int,
) -> None:
    """The in-process loop the app lifespan spawns when
    ``session_maintenance_interval_seconds > 0``. Sleeps FIRST so a
    crash-restart cycle can't hot-loop the sweep, then ticks forever; a
    failed tick is logged and the loop keeps going."""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            stats = await run_session_maintenance(pool, http, settings)
            if stats.any_action:
                _log.info(
                    "session_maintenance_tick",
                    expired_approved=stats.expired_approved,
                    expired_streaming=stats.expired_streaming,
                    would_expire=stats.would_expire,
                    stale_reported=stats.stale_reported,
                    stale_swept=stats.stale_swept,
                    paths_reprovisioned=stats.paths_reprovisioned,
                    paths_extra=stats.paths_extra,
                    paths_cleaned=stats.paths_cleaned,
                    provision_errors=stats.provision_errors,
                    deprovision_errors=stats.deprovision_errors,
                    deferred_mutations=stats.deferred_mutations,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log.warning("session_maintenance_tick_failed", error=type(exc).__name__)
