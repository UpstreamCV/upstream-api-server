"""Upstream Reliability M2b — source health + liveness monitoring.

Catches the failure M1/M2a couldn't: *data stopped flowing*. Two checks, both
over the M2a ``cv_sources`` registry:

- **external source silent** — an ``external_camera`` / ``observation_source`` row
  whose ``last_seen_at`` is older than ``expected + grace``.
- **collector offline** — a ``collector`` row whose ``last_collector_heartbeat_at``
  is older than ``expected + grace``.

``evaluate_source_health`` is pure (easy to unit test). ``run_source_liveness_sweep``
is the scheduled worker: it claims the least-recently-checked enabled rows
(claim-then-process, the ``event_delivery_attempts`` drain shape — short claim
txn that stamps ``last_liveness_checked_at`` so a poisoned source can't starve
the batch, then each source processed in its own txn), classifies health, and
opens/refreshes/auto-resolves ``cv_issues`` (category ``liveness``) through the
existing M1a issue lifecycle (which emits ``reliability.issue_opened`` /
``reliability.issue_resolved``). Idempotent + safe to run repeatedly.

Intervals are stored NULL by default and resolved here via ``default_intervals``,
so a default change applies everywhere and a PATCH "reset" is just writing NULL.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.logging import get_logger
from upstream_api.services.reliability_issues import (
    resolve_open_issues_by_dedupe,
    upsert_open_issue,
)

_logger = get_logger("reliability_source_liveness")

LIVENESS_DETECTOR_VERSION = "reliability.source_liveness.v0.1"

# Health states (must match the cv_sources.health_status CHECK).
HEALTHY = "healthy"
DELAYED = "delayed"
SILENT = "silent"
COLLECTOR_OFFLINE = "collector_offline"
UNKNOWN = "unknown"
DISABLED = "disabled"

_RESOLVE_REASON = "source_recovered"

# Central per-kind liveness defaults. Stored intervals are NULL by default and
# resolved against these, so changing a default applies everywhere immediately.
# Collector heartbeats default to ~60s (M1c), so 180+60 ≈ offline after ~4 min
# (tolerates a couple of missed beats). External sources: silent after ~15 min.
_COLLECTOR_DEFAULT = (180, 60)
_EXTERNAL_DEFAULT = (600, 300)


def default_intervals(source_kind: str | None) -> tuple[int, int]:
    """(expected_interval_seconds, liveness_grace_seconds) for a source kind."""
    if source_kind == "collector":
        return _COLLECTOR_DEFAULT
    return _EXTERNAL_DEFAULT


def effective_intervals(source: dict[str, Any]) -> tuple[int, int]:
    """Stored intervals, falling back to the per-kind default when NULL."""
    expected_default, grace_default = default_intervals(source.get("source_kind"))
    expected = source.get("expected_interval_seconds")
    grace = source.get("liveness_grace_seconds")
    return (
        expected if expected is not None else expected_default,
        grace if grace is not None else grace_default,
    )


@dataclass(frozen=True)
class HealthEvaluation:
    status: str
    reason: str | None
    message: str | None
    last_signal_at: datetime | None
    age_seconds: float | None
    expected_interval_seconds: int
    liveness_grace_seconds: int
    # Issue intent — only set for silent / collector_offline.
    issue_type: str | None = None
    severity: str | None = None
    title: str | None = None
    likely_cause: str | None = None
    recommended_action: str | None = None

    @property
    def is_issue(self) -> bool:
        return self.status in (SILENT, COLLECTOR_OFFLINE)


def _age_label(age_seconds: float) -> str:
    minutes = int(age_seconds // 60)
    if minutes < 1:
        return "less than a minute"
    if minutes == 1:
        return "1 minute"
    return f"{minutes} minutes"


def evaluate_source_health(
    source: dict[str, Any],
    now: datetime | None = None,
) -> HealthEvaluation:
    """Pure per-source health classification.

    ``now`` MUST be timezone-aware; it defaults to ``datetime.now(UTC)`` so a
    naive value is never silently subtracted. Bounds are inclusive: ``age ≤
    expected`` is healthy, ``age ≤ expected+grace`` is delayed, beyond that is
    silent (external) / collector_offline (collector). A never-seen source is
    ``unknown`` (we don't alarm on something we've never heard from)."""
    if now is None:
        now = datetime.now(UTC)
    expected, grace = effective_intervals(source)
    is_collector = source.get("source_kind") == "collector"
    last_signal = (
        source.get("last_collector_heartbeat_at") if is_collector else source.get("last_seen_at")
    )

    if not source.get("liveness_enabled", True):
        return HealthEvaluation(DISABLED, None, None, last_signal, None, expected, grace)
    if last_signal is None:
        return HealthEvaluation(UNKNOWN, None, None, None, None, expected, grace)

    age = (now - last_signal).total_seconds()
    if age <= expected:
        return HealthEvaluation(HEALTHY, None, None, last_signal, age, expected, grace)
    if age <= expected + grace:
        return HealthEvaluation(
            DELAYED,
            None,
            f"Last signal {_age_label(age)} ago (expected every {expected // 60} min).",
            last_signal,
            age,
            expected,
            grace,
        )

    src_name = source.get("source") or "source"
    if is_collector:
        return HealthEvaluation(
            COLLECTOR_OFFLINE,
            "collector_offline",
            f"Collector {src_name} has not reported a heartbeat for {_age_label(age)}.",
            last_signal,
            age,
            expected,
            grace,
            issue_type="collector_offline",
            severity="critical",
            title=f"Collector heartbeat missing from {src_name}",
            likely_cause=(
                "The Upstream Collector may be stopped, offline, or unable to "
                "upload to Upstream Cloud."
            ),
            recommended_action=(
                "Check the local agent shell, collector status, network "
                "connectivity, and API token."
            ),
        )
    key_label = source.get("external_camera_key") or src_name
    return HealthEvaluation(
        SILENT,
        "source_silent",
        f"Source {src_name} / {key_label} has not sent observations for {_age_label(age)}.",
        last_signal,
        age,
        expected,
        grace,
        issue_type="source_silent",
        severity="warning",
        title=f"No observations received from {key_label}",
        likely_cause=(
            "The external CV process, collector integration, or local source may "
            "have stopped sending observations."
        ),
        recommended_action=(
            "Check that the customer CV process is running and still posting to "
            "the local Upstream Collector."
        ),
    )


def _dedupe_key(source_id: UUID, issue_type: str) -> str:
    return f"source_liveness:{source_id}:{issue_type}"


def _evidence(source: dict[str, Any], ev: HealthEvaluation) -> dict[str, Any]:
    return {
        "source_id": str(source["id"]),
        "source": source["source"],
        "external_camera_key": source["external_camera_key"],
        "source_kind": source["source_kind"],
        "last_signal_at": ev.last_signal_at.isoformat() if ev.last_signal_at else None,
        "age_seconds": round(ev.age_seconds) if ev.age_seconds is not None else None,
        "expected_interval_seconds": ev.expected_interval_seconds,
        "liveness_grace_seconds": ev.liveness_grace_seconds,
    }


# Claim the least-recently-checked enabled rows (oldest first, skip-locked) and
# stamp last_liveness_checked_at in the same statement so a poisoned source can't
# starve the batch (it advances to the back regardless of processing outcome).
_CLAIM_SQL = """
with due as (
    select id
      from cv_sources
     where liveness_enabled
     order by last_liveness_checked_at asc nulls first
     for update skip locked
     limit $1
)
update cv_sources s
   set last_liveness_checked_at = $2
  from due
 where s.id = due.id
returning s.id
"""

_SELECT_FOR_UPDATE_SQL = """
select id, account_id, source, external_camera_key, source_kind,
       liveness_enabled, expected_interval_seconds, liveness_grace_seconds,
       last_seen_at, last_collector_heartbeat_at,
       health_status, last_liveness_issue_id, liveness_silence_started_at
  from cv_sources
 where id = $1
 for update
"""

# last_health_status_changed_at advances (to $7=now) only on an actual change;
# the SET reads the pre-update health_status. last_liveness_checked_at is
# re-stamped (already set at claim; harmless, keeps it exact).
_UPDATE_HEALTH_SQL = """
update cv_sources
   set health_status = $2,
       health_reason = $3,
       health_message = $4,
       last_liveness_issue_id = $5,
       liveness_silence_started_at = $6,
       last_health_status_changed_at = case
           when cv_sources.health_status <> $2 then $7
           else cv_sources.last_health_status_changed_at end,
       last_liveness_checked_at = $7
 where id = $1
"""


@dataclass
class LivenessSweepStats:
    evaluated: int = 0
    opened: int = 0
    resolved: int = 0
    errors: int = 0
    by_status: dict[str, int] = field(default_factory=dict)


async def run_source_liveness_sweep(
    pool: asyncpg.Pool,
    *,
    now: datetime | None = None,
    limit: int = 500,
) -> LivenessSweepStats:
    """One liveness tick over the least-recently-checked enabled sources, all
    tenants. Claim a bounded batch in a short transaction, then process each in
    its own transaction (one source's failure can't abort the run). Idempotent +
    safe to run repeatedly/concurrently."""
    if now is None:
        now = datetime.now(UTC)
    stats = LivenessSweepStats()

    async with pool.acquire() as conn, conn.transaction():
        claimed = await conn.fetch(_CLAIM_SQL, limit, now)
    ids = [r["id"] for r in claimed]

    for source_id in ids:
        try:
            async with pool.acquire() as conn, conn.transaction():
                await _process_source(conn, source_id=source_id, now=now, stats=stats)
        except Exception as exc:
            stats.errors += 1
            _logger.warning(
                "source_liveness_source_failed",
                source_id=str(source_id),
                error=type(exc).__name__,
            )
    return stats


async def _process_source(
    conn: asyncpg.Connection,
    *,
    source_id: UUID,
    now: datetime,
    stats: LivenessSweepStats,
) -> None:
    row = await conn.fetchrow(_SELECT_FOR_UPDATE_SQL, source_id)
    if row is None:
        return  # deleted between claim and process
    source = dict(row)
    if not source["liveness_enabled"]:
        return  # disabled between claim and process — the sweep ignores disabled

    ev = evaluate_source_health(source, now=now)
    stats.evaluated += 1
    stats.by_status[ev.status] = stats.by_status.get(ev.status, 0) + 1

    account_id = source["account_id"]
    issue_id = source["last_liveness_issue_id"]
    silence_started = source["liveness_silence_started_at"]

    if ev.is_issue:
        assert ev.issue_type is not None and ev.severity is not None
        issue_row, opened = await upsert_open_issue(
            conn,
            account_id=account_id,
            dedupe_key=_dedupe_key(source_id, ev.issue_type),
            issue_type=ev.issue_type,
            category="liveness",
            severity=ev.severity,
            title=ev.title or "",
            message=ev.message or "",
            likely_cause=ev.likely_cause,
            recommended_action=ev.recommended_action,
            evidence=_evidence(source, ev),
            detector_version=LIVENESS_DETECTOR_VERSION,
            camera_id=None,
            external_camera_key=source["external_camera_key"],
            source_batch_id=None,
            source_observation_ids=None,
            occurred_at=now,
        )
        issue_id = issue_row["id"]
        if opened:
            stats.opened += 1
        # Freeze the moment silence began (= last signal + threshold); set once.
        if silence_started is None and ev.last_signal_at is not None:
            silence_started = ev.last_signal_at + timedelta(
                seconds=ev.expected_interval_seconds + ev.liveness_grace_seconds
            )
    elif issue_id is not None:
        # Recovered (or otherwise no longer issue-worthy): resolve whichever
        # liveness issue is open for this source and clear the markers.
        resolved = await resolve_open_issues_by_dedupe(
            conn,
            account_id=account_id,
            dedupe_keys=[
                _dedupe_key(source_id, "source_silent"),
                _dedupe_key(source_id, "collector_offline"),
            ],
            reason=_RESOLVE_REASON,
            occurred_at=now,
        )
        stats.resolved += len(resolved)
        issue_id = None
        silence_started = None

    await conn.execute(
        _UPDATE_HEALTH_SQL,
        source_id,
        ev.status,
        ev.reason,
        ev.message,
        issue_id,
        silence_started,
        now,
    )


async def source_liveness_sweep_loop(pool: asyncpg.Pool, interval_seconds: int, limit: int) -> None:
    """In-process loop the app lifespan spawns when
    ``source_liveness_sweep_interval_seconds > 0``. Sleeps FIRST so a
    crash-restart can't hot-loop; a failed tick is logged and the loop keeps
    going; cancels cleanly on shutdown."""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            stats = await run_source_liveness_sweep(pool, limit=limit)
            if stats.opened or stats.resolved or stats.errors:
                _logger.info(
                    "source_liveness_sweep_tick",
                    evaluated=stats.evaluated,
                    opened=stats.opened,
                    resolved=stats.resolved,
                    errors=stats.errors,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.warning("source_liveness_sweep_tick_failed", error=type(exc).__name__)
