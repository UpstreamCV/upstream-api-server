"""Workflow Engine M4 — the time-based rule evaluator.

The first evaluation path that does NOT ride the observation-ingest
request: ``camera_offline`` and ``no_detections_for`` rules describe the
ABSENCE of activity, so something has to look at the clock. This module is
that something — a service function safe to call repeatedly, driven by the
optional in-process loop (``workflow_evaluator_loop``, gated by
``UPSTREAM_WORKFLOW_EVALUATOR_INTERVAL_SECONDS``) and/or the internal
endpoint ``POST /v1/internal/workflow-evaluator/run``.

Firing semantics (deliberately different from the event-driven path): a
timer rule's condition is continuous, so "match" is re-observed on every
tick while it holds. To avoid flooding history, the workflow-run INSERT
itself is guarded by a NOT EXISTS over runs for the same rule within
``cooldown_seconds`` — the cooldown is the RE-FIRE interval while the
condition persists, and ticks inside the window create NOTHING (no
suppressed run per tick; the fired run is the episode marker). Validation
enforces cooldown >= 60s for timer rules so cooldown can never be shorter
than the evaluator tick.

Duplicate prevention is the same single-statement posture as the M1 alert
guard: concurrent evaluator invocations can at worst double-fire across
two connections. The production deployment is a single api-server
instance; with replicas, disable the loop on all but one. No advisory
locks — documented best-effort.

Condition semantics (fail closed — anything unprovable is a non-match):

- camera_offline: matched when the camera has NO active streaming session
  AND no heartbeat within ``offline_for_seconds``. A camera that has never
  heartbeated matches (the rule was authored deliberately); disabled
  cameras are excluded at the rules query.
- no_detections_for: evaluated ONLY while the camera has an active
  streaming session ("camera up but quiet" is the anomaly — a down camera
  is camera_offline's job, and gating prevents double-alerting). The quiet
  clock anchors at max(last matching observation, session started_at), so
  a freshly started session never instant-fires.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.logging import get_logger
from upstream_api.schemas.rules import (
    CLASS_ATTRIBUTING_OBSERVATION_TYPES,
    NO_DETECTIONS_OBSERVATION_TYPES,
    TIMER_RULE_TYPES,
)
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.rules import _INSERT_ALERT_SQL, schedule_allows
from upstream_api.services.workflow_runs import finalize_workflow_run

_logger = get_logger("workflow_evaluator")

# Disabled cameras are intentionally quiet — never evaluate their rules.
_ENABLED_TIMER_RULES_SQL = """
select r.id, r.account_id, r.camera_id, r.name, r.rule_type, r.config,
       r.severity, r.cooldown_seconds
  from rules r
  join cameras c on c.id = r.camera_id
 where r.enabled
   and r.rule_type = any($1::text[])
   and c.status != 'disabled'
 order by r.created_at
"""

_CAMERA_LIVENESS_SQL = """
select
    exists(
        select 1
          from stream_sessions
         where camera_id = $1
           and state = 'streaming'
           and expires_at > now()
    ) as active_session,
    (select max(reported_at) from heartbeats where camera_id = $1) as last_heartbeat_at
"""

_ACTIVE_SESSION_SQL = """
select id, started_at
  from stream_sessions
 where camera_id = $1
   and state = 'streaming'
   and expires_at > now()
 order by started_at desc nulls last
 limit 1
"""

_LAST_MATCHING_OBSERVATION_SQL = """
select max(observed_at)
  from observations
 where camera_id = $1
   and type = any($2::text[])
   and ($3::text[] is null or payload->>'class_label' = any($3))
"""

# The re-fire guard: a run for this rule inside the cooldown window means
# the episode is already marked — this tick creates NOTHING. Same
# single-statement race posture as the M1 alert guard.
_INSERT_TIMER_RUN_SQL = """
insert into workflow_runs (
    account_id, camera_id, rule_id, workflow_type,
    title, summary, trigger_type, evidence, payload
)
select $1, $2, $3, 'time_based_rule', $4, $5, $6, $7::jsonb, $8::jsonb
 where not exists (
    select 1
      from workflow_runs
     where rule_id = $3
       and created_at > now() - make_interval(secs => $9)
 )
returning id
"""


@dataclass
class EvaluatorStats:
    evaluated_rules: int = 0
    schedule_skipped: int = 0
    matched_rules: int = 0
    workflow_runs_created: int = 0
    alerts_created: int = 0
    suppressed_by_cooldown: int = 0
    errors: int = 0


@dataclass(frozen=True)
class _Condition:
    evidence: dict[str, Any]
    summary: str


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _minutes(seconds: float) -> str:
    return f"{int(seconds // 60)}m"


async def _check_camera_offline(
    conn: asyncpg.Connection,
    *,
    camera_id: UUID,
    config: dict[str, Any],
    now: datetime,
) -> _Condition | None:
    threshold = config.get("offline_for_seconds")
    if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold <= 0:
        return None  # malformed stored config — fail closed
    row = await conn.fetchrow(_CAMERA_LIVENESS_SQL, camera_id)
    if row is None or row["active_session"]:
        return None
    last_heartbeat = row["last_heartbeat_at"]
    offline_for: int | None
    if last_heartbeat is None:
        offline_for = None
        summary = "Camera offline — no heartbeat has ever been reported."
    else:
        offline_for = int((now - last_heartbeat).total_seconds())
        if offline_for < threshold:
            return None
        summary = f"Camera offline for {_minutes(offline_for)} (threshold {_minutes(threshold)})."
    return _Condition(
        evidence={
            "camera_id": str(camera_id),
            "last_seen_at": _iso(last_heartbeat),
            "offline_for_seconds": offline_for,
            "threshold_seconds": threshold,
            "active_session_found": False,
        },
        summary=summary,
    )


async def _check_no_detections(
    conn: asyncpg.Connection,
    *,
    camera_id: UUID,
    config: dict[str, Any],
    now: datetime,
) -> _Condition | None:
    threshold = config.get("quiet_for_seconds")
    if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold <= 0:
        return None  # malformed stored config — fail closed
    session = await conn.fetchrow(_ACTIVE_SESSION_SQL, camera_id)
    if session is None:
        # Camera isn't streaming — that's camera_offline's job; evaluating
        # quietness on a down camera would double-alert.
        return None

    classes_raw = config.get("object_classes")
    classes = (
        [c for c in classes_raw if isinstance(c, str)]
        if isinstance(classes_raw, list) and classes_raw
        else None
    )
    types_raw = config.get("observation_types")
    if isinstance(types_raw, list) and types_raw:
        types = [t for t in types_raw if isinstance(t, str)]
    elif classes is not None:
        # A class filter is only honest over class-attributing types.
        types = sorted(CLASS_ATTRIBUTING_OBSERVATION_TYPES)
    else:
        types = list(NO_DETECTIONS_OBSERVATION_TYPES)
    if not types:
        return None  # malformed stored config — fail closed

    last_observation = await conn.fetchval(
        _LAST_MATCHING_OBSERVATION_SQL, camera_id, types, classes
    )
    session_started = session["started_at"]
    anchors = [t for t in (last_observation, session_started) if t is not None]
    if not anchors:
        return None  # can't prove how long it's been quiet — fail closed
    anchor = max(anchors)
    quiet_for = (now - anchor).total_seconds()
    if quiet_for < threshold:
        return None
    what = ", ".join(classes) if classes else "matching activity"
    return _Condition(
        evidence={
            "camera_id": str(camera_id),
            "last_matching_observation_at": _iso(last_observation),
            "session_started_at": _iso(session_started),
            "quiet_for_seconds": int(quiet_for),
            "threshold_seconds": threshold,
            "object_classes": classes,
            "observation_types": types,
            "active_session_found": True,
        },
        summary=(
            f"No {what} detected for {_minutes(quiet_for)} "
            f"(threshold {_minutes(threshold)}) while streaming."
        ),
    )


async def _evaluate_rule(
    conn: asyncpg.Connection,
    rule: Any,
    *,
    now: datetime,
    stats: EvaluatorStats,
) -> None:
    config = coerce_jsonb(rule["config"])
    if not schedule_allows(config, now):
        stats.schedule_skipped += 1
        return
    if rule["rule_type"] == "camera_offline":
        condition = await _check_camera_offline(
            conn, camera_id=rule["camera_id"], config=config, now=now
        )
    elif rule["rule_type"] == "no_detections_for":
        condition = await _check_no_detections(
            conn, camera_id=rule["camera_id"], config=config, now=now
        )
    else:
        return  # unknown timer type — fail closed
    if condition is None:
        return
    stats.matched_rules += 1

    cooldown = float(rule["cooldown_seconds"])
    run_id = await conn.fetchval(
        _INSERT_TIMER_RUN_SQL,
        rule["account_id"],
        rule["camera_id"],
        rule["id"],
        rule["name"],
        condition.summary,
        f"timer.{rule['rule_type']}",
        json.dumps(condition.evidence),
        json.dumps(
            {
                "rule": {
                    "id": str(rule["id"]),
                    "rule_type": rule["rule_type"],
                    "config": config,
                    "severity": rule["severity"],
                    "cooldown_seconds": rule["cooldown_seconds"],
                }
            }
        ),
        cooldown,
    )
    if run_id is None:
        # Inside the re-fire window — the episode is already marked.
        stats.suppressed_by_cooldown += 1
        return
    stats.workflow_runs_created += 1

    alert_id = await conn.fetchval(
        _INSERT_ALERT_SQL,
        rule["account_id"],
        rule["camera_id"],
        rule["id"],
        None,  # no triggering observation — this is a timer match
        run_id,
        rule["severity"],
        rule["name"],
        condition.summary,
        json.dumps(condition.evidence),
        cooldown,
    )
    if alert_id is not None:
        stats.alerts_created += 1
        await finalize_workflow_run(
            conn,
            workflow_run_id=run_id,
            status="completed",
            action_result="alert_created",
        )
    else:
        await finalize_workflow_run(
            conn,
            workflow_run_id=run_id,
            status="suppressed",
            action_result="alert_suppressed_cooldown",
        )


async def run_workflow_evaluator(
    pool: asyncpg.Pool,
    *,
    now: datetime | None = None,
) -> EvaluatorStats:
    """One evaluator tick over every enabled timer rule, all tenants.

    Safe to call repeatedly and concurrently (the run-insert guard makes
    overlap best-effort idempotent). One rule's failure is logged with
    scalar ids only and never stops the tick.
    """
    if now is None:
        now = datetime.now(UTC)
    stats = EvaluatorStats()
    async with pool.acquire() as conn:
        rules = await conn.fetch(_ENABLED_TIMER_RULES_SQL, sorted(TIMER_RULE_TYPES))
        for rule in rules:
            stats.evaluated_rules += 1
            try:
                await _evaluate_rule(conn, rule, now=now, stats=stats)
            except Exception as exc:
                stats.errors += 1
                _logger.warning(
                    "timer_rule_evaluation_failed",
                    rule_id=str(rule["id"]),
                    error=type(exc).__name__,
                )
    return stats


async def workflow_evaluator_loop(pool: asyncpg.Pool, interval_seconds: int) -> None:
    """The in-process loop the app lifespan spawns when
    ``workflow_evaluator_interval_seconds > 0``. Sleeps FIRST so a
    crash-restart cycle can't hot-loop evaluation, then runs ticks forever;
    a failed tick is logged and the loop keeps going."""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            stats = await run_workflow_evaluator(pool)
            if stats.matched_rules or stats.errors:
                _logger.info(
                    "workflow_evaluator_tick",
                    evaluated=stats.evaluated_rules,
                    matched=stats.matched_rules,
                    runs_created=stats.workflow_runs_created,
                    alerts_created=stats.alerts_created,
                    suppressed=stats.suppressed_by_cooldown,
                    errors=stats.errors,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.warning("workflow_evaluator_tick_failed", error=type(exc).__name__)
