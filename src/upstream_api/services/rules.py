"""Rules/Triggers M1 — rule CRUD, matching, and alert creation.

Evaluation runs inline on the observation ingest path (routers/processor.py),
savepoint-isolated so it can never fail the ingest. The matcher itself
(``rule_matches``) is a pure function over (rule_type, config, payload) so it
is trivially testable and never touches the DB.

Alert flood control is the rule's ``cooldown_seconds``: the insert is guarded
by a NOT EXISTS over the latest alert for the same rule, evaluated in the
same statement so concurrent observations can at worst double-fire across two
connections (acceptable for MVP; a unique time-bucket key can tighten this
later without an API change).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import asyncpg

from upstream_api.schemas.rules import SCHEDULE_DAYS, RuleCreateRequest
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.workflow_runs import (
    finalize_workflow_run,
    insert_workflow_run,
)

# Observation types this MVP evaluates, mapped to the rule_type they trigger.
# processor.zone_exited / dwell_started / dwell_updated / roi heartbeats with
# no matching rule simply evaluate to zero rules — never an error.
OBSERVATION_TYPE_TO_RULE_TYPE: dict[str, str] = {
    "processor.line_crossing": "line_crossing",
    "processor.roi_occupancy": "roi_occupancy",
    "processor.zone_entered": "zone_entered",
    "processor.dwell_exceeded": "dwell_exceeded",
}

_INSERT_RULE_SQL = """
insert into rules (
    account_id, camera_id, name, rule_type, enabled, config,
    severity, cooldown_seconds, created_by
)
values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
returning id, camera_id, name, rule_type, enabled, config,
          severity, cooldown_seconds, created_by, created_at, updated_at
"""

# account_id is NOT selected onto read responses (the caller is already
# account-scoped) — same posture as the observations read path.
_LIST_RULES_SQL = """
select
    id, camera_id, name, rule_type, enabled, config,
    severity, cooldown_seconds, created_by, created_at, updated_at
  from rules
 where account_id = $1
   and ($2::uuid is null or camera_id = $2)
 order by created_at desc
"""

_FETCH_RULE_SQL = """
select
    id, camera_id, name, rule_type, enabled, config,
    severity, cooldown_seconds, created_by, created_at, updated_at
  from rules
 where account_id = $1
   and id = $2
"""

# PATCH semantics: a null parameter keeps the current value.
_UPDATE_RULE_SQL = """
update rules
   set name             = coalesce($3, name),
       enabled          = coalesce($4, enabled),
       config           = coalesce($5::jsonb, config),
       severity         = coalesce($6, severity),
       cooldown_seconds = coalesce($7, cooldown_seconds)
 where account_id = $1
   and id = $2
returning id, camera_id, name, rule_type, enabled, config,
          severity, cooldown_seconds, created_by, created_at, updated_at
"""

_DELETE_RULE_SQL = """
delete from rules
 where account_id = $1
   and id = $2
"""

# Evaluation hot path. Double-scoped by account_id AND camera_id — defense in
# depth on top of the ingest router deriving both from the session row.
_ENABLED_RULES_FOR_CAMERA_SQL = """
select id, name, rule_type, config, severity, cooldown_seconds
  from rules
 where account_id = $1
   and camera_id = $2
   and rule_type = $3
   and enabled
"""

# Cooldown guard evaluated in the same statement as the insert. M2: the
# alert carries the workflow_run_id of the run whose action step created it.
_INSERT_ALERT_SQL = """
insert into alerts (
    account_id, camera_id, rule_id, observation_id, workflow_run_id,
    severity, title, message, payload
)
select $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb
 where not exists (
    select 1
      from alerts
     where rule_id = $3
       and created_at > now() - make_interval(secs => $10)
 )
returning id
"""


async def insert_rule(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    created_by: UUID | None,
    req: RuleCreateRequest,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        _INSERT_RULE_SQL,
        account_id,
        req.camera_id,
        req.name,
        req.rule_type,
        req.enabled,
        json.dumps(req.config),
        req.severity,
        req.cooldown_seconds,
        created_by,
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return dict(row)


async def list_rules(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID | None,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_LIST_RULES_SQL, account_id, camera_id)
    return [dict(r) for r in rows]


async def fetch_rule(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    rule_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(_FETCH_RULE_SQL, account_id, rule_id)
    return dict(row) if row is not None else None


async def update_rule(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    rule_id: UUID,
    name: str | None,
    enabled: bool | None,
    config: dict[str, Any] | None,
    severity: str | None,
    cooldown_seconds: int | None,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        _UPDATE_RULE_SQL,
        account_id,
        rule_id,
        name,
        enabled,
        json.dumps(config) if config is not None else None,
        severity,
        cooldown_seconds,
    )
    return dict(row) if row is not None else None


async def delete_rule(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    rule_id: UUID,
) -> bool:
    status = await conn.execute(_DELETE_RULE_SQL, account_id, rule_id)
    return bool(status == "DELETE 1")


def _classes_allowed(config: dict[str, Any], payload: dict[str, Any]) -> bool:
    """M3 object-class filter. A payload that can't prove its class
    (missing/odd ``class_label``) never matches a class-filtered rule."""
    classes = config.get("object_classes")
    if classes is None:
        return True
    label = payload.get("class_label")
    return isinstance(label, str) and isinstance(classes, list) and label in classes


def _track_ok(config: dict[str, Any], payload: dict[str, Any]) -> bool:
    """M3 require_track_id. The observation must be tied to a tracked
    object; this is single-event only — NOT same-object sequencing."""
    if not config.get("require_track_id"):
        return True
    track = payload.get("track_id")
    return isinstance(track, str) and track != ""


def rule_matches(rule_type: str, config: dict[str, Any], payload: dict[str, Any]) -> bool:
    """Pure match decision for one rule against one observation payload.

    A missing optional filter key means "match any". A payload missing the
    field a filter needs does NOT match (a rule scoped to line A must never
    fire on an observation that can't prove it came from line A — and a
    class-filtered rule must never fire on an observation that can't prove
    its object class). Time-window gating is separate (``schedule_allows``)
    because it needs the observation timestamp, not the payload.
    """
    if rule_type == "line_crossing":
        line_id = config.get("line_id")
        return line_id is None or payload.get("line_id") == line_id
    if rule_type == "roi_occupancy":
        roi_id = config.get("roi_id")
        if roi_id is not None and payload.get("roi_id") != roi_id:
            return False
        total = payload.get("total_count")
        if not isinstance(total, (int, float)):
            return False
        min_count = config.get("min_total_count", 1)
        if total < min_count:
            return False
        max_count = config.get("max_total_count")
        return max_count is None or total <= max_count
    if rule_type == "zone_entered":
        zone_id = config.get("zone_id")
        if zone_id is not None and payload.get("zone_id") != zone_id:
            return False
        return _classes_allowed(config, payload) and _track_ok(config, payload)
    if rule_type == "dwell_exceeded":
        zone_id = config.get("zone_id")
        if zone_id is not None and payload.get("zone_id") != zone_id:
            return False
        if not _classes_allowed(config, payload) or not _track_ok(config, payload):
            return False
        min_duration = config.get("min_duration_seconds")
        if min_duration is None:
            return True
        duration = payload.get("duration_seconds")
        return isinstance(duration, (int, float)) and duration >= min_duration
    return False


def schedule_allows(config: dict[str, Any], observed_at: datetime) -> bool:
    """M3 time-window gate, evaluated inline from the observation timestamp
    (no periodic evaluator). Pure and non-raising — it runs inside the
    ingest savepoint, so a malformed stored schedule degrades to "no match"
    rather than an exception.

    Semantics: the observation time is converted to the schedule's IANA
    timezone. ``days`` (default: every day) names the LOCAL day the window
    STARTS on. ``start_time < end_time`` is a same-day window matching
    ``start <= t < end``; ``start_time > end_time`` wraps midnight — e.g.
    22:00-06:00 on ["fri"] matches Friday 22:00 through Saturday 05:59.
    A naive ``observed_at`` is treated as UTC.
    """
    schedule = config.get("schedule")
    if schedule is None:
        return True
    if not isinstance(schedule, dict):
        return False
    try:
        tz = ZoneInfo(str(schedule["timezone"]))
        start_h, start_m = str(schedule["start_time"]).split(":")
        end_h, end_m = str(schedule["end_time"]).split(":")
        start = int(start_h) * 60 + int(start_m)
        end = int(end_h) * 60 + int(end_m)
    except Exception:
        return False
    if not (0 <= start <= 1439 and 0 <= end <= 1439) or start == end:
        return False
    days_raw = schedule.get("days")
    if days_raw is None:
        allowed_days = set(SCHEDULE_DAYS)
    elif isinstance(days_raw, list):
        allowed_days = {d for d in days_raw if isinstance(d, str)}
    else:
        return False
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    local = observed_at.astimezone(tz)
    minute = local.hour * 60 + local.minute
    day = SCHEDULE_DAYS[local.weekday()]
    if start < end:
        return day in allowed_days and start <= minute < end
    # Overnight wrap: tonight's tail belongs to the day the window started.
    prev_day = SCHEDULE_DAYS[(local.weekday() - 1) % 7]
    return (day in allowed_days and minute >= start) or (prev_day in allowed_days and minute < end)


def build_alert_message(rule_type: str, payload: dict[str, Any]) -> str:
    """One-line human summary of the matched observation."""
    if rule_type == "line_crossing":
        label = payload.get("line_label") or payload.get("line_id") or "a line"
        return f"Line crossing detected on {label}."
    if rule_type == "roi_occupancy":
        label = payload.get("roi_label") or payload.get("roi_id") or "an ROI"
        total = payload.get("total_count")
        return f"{total} object(s) in {label}."
    if rule_type == "zone_entered":
        label = payload.get("zone_label") or payload.get("zone_id") or "a zone"
        return f"Object entered {label}."
    if rule_type == "dwell_exceeded":
        label = payload.get("zone_label") or payload.get("zone_id") or "a zone"
        duration = payload.get("duration_seconds")
        return f"Object dwelled in {label} for {duration} second(s)."
    return "Rule matched."


async def evaluate_observation(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    observation_id: UUID,
    observation_type: str,
    payload: dict[str, Any],
    observed_at: datetime,
) -> int:
    """Evaluate enabled rules for one just-ingested observation.

    Workflow Engine M2: every MATCH produces a workflow_run — the system of
    record — whether or not the action step sends an alert. The run opens
    'active', the cooldown-guarded alert insert is the action step, and the
    run is finalized 'completed' (alert_created) or 'suppressed'
    (alert_suppressed_cooldown) in the same request. Non-matching and
    disabled rules produce no run.

    Workflow Engine M3: a rule outside its schedule window, or whose richer
    conditions (object class, track, count bounds) don't hold, is a
    NON-MATCH — no run, no alert. Schedule gating uses ``observed_at``
    inline; there is no periodic evaluator.

    Returns the number of alerts created (0 when no rule matches or every
    matching rule is inside its cooldown window).
    """
    rule_type = OBSERVATION_TYPE_TO_RULE_TYPE.get(observation_type)
    if rule_type is None:
        return 0

    rows = await conn.fetch(_ENABLED_RULES_FOR_CAMERA_SQL, account_id, camera_id, rule_type)
    created = 0
    for row in rows:
        config = coerce_jsonb(row["config"])
        if not schedule_allows(config, observed_at):
            continue
        if not rule_matches(row["rule_type"], config, payload):
            continue
        summary = build_alert_message(row["rule_type"], payload)
        run_id = await insert_workflow_run(
            conn,
            account_id=account_id,
            camera_id=camera_id,
            rule_id=row["id"],
            trigger_observation_id=observation_id,
            workflow_type="simple_rule",
            title=row["name"],
            summary=summary,
            trigger_type=observation_type,
            evidence=payload,
            # Snapshot the rule definition so the run stays interpretable
            # after the rule is edited or deleted.
            payload={
                "rule": {
                    "id": str(row["id"]),
                    "rule_type": row["rule_type"],
                    "config": config,
                    "severity": row["severity"],
                    "cooldown_seconds": row["cooldown_seconds"],
                }
            },
        )
        alert_id = await conn.fetchval(
            _INSERT_ALERT_SQL,
            account_id,
            camera_id,
            row["id"],
            observation_id,
            run_id,
            row["severity"],
            row["name"],
            summary,
            json.dumps(payload),
            float(row["cooldown_seconds"]),
        )
        if alert_id is not None:
            created += 1
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
    return created
