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
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.rules import RuleCreateRequest
from upstream_api.services.processor import coerce_jsonb

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

# Cooldown guard evaluated in the same statement as the insert.
_INSERT_ALERT_SQL = """
insert into alerts (
    account_id, camera_id, rule_id, observation_id,
    severity, title, message, payload
)
select $1, $2, $3, $4, $5, $6, $7, $8::jsonb
 where not exists (
    select 1
      from alerts
     where rule_id = $3
       and created_at > now() - make_interval(secs => $9)
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


def rule_matches(rule_type: str, config: dict[str, Any], payload: dict[str, Any]) -> bool:
    """Pure match decision for one rule against one observation payload.

    A missing optional filter key means "match any". A payload missing the
    field a filter needs does NOT match (a rule scoped to line A must never
    fire on an observation that can't prove it came from line A).
    """
    if rule_type == "line_crossing":
        line_id = config.get("line_id")
        return line_id is None or payload.get("line_id") == line_id
    if rule_type == "roi_occupancy":
        roi_id = config.get("roi_id")
        if roi_id is not None and payload.get("roi_id") != roi_id:
            return False
        min_count = config.get("min_total_count", 1)
        total = payload.get("total_count")
        return isinstance(total, (int, float)) and total >= min_count
    if rule_type == "zone_entered":
        zone_id = config.get("zone_id")
        return zone_id is None or payload.get("zone_id") == zone_id
    if rule_type == "dwell_exceeded":
        zone_id = config.get("zone_id")
        if zone_id is not None and payload.get("zone_id") != zone_id:
            return False
        min_duration = config.get("min_duration_seconds")
        if min_duration is None:
            return True
        duration = payload.get("duration_seconds")
        return isinstance(duration, (int, float)) and duration >= min_duration
    return False


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
) -> int:
    """Evaluate enabled rules for one just-ingested observation.

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
        if not rule_matches(row["rule_type"], config, payload):
            continue
        new_id = await conn.fetchval(
            _INSERT_ALERT_SQL,
            account_id,
            camera_id,
            row["id"],
            observation_id,
            row["severity"],
            row["name"],
            build_alert_message(row["rule_type"], payload),
            json.dumps(payload),
            float(row["cooldown_seconds"]),
        )
        if new_id is not None:
            created += 1
    return created
