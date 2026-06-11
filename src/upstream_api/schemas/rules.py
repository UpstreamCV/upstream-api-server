"""Rules/Triggers M1 — user-authored rules over processor observations.

``rule_type`` is open vocabulary at the DB layer; this module's ``RuleType``
Literal is the currently-supported set (rejected with 422 otherwise). Each
type matches one processor observation type family; per-type ``config``
parameters are validated by ``validate_rule_config`` so a saved rule can
always be evaluated without re-checking shapes on the hot path.

Background-evaluated types (camera_offline, no_detections_for_n_minutes) are
deferred — they need a periodic evaluator, which does not exist yet.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

RuleType = Literal[
    # Fires on processor.line_crossing (optionally one line_id).
    "line_crossing",
    # Fires on processor.roi_occupancy when total_count >= min_total_count.
    "roi_occupancy",
    # Fires on processor.zone_entered (optionally one zone_id).
    "zone_entered",
    # Fires on processor.dwell_exceeded (optionally one zone_id and/or a
    # higher min_duration_seconds than the processor threshold).
    "dwell_exceeded",
]

RULE_TYPES: tuple[RuleType, ...] = (
    "line_crossing",
    "roi_occupancy",
    "zone_entered",
    "dwell_exceeded",
)

RuleSeverity = Literal["info", "warning", "critical"]

# Per-type allowed config keys. Unknown keys are rejected so typos surface at
# authoring time, not as silently-never-matching rules.
#
# M3 (richer conditions) deliberately allows a condition ONLY on rule types
# whose production observation payloads actually carry the field it needs —
# a rule that can never match is worse than a 422 at authoring time:
#
# - object_classes / require_track_id: zone_entered + dwell_exceeded only.
#   The DeepStream worker stamps class_label / track_id on per-track zone
#   and dwell events, but line crossings come from nvdsanalytics CUMULATIVE
#   line counters (no per-object attribution), and ROI occupancy counts are
#   cross-class aggregates (M7D). Extending these to line_crossing needs a
#   processor change (per-object crossing attribution) — documented
#   follow-up, not faked here.
# - min_confidence: zone_entered + dwell_exceeded — the processor's
#   payload-enrichment side task stamps per-object detection confidence
#   onto zone/dwell events (omitted when the track wasn't detected in the
#   triggering frame, so the no-match-on-missing posture still applies).
#   Still unavailable on line_crossing (no per-object attribution).
# - object_class (roi_occupancy): the same side task populates
#   ``class_counts`` from object-attributed ROI membership; when set, the
#   min/max count bounds apply to that class's count instead of
#   total_count. Pre-enrichment events carry an empty breakdown, which
#   counts as zero — safe because min_total_count's floor is 1.
# - direction: deferred — in the line_crossing payload contract but never
#   produced (cumulative counters are direction-blind).
# - schedule: all rule types — evaluated inline from the observation
#   timestamp, no periodic evaluator involved.
_SCHEDULE_KEY = "schedule"
_ALLOWED_CONFIG_KEYS: dict[str, frozenset[str]] = {
    "line_crossing": frozenset({"line_id", _SCHEDULE_KEY}),
    "roi_occupancy": frozenset(
        {"roi_id", "min_total_count", "max_total_count", "object_class", _SCHEDULE_KEY}
    ),
    "zone_entered": frozenset(
        {"zone_id", "object_classes", "require_track_id", "min_confidence", _SCHEDULE_KEY}
    ),
    "dwell_exceeded": frozenset(
        {
            "zone_id",
            "min_duration_seconds",
            "object_classes",
            "require_track_id",
            "min_confidence",
            _SCHEDULE_KEY,
        }
    ),
}

SCHEDULE_DAYS: tuple[str, ...] = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_ALLOWED_SCHEDULE_KEYS = frozenset({"days", "start_time", "end_time", "timezone"})
_HHMM_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
MAX_OBJECT_CLASSES = 20


def _validate_schedule(schedule: object, problems: list[str]) -> None:
    if not isinstance(schedule, dict):
        problems.append("config.schedule must be an object.")
        return
    unknown = sorted(set(schedule) - _ALLOWED_SCHEDULE_KEYS)
    if unknown:
        problems.append(f"Unknown schedule keys: {', '.join(unknown)}.")
    for key in ("start_time", "end_time"):
        value = schedule.get(key)
        if not isinstance(value, str) or not _HHMM_RE.match(value):
            problems.append(f'config.schedule.{key} must be an "HH:MM" string.')
    if (
        isinstance(schedule.get("start_time"), str)
        and isinstance(schedule.get("end_time"), str)
        and schedule.get("start_time") == schedule.get("end_time")
    ):
        problems.append(
            "config.schedule start_time and end_time must differ "
            "(a zero-length window never matches)."
        )
    tz = schedule.get("timezone")
    if not isinstance(tz, str) or not tz.strip():
        problems.append("config.schedule.timezone must be a non-empty IANA name.")
    else:
        try:
            ZoneInfo(tz)
        except Exception:
            problems.append(f"config.schedule.timezone is not a known IANA zone: {tz!r}.")
    if "days" in schedule:
        days = schedule["days"]
        if (
            not isinstance(days, list)
            or not days
            or len(days) != len(set(days))
            or any(d not in SCHEDULE_DAYS for d in days)
        ):
            problems.append(
                "config.schedule.days must be a non-empty list of unique "
                f"day keys from {list(SCHEDULE_DAYS)}."
            )


def validate_rule_config(rule_type: str, config: dict[str, Any]) -> list[str]:
    """Pure per-type config validation. Returns human-readable problems."""
    problems: list[str] = []
    allowed = _ALLOWED_CONFIG_KEYS.get(rule_type)
    if allowed is None:
        return [f"Unsupported rule_type: {rule_type!r}."]
    unknown = sorted(set(config) - allowed)
    if unknown:
        problems.append(f"Unknown config keys for {rule_type}: {', '.join(unknown)}.")
    for key in ("line_id", "roi_id", "zone_id"):
        if key in config and not (isinstance(config[key], str) and config[key].strip()):
            problems.append(f"config.{key} must be a non-empty string.")
    for key in ("min_total_count", "max_total_count"):
        if key in config:
            value = config[key]
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                problems.append(f"config.{key} must be an integer >= 1.")
    if (
        isinstance(config.get("min_total_count"), int)
        and isinstance(config.get("max_total_count"), int)
        and not isinstance(config.get("min_total_count"), bool)
        and not isinstance(config.get("max_total_count"), bool)
        and config["max_total_count"] < config["min_total_count"]
    ):
        problems.append("config.max_total_count must be >= min_total_count.")
    if "min_duration_seconds" in config:
        value = config["min_duration_seconds"]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            problems.append("config.min_duration_seconds must be a number > 0.")
    if "object_classes" in config:
        classes = config["object_classes"]
        if (
            not isinstance(classes, list)
            or not classes
            or len(classes) > MAX_OBJECT_CLASSES
            or len(classes) != len({c for c in classes if isinstance(c, str)})
            or any(not (isinstance(c, str) and c.strip()) for c in classes)
        ):
            problems.append(
                "config.object_classes must be a non-empty list of up to "
                f"{MAX_OBJECT_CLASSES} unique non-empty strings."
            )
    if "require_track_id" in config and not isinstance(config["require_track_id"], bool):
        problems.append("config.require_track_id must be a boolean.")
    if "min_confidence" in config:
        value = config["min_confidence"]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not (0 < value <= 1):
            problems.append("config.min_confidence must be a number in (0, 1].")
    if "object_class" in config and not (
        isinstance(config["object_class"], str) and config["object_class"].strip()
    ):
        problems.append("config.object_class must be a non-empty string.")
    if _SCHEDULE_KEY in config:
        _validate_schedule(config[_SCHEDULE_KEY], problems)
    return problems


class RuleCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: UUID
    camera_id: UUID
    name: str = Field(min_length=1, max_length=120)
    rule_type: RuleType
    enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)
    severity: RuleSeverity = "warning"
    cooldown_seconds: int = Field(default=300, ge=0, le=86400)


class RulePatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=120)
    enabled: bool | None = None
    config: dict[str, Any] | None = None
    severity: RuleSeverity | None = None
    cooldown_seconds: int | None = Field(default=None, ge=0, le=86400)


class RuleSummary(BaseModel):
    id: UUID
    camera_id: UUID
    name: str
    # Plain str (not RuleType) so rows written by a newer deploy still read
    # back — same posture as the observations read path.
    rule_type: str
    enabled: bool
    config: dict[str, Any]
    severity: str
    cooldown_seconds: int
    created_by: UUID | None
    created_at: datetime
    updated_at: datetime


class RuleListResponse(BaseModel):
    rules: list[RuleSummary]
