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
    # --- M4 time-based rules: evaluated by the periodic workflow
    # evaluator (services/workflow_evaluator.py), NOT the observation
    # ingest path. No observation type maps to them.
    # Fires when a camera has no active streaming session AND no heartbeat
    # within offline_for_seconds.
    "camera_offline",
    # Fires when a STREAMING camera has produced no matching observation
    # for quiet_for_seconds (anchored at max(last matching observation,
    # session start) so a freshly-started session never instant-fires).
    "no_detections_for",
]

RULE_TYPES: tuple[RuleType, ...] = (
    "line_crossing",
    "roi_occupancy",
    "zone_entered",
    "dwell_exceeded",
    "camera_offline",
    "no_detections_for",
)

# M4: rule types evaluated on a timer rather than per-observation. Their
# cooldown_seconds doubles as the RE-FIRE interval while the condition
# persists, so it must be >= the evaluator tick (60s) — otherwise every
# tick would fire (validate_rule_cooldown enforces this).
TIMER_RULE_TYPES: frozenset[str] = frozenset({"camera_offline", "no_detections_for"})
MIN_TIMER_COOLDOWN_SECONDS = 60

# Duration bounds shared by offline_for_seconds / quiet_for_seconds:
# 1 minute .. 7 days.
MIN_TIMER_DURATION_SECONDS = 60
MAX_TIMER_DURATION_SECONDS = 604_800

# Observation types a no_detections_for rule may watch (the production
# processor types), and the subset that carries class_label — an
# object_classes filter is only honest over class-attributing types.
NO_DETECTIONS_OBSERVATION_TYPES: tuple[str, ...] = (
    "processor.line_crossing",
    "processor.line_crossing_object",
    "processor.roi_occupancy",
    "processor.zone_entered",
    "processor.zone_exited",
    "processor.dwell_started",
    "processor.dwell_updated",
    "processor.dwell_exceeded",
)
CLASS_ATTRIBUTING_OBSERVATION_TYPES: frozenset[str] = frozenset(
    {
        "processor.line_crossing_object",
        "processor.zone_entered",
        "processor.zone_exited",
        "processor.dwell_started",
        "processor.dwell_updated",
        "processor.dwell_exceeded",
    }
)

RuleSeverity = Literal["info", "warning", "critical"]

# Per-type allowed config keys. Unknown keys are rejected so typos surface at
# authoring time, not as silently-never-matching rules.
#
# M3 (richer conditions) deliberately allows a condition ONLY on rule types
# whose production observation payloads actually carry the field it needs —
# a rule that can never match is worse than a 422 at authoring time:
#
# - object_classes / require_track_id: zone_entered, dwell_exceeded, and
#   (per-object line crossing side task) line_crossing — the processor's
#   per-object producer attributes crossings to tracked objects. ROI
#   occupancy stays excluded (cross-class aggregate counts; use
#   object_class instead).
# - min_confidence: zone_entered + dwell_exceeded — the processor's
#   payload-enrichment side task stamps per-object detection confidence
#   onto zone/dwell events (omitted when the track wasn't detected in the
#   triggering frame, so the no-match-on-missing posture still applies).
#   The per-object line-crossing producer extends it to line_crossing.
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
    # Per-object line crossing side task: the processor now emits
    # processor.line_crossing_object with track/class/confidence
    # attribution, unlocking the three per-object conditions here. A rule
    # using ANY of them matches only per-object events; a plain rule keeps
    # matching only aggregate count events (see services/rules.py).
    "line_crossing": frozenset(
        {
            "line_id",
            "object_classes",
            "require_track_id",
            "min_confidence",
            _SCHEDULE_KEY,
        }
    ),
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
    # --- M4 time-based rules ------------------------------------------------
    "camera_offline": frozenset({"offline_for_seconds", _SCHEDULE_KEY}),
    "no_detections_for": frozenset(
        {"quiet_for_seconds", "observation_types", "object_classes", _SCHEDULE_KEY}
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
    for key in ("offline_for_seconds", "quiet_for_seconds"):
        if rule_type in TIMER_RULE_TYPES and key in allowed and key not in config:
            problems.append(f"config.{key} is required for {rule_type} rules.")
        if key in config:
            value = config[key]
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not (MIN_TIMER_DURATION_SECONDS <= value <= MAX_TIMER_DURATION_SECONDS)
            ):
                problems.append(
                    f"config.{key} must be an integer between "
                    f"{MIN_TIMER_DURATION_SECONDS} and {MAX_TIMER_DURATION_SECONDS} seconds."
                )
    if "observation_types" in config:
        types = config["observation_types"]
        if (
            not isinstance(types, list)
            or not types
            or len(types) != len({t for t in types if isinstance(t, str)})
            or any(t not in NO_DETECTIONS_OBSERVATION_TYPES for t in types)
        ):
            problems.append(
                "config.observation_types must be a non-empty list of unique "
                f"types from {list(NO_DETECTIONS_OBSERVATION_TYPES)}."
            )
    if rule_type == "no_detections_for" and "object_classes" in config:
        # A class filter is only honest over class-attributing observation
        # types; with observation_types omitted the evaluator defaults to
        # exactly that set, so the combination is always valid then.
        types = config.get("observation_types")
        if isinstance(types, list) and any(
            t not in CLASS_ATTRIBUTING_OBSERVATION_TYPES for t in types if isinstance(t, str)
        ):
            problems.append(
                "config.object_classes requires observation_types that carry "
                f"class_label: {sorted(CLASS_ATTRIBUTING_OBSERVATION_TYPES)}."
            )
    if _SCHEDULE_KEY in config:
        _validate_schedule(config[_SCHEDULE_KEY], problems)
    return problems


def validate_rule_cooldown(rule_type: str, cooldown_seconds: int) -> list[str]:
    """Timer rules re-fire per cooldown while their condition persists, so
    cooldown 0 ("every match") would fire on every evaluator tick."""
    if rule_type in TIMER_RULE_TYPES and cooldown_seconds < MIN_TIMER_COOLDOWN_SECONDS:
        return [
            f"cooldown_seconds must be >= {MIN_TIMER_COOLDOWN_SECONDS} for "
            f"{rule_type} rules (it is the re-fire interval while the "
            "condition persists)."
        ]
    return []


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
