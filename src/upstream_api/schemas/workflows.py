"""Workflow Engine M5 — workflow definitions (multi-step sequencing).

A workflow is the top-level product object; M5 ships one type:
``sequence`` — event A, then event B within N seconds, with optional
same-track correlation. ``validate_sequence_config`` enforces the same
honesty posture as rule validation: a condition (or correlation) is
allowed only on event types whose production payloads can prove it, so a
workflow that can never complete is rejected at authoring, not discovered
in the field.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from upstream_api.schemas.rules import RuleSeverity, _validate_schedule

WorkflowType = Literal["sequence"]

# Discrete EVENT types a sequence step may use. Snapshot types
# (roi_occupancy) and mock types are excluded — a step is a moment, not a
# state report.
SEQUENCE_EVENT_TYPES: tuple[str, ...] = (
    "processor.zone_entered",
    "processor.zone_exited",
    "processor.line_crossing",
    "processor.line_crossing_object",
    "processor.dwell_started",
    "processor.dwell_exceeded",
)

# Event types whose payloads always (zone/dwell) or by-definition
# (line_crossing_object) carry track_id — the only ones same_track_id
# correlation or per-object conditions may use. Aggregate line_crossing
# carries no per-object attribution.
TRACK_ATTRIBUTING_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "processor.zone_entered",
        "processor.zone_exited",
        "processor.line_crossing_object",
        "processor.dwell_started",
        "processor.dwell_exceeded",
    }
)

# Per-event-type allowed condition keys — mirrors the M3 rule posture.
STEP_CONDITION_KEYS: dict[str, frozenset[str]] = {
    "processor.zone_entered": frozenset(
        {"zone_id", "object_classes", "min_confidence", "require_track_id"}
    ),
    "processor.zone_exited": frozenset(
        {"zone_id", "object_classes", "min_confidence", "require_track_id"}
    ),
    "processor.line_crossing": frozenset({"line_id"}),
    "processor.line_crossing_object": frozenset(
        {"line_id", "object_classes", "min_confidence", "require_track_id"}
    ),
    "processor.dwell_started": frozenset(
        {"zone_id", "object_classes", "min_confidence", "require_track_id"}
    ),
    "processor.dwell_exceeded": frozenset(
        {
            "zone_id",
            "object_classes",
            "min_confidence",
            "require_track_id",
            "min_duration_seconds",
        }
    ),
}

# The payload field that scopes a step to one geometry shape, per type.
STEP_TARGET_KEY: dict[str, str] = {
    "processor.zone_entered": "zone_id",
    "processor.zone_exited": "zone_id",
    "processor.line_crossing": "line_id",
    "processor.line_crossing_object": "line_id",
    "processor.dwell_started": "zone_id",
    "processor.dwell_exceeded": "zone_id",
}

MIN_WITHIN_SECONDS = 5
MAX_WITHIN_SECONDS = 86_400

_ALLOWED_TOP_KEYS = frozenset(
    {"start", "then", "correlation", "on_success", "on_timeout", "schedule"}
)
_ALLOWED_OUTCOME_KEYS = frozenset({"create_alert", "alert_message"})
_MAX_OBJECT_CLASSES = 20


def _validate_step(
    step: object, *, key: str, requires_within: bool, problems: list[str]
) -> str | None:
    """Validate one step object; returns its event_type when usable."""
    if not isinstance(step, dict):
        problems.append(f"config.{key} must be an object.")
        return None
    allowed = {"event_type", "conditions"} | ({"within_seconds"} if requires_within else set())
    unknown = sorted(set(step) - allowed)
    if unknown:
        problems.append(f"Unknown {key} keys: {', '.join(unknown)}.")
    event_type = step.get("event_type")
    if event_type not in SEQUENCE_EVENT_TYPES:
        problems.append(f"config.{key}.event_type must be one of {list(SEQUENCE_EVENT_TYPES)}.")
        event_type = None
    if requires_within:
        within = step.get("within_seconds")
        if (
            not isinstance(within, int)
            or isinstance(within, bool)
            or not (MIN_WITHIN_SECONDS <= within <= MAX_WITHIN_SECONDS)
        ):
            problems.append(
                "config.then.within_seconds must be an integer between "
                f"{MIN_WITHIN_SECONDS} and {MAX_WITHIN_SECONDS}."
            )
    conditions = step.get("conditions")
    if conditions is not None:
        _validate_step_conditions(conditions, key=key, event_type=event_type, problems=problems)
    return event_type if isinstance(event_type, str) else None


def _validate_step_conditions(
    conditions: object, *, key: str, event_type: str | None, problems: list[str]
) -> None:
    if not isinstance(conditions, dict):
        problems.append(f"config.{key}.conditions must be an object.")
        return
    if event_type is None:
        return  # event_type problem already recorded; can't check keys.
    allowed = STEP_CONDITION_KEYS[event_type]
    unknown = sorted(set(conditions) - allowed)
    if unknown:
        problems.append(
            f"config.{key}.conditions has keys {', '.join(unknown)} that "
            f"{event_type} payloads cannot prove."
        )
    for id_key in ("zone_id", "line_id"):
        if id_key in conditions and not (
            isinstance(conditions[id_key], str) and conditions[id_key].strip()
        ):
            problems.append(f"config.{key}.conditions.{id_key} must be a non-empty string.")
    if "object_classes" in conditions:
        classes = conditions["object_classes"]
        valid = (
            isinstance(classes, list)
            and 0 < len(classes) <= _MAX_OBJECT_CLASSES
            and len(classes) == len({c for c in classes if isinstance(c, str)})
            and all(isinstance(c, str) and c.strip() for c in classes)
        )
        if not valid:
            problems.append(
                f"config.{key}.conditions.object_classes must be 1-"
                f"{_MAX_OBJECT_CLASSES} unique non-empty strings."
            )
    if "min_confidence" in conditions:
        value = conditions["min_confidence"]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not (0 < value <= 1):
            problems.append(f"config.{key}.conditions.min_confidence must be a number in (0, 1].")
    if "require_track_id" in conditions and not isinstance(conditions["require_track_id"], bool):
        problems.append(f"config.{key}.conditions.require_track_id must be a boolean.")
    if "min_duration_seconds" in conditions:
        value = conditions["min_duration_seconds"]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            problems.append(f"config.{key}.conditions.min_duration_seconds must be a number > 0.")


def _validate_outcome(outcome: object, *, key: str, problems: list[str]) -> None:
    if not isinstance(outcome, dict):
        problems.append(f"config.{key} must be an object.")
        return
    unknown = sorted(set(outcome) - _ALLOWED_OUTCOME_KEYS)
    if unknown:
        problems.append(f"Unknown {key} keys: {', '.join(unknown)}.")
    if "create_alert" in outcome and not isinstance(outcome["create_alert"], bool):
        problems.append(f"config.{key}.create_alert must be a boolean.")
    if "alert_message" in outcome:
        message = outcome["alert_message"]
        if not (isinstance(message, str) and 0 < len(message.strip()) <= 200):
            problems.append(
                f"config.{key}.alert_message must be a non-empty string (max 200 chars)."
            )


def validate_sequence_config(config: dict[str, Any]) -> list[str]:
    """Strict validation of a sequence workflow config. Empty = valid."""
    problems: list[str] = []
    unknown = sorted(set(config) - _ALLOWED_TOP_KEYS)
    if unknown:
        problems.append(f"Unknown config keys for sequence: {', '.join(unknown)}.")
    if "start" not in config:
        problems.append("config.start is required.")
    if "then" not in config:
        problems.append("config.then is required.")
    start_type = (
        _validate_step(config["start"], key="start", requires_within=False, problems=problems)
        if "start" in config
        else None
    )
    then_type = (
        _validate_step(config["then"], key="then", requires_within=True, problems=problems)
        if "then" in config
        else None
    )
    if "correlation" in config:
        correlation = config["correlation"]
        if not isinstance(correlation, dict):
            problems.append("config.correlation must be an object.")
        else:
            unknown = sorted(set(correlation) - {"same_track_id"})
            if unknown:
                problems.append(f"Unknown correlation keys: {', '.join(unknown)}.")
            same_track = correlation.get("same_track_id")
            if same_track is not None and not isinstance(same_track, bool):
                problems.append("config.correlation.same_track_id must be a boolean.")
            if same_track is True:
                for label, event_type in (("start", start_type), ("then", then_type)):
                    if event_type is not None and event_type not in TRACK_ATTRIBUTING_EVENT_TYPES:
                        problems.append(
                            f"correlation.same_track_id requires a track-attributing "
                            f"{label} event type; {event_type} carries no track_id."
                        )
    if "on_success" in config:
        _validate_outcome(config["on_success"], key="on_success", problems=problems)
    if "on_timeout" in config:
        _validate_outcome(config["on_timeout"], key="on_timeout", problems=problems)
    if "schedule" in config:
        _validate_schedule(config["schedule"], problems)
    return problems


class WorkflowCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: UUID
    camera_id: UUID
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    workflow_type: WorkflowType = "sequence"
    enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)
    severity: RuleSeverity = "warning"
    cooldown_seconds: int = Field(default=300, ge=0, le=86400)


class WorkflowPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    enabled: bool | None = None
    config: dict[str, Any] | None = None
    severity: RuleSeverity | None = None
    cooldown_seconds: int | None = Field(default=None, ge=0, le=86400)


class WorkflowSummary(BaseModel):
    id: UUID
    camera_id: UUID
    name: str
    description: str | None
    # Plain str — the DB column is open vocabulary.
    workflow_type: str
    enabled: bool
    config: dict[str, Any]
    severity: str
    cooldown_seconds: int
    created_by: UUID | None
    created_at: datetime
    updated_at: datetime


class WorkflowListResponse(BaseModel):
    workflows: list[WorkflowSummary]
