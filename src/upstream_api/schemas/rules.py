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

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

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
_ALLOWED_CONFIG_KEYS: dict[str, frozenset[str]] = {
    "line_crossing": frozenset({"line_id"}),
    "roi_occupancy": frozenset({"roi_id", "min_total_count"}),
    "zone_entered": frozenset({"zone_id"}),
    "dwell_exceeded": frozenset({"zone_id", "min_duration_seconds"}),
}


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
    if "min_total_count" in config:
        value = config["min_total_count"]
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            problems.append("config.min_total_count must be an integer >= 1.")
    if "min_duration_seconds" in config:
        value = config["min_duration_seconds"]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            problems.append("config.min_duration_seconds must be a number > 0.")
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
