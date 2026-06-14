"""Rules/Triggers M1 — rule matching + the observation-ingest evaluation hook.

Three layers, mirroring how the code is factored:

1. ``rule_matches`` / ``validate_rule_config`` — pure functions, table tests.
2. ``evaluate_observation`` — service-level, fake conn (cooldown, filtering).
3. ``POST /v1/internal/processor/observations`` — route-level: a matching
   rule creates an alert in the same request, and a failing rules layer can
   NEVER fail the observation ingest (the savepoint contract).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.schemas.rules import validate_rule_config
from upstream_api.services.rules import (
    build_alert_message,
    evaluate_observation,
    rule_matches,
    schedule_allows,
)

# A Wednesday 12:00 UTC — used wherever evaluation needs an observation time.
_EVAL_NOW = datetime(2026, 6, 10, 12, 0, 0, tzinfo=UTC)

# ---------------------------------------------------------------------------
# rule_matches — pure truth table.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rule_type", "config", "payload", "expected"),
    [
        # line_crossing: no filter matches any line.
        ("line_crossing", {}, {"line_id": "line-1"}, True),
        ("line_crossing", {"line_id": "line-1"}, {"line_id": "line-1"}, True),
        ("line_crossing", {"line_id": "line-1"}, {"line_id": "line-2"}, False),
        # A filtered rule must not fire on a payload that can't prove its line.
        ("line_crossing", {"line_id": "line-1"}, {}, False),
        # roi_occupancy: default min_total_count is 1.
        ("roi_occupancy", {}, {"roi_id": "roi-1", "total_count": 1}, True),
        ("roi_occupancy", {}, {"roi_id": "roi-1", "total_count": 0}, False),
        ("roi_occupancy", {"min_total_count": 3}, {"total_count": 2}, False),
        ("roi_occupancy", {"min_total_count": 3}, {"total_count": 3}, True),
        ("roi_occupancy", {"roi_id": "roi-1"}, {"roi_id": "roi-2", "total_count": 5}, False),
        ("roi_occupancy", {}, {"roi_id": "roi-1"}, False),  # no count → no match
        # zone_entered.
        ("zone_entered", {}, {"zone_id": "z-1"}, True),
        ("zone_entered", {"zone_id": "z-1"}, {"zone_id": "z-1"}, True),
        ("zone_entered", {"zone_id": "z-1"}, {"zone_id": "z-2"}, False),
        ("zone_entered", {"zone_id": "z-1"}, {}, False),
        # dwell_exceeded.
        ("dwell_exceeded", {}, {"zone_id": "z-1", "duration_seconds": 61.0}, True),
        (
            "dwell_exceeded",
            {"min_duration_seconds": 120},
            {"duration_seconds": 61.0},
            False,
        ),
        (
            "dwell_exceeded",
            {"min_duration_seconds": 120},
            {"duration_seconds": 121.5},
            True,
        ),
        ("dwell_exceeded", {"zone_id": "z-1"}, {"zone_id": "z-2"}, False),
        ("dwell_exceeded", {"min_duration_seconds": 120}, {}, False),
        # Unknown rule types never match.
        ("camera_offline", {}, {}, False),
        # --- M3: object class filter (zone_entered / dwell_exceeded) -------
        (
            "zone_entered",
            {"object_classes": ["person"]},
            {"zone_id": "z", "class_label": "person"},
            True,
        ),
        (
            "zone_entered",
            {"object_classes": ["person", "forklift"]},
            {"zone_id": "z", "class_label": "forklift"},
            True,
        ),
        (
            "zone_entered",
            {"object_classes": ["person"]},
            {"zone_id": "z", "class_label": "car"},
            False,
        ),
        # Payload that can't prove its class never matches a filtered rule.
        ("zone_entered", {"object_classes": ["person"]}, {"zone_id": "z"}, False),
        ("zone_entered", {"object_classes": ["person"]}, {"zone_id": "z", "class_label": 3}, False),
        # Filter omitted keeps M1 behavior.
        ("zone_entered", {}, {"zone_id": "z"}, True),
        (
            "dwell_exceeded",
            {"object_classes": ["person"]},
            {"zone_id": "z", "class_label": "person", "duration_seconds": 61.0},
            True,
        ),
        (
            "dwell_exceeded",
            {"object_classes": ["person"]},
            {"zone_id": "z", "class_label": "car", "duration_seconds": 61.0},
            False,
        ),
        (
            "dwell_exceeded",
            {"object_classes": ["person"]},
            {"zone_id": "z", "duration_seconds": 61.0},
            False,
        ),
        # Class filter composes with the duration threshold.
        (
            "dwell_exceeded",
            {"object_classes": ["person"], "min_duration_seconds": 120},
            {"zone_id": "z", "class_label": "person", "duration_seconds": 61.0},
            False,
        ),
        # --- M3: require_track_id (zone_entered / dwell_exceeded) ----------
        ("zone_entered", {"require_track_id": True}, {"zone_id": "z", "track_id": "t-7"}, True),
        ("zone_entered", {"require_track_id": True}, {"zone_id": "z"}, False),
        ("zone_entered", {"require_track_id": True}, {"zone_id": "z", "track_id": ""}, False),
        ("zone_entered", {"require_track_id": False}, {"zone_id": "z"}, True),
        (
            "dwell_exceeded",
            {"require_track_id": True},
            {"zone_id": "z", "track_id": "t-7", "duration_seconds": 5},
            True,
        ),
        (
            "dwell_exceeded",
            {"require_track_id": True},
            {"zone_id": "z", "duration_seconds": 5},
            False,
        ),
        # --- M3: max_total_count (roi_occupancy) ---------------------------
        ("roi_occupancy", {"max_total_count": 5}, {"total_count": 5}, True),
        ("roi_occupancy", {"max_total_count": 5}, {"total_count": 6}, False),
        ("roi_occupancy", {"min_total_count": 2, "max_total_count": 5}, {"total_count": 1}, False),
        ("roi_occupancy", {"min_total_count": 2, "max_total_count": 5}, {"total_count": 3}, True),
        ("roi_occupancy", {"min_total_count": 2, "max_total_count": 5}, {"total_count": 9}, False),
        # Bounds need a provable count.
        ("roi_occupancy", {"max_total_count": 5}, {}, False),
        # --- payload-enrichment unlock: min_confidence (zone/dwell) --------
        (
            "zone_entered",
            {"min_confidence": 0.7},
            {"zone_id": "z", "confidence": 0.86},
            True,
        ),
        (
            "zone_entered",
            {"min_confidence": 0.7},
            {"zone_id": "z", "confidence": 0.7},
            True,  # threshold is inclusive
        ),
        (
            "zone_entered",
            {"min_confidence": 0.7},
            {"zone_id": "z", "confidence": 0.55},
            False,
        ),
        # Missing confidence never matches a confidence-filtered rule.
        ("zone_entered", {"min_confidence": 0.7}, {"zone_id": "z"}, False),
        ("zone_entered", {"min_confidence": 0.7}, {"zone_id": "z", "confidence": "hi"}, False),
        # Threshold omitted keeps prior behavior.
        ("zone_entered", {}, {"zone_id": "z", "confidence": 0.1}, True),
        (
            "dwell_exceeded",
            {"min_confidence": 0.7, "min_duration_seconds": 30},
            {"zone_id": "z", "confidence": 0.9, "duration_seconds": 31},
            True,
        ),
        (
            "dwell_exceeded",
            {"min_confidence": 0.7},
            {"zone_id": "z", "duration_seconds": 31},
            False,
        ),
        # Confidence composes with class + track conditions.
        (
            "zone_entered",
            {"min_confidence": 0.7, "object_classes": ["person"], "require_track_id": True},
            {"zone_id": "z", "confidence": 0.8, "class_label": "person", "track_id": "t"},
            True,
        ),
        (
            "zone_entered",
            {"min_confidence": 0.7, "object_classes": ["person"], "require_track_id": True},
            {"zone_id": "z", "confidence": 0.8, "class_label": "car", "track_id": "t"},
            False,
        ),
        # --- payload-enrichment unlock: object_class counts (roi) ----------
        (
            "roi_occupancy",
            {"object_class": "person", "min_total_count": 2},
            {"total_count": 5, "class_counts": {"person": 2, "car": 3}},
            True,
        ),
        (
            "roi_occupancy",
            {"object_class": "person", "min_total_count": 3},
            {"total_count": 5, "class_counts": {"person": 2, "car": 3}},
            False,
        ),
        (
            "roi_occupancy",
            {"object_class": "person", "min_total_count": 1, "max_total_count": 2},
            {"total_count": 9, "class_counts": {"person": 2, "car": 7}},
            True,  # bounds apply to the CLASS count, not the total
        ),
        # Class absent from the breakdown counts as zero → below the min floor.
        (
            "roi_occupancy",
            {"object_class": "forklift"},
            {"total_count": 5, "class_counts": {"person": 5}},
            False,
        ),
        # Pre-enrichment events (empty breakdown) can never match a
        # class-scoped count rule.
        (
            "roi_occupancy",
            {"object_class": "person"},
            {"total_count": 5, "class_counts": {}},
            False,
        ),
        # No class_counts dict at all → can't prove anything.
        ("roi_occupancy", {"object_class": "person"}, {"total_count": 5}, False),
        (
            "roi_occupancy",
            {"object_class": "person"},
            {"total_count": 5, "class_counts": "x"},
            False,
        ),
        # --- per-object line crossing: matcher partition ---------------------
        # Plain rules match ONLY aggregate events (no event_mode)...
        ("line_crossing", {}, {"line_id": "line-1"}, True),
        ("line_crossing", {}, {"line_id": "line-1", "event_mode": "object_crossing"}, False),
        # ...and per-object-conditioned rules match ONLY per-object events —
        # one physical crossing produces both, so this prevents double-fire.
        (
            "line_crossing",
            {"object_classes": ["person"]},
            {"line_id": "line-1", "count_delta": 1},
            False,
        ),
        (
            "line_crossing",
            {"object_classes": ["person"]},
            {
                "line_id": "line-1",
                "event_mode": "object_crossing",
                "class_label": "person",
                "track_id": "7",
            },
            True,
        ),
        (
            "line_crossing",
            {"object_classes": ["person"]},
            {
                "line_id": "line-1",
                "event_mode": "object_crossing",
                "class_label": "car",
                "track_id": "7",
            },
            False,
        ),
        # Missing class never matches a class-filtered line rule.
        (
            "line_crossing",
            {"object_classes": ["person"]},
            {"line_id": "line-1", "event_mode": "object_crossing", "track_id": "7"},
            False,
        ),
        (
            "line_crossing",
            {"require_track_id": True},
            {"line_id": "line-1", "event_mode": "object_crossing", "track_id": "7"},
            True,
        ),
        (
            "line_crossing",
            {"require_track_id": True},
            {"line_id": "line-1", "event_mode": "object_crossing"},
            False,
        ),
        (
            "line_crossing",
            {"min_confidence": 0.7},
            {"line_id": "line-1", "event_mode": "object_crossing", "confidence": 0.84},
            True,
        ),
        (
            "line_crossing",
            {"min_confidence": 0.7},
            {"line_id": "line-1", "event_mode": "object_crossing", "confidence": 0.5},
            False,
        ),
        # Missing confidence never matches when configured.
        (
            "line_crossing",
            {"min_confidence": 0.7},
            {"line_id": "line-1", "event_mode": "object_crossing"},
            False,
        ),
        # line_id filter applies to per-object events too.
        (
            "line_crossing",
            {"line_id": "line-1", "min_confidence": 0.5},
            {"line_id": "line-2", "event_mode": "object_crossing", "confidence": 0.9},
            False,
        ),
    ],
)
def test_rule_matches(
    rule_type: str,
    config: dict[str, Any],
    payload: dict[str, Any],
    expected: bool,
) -> None:
    assert rule_matches(rule_type, config, payload) is expected


# ---------------------------------------------------------------------------
# validate_rule_config — pure.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rule_type", "config"),
    [
        ("line_crossing", {}),
        ("line_crossing", {"line_id": "line-1"}),
        ("roi_occupancy", {"roi_id": "roi-1", "min_total_count": 3}),
        ("zone_entered", {"zone_id": "z-1"}),
        ("dwell_exceeded", {"zone_id": "z-1", "min_duration_seconds": 90}),
        ("dwell_exceeded", {"min_duration_seconds": 90.5}),
        # --- M3 ------------------------------------------------------------
        ("zone_entered", {"object_classes": ["person", "forklift"], "require_track_id": True}),
        ("dwell_exceeded", {"object_classes": ["person"], "min_duration_seconds": 30}),
        ("roi_occupancy", {"min_total_count": 2, "max_total_count": 10}),
        ("roi_occupancy", {"max_total_count": 1}),
        (
            "line_crossing",
            {
                "line_id": "line-1",
                "schedule": {
                    "days": ["mon", "tue", "wed", "thu", "fri"],
                    "start_time": "08:00",
                    "end_time": "18:00",
                    "timezone": "America/New_York",
                },
            },
        ),
        # Overnight window + default days are both valid.
        (
            "zone_entered",
            {"schedule": {"start_time": "22:00", "end_time": "06:00", "timezone": "UTC"}},
        ),
        # --- payload-enrichment unlock --------------------------------------
        ("zone_entered", {"min_confidence": 0.7}),
        ("zone_entered", {"min_confidence": 1}),
        ("dwell_exceeded", {"min_confidence": 0.5, "object_classes": ["person"]}),
        ("roi_occupancy", {"object_class": "person", "min_total_count": 3}),
        ("roi_occupancy", {"object_class": "person", "max_total_count": 2}),
        # --- per-object line crossing unlock ---------------------------------
        ("line_crossing", {"object_classes": ["person"]}),
        ("line_crossing", {"require_track_id": True}),
        ("line_crossing", {"min_confidence": 0.7}),
        (
            "line_crossing",
            {"line_id": "line-1", "object_classes": ["person"], "min_confidence": 0.7},
        ),
        # --- M4 time-based rules ---------------------------------------------
        ("camera_offline", {"offline_for_seconds": 300}),
        ("camera_offline", {"offline_for_seconds": 604800}),
        (
            "camera_offline",
            {
                "offline_for_seconds": 300,
                "schedule": {
                    "days": ["mon", "tue", "wed", "thu", "fri"],
                    "start_time": "08:00",
                    "end_time": "18:00",
                    "timezone": "America/Denver",
                },
            },
        ),
        ("no_detections_for", {"quiet_for_seconds": 1800}),
        (
            "no_detections_for",
            {"quiet_for_seconds": 1800, "object_classes": ["person"]},
        ),
        (
            "no_detections_for",
            {
                "quiet_for_seconds": 1800,
                "observation_types": ["processor.zone_entered", "processor.line_crossing_object"],
                "object_classes": ["person"],
            },
        ),
        (
            "no_detections_for",
            {"quiet_for_seconds": 1800, "observation_types": ["processor.roi_occupancy"]},
        ),
    ],
)
def test_valid_configs_pass(rule_type: str, config: dict[str, Any]) -> None:
    assert validate_rule_config(rule_type, config) == []


@pytest.mark.parametrize(
    ("rule_type", "config"),
    [
        ("line_crossing", {"roi_id": "roi-1"}),  # wrong key for type
        ("line_crossing", {"line_id": ""}),
        ("line_crossing", {"line_id": 7}),
        ("roi_occupancy", {"min_total_count": 0}),
        ("roi_occupancy", {"min_total_count": True}),  # bool is not a count
        ("roi_occupancy", {"min_total_count": "3"}),
        ("dwell_exceeded", {"min_duration_seconds": 0}),
        ("dwell_exceeded", {"min_duration_seconds": -5}),
        ("nonsense_type", {}),
        # --- M3: conditions are rejected on types whose payloads can't
        # prove them (processor follow-up, not faked) ------------------------
        ("line_crossing", {"direction": "a_to_b"}),  # still deferred — no true a_to_b
        ("roi_occupancy", {"object_classes": ["person"]}),  # use object_class (single)
        ("roi_occupancy", {"min_confidence": 0.7}),  # counts have no confidence
        # --- payload-enrichment unlock: value validation ---------------------
        ("zone_entered", {"min_confidence": 0}),
        ("zone_entered", {"min_confidence": 1.5}),
        ("zone_entered", {"min_confidence": True}),
        ("zone_entered", {"min_confidence": "0.7"}),
        ("roi_occupancy", {"object_class": ""}),
        ("roi_occupancy", {"object_class": 7}),
        ("zone_entered", {"object_class": "person"}),  # roi-only key
        # --- M4 time-based rules ---------------------------------------------
        ("camera_offline", {}),  # duration required
        ("camera_offline", {"offline_for_seconds": 30}),  # below 60s floor
        ("camera_offline", {"offline_for_seconds": 604801}),  # above 7d cap
        ("camera_offline", {"offline_for_seconds": "300"}),
        ("camera_offline", {"offline_for_seconds": True}),
        ("camera_offline", {"offline_for_seconds": 300, "quiet_for_seconds": 60}),
        ("camera_offline", {"offline_for_seconds": 300, "object_classes": ["person"]}),
        ("no_detections_for", {}),  # duration required
        ("no_detections_for", {"quiet_for_seconds": 0}),
        ("no_detections_for", {"quiet_for_seconds": 1800, "observation_types": []}),
        (
            "no_detections_for",
            {"quiet_for_seconds": 1800, "observation_types": ["processor.mock_detection"]},
        ),
        (
            "no_detections_for",
            {
                "quiet_for_seconds": 1800,
                "observation_types": ["processor.zone_entered", "processor.zone_entered"],
            },
        ),
        # Class filter over a type that can't prove class.
        (
            "no_detections_for",
            {
                "quiet_for_seconds": 1800,
                "observation_types": ["processor.roi_occupancy"],
                "object_classes": ["person"],
            },
        ),
        # --- M3: value validation -------------------------------------------
        ("zone_entered", {"object_classes": []}),
        ("zone_entered", {"object_classes": ["person", "person"]}),
        ("zone_entered", {"object_classes": ["person", ""]}),
        ("zone_entered", {"object_classes": "person"}),
        ("zone_entered", {"object_classes": [f"c{i}" for i in range(21)]}),
        ("zone_entered", {"require_track_id": "yes"}),
        ("roi_occupancy", {"max_total_count": 0}),
        ("roi_occupancy", {"min_total_count": 5, "max_total_count": 2}),
        # --- M3: schedule validation -----------------------------------------
        ("line_crossing", {"schedule": "always"}),
        ("line_crossing", {"schedule": {"start_time": "08:00", "end_time": "18:00"}}),  # no tz
        (
            "line_crossing",
            {"schedule": {"start_time": "8am", "end_time": "18:00", "timezone": "UTC"}},
        ),
        (
            "line_crossing",
            {"schedule": {"start_time": "08:00", "end_time": "24:00", "timezone": "UTC"}},
        ),
        (
            "line_crossing",
            {"schedule": {"start_time": "08:00", "end_time": "08:00", "timezone": "UTC"}},
        ),
        (
            "line_crossing",
            {
                "schedule": {
                    "start_time": "08:00",
                    "end_time": "18:00",
                    "timezone": "Mars/Olympus",
                }
            },
        ),
        (
            "line_crossing",
            {
                "schedule": {
                    "days": ["monday"],
                    "start_time": "08:00",
                    "end_time": "18:00",
                    "timezone": "UTC",
                }
            },
        ),
        (
            "line_crossing",
            {
                "schedule": {
                    "days": [],
                    "start_time": "08:00",
                    "end_time": "18:00",
                    "timezone": "UTC",
                }
            },
        ),
        (
            "line_crossing",
            {
                "schedule": {
                    "start_time": "08:00",
                    "end_time": "18:00",
                    "timezone": "UTC",
                    "label": "work hours",
                }
            },
        ),
    ],
)
def test_invalid_configs_fail(rule_type: str, config: dict[str, Any]) -> None:
    assert validate_rule_config(rule_type, config) != []


def test_timer_rule_types_never_ride_the_ingest_path() -> None:
    """M4 timer rules are evaluated ONLY by the periodic evaluator: no
    observation type maps to them, and the event matcher never matches
    them — the smoke-tested ingest path is untouched."""
    from upstream_api.services.rules import OBSERVATION_TYPE_TO_RULE_TYPE

    assert "camera_offline" not in OBSERVATION_TYPE_TO_RULE_TYPE.values()
    assert "no_detections_for" not in OBSERVATION_TYPE_TO_RULE_TYPE.values()
    assert rule_matches("camera_offline", {"offline_for_seconds": 300}, {}) is False
    assert rule_matches("no_detections_for", {"quiet_for_seconds": 60}, {}) is False


def test_build_alert_message_uses_labels_over_ids() -> None:
    msg = build_alert_message("line_crossing", {"line_id": "line-1", "line_label": "Front door"})
    assert "Front door" in msg


# ---------------------------------------------------------------------------
# schedule_allows — pure time-window gate (M3).
# ---------------------------------------------------------------------------

_BUSINESS_HOURS = {
    "schedule": {
        "days": ["mon", "tue", "wed", "thu", "fri"],
        "start_time": "08:00",
        "end_time": "18:00",
        "timezone": "UTC",
    }
}


def _utc(day: int, hour: int, minute: int = 0) -> datetime:
    """June 2026: the 8th is a Monday, 13th/14th the weekend."""
    return datetime(2026, 6, day, hour, minute, tzinfo=UTC)


def test_no_schedule_always_allows() -> None:
    assert schedule_allows({}, _utc(8, 3)) is True


def test_inside_window_allows() -> None:
    assert schedule_allows(_BUSINESS_HOURS, _utc(8, 9, 30)) is True


def test_window_bounds_are_start_inclusive_end_exclusive() -> None:
    assert schedule_allows(_BUSINESS_HOURS, _utc(8, 8, 0)) is True
    assert schedule_allows(_BUSINESS_HOURS, _utc(8, 17, 59)) is True
    assert schedule_allows(_BUSINESS_HOURS, _utc(8, 18, 0)) is False


def test_outside_hours_blocks() -> None:
    assert schedule_allows(_BUSINESS_HOURS, _utc(8, 6)) is False


def test_wrong_day_blocks() -> None:
    assert schedule_allows(_BUSINESS_HOURS, _utc(13, 9)) is False  # Saturday


def test_days_default_to_every_day() -> None:
    cfg = {"schedule": {"start_time": "08:00", "end_time": "18:00", "timezone": "UTC"}}
    assert schedule_allows(cfg, _utc(13, 9)) is True  # Saturday


def test_timezone_conversion() -> None:
    cfg = {
        "schedule": {
            "start_time": "08:00",
            "end_time": "18:00",
            "timezone": "America/New_York",  # UTC-4 in June (EDT)
        }
    }
    # 11:00 UTC = 07:00 New York → outside; 13:00 UTC = 09:00 → inside.
    assert schedule_allows(cfg, _utc(8, 11)) is False
    assert schedule_allows(cfg, _utc(8, 13)) is True


def test_overnight_window_wraps_midnight() -> None:
    cfg = {
        "schedule": {
            "days": ["fri"],
            "start_time": "22:00",
            "end_time": "06:00",
            "timezone": "UTC",
        }
    }
    assert schedule_allows(cfg, _utc(12, 23)) is True  # Friday 23:00
    # Saturday 05:00 belongs to Friday's overnight window.
    assert schedule_allows(cfg, _utc(13, 5)) is True
    assert schedule_allows(cfg, _utc(13, 7)) is False  # past the tail
    assert schedule_allows(cfg, _utc(13, 23)) is False  # Saturday 23:00 not in days


def test_naive_observed_at_treated_as_utc() -> None:
    naive = datetime(2026, 6, 8, 9, 0, 0)  # Monday 09:00, no tzinfo
    assert schedule_allows(_BUSINESS_HOURS, naive) is True


@pytest.mark.parametrize(
    "schedule",
    [
        "always",  # not a dict
        {"start_time": "08:00", "end_time": "18:00"},  # missing timezone
        {"start_time": "08:00", "end_time": "18:00", "timezone": "Mars/Olympus"},
        {"start_time": "8am", "end_time": "18:00", "timezone": "UTC"},
        {"start_time": "08:00", "end_time": "08:00", "timezone": "UTC"},
        {"start_time": "08:00", "end_time": "18:00", "timezone": "UTC", "days": "mon"},
    ],
)
def test_malformed_stored_schedule_degrades_to_no_match(schedule: object) -> None:
    """The gate runs inside the ingest savepoint — a malformed stored
    schedule must degrade to "no match", never raise."""
    assert schedule_allows({"schedule": schedule}, _utc(8, 9)) is False


# ---------------------------------------------------------------------------
# evaluate_observation — service level with a fake conn.
# ---------------------------------------------------------------------------


class FakeEvalConn:
    """Backs the enabled-rules fetch, the workflow-run lifecycle, and the
    cooldown-guarded alert insert (Workflow Engine M2 flow).

    ``recent_alert_rule_ids`` simulates rules whose latest alert is inside
    the cooldown window: the guarded insert returns no id for them, exactly
    like the production NOT EXISTS.
    """

    def __init__(
        self,
        *,
        rules: list[dict[str, Any]] | None = None,
        recent_alert_rule_ids: set[UUID] | None = None,
    ) -> None:
        self.rules = rules or []
        self.recent_alert_rule_ids = recent_alert_rule_ids or set()
        self.alert_inserts: list[tuple[Any, ...]] = []
        self.run_inserts: list[tuple[Any, ...]] = []
        # (workflow_run_id, status, action_result) per finalize call.
        self.run_finalizes: list[tuple[Any, ...]] = []
        self.run_ids: list[UUID] = []
        # M6.1: raw args of each canonical-event insert; ON CONFLICT modeled.
        self.event_inserts: list[tuple[Any, ...]] = []
        self._event_keys: set[tuple[Any, str]] = set()

    def transaction(self) -> FakeTransaction:
        # M6.1 event emission wraps its insert in a savepoint.
        return FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from rules" in query
        account_id, camera_id, rule_type = args
        return [
            r
            for r in self.rules
            if r["account_id"] == account_id
            and r["camera_id"] == camera_id
            and r["rule_type"] == rule_type
            and r["enabled"]
        ]

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "insert into events" in query
        account_id, dedupe_key = args[0], args[12]
        if (account_id, dedupe_key) in self._event_keys:
            return None  # ON CONFLICT (account_id, dedupe_key) DO NOTHING
        self._event_keys.add((account_id, dedupe_key))
        self.event_inserts.append(args)
        return {"id": uuid4(), "seq": len(self.event_inserts)}

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "insert into workflow_runs" in query:
            self.run_inserts.append(args)
            run_id = uuid4()
            self.run_ids.append(run_id)
            return run_id
        assert "insert into alerts" in query
        rule_id = args[2]
        if rule_id in self.recent_alert_rule_ids:
            return None  # cooldown guard suppressed the insert
        self.alert_inserts.append(args)
        return uuid4()

    async def execute(self, query: str, *args: Any) -> str:
        assert "update workflow_runs" in query
        self.run_finalizes.append(args)
        return "UPDATE 1"


def _eval_rule(
    *,
    account_id: UUID,
    camera_id: UUID,
    rule_type: str = "line_crossing",
    config: dict[str, Any] | None = None,
    enabled: bool = True,
    severity: str = "warning",
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "camera_id": camera_id,
        "name": "Rule",
        "rule_type": rule_type,
        "config": config or {},
        "severity": severity,
        "cooldown_seconds": 300,
        "enabled": enabled,
    }


async def test_matching_rule_creates_alert() -> None:
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera, severity="critical")
    conn = FakeEvalConn(rules=[rule])
    observation_id = uuid4()
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=observation_id,
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1", "line_label": "Front door"},
        observed_at=_EVAL_NOW,
    )
    assert created == 1
    args = conn.alert_inserts[0]
    assert args[0] == account
    assert args[1] == camera
    assert args[2] == rule["id"]
    assert args[4] == conn.run_ids[0]  # alert linked to the workflow run
    assert args[5] == "critical"  # severity stamped from the rule
    assert args[6] == "Rule"  # rule name becomes the alert title
    assert "Front door" in args[7]
    assert json.loads(args[8]) == {"line_id": "line-1", "line_label": "Front door"}
    assert args[9] == 300.0


async def test_matching_rule_creates_workflow_run() -> None:
    """M2: a match is recorded as a workflow run — the system of record —
    with the rule snapshot in payload and the observation as evidence."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera, severity="critical")
    conn = FakeEvalConn(rules=[rule])
    observation_id = uuid4()
    await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=observation_id,
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1", "line_label": "Front door"},
        observed_at=_EVAL_NOW,
    )
    assert len(conn.run_inserts) == 1
    args = conn.run_inserts[0]
    assert args[0] == account
    assert args[1] == camera
    assert args[2] == rule["id"]
    assert args[3] == observation_id
    assert args[4] == "simple_rule"
    assert args[5] == "Rule"  # rule name becomes the run title
    assert "Front door" in args[6]  # summary
    assert args[7] == "processor.line_crossing"  # trigger_type
    assert json.loads(args[8]) == {"line_id": "line-1", "line_label": "Front door"}
    rule_snapshot = json.loads(args[9])["rule"]
    assert rule_snapshot["rule_type"] == "line_crossing"
    assert rule_snapshot["severity"] == "critical"
    assert rule_snapshot["cooldown_seconds"] == 300
    # Finalized completed with the alert as the action outcome.
    assert conn.run_finalizes == [(conn.run_ids[0], "completed", "alert_created")]


async def test_matching_rule_emits_one_alert_created_event() -> None:
    """M6.1: finalizing the run emits exactly one canonical event referencing
    the run, rule, camera, source observation, and the created alert."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera, severity="critical")
    conn = FakeEvalConn(rules=[rule])
    observation_id = uuid4()
    await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=observation_id,
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1", "line_label": "Front door"},
        observed_at=_EVAL_NOW,
    )
    assert len(conn.event_inserts) == 1
    args = conn.event_inserts[0]
    assert args[0] == account  # account_id
    assert args[1] == camera  # camera_id
    assert args[3] == "alert.created"  # event_type
    assert args[5] == "rule"  # source
    assert args[6] == observation_id  # source_observation_id
    assert args[7] == conn.run_ids[0]  # workflow_run_id
    assert args[8] is not None  # source_alert_id present
    assert args[10] == rule["id"]  # rule_id
    assert args[12] == f"workflow_run:{conn.run_ids[0]}:completed:alert_created"
    # Evidence = the triggering observation payload; payload = finalize context
    # + the rule snapshot.
    assert json.loads(args[15]) == {"line_id": "line-1", "line_label": "Front door"}
    payload = json.loads(args[14])
    assert payload["status"] == "completed"
    assert payload["action_result"] == "alert_created"
    assert payload["trigger_type"] == "processor.line_crossing"
    assert payload["rule"]["rule_type"] == "line_crossing"


async def test_cooldown_suppressed_match_emits_cooldown_event() -> None:
    """M6.1: a cooldown-suppressed match still finalizes a run, so it still
    emits one event — typed workflow.cooldown_suppressed, with no alert ref."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera)
    conn = FakeEvalConn(rules=[rule], recent_alert_rule_ids={rule["id"]})
    await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1"},
        observed_at=_EVAL_NOW,
    )
    assert len(conn.event_inserts) == 1
    args = conn.event_inserts[0]
    assert args[3] == "workflow.cooldown_suppressed"
    assert args[8] is None  # no alert -> no alert ref
    assert args[12].endswith(":suppressed:alert_suppressed_cooldown")


async def test_non_matching_rule_emits_no_event() -> None:
    """No match -> no run -> no event."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera, config={"line_id": "line-1"})
    conn = FakeEvalConn(rules=[rule])
    await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-2"},
        observed_at=_EVAL_NOW,
    )
    assert conn.event_inserts == []


async def test_unmapped_observation_type_is_a_noop() -> None:
    conn = FakeEvalConn(rules=[])
    created = await evaluate_observation(
        conn,
        account_id=uuid4(),
        camera_id=uuid4(),
        observation_id=uuid4(),
        observation_type="processor.mock_detection",
        payload={},
        observed_at=_EVAL_NOW,
    )
    assert created == 0


async def test_non_matching_config_creates_nothing() -> None:
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera, config={"line_id": "line-1"})
    conn = FakeEvalConn(rules=[rule])
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-2"},
        observed_at=_EVAL_NOW,
    )
    assert created == 0
    assert conn.alert_inserts == []
    # Non-matching rules produce no workflow run either — runs record
    # matches, not evaluations.
    assert conn.run_inserts == []


async def test_cooldown_suppresses_repeat_alert_but_still_records_run() -> None:
    """M2: cooldown suppression is itself a workflow outcome — the run is
    still created (suppressed / alert_suppressed_cooldown), no duplicate
    alert."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera)
    conn = FakeEvalConn(rules=[rule], recent_alert_rule_ids={rule["id"]})
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1"},
        observed_at=_EVAL_NOW,
    )
    assert created == 0
    assert conn.alert_inserts == []
    assert len(conn.run_inserts) == 1
    assert conn.run_finalizes == [(conn.run_ids[0], "suppressed", "alert_suppressed_cooldown")]


async def test_out_of_schedule_rule_creates_no_run_and_no_alert() -> None:
    """M3: a rule outside its time window is a NON-MATCH — no workflow run,
    no alert (Part 6 semantics)."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(
        account_id=account,
        camera_id=camera,
        config={
            "schedule": {
                "days": ["sat", "sun"],
                "start_time": "08:00",
                "end_time": "18:00",
                "timezone": "UTC",
            }
        },
    )
    conn = FakeEvalConn(rules=[rule])
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1"},
        observed_at=_EVAL_NOW,  # a Wednesday
    )
    assert created == 0
    assert conn.run_inserts == []
    assert conn.alert_inserts == []


async def test_in_schedule_richer_rule_creates_linked_run_and_alert() -> None:
    """M3 integration: richer conditions (class + track + schedule) that
    hold produce the full M2 chain — run with config snapshot, linked alert."""
    account, camera = uuid4(), uuid4()
    config = {
        "object_classes": ["person"],
        "require_track_id": True,
        "schedule": {
            "days": ["wed"],
            "start_time": "08:00",
            "end_time": "18:00",
            "timezone": "UTC",
        },
    }
    rule = _eval_rule(account_id=account, camera_id=camera, rule_type="zone_entered", config=config)
    conn = FakeEvalConn(rules=[rule])
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.zone_entered",
        payload={"zone_id": "z-1", "class_label": "person", "track_id": "t-9"},
        observed_at=_EVAL_NOW,  # Wednesday 12:00 UTC — inside the window
    )
    assert created == 1
    assert len(conn.run_inserts) == 1
    # The run's payload snapshots the FULL richer config.
    assert json.loads(conn.run_inserts[0][9])["rule"]["config"] == config
    assert conn.alert_inserts[0][4] == conn.run_ids[0]
    assert conn.run_finalizes == [(conn.run_ids[0], "completed", "alert_created")]


async def test_one_physical_crossing_never_double_fires_a_rule() -> None:
    """The partition contract: one physical crossing reaches the api-server
    as TWO observations (aggregate processor.line_crossing + per-object
    processor.line_crossing_object). A plain rule fires only on the
    aggregate; a per-object-conditioned rule fires only on the per-object
    event. Each rule produces exactly ONE workflow run + alert total."""
    account, camera = uuid4(), uuid4()
    plain_rule = _eval_rule(account_id=account, camera_id=camera, config={})
    conditioned_rule = _eval_rule(
        account_id=account,
        camera_id=camera,
        config={"object_classes": ["person"], "min_confidence": 0.7},
    )
    conn = FakeEvalConn(rules=[plain_rule, conditioned_rule])

    aggregate_payload = {
        "line_id": "line-1",
        "line_label": "line-1",
        "count_delta": 1,
        "cumulative_count": 4,
    }
    object_payload = {
        "line_id": "line-1",
        "line_label": "line-1",
        "event_mode": "object_crossing",
        "track_id": "7",
        "class_label": "person",
        "confidence": 0.84,
    }
    created_aggregate = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload=aggregate_payload,
        observed_at=_EVAL_NOW,
    )
    created_object = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing_object",
        payload=object_payload,
        observed_at=_EVAL_NOW,
    )
    # One alert per rule across BOTH observations — never two for one rule.
    assert created_aggregate == 1  # plain rule, aggregate event only
    assert created_object == 1  # conditioned rule, per-object event only
    assert len(conn.run_inserts) == 2
    assert len(conn.alert_inserts) == 2
    fired_rule_ids = {args[2] for args in conn.alert_inserts}
    assert fired_rule_ids == {plain_rule["id"], conditioned_rule["id"]}


async def test_conditioned_line_rule_ignores_aggregate_even_when_alone() -> None:
    """A per-object-conditioned rule must not fall back to aggregate
    events when no per-object event arrives (e.g. untracked crossing)."""
    account, camera = uuid4(), uuid4()
    rule = _eval_rule(account_id=account, camera_id=camera, config={"require_track_id": True})
    conn = FakeEvalConn(rules=[rule])
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1", "count_delta": 1},
        observed_at=_EVAL_NOW,
    )
    assert created == 0
    assert conn.run_inserts == []


async def test_two_matching_rules_create_two_alerts() -> None:
    account, camera = uuid4(), uuid4()
    rules = [
        _eval_rule(account_id=account, camera_id=camera),
        _eval_rule(account_id=account, camera_id=camera, config={"line_id": "line-1"}),
    ]
    conn = FakeEvalConn(rules=rules)
    created = await evaluate_observation(
        conn,
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1"},
        observed_at=_EVAL_NOW,
    )
    assert created == 2


# ---------------------------------------------------------------------------
# Route level: POST /v1/internal/processor/observations runs the evaluator
# in a savepoint and never lets it fail the ingest.
# ---------------------------------------------------------------------------


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeIngestConn:
    """Backs load_session_state + observation insert + rule evaluation."""

    def __init__(
        self,
        *,
        session_row: dict[str, Any] | None,
        rules: list[dict[str, Any]] | None = None,
        rules_fetch_raises: bool = False,
    ) -> None:
        self._session_row = session_row
        self.rules = rules or []
        self.rules_fetch_raises = rules_fetch_raises
        self.observation_inserts: list[tuple[Any, ...]] = []
        self.alert_inserts: list[tuple[Any, ...]] = []
        self.run_inserts: list[tuple[Any, ...]] = []
        self.run_finalizes: list[tuple[Any, ...]] = []
        # M6.1: raw args of each canonical-event insert; ON CONFLICT modeled.
        self.event_inserts: list[tuple[Any, ...]] = []
        self._event_keys: set[tuple[Any, str]] = set()

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions where id" in query:
            return self._session_row
        if "insert into events" in query:
            account_id, dedupe_key = args[0], args[12]
            if (account_id, dedupe_key) in self._event_keys:
                return None
            self._event_keys.add((account_id, dedupe_key))
            self.event_inserts.append(args)
            return {"id": uuid4(), "seq": len(self.event_inserts)}
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from rules" in query
        if self.rules_fetch_raises:
            raise RuntimeError("relation rules does not exist")
        _account_id, camera_id, rule_type = args
        return [
            r
            for r in self.rules
            if r["camera_id"] == camera_id and r["rule_type"] == rule_type and r["enabled"]
        ]

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "insert into observations" in query:
            self.observation_inserts.append(args)
            return uuid4()
        if "insert into workflow_runs" in query:
            self.run_inserts.append(args)
            return uuid4()
        if "insert into alerts" in query:
            self.alert_inserts.append(args)
            return uuid4()
        return None

    async def execute(self, query: str, *args: Any) -> str:
        assert "update workflow_runs" in query
        self.run_finalizes.append(args)
        return "UPDATE 1"


class AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    async def __aenter__(self) -> Any:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _client(conn: Any) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().processor_webhook_secret.get_secret_value()}


def _session_row(*, session_id: UUID, account_id: UUID, camera_id: UUID) -> dict[str, Any]:
    return {
        "id": session_id,
        "account_id": account_id,
        "agent_id": uuid4(),
        "camera_id": camera_id,
        "state": "streaming",
        "expires_at": datetime.now(tz=UTC) + timedelta(hours=1),
        "session_token_jti": uuid4(),
        "streamid": "publish:sess_0011223344556677",
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _observation_body(session_id: UUID) -> dict[str, Any]:
    return {
        "stream_session_id": str(session_id),
        "type": "processor.line_crossing",
        "payload": {"line_id": "line-1", "count_delta": 1, "cumulative_count": 4},
        "observed_at": datetime.now(tz=UTC).isoformat(),
        "processor_id": "proc-1",
        "package_id": "pkg.detect.v1",
    }


_OBS_PATH = "/v1/internal/processor/observations"


def test_ingest_with_matching_rule_creates_alert() -> None:
    session_id, account, camera = uuid4(), uuid4(), uuid4()
    rule = {
        "id": uuid4(),
        "account_id": account,
        "camera_id": camera,
        "name": "Front door",
        "rule_type": "line_crossing",
        "config": {},
        "severity": "warning",
        "cooldown_seconds": 300,
        "enabled": True,
    }
    conn = FakeIngestConn(
        session_row=_session_row(session_id=session_id, account_id=account, camera_id=camera),
        rules=[rule],
    )
    r = _client(conn).post(_OBS_PATH, json=_observation_body(session_id), headers=_headers())
    assert r.status_code == 200, r.text
    assert len(conn.observation_inserts) == 1
    assert len(conn.alert_inserts) == 1
    # M2: the match is also recorded as a workflow run, finalized completed.
    assert len(conn.run_inserts) == 1
    assert len(conn.run_finalizes) == 1
    assert conn.run_finalizes[0][1:] == ("completed", "alert_created")
    # Tenant columns derived from the session row, never the request.
    assert conn.alert_inserts[0][0] == account
    assert conn.alert_inserts[0][1] == camera
    assert conn.run_inserts[0][0] == account
    assert conn.run_inserts[0][1] == camera
    # M6.1: the run finalization emits exactly one canonical event, in the same
    # ingest transaction, with tenant columns derived from the session.
    assert len(conn.event_inserts) == 1
    assert conn.event_inserts[0][0] == account  # account_id
    assert conn.event_inserts[0][3] == "alert.created"  # event_type


def test_ingest_with_no_rules_creates_no_alert() -> None:
    session_id, account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeIngestConn(
        session_row=_session_row(session_id=session_id, account_id=account, camera_id=camera)
    )
    r = _client(conn).post(_OBS_PATH, json=_observation_body(session_id), headers=_headers())
    assert r.status_code == 200, r.text
    assert conn.alert_inserts == []


def test_ingest_with_disabled_rule_creates_no_alert() -> None:
    session_id, account, camera = uuid4(), uuid4(), uuid4()
    rule = {
        "id": uuid4(),
        "account_id": account,
        "camera_id": camera,
        "name": "Front door",
        "rule_type": "line_crossing",
        "config": {},
        "severity": "warning",
        "cooldown_seconds": 300,
        "enabled": False,
    }
    conn = FakeIngestConn(
        session_row=_session_row(session_id=session_id, account_id=account, camera_id=camera),
        rules=[rule],
    )
    r = _client(conn).post(_OBS_PATH, json=_observation_body(session_id), headers=_headers())
    assert r.status_code == 200, r.text
    assert conn.alert_inserts == []
    # Disabled rules never reach the matcher, so no workflow run either.
    assert conn.run_inserts == []


def test_rule_evaluation_failure_never_fails_ingest() -> None:
    """The savepoint contract: a rules layer that explodes (e.g. migration
    not applied yet) must not break observation ingest."""
    session_id, account, camera = uuid4(), uuid4(), uuid4()
    conn = FakeIngestConn(
        session_row=_session_row(session_id=session_id, account_id=account, camera_id=camera),
        rules_fetch_raises=True,
    )
    r = _client(conn).post(_OBS_PATH, json=_observation_body(session_id), headers=_headers())
    assert r.status_code == 200, r.text
    assert len(conn.observation_inserts) == 1
    assert conn.alert_inserts == []
