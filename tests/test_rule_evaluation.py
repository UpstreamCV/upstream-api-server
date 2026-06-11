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
        ("line_crossing", {"object_classes": ["person"]}),
        ("line_crossing", {"require_track_id": True}),
        ("line_crossing", {"direction": "a_to_b"}),  # deferred — never produced
        ("zone_entered", {"min_confidence": 0.7}),  # deferred — not in payloads
        ("roi_occupancy", {"object_classes": ["person"]}),  # aggregate counts only
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
        conn,  # type: ignore[arg-type]
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
        conn,  # type: ignore[arg-type]
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


async def test_unmapped_observation_type_is_a_noop() -> None:
    conn = FakeEvalConn(rules=[])
    created = await evaluate_observation(
        conn,  # type: ignore[arg-type]
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
        conn,  # type: ignore[arg-type]
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
        conn,  # type: ignore[arg-type]
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
        conn,  # type: ignore[arg-type]
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
        conn,  # type: ignore[arg-type]
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


async def test_two_matching_rules_create_two_alerts() -> None:
    account, camera = uuid4(), uuid4()
    rules = [
        _eval_rule(account_id=account, camera_id=camera),
        _eval_rule(account_id=account, camera_id=camera, config={"line_id": "line-1"}),
    ]
    conn = FakeEvalConn(rules=rules)
    created = await evaluate_observation(
        conn,  # type: ignore[arg-type]
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

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions where id" in query:
            return self._session_row
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
