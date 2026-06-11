"""Workflow Engine M4 — time-based evaluator tests.

Three layers, mirroring the codebase's test style:

1. Condition checks (``_check_camera_offline`` / ``_check_no_detections``)
   against a fake conn — thresholds, gates, fail-closed paths.
2. ``run_workflow_evaluator`` end-to-end with fakes — firing creates a run
   + linked alert, cooldown re-fire guard creates NOTHING inside the
   window, schedule gating, per-rule error isolation.
3. The internal endpoint — auth + stats shape.

The fakes re-implement the documented SQL predicates; SQL-pinning tests
keep them from drifting.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.services.workflow_evaluator import (
    _ACTIVE_SESSION_SQL,
    _CAMERA_LIVENESS_SQL,
    _ENABLED_TIMER_RULES_SQL,
    _INSERT_TIMER_RUN_SQL,
    _LAST_MATCHING_OBSERVATION_SQL,
    _check_camera_offline,
    _check_no_detections,
    run_workflow_evaluator,
)

_NOW = datetime(2026, 6, 10, 12, 0, 0, tzinfo=UTC)  # a Wednesday


# ---------------------------------------------------------------------------
# Fake conn/pool.
# ---------------------------------------------------------------------------


class FakeConn:
    """Backs every query the evaluator issues, discriminated by SQL text.

    Liveness/session/observation answers are configured per camera id;
    run/alert inserts are recorded, with ``rules_in_cooldown`` simulating
    the NOT EXISTS re-fire guard (and the alert guard) returning no id.
    """

    def __init__(
        self,
        *,
        rules: list[dict[str, Any]] | None = None,
        active_session_by_camera: dict[UUID, dict[str, Any] | None] | None = None,
        last_heartbeat_by_camera: dict[UUID, datetime | None] | None = None,
        last_observation_by_camera: dict[UUID, datetime | None] | None = None,
        rules_in_cooldown: set[UUID] | None = None,
        liveness_raises: bool = False,
    ) -> None:
        self.rules = rules or []
        self.active_session_by_camera = active_session_by_camera or {}
        self.last_heartbeat_by_camera = last_heartbeat_by_camera or {}
        self.last_observation_by_camera = last_observation_by_camera or {}
        self.rules_in_cooldown = rules_in_cooldown or set()
        self.liveness_raises = liveness_raises
        self.expired_sequence_runs: list[dict[str, Any]] = []
        self.run_inserts: list[tuple[Any, ...]] = []
        self.alert_inserts: list[tuple[Any, ...]] = []
        self.run_finalizes: list[tuple[Any, ...]] = []
        self.observation_query_args: list[tuple[Any, ...]] = []
        self.run_ids: list[UUID] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from workflow_runs r" in query:
            return list(self.expired_sequence_runs)
        assert "from rules" in query
        return list(self.rules)

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        camera_id = args[0]
        if "as active_session" in query:
            if self.liveness_raises:
                raise RuntimeError("db went away")
            session = self.active_session_by_camera.get(camera_id)
            return {
                "active_session": session is not None,
                "last_heartbeat_at": self.last_heartbeat_by_camera.get(camera_id),
            }
        if "from stream_sessions" in query:
            return self.active_session_by_camera.get(camera_id)
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "from observations" in query:
            self.observation_query_args.append(args)
            return self.last_observation_by_camera.get(args[0])
        if "insert into workflow_runs" in query:
            rule_id = args[2]
            if rule_id in self.rules_in_cooldown:
                return None  # NOT EXISTS re-fire guard
            self.run_inserts.append(args)
            run_id = uuid4()
            self.run_ids.append(run_id)
            return run_id
        if "insert into alerts" in query:
            self.alert_inserts.append(args)
            return uuid4()
        raise AssertionError(f"unexpected fetchval: {query}")

    async def execute(self, query: str, *args: Any) -> str:
        assert "update workflow_runs" in query
        self.run_finalizes.append(args)
        return "UPDATE 1"


class AcquireCtx:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _timer_rule(
    *,
    rule_type: str = "camera_offline",
    config: dict[str, Any] | None = None,
    severity: str = "warning",
    cooldown_seconds: int = 300,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": uuid4(),
        "camera_id": uuid4(),
        "name": "Timer rule",
        "rule_type": rule_type,
        "config": config
        or (
            {"offline_for_seconds": 300}
            if rule_type == "camera_offline"
            else {"quiet_for_seconds": 1800}
        ),
        "severity": severity,
        "cooldown_seconds": cooldown_seconds,
    }


# ---------------------------------------------------------------------------
# camera_offline condition.
# ---------------------------------------------------------------------------


async def test_offline_above_threshold_matches_with_evidence() -> None:
    camera = uuid4()
    conn = FakeConn(last_heartbeat_by_camera={camera: _NOW - timedelta(seconds=420)})
    result = await _check_camera_offline(
        conn,
        camera_id=camera,
        config={"offline_for_seconds": 300},
        now=_NOW,
    )
    assert result is not None
    assert result.evidence["offline_for_seconds"] == 420
    assert result.evidence["threshold_seconds"] == 300
    assert result.evidence["active_session_found"] is False
    assert "7m" in result.summary


async def test_offline_below_threshold_does_not_match() -> None:
    camera = uuid4()
    conn = FakeConn(last_heartbeat_by_camera={camera: _NOW - timedelta(seconds=100)})
    assert (
        await _check_camera_offline(
            conn,
            camera_id=camera,
            config={"offline_for_seconds": 300},
            now=_NOW,
        )
        is None
    )


async def test_active_session_means_online() -> None:
    camera = uuid4()
    conn = FakeConn(
        active_session_by_camera={camera: {"id": uuid4(), "started_at": _NOW}},
        # Heartbeats can lag a healthy stream — the active session wins.
        last_heartbeat_by_camera={camera: _NOW - timedelta(hours=2)},
    )
    assert (
        await _check_camera_offline(
            conn,
            camera_id=camera,
            config={"offline_for_seconds": 300},
            now=_NOW,
        )
        is None
    )


async def test_never_heartbeated_camera_matches() -> None:
    camera = uuid4()
    conn = FakeConn()
    result = await _check_camera_offline(
        conn,
        camera_id=camera,
        config={"offline_for_seconds": 300},
        now=_NOW,
    )
    assert result is not None
    assert result.evidence["last_seen_at"] is None
    assert result.evidence["offline_for_seconds"] is None


async def test_malformed_threshold_fails_closed() -> None:
    camera = uuid4()
    conn = FakeConn()
    for bad in ({}, {"offline_for_seconds": "300"}, {"offline_for_seconds": True}):
        assert (
            await _check_camera_offline(
                conn,
                camera_id=camera,
                config=bad,
                now=_NOW,
            )
            is None
        )


# ---------------------------------------------------------------------------
# no_detections_for condition.
# ---------------------------------------------------------------------------


def _streaming(started_seconds_ago: float) -> dict[str, Any]:
    return {"id": uuid4(), "started_at": _NOW - timedelta(seconds=started_seconds_ago)}


async def test_quiet_above_threshold_matches() -> None:
    camera = uuid4()
    conn = FakeConn(
        active_session_by_camera={camera: _streaming(7200)},
        last_observation_by_camera={camera: _NOW - timedelta(seconds=2400)},
    )
    result = await _check_no_detections(
        conn,
        camera_id=camera,
        config={"quiet_for_seconds": 1800},
        now=_NOW,
    )
    assert result is not None
    assert result.evidence["quiet_for_seconds"] == 2400
    assert result.evidence["active_session_found"] is True


async def test_recent_observation_prevents_match() -> None:
    camera = uuid4()
    conn = FakeConn(
        active_session_by_camera={camera: _streaming(7200)},
        last_observation_by_camera={camera: _NOW - timedelta(seconds=60)},
    )
    assert (
        await _check_no_detections(
            conn,
            camera_id=camera,
            config={"quiet_for_seconds": 1800},
            now=_NOW,
        )
        is None
    )


async def test_not_streaming_camera_is_not_quiet() -> None:
    """A down camera is camera_offline's job — no double-alerting."""
    camera = uuid4()
    conn = FakeConn(last_observation_by_camera={camera: _NOW - timedelta(hours=9)})
    assert (
        await _check_no_detections(
            conn,
            camera_id=camera,
            config={"quiet_for_seconds": 1800},
            now=_NOW,
        )
        is None
    )


async def test_fresh_session_never_instant_fires() -> None:
    """Anchor = max(last observation, session start): a session started 2
    minutes ago can't be 'quiet for 30 minutes' even with an old (or no)
    last observation."""
    camera = uuid4()
    conn = FakeConn(
        active_session_by_camera={camera: _streaming(120)},
        last_observation_by_camera={camera: _NOW - timedelta(hours=5)},
    )
    assert (
        await _check_no_detections(
            conn,
            camera_id=camera,
            config={"quiet_for_seconds": 1800},
            now=_NOW,
        )
        is None
    )


async def test_class_filter_defaults_to_class_attributing_types() -> None:
    camera = uuid4()
    conn = FakeConn(
        active_session_by_camera={camera: _streaming(7200)},
        last_observation_by_camera={camera: None},
    )
    result = await _check_no_detections(
        conn,
        camera_id=camera,
        config={"quiet_for_seconds": 1800, "object_classes": ["person"]},
        now=_NOW,
    )
    assert result is not None
    _camera_arg, types_arg, classes_arg = conn.observation_query_args[0]
    assert classes_arg == ["person"]
    # Class filters only query class-attributing types.
    assert "processor.roi_occupancy" not in types_arg
    assert "processor.line_crossing" not in types_arg
    assert "processor.zone_entered" in types_arg
    assert "processor.line_crossing_object" in types_arg
    assert result.evidence["object_classes"] == ["person"]


async def test_explicit_observation_types_are_forwarded() -> None:
    camera = uuid4()
    conn = FakeConn(
        active_session_by_camera={camera: _streaming(7200)},
        last_observation_by_camera={camera: None},
    )
    await _check_no_detections(
        conn,
        camera_id=camera,
        config={
            "quiet_for_seconds": 1800,
            "observation_types": ["processor.zone_entered"],
        },
        now=_NOW,
    )
    assert conn.observation_query_args[0][1] == ["processor.zone_entered"]
    assert conn.observation_query_args[0][2] is None


# ---------------------------------------------------------------------------
# run_workflow_evaluator — end to end with fakes.
# ---------------------------------------------------------------------------


async def test_offline_rule_fires_run_and_linked_alert() -> None:
    rule = _timer_rule(severity="critical")
    conn = FakeConn(
        rules=[rule],
        last_heartbeat_by_camera={rule["camera_id"]: _NOW - timedelta(seconds=900)},
    )
    stats = await run_workflow_evaluator(FakePool(conn), now=_NOW)
    assert stats.evaluated_rules == 1
    assert stats.matched_rules == 1
    assert stats.workflow_runs_created == 1
    assert stats.alerts_created == 1
    assert stats.suppressed_by_cooldown == 0
    assert stats.errors == 0
    # Run shape: trigger_type + evidence + rule snapshot.
    run_args = conn.run_inserts[0]
    assert run_args[0] == rule["account_id"]
    assert run_args[1] == rule["camera_id"]
    assert run_args[2] == rule["id"]
    assert run_args[5] == "timer.camera_offline"
    evidence = json.loads(run_args[6])
    assert evidence["offline_for_seconds"] == 900
    assert json.loads(run_args[7])["rule"]["rule_type"] == "camera_offline"
    # Alert linked to the run, no triggering observation, severity stamped.
    alert_args = conn.alert_inserts[0]
    assert alert_args[3] is None
    assert alert_args[4] == conn.run_ids[0]
    assert alert_args[5] == "critical"
    assert conn.run_finalizes == [(conn.run_ids[0], "completed", "alert_created")]


async def test_cooldown_window_creates_nothing_per_tick() -> None:
    """Inside the re-fire window the guarded run insert returns no id —
    no run, no alert, no suppressed-run spam per tick."""
    rule = _timer_rule()
    conn = FakeConn(
        rules=[rule],
        last_heartbeat_by_camera={rule["camera_id"]: _NOW - timedelta(seconds=900)},
        rules_in_cooldown={rule["id"]},
    )
    stats = await run_workflow_evaluator(FakePool(conn), now=_NOW)
    assert stats.matched_rules == 1
    assert stats.suppressed_by_cooldown == 1
    assert stats.workflow_runs_created == 0
    assert conn.run_inserts == []
    assert conn.alert_inserts == []
    assert conn.run_finalizes == []


async def test_repeated_ticks_do_not_spam() -> None:
    """First tick fires; subsequent ticks inside the window are no-ops."""
    rule = _timer_rule()
    conn = FakeConn(
        rules=[rule],
        last_heartbeat_by_camera={rule["camera_id"]: _NOW - timedelta(seconds=900)},
    )
    pool = FakePool(conn)
    await run_workflow_evaluator(pool, now=_NOW)
    # The fired run now sits inside the cooldown window.
    conn.rules_in_cooldown = {rule["id"]}
    for _ in range(3):
        await run_workflow_evaluator(pool, now=_NOW)
    assert len(conn.run_inserts) == 1
    assert len(conn.alert_inserts) == 1


async def test_schedule_blocks_evaluation() -> None:
    rule = _timer_rule(
        config={
            "offline_for_seconds": 300,
            "schedule": {
                "days": ["sat", "sun"],
                "start_time": "08:00",
                "end_time": "18:00",
                "timezone": "UTC",
            },
        }
    )
    conn = FakeConn(
        rules=[rule],
        last_heartbeat_by_camera={rule["camera_id"]: _NOW - timedelta(hours=3)},
    )
    stats = await run_workflow_evaluator(
        FakePool(conn), now=_NOW
    )  # Wednesday  # type: ignore[arg-type]
    assert stats.schedule_skipped == 1
    assert stats.matched_rules == 0
    assert conn.run_inserts == []


async def test_one_rule_error_does_not_stop_the_tick() -> None:
    bad = _timer_rule()
    good = _timer_rule()
    conn = FakeConn(
        rules=[bad, good],
        last_heartbeat_by_camera={good["camera_id"]: _NOW - timedelta(seconds=900)},
        liveness_raises=False,
    )

    # Make only the FIRST rule's liveness check blow up.
    original_fetchrow = conn.fetchrow

    async def flaky_fetchrow(query: str, *args: Any) -> dict[str, Any] | None:
        if "as active_session" in query and args[0] == bad["camera_id"]:
            raise RuntimeError("db went away")
        return await original_fetchrow(query, *args)

    conn.fetchrow = flaky_fetchrow  # type: ignore[method-assign]
    stats = await run_workflow_evaluator(FakePool(conn), now=_NOW)
    assert stats.errors == 1
    assert stats.workflow_runs_created == 1  # the good rule still fired


# ---------------------------------------------------------------------------
# Internal endpoint.
# ---------------------------------------------------------------------------


def _client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().processor_webhook_secret.get_secret_value()}


def test_endpoint_requires_internal_secret() -> None:
    conn = FakeConn()
    r = _client(conn).post("/v1/internal/workflow-evaluator/run")
    assert r.status_code == 401


def test_endpoint_rejects_wrong_secret() -> None:
    conn = FakeConn()
    r = _client(conn).post(
        "/v1/internal/workflow-evaluator/run",
        headers={"X-Upstream-Webhook-Secret": "wrong"},
    )
    assert r.status_code == 401


def test_endpoint_returns_stats_shape() -> None:
    rule = _timer_rule()
    conn = FakeConn(
        rules=[rule],
        last_heartbeat_by_camera={rule["camera_id"]: datetime.now(UTC) - timedelta(seconds=900)},
    )
    r = _client(conn).post("/v1/internal/workflow-evaluator/run", headers=_headers())
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body.keys()) == {
        "evaluated_rules",
        "schedule_skipped",
        "matched_rules",
        "workflow_runs_created",
        "alerts_created",
        "suppressed_by_cooldown",
        "sequence_runs_expired",
        "errors",
    }
    assert body["evaluated_rules"] == 1
    assert body["workflow_runs_created"] == 1
    assert body["alerts_created"] == 1


# ---------------------------------------------------------------------------
# Pin the production SQL so the fakes cannot drift.
# ---------------------------------------------------------------------------


def test_timer_rules_sql_pins_predicates() -> None:
    sql = _ENABLED_TIMER_RULES_SQL
    assert "where r.enabled" in sql
    assert "r.rule_type = any($1::text[])" in sql
    assert "c.status != 'disabled'" in sql


def test_liveness_sql_pins_predicates() -> None:
    assert "state = 'streaming'" in _CAMERA_LIVENESS_SQL
    assert "expires_at > now()" in _CAMERA_LIVENESS_SQL
    assert "max(reported_at) from heartbeats" in _CAMERA_LIVENESS_SQL
    assert "state = 'streaming'" in _ACTIVE_SESSION_SQL


def test_observation_sql_pins_predicates() -> None:
    sql = _LAST_MATCHING_OBSERVATION_SQL
    assert "type = any($2::text[])" in sql
    assert "($3::text[] is null or payload->>'class_label' = any($3))" in sql


def test_timer_run_sql_pins_refire_guard() -> None:
    sql = _INSERT_TIMER_RUN_SQL
    assert "'time_based_rule'" in sql
    assert "where not exists" in sql
    assert "where rule_id = $3" in sql
    assert "make_interval(secs => $9)" in sql
