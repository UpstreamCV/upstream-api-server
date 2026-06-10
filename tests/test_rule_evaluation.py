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
)

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
    ],
)
def test_invalid_configs_fail(rule_type: str, config: dict[str, Any]) -> None:
    assert validate_rule_config(rule_type, config) != []


def test_build_alert_message_uses_labels_over_ids() -> None:
    msg = build_alert_message("line_crossing", {"line_id": "line-1", "line_label": "Front door"})
    assert "Front door" in msg


# ---------------------------------------------------------------------------
# evaluate_observation — service level with a fake conn.
# ---------------------------------------------------------------------------


class FakeEvalConn:
    """Backs the enabled-rules fetch + the cooldown-guarded alert insert.

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
        assert "insert into alerts" in query
        rule_id = args[2]
        if rule_id in self.recent_alert_rule_ids:
            return None  # cooldown guard suppressed the insert
        self.alert_inserts.append(args)
        return uuid4()


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
    created = await evaluate_observation(
        conn,  # type: ignore[arg-type]
        account_id=account,
        camera_id=camera,
        observation_id=uuid4(),
        observation_type="processor.line_crossing",
        payload={"line_id": "line-1", "line_label": "Front door"},
    )
    assert created == 1
    args = conn.alert_inserts[0]
    assert args[0] == account
    assert args[1] == camera
    assert args[2] == rule["id"]
    assert args[4] == "critical"  # severity stamped from the rule
    assert args[5] == "Rule"  # rule name becomes the alert title
    assert "Front door" in args[6]
    assert json.loads(args[7]) == {"line_id": "line-1", "line_label": "Front door"}
    assert args[8] == 300.0


async def test_unmapped_observation_type_is_a_noop() -> None:
    conn = FakeEvalConn(rules=[])
    created = await evaluate_observation(
        conn,  # type: ignore[arg-type]
        account_id=uuid4(),
        camera_id=uuid4(),
        observation_id=uuid4(),
        observation_type="processor.mock_detection",
        payload={},
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
    )
    assert created == 0
    assert conn.alert_inserts == []


async def test_cooldown_suppresses_repeat_alert() -> None:
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
    )
    assert created == 0
    assert conn.alert_inserts == []


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
        if "insert into alerts" in query:
            self.alert_inserts.append(args)
            return uuid4()
        return None


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
    # Tenant columns derived from the session row, never the request.
    assert conn.alert_inserts[0][0] == account
    assert conn.alert_inserts[0][1] == camera


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
