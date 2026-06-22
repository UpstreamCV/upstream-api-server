"""Upstream Reliability M1a — observation detector tests.

The rule functions are pure (``_Facts`` in, verdicts out) so they're tested
directly. A final integration test drives ``evaluate_observation_batch`` against
a fake conn to confirm it opens an issue and promotes the canonical event.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from upstream_api.schemas.reliability_observations import ReliabilityObservationItem
from upstream_api.services.reliability_observation_detectors import (
    _Entity,
    _Facts,
    _Obs,
    _Open,
    _Resolved,
    detect_confidence,
    detect_detection_counts,
    detect_duplicate_events,
    detect_event_counts,
    detect_liveness,
    evaluate_observation_batch,
)

_NOW = datetime(2026, 6, 22, 18, 0, 0, tzinfo=UTC)
_ENTITY = _Entity(key_prefix="camera:abc", camera_id=uuid4(), external_camera_key=None)


def _obs(type_: str, payload: dict[str, Any]) -> _Obs:
    return _Obs(type=type_, payload=payload, observed_at=_NOW)


def _facts(current: list[_Obs], baseline: list[_Obs]) -> _Facts:
    return _Facts(now=_NOW, current=current, baseline=baseline, baseline_windows=6)


def _opens(verdicts: list[Any]) -> list[_Open]:
    return [v for v in verdicts if isinstance(v, _Open)]


def _event(name: str = "forklift", **extra: Any) -> _Obs:
    return _obs("event_observed", {"event_name": name, **extra})


def _summary(field: str, by_class: dict[str, float]) -> _Obs:
    return _obs("detection_summary", {field: by_class})


# ---------------------------------------------------------------------------
# Event count drop / spike.
# ---------------------------------------------------------------------------


def test_event_count_drop_opens_critical_when_silent() -> None:
    facts = _facts(current=[], baseline=[_event() for _ in range(12)])
    opens = _opens(detect_event_counts(facts, _ENTITY))
    assert len(opens) == 1
    assert opens[0].suffix == "event_count_drop:forklift"
    assert opens[0].category == "count"
    assert opens[0].severity == "critical"  # current count == 0


def test_event_count_healthy_resolves() -> None:
    facts = _facts(current=[_event(), _event()], baseline=[_event() for _ in range(12)])
    verdicts = detect_event_counts(facts, _ENTITY)
    assert _opens(verdicts) == []
    assert any(
        isinstance(v, _Resolved) and v.suffix == "event_count_drop:forklift" for v in verdicts
    )


def test_event_count_sparse_baseline_is_noop() -> None:
    facts = _facts(current=[], baseline=[_event() for _ in range(3)])  # < min baseline
    assert detect_event_counts(facts, _ENTITY) == []


def test_event_count_spike_opens() -> None:
    facts = _facts(current=[_event() for _ in range(10)], baseline=[_event() for _ in range(12)])
    opens = _opens(detect_event_counts(facts, _ENTITY))
    assert any(v.suffix == "event_count_spike:forklift" for v in opens)


# ---------------------------------------------------------------------------
# Detection count + confidence.
# ---------------------------------------------------------------------------


def test_detection_count_drop_opens() -> None:
    base = [_summary("counts_by_class", {"forklift": 10}) for _ in range(4)]
    current = [_summary("counts_by_class", {"forklift": 1})]
    opens = _opens(detect_detection_counts(_facts(current, base), _ENTITY))
    assert len(opens) == 1
    assert opens[0].suffix == "detection_count_drop:forklift"


def test_detection_count_no_current_summary_is_noop() -> None:
    base = [_summary("counts_by_class", {"forklift": 10}) for _ in range(4)]
    assert detect_detection_counts(_facts([], base), _ENTITY) == []


def test_confidence_drop_opens_critical() -> None:
    base = [_summary("avg_confidence_by_class", {"forklift": 0.8}) for _ in range(4)]
    current = [_summary("avg_confidence_by_class", {"forklift": 0.5})]
    opens = _opens(detect_confidence(_facts(current, base), _ENTITY))
    assert len(opens) == 1
    assert opens[0].suffix == "confidence_drop:forklift"
    assert opens[0].category == "confidence"
    assert opens[0].severity == "critical"  # drop of 0.30


def test_confidence_stable_resolves() -> None:
    base = [_summary("avg_confidence_by_class", {"forklift": 0.80}) for _ in range(4)]
    current = [_summary("avg_confidence_by_class", {"forklift": 0.78})]
    verdicts = detect_confidence(_facts(current, base), _ENTITY)
    assert _opens(verdicts) == []
    assert any(isinstance(v, _Resolved) for v in verdicts)


# ---------------------------------------------------------------------------
# Duplicate events + liveness.
# ---------------------------------------------------------------------------


def test_duplicate_events_open() -> None:
    current = [_event(name="cross", track_id="t1") for _ in range(5)]
    opens = _opens(detect_duplicate_events(_facts(current, []), _ENTITY))
    assert len(opens) == 1
    assert opens[0].suffix == "duplicate_events:cross"


def test_duplicate_events_below_threshold_resolves() -> None:
    current = [_event(name="cross", track_id="t1") for _ in range(4)]
    verdicts = detect_duplicate_events(_facts(current, []), _ENTITY)
    assert _opens(verdicts) == []
    assert any(isinstance(v, _Resolved) for v in verdicts)


def test_liveness_is_a_noop_stub() -> None:
    assert detect_liveness(_facts([], []), _ENTITY) == []


# ---------------------------------------------------------------------------
# Engine integration.
# ---------------------------------------------------------------------------


class FakeTxn:
    async def __aenter__(self) -> FakeTxn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class FakeEngineConn:
    def __init__(self, history: list[dict[str, Any]]) -> None:
        self.history = history
        self.inserts: list[tuple[Any, ...]] = []
        self.events: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTxn:
        return FakeTxn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from cv_observations" in query:
            return self.history
        if "from event_destinations" in query:
            return []
        return []  # resolve-by-dedupe finds nothing open

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "insert into events" in query:
            self.events.append(args)
            return {"id": uuid4(), "seq": len(self.events), "created_at": _NOW}
        if "insert into cv_issues" in query:
            self.inserts.append(args)
            return {"id": uuid4(), "status": "open"}
        if "from cv_issues" in query and "for update" in query:
            return None  # no open issue -> insert path
        return None


async def test_engine_opens_issue_and_emits_event_on_drop() -> None:
    camera = uuid4()
    # 12 baseline forklift events 30 min ago; nothing in the current window.
    history = [
        {
            "type": "event_observed",
            "payload": {"event_name": "forklift"},
            "observed_at": _NOW - timedelta(minutes=30),
        }
        for _ in range(12)
    ]
    conn = FakeEngineConn(history)
    observations = [
        ReliabilityObservationItem(
            type="event_observed",
            camera_id=camera,
            observed_at=_NOW,
            payload={"event_name": "forklift"},
        )
    ]
    await evaluate_observation_batch(
        conn,
        account_id=uuid4(),
        source="edge-box-4",
        batch_id=uuid4(),
        observations=observations,
        now=_NOW,
    )
    assert len(conn.inserts) == 1  # one issue opened
    dedupe_key = conn.inserts[0][12]
    assert dedupe_key == f"camera:{camera}:event_count_drop:forklift"
    assert len(conn.events) == 1  # reliability.issue_opened promoted
    assert conn.events[0][3] == "reliability.issue_opened"
