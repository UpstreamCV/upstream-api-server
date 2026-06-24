"""Upstream Reliability M2b — pure health-evaluation unit tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from upstream_api.services.reliability_source_liveness import (
    default_intervals,
    effective_intervals,
    evaluate_source_health,
)

_NOW = datetime(2026, 6, 25, 12, 0, 0, tzinfo=UTC)


def _source(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "id": "s-1",
        "source": "edge-box-4",
        "external_camera_key": "dock-12",
        "source_kind": "external_camera",
        "liveness_enabled": True,
        "expected_interval_seconds": 600,
        "liveness_grace_seconds": 300,
        "last_seen_at": None,
        "last_collector_heartbeat_at": None,
    }
    base.update(over)
    return base


def _ago(seconds: int) -> datetime:
    return _NOW - timedelta(seconds=seconds)


def test_default_intervals_by_kind() -> None:
    assert default_intervals("collector") == (180, 60)
    assert default_intervals("external_camera") == (600, 300)
    assert default_intervals("observation_source") == (600, 300)
    assert default_intervals(None) == (600, 300)


def test_effective_intervals_null_falls_back_to_default() -> None:
    src = _source(
        source_kind="collector",
        expected_interval_seconds=None,
        liveness_grace_seconds=None,
    )
    assert effective_intervals(src) == (180, 60)


def test_disabled_source_is_disabled() -> None:
    ev = evaluate_source_health(_source(liveness_enabled=False, last_seen_at=_ago(10)), _NOW)
    assert ev.status == "disabled"
    assert not ev.is_issue


def test_no_signal_is_unknown() -> None:
    assert evaluate_source_health(_source(last_seen_at=None), _NOW).status == "unknown"
    collector = _source(source_kind="collector", external_camera_key=None)
    assert evaluate_source_health(collector, _NOW).status == "unknown"


def test_external_source_healthy_delayed_silent() -> None:
    # expected=600, grace=300 → healthy ≤600, delayed ≤900, silent beyond.
    assert evaluate_source_health(_source(last_seen_at=_ago(60)), _NOW).status == "healthy"
    assert evaluate_source_health(_source(last_seen_at=_ago(700)), _NOW).status == "delayed"
    silent = evaluate_source_health(_source(last_seen_at=_ago(1000)), _NOW)
    assert silent.status == "silent"
    assert silent.is_issue
    assert silent.issue_type == "source_silent"
    assert silent.severity == "warning"
    assert "dock-12" in (silent.title or "")


def test_collector_healthy_delayed_offline() -> None:
    # expected=180, grace=60 → healthy ≤180, delayed ≤240, offline beyond.
    def collector(age: int) -> dict[str, object]:
        return _source(
            source="edge-box-4",
            source_kind="collector",
            external_camera_key=None,
            expected_interval_seconds=180,
            liveness_grace_seconds=60,
            last_collector_heartbeat_at=_ago(age),
        )

    assert evaluate_source_health(collector(30), _NOW).status == "healthy"
    assert evaluate_source_health(collector(200), _NOW).status == "delayed"
    offline = evaluate_source_health(collector(600), _NOW)
    assert offline.status == "collector_offline"
    assert offline.is_issue
    assert offline.issue_type == "collector_offline"
    assert offline.severity == "critical"
    assert "edge-box-4" in (offline.title or "")


def test_threshold_uses_expected_plus_grace_inclusive() -> None:
    # exactly at expected → healthy; exactly at expected+grace → delayed; +1 → silent.
    assert evaluate_source_health(_source(last_seen_at=_ago(600)), _NOW).status == "healthy"
    assert evaluate_source_health(_source(last_seen_at=_ago(900)), _NOW).status == "delayed"
    assert evaluate_source_health(_source(last_seen_at=_ago(901)), _NOW).status == "silent"


def test_uses_effective_default_when_intervals_null() -> None:
    # external with NULL intervals → 600/300; 700s ago → delayed.
    src = _source(
        expected_interval_seconds=None, liveness_grace_seconds=None, last_seen_at=_ago(700)
    )
    assert evaluate_source_health(src, _NOW).status == "delayed"


def test_naive_now_is_rejected() -> None:
    naive = datetime(2026, 6, 25, 12, 0, 0)  # intentionally tz-naive
    with pytest.raises(TypeError):
        evaluate_source_health(_source(last_seen_at=_ago(60)), naive)
