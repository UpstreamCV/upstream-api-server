"""Upstream Reliability M2a — source registry service tests.

Exercises ``upsert_sources_from_observation_batch`` aggregation against a fake
connection that captures the ``executemany`` upsert args, and a thin
``list_sources`` passthrough. The aggregation (grouping, per-type counts/
timestamps, model extraction, source_kind classification, zero-not-null
counters) is where the registry's correctness lives.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from upstream_api.schemas.reliability_observations import ReliabilityObservationItem
from upstream_api.services.reliability_sources import (
    list_sources,
    upsert_sources_from_observation_batch,
)

_T0 = datetime(2026, 6, 24, 18, 0, 0, tzinfo=UTC)

# Column order of services/reliability_sources.py::_UPSERT_SQL VALUES (19 cols).
_COLS = (
    "account_id",
    "source",
    "external_camera_key",
    "display_name",
    "source_kind",
    "first_seen_at",
    "last_seen_at",
    "last_observation_at",
    "last_event_at",
    "last_detection_summary_at",
    "last_collector_heartbeat_at",
    "last_config_changed_at",
    "last_model_name",
    "last_model_version",
    "observation_count_total",
    "event_count_total",
    "detection_summary_count_total",
    "collector_heartbeat_count_total",
    "config_changed_count_total",
)


def _row(tpl: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(_COLS, tpl, strict=True))


class FakeUpsertConn:
    def __init__(self) -> None:
        self.upserts: list[tuple[Any, ...]] = []

    async def executemany(self, query: str, args: list[tuple[Any, ...]]) -> None:
        assert "insert into cv_sources" in query
        # The ON CONFLICT target must stay byte-identical to the unique index.
        assert "on conflict (account_id, source, coalesce(external_camera_key, ''))" in query
        self.upserts.extend(args)


class FakeListConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[Any, ...]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from cv_sources" in query
        self.calls.append(args)
        return self.rows


def _obs(
    type_: str,
    *,
    key: str | None,
    at: datetime,
    payload: dict[str, Any] | None = None,
) -> ReliabilityObservationItem:
    return ReliabilityObservationItem(
        type=type_, external_camera_key=key, observed_at=at, payload=payload or {}
    )


async def _upsert(observations: list[ReliabilityObservationItem]) -> list[dict[str, Any]]:
    conn = FakeUpsertConn()
    await upsert_sources_from_observation_batch(
        conn,  # type: ignore[arg-type]
        account_id=uuid4(),
        source="edge-box-4",
        observations=observations,
    )
    return [_row(t) for t in conn.upserts]


async def test_event_observed_creates_external_source() -> None:
    rows = await _upsert([_obs("event_observed", key="dock-12", at=_T0)])
    assert len(rows) == 1
    r = rows[0]
    assert r["source"] == "edge-box-4"
    assert r["external_camera_key"] == "dock-12"
    assert r["display_name"] == "dock-12"
    assert r["source_kind"] == "external_camera"
    assert r["observation_count_total"] == 1
    assert r["event_count_total"] == 1
    assert r["last_event_at"] == _T0
    assert r["first_seen_at"] == _T0 and r["last_seen_at"] == _T0
    # Absent types: counts are literal 0 (NOT None) and timestamps are None.
    assert r["detection_summary_count_total"] == 0
    assert r["collector_heartbeat_count_total"] == 0
    assert r["config_changed_count_total"] == 0
    assert r["last_detection_summary_at"] is None
    assert r["last_model_name"] is None and r["last_model_version"] is None


async def test_detection_summary_updates_model_metadata() -> None:
    rows = await _upsert(
        [
            _obs(
                "detection_summary",
                key="dock-12",
                at=_T0,
                payload={"model_name": "detector-v1", "model_version": "2026-06-22"},
            )
        ]
    )
    r = rows[0]
    assert r["detection_summary_count_total"] == 1
    assert r["last_detection_summary_at"] == _T0
    assert r["last_model_name"] == "detector-v1"
    assert r["last_model_version"] == "2026-06-22"


async def test_model_extraction_takes_latest_nonempty() -> None:
    rows = await _upsert(
        [
            _obs("detection_summary", key="dock-12", at=_T0, payload={"model_name": "m1"}),
            _obs(
                "detection_summary",
                key="dock-12",
                at=_T0 + timedelta(seconds=10),
                payload={"model_name": "m2"},
            ),
            # Newest, but empty model_name — must NOT clobber m2.
            _obs(
                "detection_summary",
                key="dock-12",
                at=_T0 + timedelta(seconds=20),
                payload={"model_name": "   "},
            ),
        ]
    )
    assert rows[0]["last_model_name"] == "m2"
    assert rows[0]["detection_summary_count_total"] == 3


async def test_collector_heartbeat_creates_collector_source() -> None:
    rows = await _upsert([_obs("collector_heartbeat", key=None, at=_T0)])
    r = rows[0]
    assert r["external_camera_key"] is None
    assert r["source_kind"] == "collector"
    assert r["display_name"] == "edge-box-4"  # falls back to source
    assert r["collector_heartbeat_count_total"] == 1
    assert r["last_collector_heartbeat_at"] == _T0


async def test_keyless_normal_observation_is_observation_source() -> None:
    rows = await _upsert([_obs("event_observed", key=None, at=_T0)])
    assert rows[0]["source_kind"] == "observation_source"
    assert rows[0]["event_count_total"] == 1


async def test_unknown_type_updates_last_seen_without_failing() -> None:
    rows = await _upsert([_obs("pipeline_heartbeat", key="dock-9", at=_T0)])
    r = rows[0]
    assert r["observation_count_total"] == 1
    assert r["last_observation_at"] == _T0
    assert r["event_count_total"] == 0
    assert r["detection_summary_count_total"] == 0
    assert r["collector_heartbeat_count_total"] == 0
    assert r["config_changed_count_total"] == 0


async def test_multiple_keys_and_observations_aggregate_per_group() -> None:
    rows = await _upsert(
        [
            _obs("event_observed", key="dock-12", at=_T0),
            _obs("detection_summary", key="dock-12", at=_T0 + timedelta(seconds=5)),
            _obs("event_observed", key="dock-13", at=_T0 + timedelta(seconds=1)),
            _obs("config_changed", key="dock-13", at=_T0 + timedelta(seconds=2)),
        ]
    )
    by_key = {r["external_camera_key"]: r for r in rows}
    assert set(by_key) == {"dock-12", "dock-13"}
    assert by_key["dock-12"]["observation_count_total"] == 2
    assert by_key["dock-12"]["event_count_total"] == 1
    assert by_key["dock-12"]["detection_summary_count_total"] == 1
    assert by_key["dock-12"]["last_seen_at"] == _T0 + timedelta(seconds=5)
    assert by_key["dock-12"]["first_seen_at"] == _T0
    assert by_key["dock-13"]["event_count_total"] == 1
    assert by_key["dock-13"]["config_changed_count_total"] == 1
    assert by_key["dock-13"]["last_config_changed_at"] == _T0 + timedelta(seconds=2)


async def test_empty_observations_is_noop() -> None:
    conn = FakeUpsertConn()
    await upsert_sources_from_observation_batch(
        conn,  # type: ignore[arg-type]
        account_id=uuid4(),
        source="edge-box-4",
        observations=[],
    )
    assert conn.upserts == []


async def test_list_sources_passes_filters_and_returns_dicts() -> None:
    account = uuid4()
    conn = FakeListConn([{"id": uuid4(), "source": "edge-box-4", "status_hint": "active"}])
    out = await list_sources(
        conn,  # type: ignore[arg-type]
        account_id=account,
        source_kind="external_camera",
        status_hint="active",
        limit=25,
    )
    assert out[0]["source"] == "edge-box-4"
    assert conn.calls[0] == (account, "external_camera", "active", 25)
