"""P5-J — GET /v1/cameras/{camera_id}/processing/evidence.

Two layers, mirroring tests/test_observations.py:
- Service-level: drive ``build_processing_evidence`` directly with a fixed
  ``now`` + a fake conn that re-implements the windowed/inferred/history SQL, so
  windowing, the half-open boundary, inferred-vs-observed counting, empty cases,
  and warnings are genuinely exercised. ``build_package_config_response`` and
  ``fetch_latest_processor_status`` are monkeypatched (already tested elsewhere).
- Router-level: TestClient + fake pool for auth/ownership/range/shape.
- A pin test guards the production SQL against drift.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services import processing_evidence as svc
from upstream_api.services.processing_evidence import (
    _INFERRED_EXITS_SQL,
    _OBSERVATION_COUNTS_SQL,
    _PACKAGE_HISTORY_SQL,
    WARN_DWELL_CLOSURE_NOT_PERSISTED,
    WARN_PACKAGE_HISTORY_UNAVAILABLE,
    WARN_TRACKING_SESSION_LEVEL,
    build_processing_evidence,
)

_NOW = datetime(2026, 6, 17, 12, 0, 0, tzinfo=UTC)
_PATH = "/v1/cameras/{camera_id}/processing/evidence"

_DEFAULT_PAYLOAD = {
    "model_id": "yolox_s_v1",
    "tracker_profile_id": "deepstream_iou_fast_v1",
    "tracker_algorithm": "iou",
    "analytics_profile_id": "deepstream_nvdsanalytics_probe_v1",
    "unique_tracker_ids_count": 8,
    "tracker_ids_sample": ["1", "2", "3"],
}


# ---------------------------------------------------------------------------
# Fake conn — re-implements the windowed/inferred/history SQL semantics.
# ---------------------------------------------------------------------------


class FakeConn:
    def __init__(
        self,
        *,
        camera_account_id: UUID | None = None,
        obs_rows: list[dict[str, Any]] | None = None,
        history_rows: list[dict[str, Any]] | None = None,
        audit_table: bool = True,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.obs_rows = obs_rows or []
        self.history_rows = history_rows or []
        self.audit_table = audit_table

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}
        if "filter (where payload" in query:  # inferred/timeout scalar
            account_id, camera_id, now, hours = args
            start = now - timedelta(hours=hours)
            rel = [
                r
                for r in self.obs_rows
                if r["account_id"] == account_id
                and r["camera_id"] == camera_id
                and r["type"] == "processor.zone_exited"
                and start <= r["observed_at"] < now
            ]
            return {
                "inferred_zone_exits": sum(
                    1 for r in rel if (r.get("payload") or {}).get("inferred") is True
                ),
                "track_missing_timeout_exits": sum(
                    1
                    for r in rel
                    if (r.get("payload") or {}).get("exit_reason") == "track_missing_timeout"
                ),
            }
        raise AssertionError(f"unexpected fetchrow: {query[:60]}")

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "group by type" in query:  # count-by-type
            account_id, camera_id, now, hours = args
            start = now - timedelta(hours=hours)
            counts = Counter(
                r["type"]
                for r in self.obs_rows
                if r["account_id"] == account_id
                and r["camera_id"] == camera_id
                and start <= r["observed_at"] < now
            )
            return [{"type": t, "count": n} for t, n in sorted(counts.items())]
        if "from account_audit_logs" in query:
            account_id, event_types, resource_id, limit = args
            rel = [
                r
                for r in self.history_rows
                if r["account_id"] == account_id
                and r["event_type"] in event_types
                and r["resource_id"] == resource_id
            ]
            rel.sort(key=lambda r: r["created_at"], reverse=True)
            return rel[:limit]
        raise AssertionError(f"unexpected fetch: {query[:60]}")

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "to_regclass" in query:
            return self.audit_table
        raise AssertionError(f"unexpected fetchval: {query[:60]}")


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


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _pkg(
    *,
    requested: tuple[str, int] | None = ("pkg.iou", 2),
    applied: str | None = "pkg.iou",
    state: str = "active",
) -> SimpleNamespace:
    return SimpleNamespace(
        requested=SimpleNamespace(package_id=requested[0], revision=requested[1])
        if requested
        else None,
        applied=SimpleNamespace(package_id=applied) if applied else None,
        state=state,
        warnings=[],
    )


def _status(payload: dict[str, Any] | None = _DEFAULT_PAYLOAD) -> dict[str, Any]:
    return {
        "status": "processing",
        "last_frame_at": _NOW,
        "last_observation_at": None,
        "error_code": None,
        "updated_at": _NOW,
        "payload": payload,
    }


def _patch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pkg: SimpleNamespace | None = None,
    status: dict[str, Any] | None = "__default__",  # type: ignore[assignment]
) -> None:
    pkg = pkg or _pkg()
    status_row = _status() if status == "__default__" else status

    async def fake_pkg(conn: Any, *, account_id: UUID, camera_id: UUID) -> Any:
        return pkg

    async def fake_status(
        conn: Any, *, account_id: UUID, camera_id: UUID, stream_session_id: Any
    ) -> Any:
        return status_row

    monkeypatch.setattr(svc, "build_package_config_response", fake_pkg)
    monkeypatch.setattr(svc, "fetch_latest_processor_status", fake_status)


def _obs(
    *,
    account_id: UUID,
    camera_id: UUID,
    type_: str,
    observed_at: datetime,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "account_id": account_id,
        "camera_id": camera_id,
        "type": type_,
        "observed_at": observed_at,
        "payload": payload or {},
    }


def _run(conn: FakeConn, *, account_id: UUID, camera_id: UUID, window_hours: int = 24) -> Any:
    return asyncio.run(
        build_processing_evidence(
            conn,
            account_id=account_id,
            camera_id=camera_id,
            window_hours=window_hours,
            now=_NOW,
        )
    )


# ---------------------------------------------------------------------------
# Service-level: windowed counts.
# ---------------------------------------------------------------------------


def test_observation_counts_grouped_by_type(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    rows = [
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_entered",
            observed_at=_NOW - timedelta(minutes=5),
        ),
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_entered",
            observed_at=_NOW - timedelta(minutes=6),
        ),
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.line_crossing",
            observed_at=_NOW - timedelta(minutes=7),
        ),
    ]
    res = _run(FakeConn(obs_rows=rows), account_id=acc, camera_id=cam)
    counts = {c.type: c.count for c in res.observation_counts}
    assert counts == {"processor.zone_entered": 2, "processor.line_crossing": 1}


def test_window_is_half_open(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    rows = [
        _obs(
            account_id=acc, camera_id=cam, type_="processor.zone_entered", observed_at=_NOW
        ),  # excluded (== now)
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_entered",
            observed_at=_NOW - timedelta(hours=24),
        ),  # included (== start)
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_entered",
            observed_at=_NOW - timedelta(hours=25),
        ),  # excluded (too old)
    ]
    res = _run(FakeConn(obs_rows=rows), account_id=acc, camera_id=cam, window_hours=24)
    counts = {c.type: c.count for c in res.observation_counts}
    assert counts == {"processor.zone_entered": 1}


def test_1h_window_excludes_older(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    rows = [
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.roi_occupancy",
            observed_at=_NOW - timedelta(minutes=30),
        ),
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.roi_occupancy",
            observed_at=_NOW - timedelta(hours=2),
        ),
    ]
    res = _run(FakeConn(obs_rows=rows), account_id=acc, camera_id=cam, window_hours=1)
    counts = {c.type: c.count for c in res.observation_counts}
    assert counts == {"processor.roi_occupancy": 1}
    assert res.window.range == "1h"


def test_no_observations_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    res = _run(FakeConn(obs_rows=[]), account_id=acc, camera_id=cam)
    assert res.observation_counts == []
    assert res.lifecycle.inferred_zone_exits == 0
    assert res.lifecycle.track_missing_timeout_exits == 0


# ---------------------------------------------------------------------------
# Service-level: inferred exits.
# ---------------------------------------------------------------------------


def test_inferred_exits_counted_from_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    rows = [
        # inferred (counts)
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_exited",
            observed_at=_NOW - timedelta(minutes=1),
            payload={"inferred": True, "exit_reason": "track_missing_timeout"},
        ),
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_exited",
            observed_at=_NOW - timedelta(minutes=2),
            payload={"inferred": True, "exit_reason": "track_missing_timeout"},
        ),
        # observed exit WITHOUT the inferred key (does NOT count)
        _obs(
            account_id=acc,
            camera_id=cam,
            type_="processor.zone_exited",
            observed_at=_NOW - timedelta(minutes=3),
            payload={"zone_id": "lobby"},
        ),
    ]
    res = _run(FakeConn(obs_rows=rows), account_id=acc, camera_id=cam)
    assert res.lifecycle.inferred_zone_exits == 2
    assert res.lifecycle.track_missing_timeout_exits == 2
    # And the total zone_exited count includes the observed one.
    counts = {c.type: c.count for c in res.observation_counts}
    assert counts["processor.zone_exited"] == 3


# ---------------------------------------------------------------------------
# Service-level: lineage / status / tracking.
# ---------------------------------------------------------------------------


def test_lineage_includes_tracker_algorithm_from_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    # tracker_algorithm is NOT on AppliedPackageConfig — it must come from the
    # raw status payload. This locks in the direct-payload read.
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    res = _run(FakeConn(obs_rows=[]), account_id=acc, camera_id=cam)
    assert res.lineage.tracker_algorithm == "iou"
    assert res.lineage.model_id == "yolox_s_v1"
    assert res.tracking.unique_tracker_ids_count == 8
    assert res.tracking.tracker_ids_sample == ["1", "2", "3"]
    assert res.tracking.is_session_level is True


def test_no_status_row_yields_null_lineage(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch, status=None)
    acc, cam = uuid4(), uuid4()
    res = _run(FakeConn(obs_rows=[]), account_id=acc, camera_id=cam)
    assert res.lineage.model_id is None
    assert res.lineage.tracker_algorithm is None
    assert res.status.status is None
    assert res.tracking.unique_tracker_ids_count is None
    # Package state still comes through from build_package_config_response.
    assert res.package.state == "active"


# ---------------------------------------------------------------------------
# Service-level: package history + warnings.
# ---------------------------------------------------------------------------


def test_package_history_both_event_types_desc(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    history = [
        {
            "id": uuid4(),
            "account_id": acc,
            "event_type": "camera.processing_package_requested",
            "resource_id": str(cam),
            "actor_type": "user",
            "actor_user_id": uuid4(),
            "summary": "requested",
            "metadata": {"package_id": "pkg.iou"},
            "created_at": _NOW - timedelta(minutes=10),
        },
        {
            "id": uuid4(),
            "account_id": acc,
            "event_type": "camera.processing_package_applied",
            "resource_id": str(cam),
            "actor_type": "user",
            "actor_user_id": uuid4(),
            "summary": "applied",
            "metadata": {"package_id": "pkg.iou"},
            "created_at": _NOW - timedelta(minutes=5),
        },
    ]
    res = _run(FakeConn(obs_rows=[], history_rows=history), account_id=acc, camera_id=cam)
    assert [h.event_type for h in res.package_history] == [
        "camera.processing_package_applied",
        "camera.processing_package_requested",
    ]
    assert res.package_history[0].package_id == "pkg.iou"
    assert WARN_PACKAGE_HISTORY_UNAVAILABLE not in res.warnings


def test_package_history_table_missing_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    res = _run(FakeConn(obs_rows=[], audit_table=False), account_id=acc, camera_id=cam)
    assert res.package_history == []
    assert WARN_PACKAGE_HISTORY_UNAVAILABLE in res.warnings


def test_standing_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    res = _run(FakeConn(obs_rows=[]), account_id=acc, camera_id=cam)
    # dwell-closure is a known platform gap → always warned.
    assert WARN_DWELL_CLOSURE_NOT_PERSISTED in res.warnings
    # tracking present (default payload) → session-level warning.
    assert WARN_TRACKING_SESSION_LEVEL in res.warnings


def test_no_tracking_no_session_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch, status=None)
    acc, cam = uuid4(), uuid4()
    res = _run(FakeConn(obs_rows=[]), account_id=acc, camera_id=cam)
    assert WARN_TRACKING_SESSION_LEVEL not in res.warnings


# ---------------------------------------------------------------------------
# Router-level: auth / ownership / range / shape.
# ---------------------------------------------------------------------------


def _client(conn: FakeConn, *, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def test_router_200_default_range(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    r = _client(FakeConn(camera_account_id=acc), account_id=acc).get(_PATH.format(camera_id=cam))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["window"]["range"] == "24h"
    assert body["lineage"]["tracker_algorithm"] == "iou"
    assert dwell_warn_present(body)


def dwell_warn_present(body: dict[str, Any]) -> bool:
    return WARN_DWELL_CLOSURE_NOT_PERSISTED in body["warnings"]


def test_router_range_1h(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    r = _client(FakeConn(camera_account_id=acc), account_id=acc).get(
        _PATH.format(camera_id=cam), params={"range": "1h"}
    )
    assert r.status_code == 200, r.text
    assert r.json()["window"]["range"] == "1h"


def test_router_bad_range_422(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    r = _client(FakeConn(camera_account_id=acc), account_id=acc).get(
        _PATH.format(camera_id=cam), params={"range": "7d"}
    )
    assert r.status_code == 422


def test_router_foreign_camera_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    caller_acc, other_acc, cam = uuid4(), uuid4(), uuid4()
    r = _client(FakeConn(camera_account_id=other_acc), account_id=caller_acc).get(
        _PATH.format(camera_id=cam)
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_router_unknown_camera_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    acc, cam = uuid4(), uuid4()
    r = _client(FakeConn(camera_account_id=None), account_id=acc).get(_PATH.format(camera_id=cam))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_router_unauthenticated_401() -> None:
    acc, cam = uuid4(), uuid4()
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(FakeConn(camera_account_id=acc))
    r = TestClient(app).get(_PATH.format(camera_id=cam))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"


# ---------------------------------------------------------------------------
# Pin the production SQL.
# ---------------------------------------------------------------------------


def test_sql_pins() -> None:
    assert "group by type" in _OBSERVATION_COUNTS_SQL
    assert "make_interval(hours => $4::int)" in _OBSERVATION_COUNTS_SQL
    assert (
        "observed_at >= $3::timestamptz - make_interval(hours => $4::int)"
        in _OBSERVATION_COUNTS_SQL
    )
    assert "observed_at <  $3::timestamptz" in _OBSERVATION_COUNTS_SQL

    assert "payload->>'inferred' = 'true'" in _INFERRED_EXITS_SQL
    assert "payload->>'exit_reason' = 'track_missing_timeout'" in _INFERRED_EXITS_SQL
    assert "type = 'processor.zone_exited'" in _INFERRED_EXITS_SQL

    assert "event_type = any($2::text[])" in _PACKAGE_HISTORY_SQL
    assert "resource_type = 'camera'" in _PACKAGE_HISTORY_SQL
    assert "order by created_at desc" in _PACKAGE_HISTORY_SQL
