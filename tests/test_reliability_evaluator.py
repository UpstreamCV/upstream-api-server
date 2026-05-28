"""Reliability evaluator — rule-level tests.

Drives ``evaluate_camera`` against a fake asyncpg connection that returns
canned rows for each fact query. The evaluator is async and writes through
``services/reliability_repo``, so the fake conn also captures upserts /
resolves so each rule's verdict can be asserted on without a real DB.

One assertion style across tests: build facts → run evaluator → inspect the
captured upsert / resolve calls. The summary roll-up has its own tests in
``test_reliability_endpoints.py``.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from upstream_api.services.reliability_evaluator import (
    DETECTOR_VERSION,
    evaluate_camera,
)

# ---------------------------------------------------------------------------
# Fake conn.
# ---------------------------------------------------------------------------


class FakeEvalConn:
    """Routes the evaluator's queries to dict facts and records all writes.

    Recognized query substrings (one per fact / write target):
      - "from stream_sessions"          -> latest session row or None
      - "from snapshots"                -> latest snapshot row or None
      - "max(observed_at)"              -> {"observed_at": dt} or None
      - "from camera_geometry_configs"  -> {"?column?": 1} or None
      - "from processor_statuses"       -> processor status row or None
                                           (matches the SELECT inside
                                           fetch_latest_processor_status; the
                                           helper unwraps to a plain dict)
      - "from reliability_findings ... for update" -> open-finding lookup
                                           (returns None unless preseeded)
      - "insert into reliability_findings" -> echoes a row (captures args)
      - "update reliability_findings ... set status = 'resolved'" -> returns []
    """

    def __init__(
        self,
        *,
        session: dict[str, Any] | None = None,
        snapshot: dict[str, Any] | None = None,
        observation_at: datetime | None = None,
        processor_status: dict[str, Any] | None = None,
        has_saved_geometry: bool = False,
        preopen_finding_ids: dict[str, UUID] | None = None,
    ) -> None:
        self.session = session
        self.snapshot = snapshot
        self.observation_at = observation_at
        self.processor_status = processor_status
        self.has_saved_geometry = has_saved_geometry
        # preopen_finding_ids maps dedupe_key -> UUID of a row that pretends to
        # already be open. Lets us pin "re-open refreshes instead of inserts".
        self.preopen_finding_ids = preopen_finding_ids or {}

        self.upserts: list[dict[str, Any]] = []
        self.updates: list[dict[str, Any]] = []
        self.resolves: list[dict[str, Any]] = []

    def transaction(self) -> FakeEvalTx:
        return FakeEvalTx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions" in query:
            return self.session
        if "from snapshots" in query:
            return self.snapshot
        if "max(observed_at)" in query:
            return {"observed_at": self.observation_at}
        if "from camera_geometry_configs" in query:
            return {"?column?": 1} if self.has_saved_geometry else None
        if "from processor_statuses" in query:
            return self.processor_status
        if "from reliability_findings" in query and "for update" in query:
            dedupe_key = args[2]
            existing = self.preopen_finding_ids.get(dedupe_key)
            return {"id": existing} if existing is not None else None
        if "insert into reliability_findings" in query:
            row = _echo_insert(args)
            self.upserts.append(row)
            return row
        # Discriminator: the refresh UPDATE sets stream_session_id first; the
        # resolve UPDATE sets status. The RETURNING clauses both mention
        # `resolved_at`, so a naive substring check on "resolved" would
        # mismatch.
        is_refresh = "update reliability_findings" in query and "set stream_session_id" in query
        if is_refresh:
            # This is the open-row refresh UPDATE. Echo a row and capture.
            row = _echo_update(args)
            self.updates.append(row)
            return row
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "update reliability_findings" in query and "resolved" in query:
            self.resolves.append(
                {
                    "account_id": args[0],
                    "camera_id": args[1],
                    "dedupe_keys": list(args[2]),
                    "reason": args[3],
                }
            )
            return []
        return []


class FakeEvalTx:
    async def __aenter__(self) -> FakeEvalTx:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


def _echo_insert(args: tuple[Any, ...]) -> dict[str, Any]:
    """Build a plausible row from the INSERT args.

    Column order in _INSERT_OPEN_SQL:
      $1 account_id, $2 camera_id, $3 stream_session_id, $4 processor_status_id,
      $5 finding_type, $6 category, $7 severity, $8 title, $9 message,
      $10 likely_cause, $11 recommended_action, $12 evidence, $13 dedupe_key,
      $14 detector_version.
    """
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "camera_id": args[1],
        "stream_session_id": args[2],
        "processor_status_id": args[3],
        "finding_type": args[4],
        "category": args[5],
        "severity": args[6],
        "status": "open",
        "title": args[7],
        "message": args[8],
        "likely_cause": args[9],
        "recommended_action": args[10],
        "evidence": json.loads(args[11]) if isinstance(args[11], str) else args[11],
        "dedupe_key": args[12],
        "detector_version": args[13],
        "first_seen_at": now,
        "last_seen_at": now,
        "resolved_at": None,
        "resolved_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def _echo_update(args: tuple[Any, ...]) -> dict[str, Any]:
    """Build a row from the UPDATE args.

    Column order in _UPDATE_OPEN_SQL:
      $1 id, $2 account_id, $3 camera_id, $4 stream_session_id,
      $5 processor_status_id, $6 severity, $7 title, $8 message,
      $9 likely_cause, $10 recommended_action, $11 evidence, $12 detector_version.
    """
    now = datetime.now(tz=UTC)
    return {
        "id": args[0],
        "camera_id": args[2],
        "stream_session_id": args[3],
        "processor_status_id": args[4],
        "finding_type": "<refresh>",
        "category": "<refresh>",
        "severity": args[5],
        "status": "open",
        "title": args[6],
        "message": args[7],
        "likely_cause": args[8],
        "recommended_action": args[9],
        "evidence": json.loads(args[10]) if isinstance(args[10], str) else args[10],
        "dedupe_key": "<refresh>",
        "detector_version": args[11],
        "first_seen_at": now,
        "last_seen_at": now,
        "resolved_at": None,
        "resolved_reason": None,
        "created_at": now,
        "updated_at": now,
    }


# ---------------------------------------------------------------------------
# Builders.
# ---------------------------------------------------------------------------


def _session(*, state: str = "streaming", age_seconds: float = 5) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "state": state,
        "created_at": now - timedelta(seconds=age_seconds + 30),
        "started_at": now - timedelta(seconds=age_seconds + 30),
        "ended_at": None,
        "expires_at": now + timedelta(seconds=3600),
    }


def _snapshot(*, age_seconds: float) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "captured_at": now - timedelta(seconds=age_seconds),
        "created_at": now - timedelta(seconds=age_seconds),
        "session_id": None,
    }


def _processor_status(
    *,
    age_seconds: float = 5,
    applied_geometry: dict[str, Any] | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    payload: dict[str, Any] = {}
    if applied_geometry is not None:
        payload["applied_geometry"] = applied_geometry
    return {
        "id": uuid4(),
        "camera_id": uuid4(),
        "stream_session_id": uuid4(),
        "processor_id": "proc-1",
        "runtime": "python",
        "package_id": "pkg",
        "status": "processing",
        "fps_input": 30.0,
        "fps_processed": 29.8,
        "last_frame_at": now - timedelta(seconds=age_seconds),
        "last_observation_at": now - timedelta(seconds=age_seconds),
        "error_code": error_code,
        "error_message": None,
        "payload": payload,
        "created_at": now - timedelta(seconds=age_seconds + 60),
        "updated_at": now - timedelta(seconds=age_seconds),
    }


def _run(conn: FakeEvalConn, account_id: UUID, camera_id: UUID) -> None:
    asyncio.run(evaluate_camera(conn, account_id=account_id, camera_id=camera_id))


def _by_dedupe(upserts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {u["dedupe_key"]: u for u in upserts}


# ---------------------------------------------------------------------------
# stream_valid_processor_silent.
# ---------------------------------------------------------------------------


def test_stream_valid_processor_silent_triggers_when_stream_live_no_processor_status() -> None:
    """Session is streaming, but there's no processor status row at all and no
    observations — the worst case of the rule.
    """
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=10),
        observation_at=None,
        processor_status=None,
    )
    _run(conn, account, camera)
    keys = [u["dedupe_key"] for u in conn.upserts]
    silent_key = f"camera:{camera}:stream:stream_valid_processor_silent"
    assert silent_key in keys
    upsert = _by_dedupe(conn.upserts)[silent_key]
    assert upsert["severity"] == "critical"
    assert upsert["category"] == "stream"
    assert upsert["finding_type"] == "stream_valid_processor_silent"
    assert "processor_status_age_seconds" in upsert["evidence"]


def test_stream_valid_processor_silent_resolves_when_processor_fresh() -> None:
    """Session live + processor status & observation fresh → the rule resolves
    rather than going silent on a healthy run."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    silent_key = f"camera:{camera}:stream:stream_valid_processor_silent"
    assert all(u["dedupe_key"] != silent_key for u in conn.upserts)
    assert any(silent_key in r["dedupe_keys"] for r in conn.resolves)


def test_stream_valid_processor_silent_skipped_when_no_session_and_no_recent_frame() -> None:
    """No session AND no recent snapshot → insufficient data; rule does nothing
    (neither open nor resolve). This is how the "unknown" verdict survives."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=None,
        snapshot=None,
        observation_at=None,
        processor_status=None,
    )
    _run(conn, account, camera)
    silent_key = f"camera:{camera}:stream:stream_valid_processor_silent"
    assert all(u["dedupe_key"] != silent_key for u in conn.upserts)
    assert all(silent_key not in r["dedupe_keys"] for r in conn.resolves)


# ---------------------------------------------------------------------------
# latest_frame_stale.
# ---------------------------------------------------------------------------


def test_latest_frame_stale_triggers_when_snapshot_older_than_threshold() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=120),
        # Processor fresh — isolate the frame-stale rule.
        processor_status=_processor_status(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
    )
    _run(conn, account, camera)
    stale_key = f"camera:{camera}:stream:latest_frame_stale"
    assert stale_key in _by_dedupe(conn.upserts)
    upsert = _by_dedupe(conn.upserts)[stale_key]
    assert upsert["severity"] == "warning"
    assert upsert["evidence"]["latest_frame_age_seconds"] >= 120
    assert upsert["evidence"]["threshold_seconds"] == 60


def test_latest_frame_stale_resolves_when_fresh() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
    )
    _run(conn, account, camera)
    stale_key = f"camera:{camera}:stream:latest_frame_stale"
    assert all(u["dedupe_key"] != stale_key for u in conn.upserts)
    assert any(stale_key in r["dedupe_keys"] for r in conn.resolves)


def test_latest_frame_stale_skipped_without_snapshot() -> None:
    """No snapshot at all → can't claim staleness; rule emits nothing."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=None,
        processor_status=_processor_status(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
    )
    _run(conn, account, camera)
    stale_key = f"camera:{camera}:stream:latest_frame_stale"
    assert all(u["dedupe_key"] != stale_key for u in conn.upserts)
    assert all(stale_key not in r["dedupe_keys"] for r in conn.resolves)


def test_latest_frame_stale_skipped_when_session_not_live() -> None:
    """Session is `ended` → the camera is intentionally idle; stale frames are
    not a reliability problem in that state."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="ended"),
        snapshot=_snapshot(age_seconds=600),
        processor_status=_processor_status(age_seconds=600),
    )
    _run(conn, account, camera)
    stale_key = f"camera:{camera}:stream:latest_frame_stale"
    assert all(u["dedupe_key"] != stale_key for u in conn.upserts)
    assert all(stale_key not in r["dedupe_keys"] for r in conn.resolves)


# ---------------------------------------------------------------------------
# scene_too_dark_for_reliable_detection (stubbed).
# ---------------------------------------------------------------------------


def test_scene_too_dark_rule_is_a_no_op_for_now() -> None:
    """The brightness rule is reserved (M1). It must never insert or resolve a
    finding today regardless of fact shape — otherwise we lie about a
    capability we don't yet have."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:scene_quality:scene_too_dark_for_reliable_detection"
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    assert all(dark_key not in r["dedupe_keys"] for r in conn.resolves)


# ---------------------------------------------------------------------------
# processor_using_default_geometry.
# ---------------------------------------------------------------------------


def test_default_geometry_warns_when_saved_exists() -> None:
    """Processor is on default geometry AND a saved geometry row exists →
    severity=warning (the saved geometry is being ignored). This is the most
    common reliability surprise in P6 testing."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(
            age_seconds=5,
            applied_geometry={"source": "default", "line_count": 1, "roi_count": 1},
        ),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        has_saved_geometry=True,
    )
    _run(conn, account, camera)
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    upsert = _by_dedupe(conn.upserts)[geom_key]
    assert upsert["severity"] == "warning"
    assert upsert["category"] == "geometry_config"
    assert upsert["evidence"]["has_saved_geometry"] is True


def test_default_geometry_info_when_no_saved_row() -> None:
    """Processor on default + no saved geometry yet → severity=info (it's the
    first-run state, not a regression)."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(
            age_seconds=5,
            applied_geometry={"source": "default", "line_count": 1, "roi_count": 1},
        ),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        has_saved_geometry=False,
    )
    _run(conn, account, camera)
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    upsert = _by_dedupe(conn.upserts)[geom_key]
    assert upsert["severity"] == "info"


def test_default_geometry_warns_on_fallback_source() -> None:
    """`source=fallback_default_on_error` → operator-saved geometry was loaded
    but failed validation; severity=warning regardless of has_saved_geometry."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "fallback_default_on_error",
                "revision": 9,
                "error_reason": "geometry envelope has no enabled rois",
            },
        ),
    )
    _run(conn, account, camera)
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    upsert = _by_dedupe(conn.upserts)[geom_key]
    assert upsert["severity"] == "warning"
    assert upsert["evidence"]["applied_geometry_source"] == "fallback_default_on_error"
    assert upsert["evidence"]["error_reason"] == "geometry envelope has no enabled rois"


def test_default_geometry_warns_when_status_error_code_geometry_invalid() -> None:
    """error_code=geometry_invalid at the row level → degrade; the processor
    fell back to defaults. Severity=warning."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(age_seconds=5, error_code="geometry_invalid"),
    )
    _run(conn, account, camera)
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    upsert = _by_dedupe(conn.upserts)[geom_key]
    assert upsert["severity"] == "warning"
    assert upsert["evidence"]["error_code"] == "geometry_invalid"


def test_default_geometry_resolves_when_source_operator_saved() -> None:
    """`source=operator_saved` → the happy path; an existing open finding
    under this dedupe_key gets resolved."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "operator_saved",
                "revision": 9,
                "line_count": 2,
                "roi_count": 1,
            },
        ),
    )
    _run(conn, account, camera)
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    assert all(u["dedupe_key"] != geom_key for u in conn.upserts)
    assert any(geom_key in r["dedupe_keys"] for r in conn.resolves)


def test_default_geometry_skipped_when_applied_block_missing() -> None:
    """`applied_geometry` block absent → we can't tell what the processor is
    running on; rule does nothing (matches the dashboard's
    `missing_applied_geometry` panel state)."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        processor_status=_processor_status(age_seconds=5),  # payload empty.
    )
    _run(conn, account, camera)
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    assert all(u["dedupe_key"] != geom_key for u in conn.upserts)
    assert all(geom_key not in r["dedupe_keys"] for r in conn.resolves)


# ---------------------------------------------------------------------------
# Cross-cutting.
# ---------------------------------------------------------------------------


def test_detector_version_is_stamped_on_every_upsert() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=120),
        processor_status=_processor_status(
            age_seconds=5,
            applied_geometry={"source": "default", "line_count": 1, "roi_count": 1},
        ),
        has_saved_geometry=True,
    )
    _run(conn, account, camera)
    assert conn.upserts, "rule should have produced findings"
    for u in conn.upserts:
        assert u["detector_version"] == DETECTOR_VERSION


def test_reopen_takes_the_update_path_not_insert() -> None:
    """When an open row already exists under the dedupe_key, the evaluator
    must refresh it (UPDATE) rather than insert a second open row — that's the
    invariant the partial unique index enforces."""
    account, camera = uuid4(), uuid4()
    geom_key = f"camera:{camera}:geometry:processor_using_default_geometry"
    existing_id = uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        processor_status=_processor_status(
            age_seconds=5,
            applied_geometry={"source": "default", "line_count": 1, "roi_count": 1},
        ),
        has_saved_geometry=True,
        preopen_finding_ids={geom_key: existing_id},
    )
    _run(conn, account, camera)
    # No insert under that dedupe_key.
    assert all(u["dedupe_key"] != geom_key for u in conn.upserts)
    # Captured an UPDATE that touched the preopen row.
    assert any(u["id"] == existing_id for u in conn.updates)
