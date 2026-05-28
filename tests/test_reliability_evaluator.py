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

from upstream_api.config import get_settings
from upstream_api.services import snapshot_brightness as snapshot_brightness_module
from upstream_api.services.image_analysis import ImageBrightness
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
        # M2: latest_perception_snapshots row. The evaluator pulls confidence
        # values out of perception["snapshot"]["objects"]. Tests use the real
        # extractor end-to-end rather than monkeypatching it, so the fake
        # just needs to return the row.
        perception: dict[str, Any] | None = None,
        preopen_finding_ids: dict[str, UUID] | None = None,
    ) -> None:
        self.session = session
        self.snapshot = snapshot
        self.observation_at = observation_at
        self.processor_status = processor_status
        self.has_saved_geometry = has_saved_geometry
        self.perception = perception
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
        if "from latest_perception_snapshots" in query:
            return self.perception
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


def _snapshot(
    *,
    age_seconds: float,
    storage_key: str | None = None,
    width: int | None = None,
    height: int | None = None,
) -> dict[str, Any]:
    """Build a snapshot row.

    ``storage_key`` defaults to ``None`` so existing tests (which predate the
    brightness rule) still skip the brightness load — ``_maybe_load_brightness``
    treats a falsy storage_key as "nothing to evaluate". Brightness-rule tests
    pass an explicit storage_key (and use ``_brightness`` to inject the
    canned reading).
    """
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "captured_at": now - timedelta(seconds=age_seconds),
        "created_at": now - timedelta(seconds=age_seconds),
        "session_id": None,
        "storage_key": storage_key,
        "width": width,
        "height": height,
    }


def _install_brightness(monkeypatch: Any, reading: ImageBrightness | None) -> dict[str, int]:
    """Monkeypatch the brightness loader to return ``reading``.

    Returns a ``calls`` dict the caller can read post-test to confirm the
    loader was (or wasn't) invoked — important for the stale-frame
    short-circuit test.
    """
    counter = {"calls": 0}

    async def _stub(*, settings: Any, snapshot_id: Any, storage_key: str) -> Any:
        counter["calls"] += 1
        return reading

    monkeypatch.setattr(snapshot_brightness_module, "get_brightness_for_snapshot", _stub)
    return counter


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
    asyncio.run(
        evaluate_camera(
            conn,
            settings=get_settings(),
            account_id=account_id,
            camera_id=camera_id,
        )
    )


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


def test_stream_valid_processor_silent_skipped_when_session_expired() -> None:
    """A session whose ``expires_at`` is in the past does NOT count as live,
    even if ``state`` column still says "streaming". This is the M2.1 fix --
    state cleanup is best-effort and can lag the eligibility-gate by hours
    in practice; the reliability rule must not produce a critical finding
    for a workflow that has actually been over since yesterday."""
    account, camera = uuid4(), uuid4()
    now = datetime.now(tz=UTC)
    expired_session = {
        "id": uuid4(),
        "state": "streaming",
        "created_at": now - timedelta(hours=24),
        "started_at": now - timedelta(hours=24),
        "ended_at": None,
        # Expired hours ago.
        "expires_at": now - timedelta(hours=12),
    }
    conn = FakeEvalConn(
        session=expired_session,
        # Even a fresh-looking snapshot from the previous run would have
        # triggered the rule under the old logic via the snapshot_fresh
        # fallback; pin that we still fire on snapshot_fresh, but NOT on
        # state alone for an expired session.
        snapshot=None,
        observation_at=None,
        processor_status=None,
    )
    _run(conn, account, camera)
    silent_key = f"camera:{camera}:stream:stream_valid_processor_silent"
    assert all(u["dedupe_key"] != silent_key for u in conn.upserts)


def test_stream_valid_processor_silent_skipped_when_session_ended() -> None:
    """``ended_at`` set means the session is over -- not live -- regardless
    of what the ``state`` column says."""
    account, camera = uuid4(), uuid4()
    now = datetime.now(tz=UTC)
    ended_session = {
        "id": uuid4(),
        "state": "streaming",  # stale state column
        "created_at": now - timedelta(hours=2),
        "started_at": now - timedelta(hours=2),
        "ended_at": now - timedelta(minutes=15),  # ended 15 min ago
        "expires_at": now + timedelta(hours=1),
    }
    conn = FakeEvalConn(
        session=ended_session,
        snapshot=None,
        observation_at=None,
        processor_status=None,
    )
    _run(conn, account, camera)
    silent_key = f"camera:{camera}:stream:stream_valid_processor_silent"
    assert all(u["dedupe_key"] != silent_key for u in conn.upserts)


def test_stream_valid_processor_silent_copy_does_not_say_stream_is_live() -> None:
    """The rule's title/message must not imply a "live video player" --
    operators read "Stream is live" as "I should see live video" and get
    confused when their WebRTC viewer is empty. The actual claim is about
    upload activity, not WebRTC playability."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=10),
        observation_at=None,
        processor_status=None,
    )
    _run(conn, account, camera)
    silent_key = f"camera:{camera}:stream:stream_valid_processor_silent"
    upsert = _by_dedupe(conn.upserts)[silent_key]
    title_lc = upsert["title"].lower()
    message_lc = upsert["message"].lower()
    # Negative assertions: no "stream is live", no "stream appears live".
    assert "stream is live" not in title_lc
    assert "stream is live" not in message_lc
    assert "stream appears live" not in message_lc
    # Positive assertions: copy talks about frame upload, not playability.
    assert "sending frames" in title_lc or "uploading frames" in message_lc


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


# ---------------------------------------------------------------------------
# scene_too_dark_for_reliable_detection (M1 — first production CV signal).
# ---------------------------------------------------------------------------


_DARK_KEY_SUFFIX = "scene_quality:scene_too_dark_for_reliable_detection"


def test_scene_too_dark_triggers_on_dark_snapshot(monkeypatch: Any) -> None:
    """Brightness 20 < threshold 45 → warning opens with the canonical evidence
    shape and the operator-facing copy from the brief."""
    account, camera = uuid4(), uuid4()
    _install_brightness(
        monkeypatch,
        ImageBrightness(mean=20.0, width=1280, height=720),
    )
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5, storage_key="acct/cam/d/k.jpg"),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    upserts = _by_dedupe(conn.upserts)
    assert dark_key in upserts
    upsert = upserts[dark_key]
    assert upsert["finding_type"] == "scene_too_dark_for_reliable_detection"
    assert upsert["category"] == "scene_quality"
    assert upsert["severity"] == "warning"
    assert upsert["likely_cause"] == "low_light"

    ev = upsert["evidence"]
    assert ev["brightness_mean"] == 20.0
    assert ev["brightness_threshold"] == 45
    assert ev["image_width"] == 1280
    assert ev["image_height"] == 720
    assert ev["source"] == "latest_frame_snapshot"
    assert "snapshot_id" in ev
    assert "captured_at" in ev

    # Pin the no-secrets posture: no storage URLs / keys / bucket names.
    leaky_substrings = ("http://", "https://", "supabase", "acct/cam", "service_role")
    serialized = json.dumps(ev).lower()
    for s in leaky_substrings:
        assert s not in serialized, ev


def test_scene_too_dark_resolves_when_brightness_recovers(monkeypatch: Any) -> None:
    """Bright snapshot → _RuleResolved with the brightness-specific reason."""
    account, camera = uuid4(), uuid4()
    _install_brightness(
        monkeypatch,
        ImageBrightness(mean=180.0, width=1280, height=720),
    )
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5, storage_key="acct/cam/d/k.jpg"),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    # The resolve call used the brightness-specific reason, not bulk "healthy".
    matching = [
        r
        for r in conn.resolves
        if dark_key in r["dedupe_keys"] and r["reason"] == "brightness_above_threshold"
    ]
    assert matching, conn.resolves


def test_scene_too_dark_skipped_when_snapshot_is_stale(monkeypatch: Any) -> None:
    """Stale frame → brightness load is bypassed entirely. The stale-frame rule
    owns this case; warning about darkness on out-of-date evidence would be
    lying about current scene state."""
    account, camera = uuid4(), uuid4()
    calls = _install_brightness(
        monkeypatch,
        ImageBrightness(mean=10.0, width=1, height=1),
    )
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=600, storage_key="acct/cam/d/k.jpg"),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    # No finding either way.
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    assert all(dark_key not in r["dedupe_keys"] for r in conn.resolves)
    # And critically: the loader was not invoked. The download cost is paid
    # only when there's a fresh-enough snapshot to warn about.
    assert calls["calls"] == 0


def test_scene_too_dark_skipped_when_brightness_loader_returns_none(
    monkeypatch: Any,
) -> None:
    """Download or decode failed → None → rule does nothing. The summary
    endpoint must not 500, and we must not flip a finding closed based on
    missing data."""
    account, camera = uuid4(), uuid4()
    _install_brightness(monkeypatch, None)
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5, storage_key="acct/cam/d/k.jpg"),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    assert all(dark_key not in r["dedupe_keys"] for r in conn.resolves)


def test_scene_too_dark_skipped_when_no_snapshot(monkeypatch: Any) -> None:
    account, camera = uuid4(), uuid4()
    calls = _install_brightness(monkeypatch, ImageBrightness(mean=10.0, width=1, height=1))
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=None,
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    # Loader never even called.
    assert calls["calls"] == 0


def test_scene_too_dark_skipped_when_snapshot_missing_storage_key(
    monkeypatch: Any,
) -> None:
    """Defensive: a snapshot row missing storage_key (unusual in prod, common
    in legacy test stubs) must not crash the evaluator or invoke the
    loader."""
    account, camera = uuid4(), uuid4()
    calls = _install_brightness(monkeypatch, ImageBrightness(mean=10.0, width=1, height=1))
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5, storage_key=None),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
    )
    _run(conn, account, camera)
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    assert calls["calls"] == 0


def test_scene_too_dark_dedupes_via_refresh_path(monkeypatch: Any) -> None:
    """An existing open dark-scene finding under the same dedupe_key must be
    refreshed (UPDATE), not duplicated. Same invariant the partial unique
    index enforces."""
    account, camera = uuid4(), uuid4()
    _install_brightness(monkeypatch, ImageBrightness(mean=15.0, width=640, height=480))
    dark_key = f"camera:{camera}:{_DARK_KEY_SUFFIX}"
    existing_id = uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5, storage_key="acct/cam/d/k.jpg"),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        preopen_finding_ids={dark_key: existing_id},
    )
    _run(conn, account, camera)
    assert all(u["dedupe_key"] != dark_key for u in conn.upserts)
    assert any(u["id"] == existing_id for u in conn.updates)


def test_detector_version_pins_to_current_milestone() -> None:
    """Detector version bumps every time a shipped rule's behavior changes,
    so historical rows stay attributable. v0.1 was M0 plumbing, v0.2 added
    M1 brightness for real, v0.3 added M2 unstable_detection_confidence,
    v0.4 tightened _is_session_live + reworded stream_valid_processor_silent
    away from misleading 'stream is live' language."""
    assert DETECTOR_VERSION == "reliability.v0.4"


# ---------------------------------------------------------------------------
# unstable_detection_confidence (M2 — first detection-quality signal).
# ---------------------------------------------------------------------------


_CONFIDENCE_KEY_SUFFIX = "detection:unstable_detection_confidence"


def _perception(
    *,
    confidences: list[float],
    age_seconds: float = 5,
    snapshot_id: UUID | None = None,
    stream_session_id: UUID | None = None,
    snapshot_extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a latest_perception_snapshots row.

    ``confidences`` populates ``snapshot.objects[].confidence`` so the real
    extractor + stats path runs end-to-end inside the evaluator. Per-detection
    extras (track_id, class_id, label, bbox) are omitted to keep the test
    payload tight — the extractor's anti-test in test_detection_confidence
    already pins that those fields aren't mistaken for confidence.
    """
    now = datetime.now(tz=UTC)
    payload: dict[str, Any] = {
        "objects": [{"confidence": c} for c in confidences],
    }
    if snapshot_extras:
        payload.update(snapshot_extras)
    return {
        "id": snapshot_id or uuid4(),
        "camera_id": uuid4(),
        "stream_session_id": stream_session_id or uuid4(),
        "processor_id": "proc-1",
        "package_id": "pkg",
        "snapshot": payload,
        "observed_at": now - timedelta(seconds=age_seconds),
        "updated_at": now,
    }


def test_unstable_confidence_opens_on_low_avg() -> None:
    """8 detections clustered at ~0.30 → avg below threshold → warning opens
    with full evidence."""
    account, camera = uuid4(), uuid4()
    snapshot_id = uuid4()
    session_id = uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            confidences=[0.30, 0.32, 0.28, 0.35, 0.31, 0.29, 0.33, 0.30],
            snapshot_id=snapshot_id,
            stream_session_id=session_id,
        ),
    )
    _run(conn, account, camera)
    upserts = _by_dedupe(conn.upserts)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert key in upserts
    u = upserts[key]
    assert u["finding_type"] == "unstable_detection_confidence"
    assert u["category"] == "detection"
    assert u["severity"] == "warning"
    assert u["likely_cause"] == "weak_or_unstable_model_confidence"
    ev = u["evidence"]
    assert ev["confidence_sample_count"] == 8
    assert ev["avg_confidence"] < 0.45
    assert ev["source"] == "latest_perception_snapshot"
    assert ev["snapshot_id"] == str(snapshot_id)
    assert ev["captured_at"] is not None
    # M3 reserves these as null; the schema is stable.
    assert ev["previous_avg_confidence"] is None
    assert ev["drop_ratio"] is None
    # No secrets leak.
    serialized = json.dumps(ev).lower()
    for leak in ("http", "supabase", "service_role", "bucket", "storage_key"):
        assert leak not in serialized, ev


def test_unstable_confidence_opens_on_high_stddev() -> None:
    """Bimodal payload (some very high, some very low) trips the stddev
    threshold even when avg sits near the middle."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            confidences=[0.95, 0.90, 0.10, 0.15, 0.92, 0.08, 0.94, 0.05],
        ),
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    u = _by_dedupe(conn.upserts)[key]
    assert u["evidence"]["confidence_stddev"] > 0.20


def test_unstable_confidence_opens_on_high_low_ratio() -> None:
    """More than half of samples below threshold trips the low-ratio path
    even when avg sits near the boundary."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            # 5 below 0.45, 1 above -> low_ratio 5/6 ~= 0.83
            confidences=[0.10, 0.20, 0.30, 0.40, 0.42, 0.85],
        ),
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    u = _by_dedupe(conn.upserts)[key]
    assert u["evidence"]["low_confidence_ratio"] > 0.50


def test_unstable_confidence_resolves_when_healthy() -> None:
    """8 detections at 0.75-0.95 → all three thresholds pass → resolve with
    the rule-specific reason."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            confidences=[0.75, 0.80, 0.82, 0.85, 0.88, 0.90, 0.92, 0.95],
        ),
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != key for u in conn.upserts)
    matching = [
        r
        for r in conn.resolves
        if key in r["dedupe_keys"] and r["reason"] == "detection_confidence_stabilized"
    ]
    assert matching, conn.resolves


def test_unstable_confidence_skipped_when_too_few_samples() -> None:
    """Fewer than MIN_SAMPLE_COUNT detections → rule returns None.

    Critical that this is NOT a resolve: a model that has stopped producing
    detections should not silently close a previously-opened unstable-
    confidence finding."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(confidences=[0.1, 0.2, 0.3]),
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != key for u in conn.upserts)
    assert all(key not in r["dedupe_keys"] for r in conn.resolves)


def test_unstable_confidence_skipped_when_no_perception() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=None,
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != key for u in conn.upserts)
    assert all(key not in r["dedupe_keys"] for r in conn.resolves)


def test_unstable_confidence_skipped_when_perception_is_stale() -> None:
    """Stale perception → fact gatherer bypasses stats compute → rule
    returns None. The latest_frame_stale rule owns the staleness case."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=600),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            confidences=[0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
            age_seconds=600,
        ),
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != key for u in conn.upserts)
    assert all(key not in r["dedupe_keys"] for r in conn.resolves)


def test_unstable_confidence_skipped_when_objects_empty() -> None:
    """No detections in the frame → no confidence samples → rule returns
    None. A camera that isn't seeing anything isn't a confidence problem."""
    account, camera = uuid4(), uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(confidences=[]),
    )
    _run(conn, account, camera)
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert all(u["dedupe_key"] != key for u in conn.upserts)
    assert all(key not in r["dedupe_keys"] for r in conn.resolves)


def test_unstable_confidence_coexists_with_dark_scene(monkeypatch: Any) -> None:
    """A dark frame AND unstable confidence are two different signals worth
    surfacing. The brief permits both opening; pin that they do."""
    account, camera = uuid4(), uuid4()
    _install_brightness(monkeypatch, ImageBrightness(mean=10.0, width=1280, height=720))
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5, storage_key="acct/cam/d/k.jpg"),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            confidences=[0.20, 0.25, 0.30, 0.22, 0.28, 0.24, 0.21, 0.26],
        ),
    )
    _run(conn, account, camera)
    keys = {u["dedupe_key"] for u in conn.upserts}
    dark_key = f"camera:{camera}:scene_quality:scene_too_dark_for_reliable_detection"
    conf_key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    assert dark_key in keys
    assert conf_key in keys


def test_unstable_confidence_dedupe_refreshes_existing(monkeypatch: Any) -> None:
    """A pre-existing open finding under the same dedupe_key must refresh
    (UPDATE), not insert a second row."""
    account, camera = uuid4(), uuid4()
    key = f"camera:{camera}:{_CONFIDENCE_KEY_SUFFIX}"
    existing_id = uuid4()
    conn = FakeEvalConn(
        session=_session(state="streaming"),
        snapshot=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status=_processor_status(age_seconds=5),
        perception=_perception(
            confidences=[0.20, 0.25, 0.30, 0.22, 0.28, 0.24, 0.21, 0.26],
        ),
        preopen_finding_ids={key: existing_id},
    )
    _run(conn, account, camera)
    assert all(u["dedupe_key"] != key for u in conn.upserts)
    assert any(u["id"] == existing_id for u in conn.updates)
