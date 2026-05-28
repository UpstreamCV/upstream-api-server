"""Reliability endpoints — route-level tests.

Mirrors test_processor_status_latest.py's posture: TestClient + a fake asyncpg
pool, no real DB. The fake conn here is richer because the reliability router
runs the full evaluator on read, but the structure (FakePool wrapping a conn,
dependency_overrides for the caller) is identical.

We assert on the summary/findings response shapes and on the categorical roll-
up; the per-rule trigger logic lives in test_reliability_evaluator.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller

# ---------------------------------------------------------------------------
# Fake pool plumbing.
# ---------------------------------------------------------------------------


class FakeTx:
    async def __aenter__(self) -> FakeTx:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
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


class FakeReliabilityConn:
    """Combined fake for the camera-ownership probe, the evaluator's fact
    queries, the upsert/resolve writes, and the open-findings list.

    Constructor args drive per-query behavior; all writes are captured so a
    test can assert on what the rule layer decided.
    """

    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        session_row: dict[str, Any] | None = None,
        snapshot_row: dict[str, Any] | None = None,
        observation_at: datetime | None = None,
        processor_status_row: dict[str, Any] | None = None,
        has_saved_geometry: bool = False,
        # M2: latest_perception_snapshots row driving the
        # unstable_detection_confidence rule.
        perception_row: dict[str, Any] | None = None,
        # Initial open findings the evaluator will see + the list endpoint will
        # return after evaluation. The evaluator's upserts mutate this map by
        # dedupe_key so re-opens look like UPDATE not INSERT.
        initial_open_findings: list[dict[str, Any]] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.session_row = session_row
        self.snapshot_row = snapshot_row
        self.observation_at = observation_at
        self.processor_status_row = processor_status_row
        self.has_saved_geometry = has_saved_geometry
        self.perception_row = perception_row
        # Indexed by dedupe_key so the evaluator's lookup-for-update returns
        # an existing id when one is present.
        self.open_findings: dict[str, dict[str, Any]] = {
            f["dedupe_key"]: f for f in (initial_open_findings or [])
        }
        self.resolves: list[dict[str, Any]] = []

    def transaction(self) -> FakeTx:
        return FakeTx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        # Camera-ownership probe in the router.
        if "from cameras where id" in query:
            if self.camera_account_id is None:
                return None
            return {"account_id": self.camera_account_id}

        # Evaluator fact queries.
        if "from stream_sessions" in query:
            return self.session_row
        if "from snapshots" in query:
            return self.snapshot_row
        if "max(observed_at)" in query:
            return {"observed_at": self.observation_at}
        if "from camera_geometry_configs" in query:
            return {"?column?": 1} if self.has_saved_geometry else None
        if "from processor_statuses" in query:
            return self.processor_status_row
        if "from latest_perception_snapshots" in query:
            return self.perception_row

        # Reliability repo: lookup-for-update.
        if "from reliability_findings" in query and "for update" in query:
            dedupe_key = args[2]
            existing = self.open_findings.get(dedupe_key)
            return {"id": existing["id"]} if existing else None

        # Reliability repo: INSERT new open.
        if "insert into reliability_findings" in query:
            row = _row_from_insert_args(args)
            self.open_findings[row["dedupe_key"]] = row
            return row

        # Reliability repo: UPDATE open (refresh). Discriminator is the
        # `set stream_session_id` lead — the resolve UPDATE starts with
        # `set status`. The RETURNING clause includes `resolved_at`, so a
        # naive `"resolved" not in query` check would mismatch.
        if "update reliability_findings" in query and "set stream_session_id" in query:
            return self._refresh_existing(args)

        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        # Repo: list open for camera (severity-first ordering).
        if "from reliability_findings" in query and "status     = 'open'" in query:
            limit = int(args[2])
            rows = [r for r in self.open_findings.values() if r["status"] == "open"]
            rows.sort(
                key=lambda r: (
                    _SEV_RANK[r["severity"]],
                    -int(r["last_seen_at"].timestamp()),
                )
            )
            return rows[:limit]

        # M4: list by status (newest-resolved-first ordering, used by the
        # Reliability tab's "Recently resolved" section).
        if "from reliability_findings" in query and "and status     = $3" in query:
            wanted_status = args[2]
            limit = int(args[3])
            rows = [r for r in self.open_findings.values() if r["status"] == wanted_status]
            rows.sort(
                key=lambda r: -int((r.get("resolved_at") or r["last_seen_at"]).timestamp()),
            )
            return rows[:limit]

        # Repo: resolve open by dedupe_keys.
        if "update reliability_findings" in query and "resolved" in query:
            keys = list(args[2])
            self.resolves.append({"keys": keys, "reason": args[3]})
            for k in keys:
                row = self.open_findings.get(k)
                if row is not None and row["status"] == "open":
                    row["status"] = "resolved"
                    row["resolved_at"] = datetime.now(tz=UTC)
                    row["resolved_reason"] = args[3]
            return []

        return []

    def _refresh_existing(self, args: tuple[Any, ...]) -> dict[str, Any] | None:
        finding_id = args[0]
        for row in self.open_findings.values():
            if row["id"] == finding_id:
                row["severity"] = args[5]
                row["title"] = args[6]
                row["message"] = args[7]
                row["likely_cause"] = args[8]
                row["recommended_action"] = args[9]
                # Keep as a JSON string to mirror asyncpg's JSONB return; see
                # comment in _row_from_insert_args.
                row["evidence"] = args[10] if isinstance(args[10], str) else json.dumps(args[10])
                row["last_seen_at"] = datetime.now(tz=UTC)
                return row
        return None


_SEV_RANK = {"critical": 0, "warning": 1, "info": 2}


def _row_from_insert_args(args: tuple[Any, ...]) -> dict[str, Any]:
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
        # Store evidence the same way asyncpg returns JSONB to the reader:
        # as a JSON string. The previous "deserialize back to dict" was a
        # test-only shortcut that masked a real production bug — the
        # response builder was checking ``isinstance(..., dict) else {}`` and
        # silently dropping every finding's evidence in prod (where asyncpg
        # gives a str, not a dict).
        "evidence": args[11] if isinstance(args[11], str) else json.dumps(args[11]),
        "dedupe_key": args[12],
        "detector_version": args[13],
        "first_seen_at": now,
        "last_seen_at": now,
        "resolved_at": None,
        "resolved_reason": None,
        "created_at": now,
        "updated_at": now,
    }


# ---------------------------------------------------------------------------
# Client builders.
# ---------------------------------------------------------------------------


def _caller_client(conn: Any, *, account_id: UUID) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def _caller_client_no_auth(conn: Any) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Fact builders.
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

    ``storage_key`` is optional so the existing pre-M1 endpoint tests (which
    don't care about brightness) still skip the brightness load entirely —
    ``_maybe_load_brightness`` treats a falsy storage_key as "nothing to
    evaluate". M1's dark-scene tests pass an explicit storage_key and stub
    the loader."""
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


_SUMMARY_PATH = "/v1/cameras/{camera_id}/reliability/summary"
_FINDINGS_PATH = "/v1/cameras/{camera_id}/reliability/findings"


# ===========================================================================
# Auth + scoping.
# ===========================================================================


def test_unauthenticated_summary_returns_401() -> None:
    conn = FakeReliabilityConn(camera_account_id=uuid4())
    r = _caller_client_no_auth(conn).get(_SUMMARY_PATH.format(camera_id=uuid4()))
    assert r.status_code == 401


def test_cross_account_summary_returns_camera_not_found() -> None:
    """Same security posture as the processor-status endpoint: unknown camera
    and other-account camera look identical to deny enumeration."""
    caller_account, camera_owner = uuid4(), uuid4()
    conn = FakeReliabilityConn(camera_account_id=camera_owner)
    r = _caller_client(conn, account_id=caller_account).get(_SUMMARY_PATH.format(camera_id=uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_unknown_camera_summary_returns_camera_not_found() -> None:
    account = uuid4()
    conn = FakeReliabilityConn(camera_account_id=None)
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=uuid4()))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"


def test_findings_endpoint_also_404s_on_cross_account() -> None:
    caller_account, camera_owner = uuid4(), uuid4()
    conn = FakeReliabilityConn(camera_account_id=camera_owner)
    r = _caller_client(conn, account_id=caller_account).get(
        _FINDINGS_PATH.format(camera_id=uuid4())
    )
    assert r.status_code == 404


# ===========================================================================
# Summary status verdicts.
# ===========================================================================


def test_summary_healthy_when_no_findings_and_signal_present() -> None:
    """Fresh processor status + fresh snapshot + no rule trips → healthy."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "operator_saved",
                "revision": 9,
                "line_count": 2,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "healthy"
    assert body["highest_severity"] is None
    assert body["open_findings_count"] == 0
    assert body["top_findings"] == []
    assert body["recommended_next_action"] is None
    # last_evaluated_at is always populated, even when healthy.
    assert body["last_evaluated_at"] is not None


def test_summary_unknown_when_no_recent_signals() -> None:
    """No session, no snapshot, no processor status → we don't know enough to
    say healthy. Verdict is `unknown`, not `healthy`."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(camera_account_id=account)
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "unknown"
    assert body["open_findings_count"] == 0


def test_summary_needs_review_when_a_warning_is_open() -> None:
    """Default-geometry while a saved row exists → warning. The summary rolls
    up to `needs_review` and the top finding's recommended action is exposed."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "default",
                "revision": None,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "needs_review"
    assert body["highest_severity"] == "warning"
    assert body["open_findings_count"] == 1
    top = body["top_findings"][0]
    assert top["finding_type"] == "processor_using_default_geometry"
    assert top["severity"] == "warning"
    assert body["recommended_next_action"] == top["recommended_action"]


def test_summary_critical_when_critical_and_warning_open() -> None:
    """Critical outranks warning in the roll-up; recommended action comes from
    the critical finding, not the warning."""
    account, camera = uuid4(), uuid4()
    # Stream live, processor silent (critical) + on default geometry (warning).
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=None,
        processor_status_row=_processor_status(
            age_seconds=300,  # stale enough to silently-trigger
            applied_geometry={
                "source": "default",
                "revision": None,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "critical"
    assert body["highest_severity"] == "critical"
    # The processor-using-default-geometry rule is skipped here because the
    # processor status is too stale to claim what geometry it's running on —
    # so only the critical fires. Pin that.
    assert body["open_findings_count"] == 1
    assert body["top_findings"][0]["finding_type"] == "stream_valid_processor_silent"


# ===========================================================================
# Findings endpoint.
# ===========================================================================


def test_findings_endpoint_returns_open_findings() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),  # triggers latest_frame_stale
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    r = _caller_client(conn, account_id=account).get(_FINDINGS_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    types = [f["finding_type"] for f in body["findings"]]
    assert "latest_frame_stale" in types
    assert body["last_evaluated_at"] is not None


def test_findings_endpoint_respects_limit() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),
        processor_status_row=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "default",
                "revision": None,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    r = _caller_client(conn, account_id=account).get(
        _FINDINGS_PATH.format(camera_id=camera) + "?limit=1"
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["findings"]) == 1
    # Severity ordering — both findings are `warning`; the response just caps.


# ===========================================================================
# Dedupe + resolve behavior.
# ===========================================================================


def test_repeat_evaluation_refreshes_instead_of_inserting() -> None:
    """Two reads under the same conditions must not stack up two open rows
    under the same dedupe_key. The second call refreshes."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    client = _caller_client(conn, account_id=account)
    client.get(_SUMMARY_PATH.format(camera_id=camera))
    client.get(_SUMMARY_PATH.format(camera_id=camera))
    open_rows = [r for r in conn.open_findings.values() if r["status"] == "open"]
    keys = [r["dedupe_key"] for r in open_rows]
    # No duplicate open rows under the same dedupe_key.
    assert len(keys) == len(set(keys))


def test_returning_to_healthy_resolves_open_findings() -> None:
    """First read trips latest_frame_stale; the snapshot then goes fresh and a
    second read must flip the open row to resolved."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    client = _caller_client(conn, account_id=account)
    client.get(_SUMMARY_PATH.format(camera_id=camera))
    assert any(
        r["finding_type"] == "latest_frame_stale" and r["status"] == "open"
        for r in conn.open_findings.values()
    )
    # Heal the snapshot and re-evaluate.
    conn.snapshot_row = _snapshot(age_seconds=5)
    client.get(_SUMMARY_PATH.format(camera_id=camera))
    open_types = [r["finding_type"] for r in conn.open_findings.values() if r["status"] == "open"]
    assert "latest_frame_stale" not in open_types
    # And we recorded the resolve call.
    assert any(any("latest_frame_stale" in k for k in r["keys"]) for r in conn.resolves)


# ===========================================================================
# scene_too_dark_for_reliable_detection (M1 — first production CV signal).
# ===========================================================================


_DARK_TYPE = "scene_too_dark_for_reliable_detection"


def _install_brightness(monkeypatch: Any, reading: Any) -> dict[str, int]:
    """Patch the brightness loader through the evaluator's import alias.

    The evaluator imports the module as ``snapshot_brightness_module`` and
    calls ``snapshot_brightness_module.get_brightness_for_snapshot(...)`` —
    patching the attribute on that module is what the rule actually sees.
    """
    from upstream_api.services import (
        snapshot_brightness as snapshot_brightness_module,
    )

    counter = {"calls": 0}

    async def _stub(*, settings: Any, snapshot_id: Any, storage_key: str) -> Any:
        counter["calls"] += 1
        return reading

    monkeypatch.setattr(snapshot_brightness_module, "get_brightness_for_snapshot", _stub)
    return counter


def test_dark_scene_summary_returns_needs_review(monkeypatch: Any) -> None:
    """Dark snapshot → summary status=needs_review, top finding is dark-scene
    with the full evidence shape."""
    from upstream_api.services.image_analysis import ImageBrightness

    account, camera = uuid4(), uuid4()
    _install_brightness(monkeypatch, ImageBrightness(mean=22.0, width=1280, height=720))
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(
            age_seconds=5, storage_key="acct/cam/d/k.jpg", width=1280, height=720
        ),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "needs_review"
    assert body["highest_severity"] == "warning"
    top = body["top_findings"][0]
    assert top["finding_type"] == _DARK_TYPE
    assert top["category"] == "scene_quality"
    assert top["likely_cause"] == "low_light"

    ev = top["evidence"]
    assert ev["brightness_mean"] == 22.0
    assert ev["brightness_threshold"] == 45
    assert ev["image_width"] == 1280
    assert ev["image_height"] == 720
    assert ev["source"] == "latest_frame_snapshot"

    # Recommended next action mirrors the finding's recommendation.
    assert body["recommended_next_action"] == top["recommended_action"]


def test_stale_frame_short_circuits_dark_scene(monkeypatch: Any) -> None:
    """When the latest frame is stale, only ``latest_frame_stale`` should open
    — even if the brightness loader would have reported "dark". Otherwise
    we'd be warning about scene quality from out-of-date evidence."""
    from upstream_api.services.image_analysis import ImageBrightness

    account, camera = uuid4(), uuid4()
    calls = _install_brightness(monkeypatch, ImageBrightness(mean=5.0, width=1, height=1))
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(
            age_seconds=600, storage_key="acct/cam/d/k.jpg", width=640, height=480
        ),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    types = {f["finding_type"] for f in body["top_findings"]}
    assert "latest_frame_stale" in types
    assert _DARK_TYPE not in types
    # The brightness loader was never called — the precondition skipped it.
    assert calls["calls"] == 0


def test_brightness_loader_failure_does_not_crash_summary(monkeypatch: Any) -> None:
    """Loader returns None (download/decode failure) → summary is healthy
    instead of needs_review, no 500."""
    account, camera = uuid4(), uuid4()
    _install_brightness(monkeypatch, None)
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5, storage_key="acct/cam/d/k.jpg", width=1, height=1),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "operator_saved",
                "revision": 1,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "healthy"


def test_dark_scene_findings_endpoint_lists_the_finding(monkeypatch: Any) -> None:
    """Findings endpoint surfaces the dark-scene row with the full record
    shape."""
    from upstream_api.services.image_analysis import ImageBrightness

    account, camera = uuid4(), uuid4()
    _install_brightness(monkeypatch, ImageBrightness(mean=10.0, width=320, height=240))
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(
            age_seconds=5, storage_key="acct/cam/d/k.jpg", width=320, height=240
        ),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    r = _caller_client(conn, account_id=account).get(_FINDINGS_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    types = [f["finding_type"] for f in body["findings"]]
    assert _DARK_TYPE in types


# ===========================================================================
# unstable_detection_confidence (M2 — first detection-quality signal).
# ===========================================================================


_CONFIDENCE_TYPE = "unstable_detection_confidence"


def _perception_row(
    *,
    confidences: list[float],
    age_seconds: float = 5,
) -> dict[str, Any]:
    """latest_perception_snapshots row with ``objects[].confidence`` populated."""
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "camera_id": uuid4(),
        "stream_session_id": uuid4(),
        "processor_id": "proc-1",
        "package_id": "pkg",
        "snapshot": {"objects": [{"confidence": c} for c in confidences]},
        "observed_at": now - timedelta(seconds=age_seconds),
        "updated_at": now,
    }


def test_unstable_confidence_summary_returns_needs_review() -> None:
    """8 detections at avg ~0.30 → summary rolls up to needs_review with the
    confidence finding at the top and full evidence wired through coerce_jsonb."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
        perception_row=_perception_row(
            confidences=[0.30, 0.28, 0.32, 0.35, 0.31, 0.29, 0.33, 0.30],
        ),
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "needs_review"
    assert body["highest_severity"] == "warning"
    top = body["top_findings"][0]
    assert top["finding_type"] == _CONFIDENCE_TYPE
    assert top["category"] == "detection"
    assert top["severity"] == "warning"
    assert top["likely_cause"] == "weak_or_unstable_model_confidence"
    ev = top["evidence"]
    # Evidence must actually round-trip (the post-M1 coerce_jsonb fix).
    assert ev["confidence_sample_count"] == 8
    assert ev["avg_confidence"] < 0.45
    assert ev["source"] == "latest_perception_snapshot"
    assert "thresholds" in ev
    assert ev["previous_avg_confidence"] is None
    assert ev["drop_ratio"] is None


def test_healthy_confidence_does_not_open_finding() -> None:
    """All thresholds pass → no confidence finding opens; summary stays
    healthy (no other findings active)."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "operator_saved",
                "revision": 1,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
        perception_row=_perception_row(
            confidences=[0.75, 0.80, 0.85, 0.88, 0.90, 0.82, 0.87, 0.92],
        ),
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    body = r.json()
    assert body["status"] == "healthy"
    assert body["open_findings_count"] == 0


def test_critical_outranks_confidence_warning_in_summary() -> None:
    """A critical stream finding still wins the highest_severity slot when
    a warning-severity confidence finding is also open."""
    account, camera = uuid4(), uuid4()
    # Processor status stale -> stream_valid_processor_silent (critical),
    # plus low-confidence detections -> unstable_detection_confidence
    # (warning).
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=None,
        processor_status_row=_processor_status(age_seconds=600),
        perception_row=_perception_row(
            confidences=[0.20, 0.22, 0.25, 0.28, 0.30, 0.32, 0.26, 0.24],
        ),
    )
    r = _caller_client(conn, account_id=account).get(_SUMMARY_PATH.format(camera_id=camera))
    body = r.json()
    assert body["status"] == "critical"
    assert body["highest_severity"] == "critical"
    # Both findings should be open in the underlying table.
    open_types = {f["finding_type"] for f in conn.open_findings.values() if f["status"] == "open"}
    assert "stream_valid_processor_silent" in open_types
    assert _CONFIDENCE_TYPE in open_types
    # And the critical wins the top slot.
    assert body["top_findings"][0]["severity"] == "critical"


def test_confidence_finding_appears_in_findings_endpoint() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
        perception_row=_perception_row(
            confidences=[0.10, 0.20, 0.25, 0.30, 0.35, 0.40, 0.42, 0.45],
        ),
    )
    r = _caller_client(conn, account_id=account).get(_FINDINGS_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    types = [f["finding_type"] for f in r.json()["findings"]]
    assert _CONFIDENCE_TYPE in types


# ===========================================================================
# ?status= query param (M4 Reliability tab "Recently resolved").
# ===========================================================================


def test_findings_default_returns_open_only() -> None:
    """Backward compat: omitting status returns open findings, matching the
    M0 contract the Monitor card depends on."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),  # triggers latest_frame_stale
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    client = _caller_client(conn, account_id=account)
    # First read opens the finding.
    client.get(_FINDINGS_PATH.format(camera_id=camera))
    # Heal the frame to resolve.
    conn.snapshot_row = _snapshot(age_seconds=5)
    client.get(_FINDINGS_PATH.format(camera_id=camera))
    # Default (open) should now return no findings -- the only one resolved.
    r = client.get(_FINDINGS_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    body = r.json()
    assert all(f["status"] == "open" for f in body["findings"])


def test_findings_with_status_resolved_returns_resolved_history() -> None:
    """M4 tab consumes ``?status=resolved`` for the Recently resolved section."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    client = _caller_client(conn, account_id=account)
    client.get(_FINDINGS_PATH.format(camera_id=camera))  # opens
    conn.snapshot_row = _snapshot(age_seconds=5)
    client.get(_FINDINGS_PATH.format(camera_id=camera))  # resolves
    r = client.get(_FINDINGS_PATH.format(camera_id=camera) + "?status=resolved")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["findings"]) == 1
    f = body["findings"][0]
    assert f["status"] == "resolved"
    assert f["finding_type"] == "latest_frame_stale"
    assert f["resolved_at"] is not None
    assert f["resolved_reason"] == "healthy"


def test_findings_with_bogus_status_returns_422() -> None:
    """Closed Literal enum -- anything else is rejected by FastAPI before
    the handler runs. Prevents the dashboard from accidentally passing
    something the SQL can't satisfy."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(camera_account_id=account)
    r = _caller_client(conn, account_id=account).get(
        _FINDINGS_PATH.format(camera_id=camera) + "?status=banana"
    )
    assert r.status_code == 422


def test_findings_status_open_is_explicit_alias_for_default() -> None:
    """Explicit ``?status=open`` returns the same shape as omitting status."""
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    client = _caller_client(conn, account_id=account)
    client.get(_FINDINGS_PATH.format(camera_id=camera))  # opens
    default = client.get(_FINDINGS_PATH.format(camera_id=camera)).json()["findings"]
    explicit = client.get(_FINDINGS_PATH.format(camera_id=camera) + "?status=open").json()[
        "findings"
    ]
    assert [f["finding_type"] for f in default] == [f["finding_type"] for f in explicit]


def test_findings_status_resolved_is_empty_when_nothing_resolved() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=_session(state="streaming"),
        snapshot_row=_snapshot(age_seconds=120),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=_processor_status(age_seconds=5),
    )
    client = _caller_client(conn, account_id=account)
    client.get(_FINDINGS_PATH.format(camera_id=camera))  # opens, never resolved
    r = client.get(_FINDINGS_PATH.format(camera_id=camera) + "?status=resolved")
    assert r.status_code == 200
    assert r.json()["findings"] == []
