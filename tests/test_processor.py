"""M8.0 Processor Readiness Gate — route-level tests.

Pattern mirrors tests/test_ingest_validation_failed.py: TestClient + a fake
asyncpg pool, no real DB / network. The eligibility matrix uses a fake whose
``fetch`` re-implements the documented WHERE clause so every exclusion case
is genuinely exercised; a separate test pins the production SQL so the
simulation cannot silently drift from it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool
from upstream_api.services.processor import _ELIGIBLE_SESSIONS_SQL

_RTSP_BASE = "rtsp://mediamtx.test:8554"
_STREAM_KEY = "sess_0123456789abcdef"
_STREAMID = f"publish:{_STREAM_KEY}"


# ---------------------------------------------------------------------------
# Eligibility fake — re-implements the SQL WHERE clause.
# ---------------------------------------------------------------------------


@dataclass
class _Candidate:
    """A stream_sessions row plus its validation history (newest first)."""

    state: str
    expires_in: timedelta
    validations: list[bool]  # passed flags, newest first; [] = no rows
    streamid: str = _STREAMID
    metadata: dict[str, Any] = field(default_factory=dict)
    account_id: UUID = field(default_factory=uuid4)
    camera_id: UUID = field(default_factory=uuid4)
    session_id: UUID = field(default_factory=uuid4)
    # P6-A: the LEFT JOIN onto camera_geometry_configs. None = no saved row.
    geometry_revision: int | None = None
    geometry_config: dict[str, Any] | None = None
    # M7B: the LEFT JOIN onto camera_processing_configs. None = no saved row.
    # Mirrors the geometry envelope exactly; the LEFT join means a camera with
    # no processing config row is still eligible (the field just comes back
    # null). STAMP-ONLY: wire-carried for lineage, runtime unchanged.
    processing_revision: int | None = None
    processing_config: dict[str, Any] | None = None
    # M3: account-level processing master switch. ``None`` matches the
    # production default (key absent in ``accounts.metadata`` -> coalesce
    # to true). Explicit ``False`` drops the candidate, mirroring the new
    # ``and coalesce(...) = true`` predicate in the SQL.
    account_processing_enabled: bool | None = None

    def is_eligible(self) -> bool:
        if self.state != "streaming":
            return False
        if self.expires_in <= timedelta(0):  # expires_at > now()
            return False
        if not self.validations:  # inner lateral join drops it
            return False
        if self.account_processing_enabled is False:  # M3 master switch
            return False
        return self.validations[0] is True  # latest validation only

    def as_row(self) -> dict[str, Any]:
        now = datetime.now(tz=UTC)
        return {
            "session_id": self.session_id,
            "account_id": self.account_id,
            "camera_id": self.camera_id,
            "streamid": self.streamid,
            "started_at": now - timedelta(minutes=5),
            "lim_codec": "h264",
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_bitrate_kbps": 3000,
            "camera_metadata": self.metadata,
            "v_passed": self.validations[0],
            "v_observed": {"codec": "h264", "width": 1280},
            "v_expected": {"codec": "h264", "width": 1280},
            "v_mismatch_fields": [],
            "v_created_at": now - timedelta(seconds=10),
            "gc_revision": self.geometry_revision,
            "gc_config": self.geometry_config,
            "pc_revision": self.processing_revision,
            "pc_config": self.processing_config,
        }


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeEligibleConn:
    def __init__(self, candidates: list[_Candidate]) -> None:
        self.candidates = candidates
        self.last_query: str | None = None
        self.last_limit: int | None = None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.last_query = query
        self.last_limit = args[0] if args else None
        assert "from stream_sessions ss" in query
        return [c.as_row() for c in self.candidates if c.is_eligible()][: self.last_limit]


class FakeSessionConn:
    """Backs load_session_state + the status upsert / observation insert."""

    def __init__(self, session_row: dict[str, Any] | None) -> None:
        self._session_row = session_row
        self.status_upserts: list[tuple[str, tuple[Any, ...]]] = []
        self.observation_inserts: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions where id" in query:
            return self._session_row
        if "insert into processor_statuses" in query:
            self.status_upserts.append((query, args))
            now = datetime.now(tz=UTC)
            return {
                "id": uuid4(),
                "account_id": args[0],
                "camera_id": args[1],
                "stream_session_id": args[2],
                "processor_id": args[3],
                "runtime": args[4],
                "package_id": args[5],
                "status": args[6],
                "created_at": now,
                "updated_at": now,
            }
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "insert into observations" in query:
            self.observation_inserts.append((query, args))
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
    base = get_settings()
    overridden: Settings = base.model_copy(update={"mediamtx_rtsp_base": _RTSP_BASE})
    app.dependency_overrides[get_settings] = lambda: overridden
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
        "streamid": _STREAMID,
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


_FUTURE = timedelta(hours=1)
_PAST = timedelta(hours=-1)


# ---------------------------------------------------------------------------
# Auth — applies to all three endpoints.
# ---------------------------------------------------------------------------


def test_eligible_sessions_requires_processor_secret() -> None:
    client = _client(FakeEligibleConn([]))
    r = client.get("/v1/internal/processor/eligible-sessions")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_eligible_sessions_rejects_wrong_secret() -> None:
    client = _client(FakeEligibleConn([]))
    r = client.get(
        "/v1/internal/processor/eligible-sessions",
        headers={"X-Upstream-Webhook-Secret": "definitely-wrong"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_processor_secret_is_distinct_from_ingest_secret() -> None:
    """The ingest webhook secret must NOT authenticate a processor route."""
    client = _client(FakeEligibleConn([]))
    r = client.get(
        "/v1/internal/processor/eligible-sessions",
        headers={
            "X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()
        },
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_eligible_sessions_correct_secret_works() -> None:
    client = _client(FakeEligibleConn([]))
    r = client.get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.status_code == 200
    assert r.json() == {"sessions": []}


# ---------------------------------------------------------------------------
# Eligibility matrix.
# ---------------------------------------------------------------------------


def test_streaming_validated_unexpired_is_included() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    client = _client(conn)
    r = client.get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.status_code == 200
    sessions = r.json()["sessions"]
    assert len(sessions) == 1
    assert sessions[0]["processor_eligible"] is True


def test_approved_session_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("approved", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_ended_session_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("ended", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_expired_state_session_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("expired", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_failed_session_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("failed", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_streaming_but_expired_by_clock_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _PAST, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_streaming_with_no_validation_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_streaming_latest_validation_failed_excluded() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [False])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_older_pass_but_latest_fail_excluded() -> None:
    # validations newest-first: latest=False, older=True.
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [False, True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"] == []


def test_older_fail_but_latest_pass_included() -> None:
    # validations newest-first: latest=True, older=False.
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True, False])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert len(r.json()["sessions"]) == 1


def test_mixed_set_returns_only_eligible() -> None:
    conn = FakeEligibleConn(
        [
            _Candidate("streaming", _FUTURE, [True]),
            _Candidate("approved", _FUTURE, [True]),
            _Candidate("streaming", _FUTURE, [False]),
            _Candidate("streaming", _PAST, [True]),
            _Candidate("streaming", _FUTURE, [True]),
        ]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert len(r.json()["sessions"]) == 2


def test_account_processing_disabled_drops_session() -> None:
    """M3 master switch off -> the SQL's ``coalesce(... 'processing_enabled' ...)``
    predicate filters the row out. The dashboard toggle's only mechanism."""
    conn = FakeEligibleConn(
        [
            _Candidate("streaming", _FUTURE, [True], account_processing_enabled=False),
            _Candidate("streaming", _FUTURE, [True], account_processing_enabled=True),
            _Candidate("streaming", _FUTURE, [True]),  # absent key = default true
        ]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    # Only the two sessions whose account flag is not False survive.
    assert len(r.json()["sessions"]) == 2


# ---------------------------------------------------------------------------
# Eligibility response shaping + safe-field guarantees.
# ---------------------------------------------------------------------------


def test_response_contains_internal_rtsp_url_built_from_base_and_streamid() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    s = r.json()["sessions"][0]
    assert s["internal_rtsp_url"] == f"{_RTSP_BASE}/{_STREAM_KEY}"
    assert s["stream_key"] == _STREAM_KEY
    assert s["runtime"] == "mock"
    assert s["geometry_config"] is None
    # M7B: the processing-config envelope sits next to geometry_config and is
    # null on the default (no-saved-row) path, same as geometry.
    assert s["processing_config"] is None
    assert s["approved"] == {
        "codec": "h264",
        "width": 1280,
        "height": 720,
        "fps": 30,
        "bitrate_kbps": 3000,
    }


def test_response_never_leaks_srt_url_tokens_or_secrets() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    raw = r.text.lower()
    assert "srt://" not in raw
    assert "srt_url" not in raw
    assert "passphrase" not in raw
    assert "authorization" not in raw
    assert "secret" not in raw
    assert "session_token" not in raw


def test_package_id_defaults_to_mock_when_metadata_absent() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True], metadata={})])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["package_id"] == "mock.detect.v0"


def test_package_id_override_from_camera_metadata() -> None:
    conn = FakeEligibleConn(
        [_Candidate("streaming", _FUTURE, [True], metadata={"package_id": "custom.pkg.v2"})]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["package_id"] == "custom.pkg.v2"


def test_limit_query_param_is_passed_through() -> None:
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    _client(conn).get("/v1/internal/processor/eligible-sessions?limit=5", headers=_headers())
    assert conn.last_limit == 5


def test_geometry_config_null_when_no_saved_row() -> None:
    """P6-A: a camera with no saved geometry config carries a null
    ``geometry_config`` — the processor applies its own probe default."""
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["geometry_config"] is None


def test_geometry_config_envelope_when_saved_row_exists() -> None:
    """P6-A: a saved config is carried as a ``{revision, config}`` envelope
    so the processor can both apply the geometry and log its revision."""
    cfg = {
        "version": 1,
        "frame": {"width": 1280, "height": 720, "coordinate_space": "normalized"},
        "rois": [],
        "lines": [],
    }
    conn = FakeEligibleConn(
        [_Candidate("streaming", _FUTURE, [True], geometry_revision=3, geometry_config=cfg)]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["geometry_config"] == {"revision": 3, "config": cfg}


# ---------------------------------------------------------------------------
# M7B: processing_config envelope (mirrors the P6-A geometry_config envelope).
# STAMP-ONLY — wire-carried for lineage; the runtime applies nothing from it.
# ---------------------------------------------------------------------------


def test_processing_config_null_when_no_saved_row() -> None:
    """M7B: a camera with no ``camera_processing_configs`` row carries a null
    ``processing_config`` — the processor falls back to its runtime defaults
    (trafficcamnet_v1 / all classes / iou)."""
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["processing_config"] is None


def test_processing_config_envelope_when_saved_row_exists() -> None:
    """M7B: a saved processing config is carried as a ``{revision, config}``
    envelope (the nested config is ``CameraProcessingConfig.model_dump()``),
    round-tripping verbatim so the processor can log its lineage."""
    cfg = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": ["car", "truck"],
        "tracker": "iou",
        "confidence_threshold": 0.45,
        "processing_fps": 10,
    }
    conn = FakeEligibleConn(
        [_Candidate("streaming", _FUTURE, [True], processing_revision=7, processing_config=cfg)]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["processing_config"] == {"revision": 7, "config": cfg}


def test_processing_config_envelope_carries_nullable_nested_fields() -> None:
    """M7B: the nested config's optional fields (tracker, confidence_threshold,
    processing_fps) may be null and must round-trip as null, not be dropped."""
    cfg = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": [],
        "tracker": None,
        "confidence_threshold": None,
        "processing_fps": None,
    }
    conn = FakeEligibleConn(
        [_Candidate("streaming", _FUTURE, [True], processing_revision=1, processing_config=cfg)]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["processing_config"] == {"revision": 1, "config": cfg}


def test_camera_with_no_processing_config_still_eligible() -> None:
    """M7B: the join onto ``camera_processing_configs`` is a LEFT join, never
    an INNER join — a camera with no saved processing config must still appear
    in the eligible list (with ``processing_config`` null)."""
    conn = FakeEligibleConn([_Candidate("streaming", _FUTURE, [True])])
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    sessions = r.json()["sessions"]
    assert len(sessions) == 1
    assert sessions[0]["processor_eligible"] is True
    assert sessions[0]["processing_config"] is None


def test_processing_and_geometry_configs_are_independent() -> None:
    """M7B regression: the two LEFT joins are independent. A camera may have a
    saved processing config but no geometry config (and vice versa); each
    envelope is populated/nulled on its own row, neither affecting the other."""
    pcfg = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": ["car"],
        "tracker": "iou",
        "confidence_threshold": None,
        "processing_fps": None,
    }
    conn = FakeEligibleConn(
        [_Candidate("streaming", _FUTURE, [True], processing_revision=2, processing_config=pcfg)]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    s = r.json()["sessions"][0]
    assert s["processing_config"] == {"revision": 2, "config": pcfg}
    # Geometry was never set on this candidate, so its envelope stays null.
    assert s["geometry_config"] is None


def test_geometry_and_processing_configs_coexist() -> None:
    """M7B regression: when a camera has both a geometry and a processing
    config row, both envelopes round-trip side by side and unchanged."""
    gcfg = {
        "version": 1,
        "frame": {"width": 1280, "height": 720, "coordinate_space": "normalized"},
        "rois": [],
        "lines": [],
    }
    pcfg = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": ["car", "bus"],
        "tracker": "iou",
        "confidence_threshold": 0.5,
        "processing_fps": 15,
    }
    conn = FakeEligibleConn(
        [
            _Candidate(
                "streaming",
                _FUTURE,
                [True],
                geometry_revision=4,
                geometry_config=gcfg,
                processing_revision=9,
                processing_config=pcfg,
            )
        ]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    s = r.json()["sessions"][0]
    # P6-A geometry envelope is unchanged by the M7B addition (regression).
    assert s["geometry_config"] == {"revision": 4, "config": gcfg}
    assert s["processing_config"] == {"revision": 9, "config": pcfg}


def test_processing_config_does_not_affect_package_id() -> None:
    """M7B regression: package_id resolution is independent of the processing
    config envelope (M7B is STAMP-ONLY — it changes no runtime selection,
    including package routing)."""
    pcfg = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": [],
        "tracker": "iou",
        "confidence_threshold": None,
        "processing_fps": None,
    }
    # Default package_id (no metadata override) is unchanged by a saved config.
    default_conn = FakeEligibleConn(
        [
            _Candidate(
                "streaming",
                _FUTURE,
                [True],
                metadata={},
                processing_revision=1,
                processing_config=pcfg,
            )
        ]
    )
    r = _client(default_conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["package_id"] == "mock.detect.v0"
    # An explicit metadata override is likewise unaffected by the config row.
    override_conn = FakeEligibleConn(
        [
            _Candidate(
                "streaming",
                _FUTURE,
                [True],
                metadata={"package_id": "custom.pkg.v2"},
                processing_revision=1,
                processing_config=pcfg,
            )
        ]
    )
    r = _client(override_conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    assert r.json()["sessions"][0]["package_id"] == "custom.pkg.v2"


def test_processing_config_does_not_affect_processing_enabled_gate() -> None:
    """M7B regression: the M3 account-level ``processing_enabled`` gate is
    unaffected by a saved processing config. A camera with a config row whose
    account has processing disabled is STILL dropped from the eligible list."""
    pcfg = {
        "version": 1,
        "model_key": "trafficcamnet_v1",
        "selected_classes": ["car"],
        "tracker": "iou",
        "confidence_threshold": None,
        "processing_fps": None,
    }
    conn = FakeEligibleConn(
        [
            # Account disabled + has a saved config -> still dropped.
            _Candidate(
                "streaming",
                _FUTURE,
                [True],
                account_processing_enabled=False,
                processing_revision=1,
                processing_config=pcfg,
            ),
            # Account enabled + has a saved config -> survives, config carried.
            _Candidate(
                "streaming",
                _FUTURE,
                [True],
                account_processing_enabled=True,
                processing_revision=2,
                processing_config=pcfg,
            ),
        ]
    )
    r = _client(conn).get("/v1/internal/processor/eligible-sessions", headers=_headers())
    sessions = r.json()["sessions"]
    assert len(sessions) == 1
    assert sessions[0]["processing_config"] == {"revision": 2, "config": pcfg}


def test_production_eligibility_sql_pins_every_predicate() -> None:
    """Guards the FakeEligibleConn simulation against drifting from the
    real query that runs in production."""
    sql = _ELIGIBLE_SESSIONS_SQL
    assert "ss.state = 'streaming'" in sql
    assert "ss.expires_at > now()" in sql
    assert "v.passed = true" in sql
    assert "join lateral" in sql
    assert "order by created_at desc" in sql
    assert "limit 1" in sql
    # P6-A: geometry config is LEFT-joined so a camera with no config still
    # appears in the result (geometry_config just comes back null).
    assert "left join camera_geometry_configs gc on gc.camera_id = ss.camera_id" in sql
    # M7B: processing config is LEFT-joined for the same reason — a camera with
    # no camera_processing_configs row stays eligible (processing_config null).
    # The UNIQUE index on camera_id means this join cannot fan out rows.
    assert "left join camera_processing_configs pc on pc.camera_id = ss.camera_id" in sql
    assert "pc.revision" in sql
    assert "pc_revision" in sql
    assert "pc.config" in sql
    assert "pc_config" in sql
    # The SRT publish URL must never be selected onto a processor surface.
    assert "srt_url" not in sql
    # M3: account-level processing master switch. Absent key (NULL JSONB
    # path) coalesces to true, so existing accounts keep being processed.
    # Explicit ``false`` drops every session under the account from the
    # eligible list -- that's how the dashboard's "Processing: Off" toggle
    # takes effect within one poll cycle without restarting any service.
    assert "join accounts a on a.id = ss.account_id" in sql
    assert "coalesce((a.metadata->>'processing_enabled')::boolean, true) = true" in sql


# ---------------------------------------------------------------------------
# POST /v1/internal/processor/status
# ---------------------------------------------------------------------------


def _status_body(session_id: UUID, *, status: str = "processing") -> dict[str, Any]:
    return {
        "processor_id": "proc-1",
        "stream_session_id": str(session_id),
        "runtime": "mock",
        "package_id": "mock.detect.v0",
        "status": status,
        "fps_input": 30.0,
        "fps_processed": 29.5,
        "payload": {"note": "ok"},
    }


def test_status_write_succeeds_and_derives_tenant_from_session() -> None:
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    client = _client(conn)
    r = client.post(
        "/v1/internal/processor/status",
        json=_status_body(session_id),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Tenant echoed from the session row, not the request (request has no
    # account_id/camera_id fields at all).
    assert body["account_id"] == str(account_id)
    assert body["camera_id"] == str(camera_id)
    assert body["status"] == "processing"
    # The upsert was called with the session-derived tenant ids ($1,$2).
    assert len(conn.status_upserts) == 1
    _q, args = conn.status_upserts[0]
    assert args[0] == account_id
    assert args[1] == camera_id


def test_status_invalid_value_rejected() -> None:
    session_id = uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=uuid4(), camera_id=uuid4())
    )
    r = _client(conn).post(
        "/v1/internal/processor/status",
        json=_status_body(session_id, status="exploded"),
        headers=_headers(),
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


def test_status_unknown_session_rejected() -> None:
    conn = FakeSessionConn(None)
    r = _client(conn).post(
        "/v1/internal/processor/status",
        json=_status_body(uuid4()),
        headers=_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
    assert conn.status_upserts == []


def test_status_requires_processor_secret() -> None:
    conn = FakeSessionConn(None)
    r = _client(conn).post(
        "/v1/internal/processor/status",
        json=_status_body(uuid4()),
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
    assert conn.status_upserts == []


# ---------------------------------------------------------------------------
# POST /v1/internal/processor/observations
# ---------------------------------------------------------------------------


def _obs_body(session_id: UUID, *, type_: str = "processor.mock_detection") -> dict[str, Any]:
    return {
        "stream_session_id": str(session_id),
        "type": type_,
        "payload": {"objects": 3},
        "observed_at": datetime.now(tz=UTC).isoformat(),
        "processor_id": "proc-1",
        "package_id": "mock.detect.v0",
    }


def test_observation_write_succeeds_and_derives_tenant() -> None:
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert UUID(body["id"])
    assert len(conn.observation_inserts) == 1
    _q, args = conn.observation_inserts[0]
    # $1 account_id, $2 camera_id derived from the session row.
    assert args[0] == account_id
    assert args[1] == camera_id


def test_observation_invalid_type_rejected() -> None:
    session_id = uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=uuid4(), camera_id=uuid4())
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.real_detection"),
        headers=_headers(),
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"
    assert conn.observation_inserts == []


def test_observation_line_crossing_accepted() -> None:
    """P4-B widens the ``Literal`` whitelist with the first real
    DeepStream-backed observation type. The processing-layer rolls out
    types one-at-a-time; this test pins the wire contract for
    ``processor.line_crossing`` so a future Literal edit that drops
    or renames it fails fast."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.line_crossing"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert UUID(body["id"])
    # Tenant columns derived from the session, not the request body.
    assert len(conn.observation_inserts) == 1
    _q, args = conn.observation_inserts[0]
    assert args[0] == account_id
    assert args[1] == camera_id
    # And: ``type`` value reached the insert as the new literal.
    assert args[5] == "processor.line_crossing"


def test_observation_roi_occupancy_accepted() -> None:
    """P4-C widens the ``Literal`` whitelist with the second real
    DeepStream-backed observation type. The analytics worker emits
    these snapshots gated by an on-change-OR-interval policy (never
    per-frame); the api-server side only needs to accept the literal."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.roi_occupancy"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert UUID(body["id"])
    assert len(conn.observation_inserts) == 1
    _q, args = conn.observation_inserts[0]
    assert args[0] == account_id
    assert args[1] == camera_id
    assert args[5] == "processor.roi_occupancy"


def test_observation_zone_entered_accepted() -> None:
    """P4-D adds per-track zone transition observations. The processor
    pairs the stable tracker_id with per-object roi_labels and emits
    one zone_entered per stable outside-to-inside transition."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.zone_entered"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert UUID(body["id"])
    assert len(conn.observation_inserts) == 1
    _q, args = conn.observation_inserts[0]
    assert args[0] == account_id
    assert args[1] == camera_id
    assert args[5] == "processor.zone_entered"


def test_observation_zone_exited_accepted() -> None:
    """Pairs with ``zone_entered``: emitted on stable inside-to-outside
    transition (object observed without the zone label for N frames)."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.zone_exited"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert UUID(body["id"])
    assert len(conn.observation_inserts) == 1
    _q, args = conn.observation_inserts[0]
    assert args[0] == account_id
    assert args[1] == camera_id
    assert args[5] == "processor.zone_exited"


def test_observation_dwell_started_accepted() -> None:
    """P4-E adds dwell observations layered on top of P4-D's zone
    membership. ``dwell_started`` fires once when a tracked object
    enters a zone; the api-server side just needs to accept the
    literal."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.dwell_started"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert UUID(body["id"])
    _q, args = conn.observation_inserts[0]
    assert args[0] == account_id
    assert args[1] == camera_id
    assert args[5] == "processor.dwell_started"


def test_observation_dwell_updated_accepted() -> None:
    """``dwell_updated`` emits periodically while the tracked object is
    still inside the zone (default 30s interval)."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.dwell_updated"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    _q, args = conn.observation_inserts[0]
    assert args[5] == "processor.dwell_updated"


def test_observation_dwell_exceeded_accepted() -> None:
    """``dwell_exceeded`` fires once when the configured threshold
    is crossed (default 60s)."""
    session_id, account_id, camera_id = uuid4(), uuid4(), uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=account_id, camera_id=camera_id)
    )
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(session_id, type_="processor.dwell_exceeded"),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    _q, args = conn.observation_inserts[0]
    assert args[5] == "processor.dwell_exceeded"


def test_future_observation_types_still_rejected() -> None:
    """The processing-layer taxonomy still defers track / telemetry /
    debug types until their own milestones. The api-server whitelist
    must NOT have accidentally grown — a Literal widening that adds
    the wrong types fails here. P4-E flips the three dwell types out
    of this guard since they're now supported."""
    session_id = uuid4()
    conn = FakeSessionConn(
        _session_row(session_id=session_id, account_id=uuid4(), camera_id=uuid4())
    )
    for deferred_type in (
        "processor.track_update",
        "processor.track_state",
        "processor.direction_detected",
        "processor.detection_seen",
        "processor.frame_summary",
        "processor.scene_quality_warning",
    ):
        r = _client(conn).post(
            "/v1/internal/processor/observations",
            json=_obs_body(session_id, type_=deferred_type),
            headers=_headers(),
        )
        assert r.status_code == 422, (
            f"{deferred_type!r} unexpectedly accepted (status {r.status_code})"
        )
        assert r.json()["error"]["code"] == "validation_error"


def test_observation_unknown_session_rejected() -> None:
    conn = FakeSessionConn(None)
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(uuid4()),
        headers=_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
    assert conn.observation_inserts == []


def test_observation_requires_processor_secret() -> None:
    conn = FakeSessionConn(None)
    r = _client(conn).post(
        "/v1/internal/processor/observations",
        json=_obs_body(uuid4()),
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
    assert conn.observation_inserts == []
