"""M9.3 observability tests: GET /metrics gating + counter/histogram wiring.

No real DB — the asyncpg pool is replaced by the same FakeConn/FakePool
pattern as the other route tests (duplicated per file by repo convention;
see the note in tests/test_ingest_mediamtx_auth.py).

Counter assertions are delta-based (`before + 1`) against the module-level
registry in ``upstream_api.observability.metrics`` because collectors are
process-global across the whole test session.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool
from upstream_api.observability.metrics import registry, track_ingest
from upstream_api.security.streamid import build_streamid, mint_stream_key
from upstream_api.services.limits import RequestedStream
from upstream_api.services.session_lifecycle import start_session
from upstream_api.services.stream_validation import mark_ended, mark_failed, mark_streaming


def _sample(name: str, labels: dict[str, str] | None = None) -> float:
    return registry.get_sample_value(name, labels) or 0.0


def _transitions(frm: str, to: str) -> float:
    return _sample("session_transitions_total", {"from_state": frm, "to_state": to})


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeConn:
    """Streamid-keyed session rows; enough for the mediamtx-auth gate."""

    def __init__(self, session_rows: dict[str, dict[str, Any]] | None = None) -> None:
        self.session_rows = session_rows or {}

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions" in query and "where streamid" in query:
            return self.session_rows.get(args[0])
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


class FakeTx:
    async def __aenter__(self) -> FakeTx:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class SnapshotIngestFakeConn:
    """Serves the queries ingest_upload_snapshot makes (session row by id,
    quota, insert); trimmed from tests/test_ingest_snapshots.py."""

    def __init__(self, session_row: dict[str, Any] | None) -> None:
        self.session_row = session_row

    def transaction(self) -> FakeTx:
        return FakeTx()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from stream_sessions" in query:
            return self.session_row
        if "from account_limits" in query:
            return {"snapshot_storage_mb": 1024}
        if "insert into snapshots" in query:
            return {
                "id": args[0],
                "camera_id": args[3],
                "session_id": args[4],
                "storage_key": args[5],
                "bytes": args[8],
                "width": args[6],
                "height": args[7],
                "captured_at": args[9],
                "created_at": datetime.now(tz=UTC),
            }
        return None

    async def fetchval(self, query: str, *_args: Any) -> Any:
        if "to_regclass" in query:
            return False  # usage_events table absent; record_event no-ops
        if "from snapshots" in query:
            return 0  # used_bytes
        return None

    async def execute(self, _query: str, *_args: Any) -> str:
        return "OK"


class TransitionFakeConn:
    """Serves the `update ... returning` + probe queries for the mark_* helpers
    (same shape as tests/test_stream_validation.py)."""

    def __init__(
        self,
        *,
        update_returns: dict[str, Any] | None,
        current_state: str | None = "approved",
    ) -> None:
        self.update_returns = update_returns
        self.current_state = current_state

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "update stream_sessions" in query:
            return self.update_returns
        if "from stream_sessions" in query:
            if self.current_state is None:
                return None
            return {
                "id": args[0],
                "account_id": uuid4(),
                "agent_id": uuid4(),
                "camera_id": uuid4(),
                "state": self.current_state,
                "expires_at": datetime.now(tz=UTC),
                "session_token_jti": uuid4(),
                "streamid": "publish:sess_0123456789abcdef",
                "lim_width": 1280,
                "lim_height": 720,
                "lim_fps": 30,
                "lim_codec": "h264",
                "lim_bitrate_kbps": 3000,
                "lim_latency_mode": "balanced",
            }
        return None

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"


class RollbackTx:
    """Transaction context whose exit raises — the body's statements succeed,
    then COMMIT fails (or a later in-transaction statement did), so everything
    rolls back. Mirrors the `async with conn.transaction():` caller pattern."""

    async def __aenter__(self) -> RollbackTx:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        raise RuntimeError("simulated commit failure / rollback")


class RollbackConn(TransitionFakeConn):
    """TransitionFakeConn whose transaction never commits."""

    def transaction(self) -> RollbackTx:
        return RollbackTx()


class StartSessionFakeConn:
    """Serves the queries `start_session` makes (camera, limits, insert);
    trimmed from tests/test_stream_sessions_router.py."""

    def __init__(self, account_id: UUID) -> None:
        self.account_id = account_id

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return {
                "id": args[0],
                "account_id": self.account_id,
                "agent_id": uuid4(),
                "source_type": "webcam",
            }
        if "from account_limits" in query:
            return {
                "max_cameras": 10,
                "max_concurrent_sessions": 5,
                "max_fps": 60,
                "max_width": 4096,
                "max_height": 2160,
                "max_bitrate_kbps": 12000,
                "allowed_codecs": ["h264"],
                "allowed_stream_types": ["webcam", "rtsp"],
                "monthly_minutes_quota": 600,
                "snapshot_storage_mb": 1024,
                "session_ttl_seconds": None,
            }
        if "insert into stream_sessions" in query:
            return {
                "id": args[0],
                "state": "approved",
                "expires_at": datetime.now(tz=UTC),
                "created_at": datetime.now(tz=UTC),
                "started_at": None,
                "ended_at": None,
            }
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "status from accounts" in query:
            return "active"
        if "to_regclass" in query:
            return False
        if "exists" in query.lower():
            return False
        if "count(*)" in query:
            return 0
        return None

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"


def _session_row(*, session_id: UUID, state: str, expires_at: datetime) -> dict[str, Any]:
    return {
        "id": session_id,
        "account_id": uuid4(),
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "state": state,
        "expires_at": expires_at,
        "session_token_jti": uuid4(),
        "streamid": "publish:sess_0123456789abcdef",
        "lim_width": 1280,
        "lim_height": 720,
        "lim_fps": 30,
        "lim_codec": "h264",
        "lim_bitrate_kbps": 3000,
        "lim_latency_mode": "balanced",
    }


def _token() -> str:
    token = get_settings().prometheus_auth_token
    assert token is not None, "conftest sets UPSTREAM_PROMETHEUS_AUTH_TOKEN"
    return token.get_secret_value()


# ---------------------------------------------------------------------------
# GET /metrics gating
# ---------------------------------------------------------------------------


def test_metrics_404_when_token_unset() -> None:
    app = create_app()
    base = get_settings()
    unset: Settings = base.model_copy(update={"prometheus_auth_token": None})
    app.dependency_overrides[get_settings] = lambda: unset
    client = TestClient(app)
    r = client.get("/metrics", headers={"Authorization": f"Bearer {_token()}"})
    assert r.status_code == 404


def test_metrics_404_when_token_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    """UPSTREAM_PROMETHEUS_AUTH_TOKEN='' must behave as unset (404), not as a
    configured-but-unmatchable token (permanently-401 endpoint) — the config
    validator coerces empty to None. setenv("") rather than delenv: Settings
    also reads the repo .env, and an explicit empty value overrides it (same
    pattern as test_perception_stream_token.py)."""
    monkeypatch.setenv("UPSTREAM_PROMETHEUS_AUTH_TOKEN", "")
    get_settings.cache_clear()
    client = TestClient(create_app())
    r = client.get("/metrics", headers={"Authorization": "Bearer anything"})
    assert r.status_code == 404


def test_metrics_401_on_missing_header() -> None:
    client = TestClient(create_app())
    r = client.get("/metrics")
    assert r.status_code == 401


def test_metrics_401_on_empty_bearer_credential() -> None:
    """'Authorization: Bearer ' (scheme only, empty credential) is rejected
    before the constant-time compare ever runs."""
    client = TestClient(create_app())
    r = client.get("/metrics", headers={"Authorization": "Bearer "})
    assert r.status_code == 401


def test_metrics_401_on_wrong_token() -> None:
    client = TestClient(create_app())
    r = client.get("/metrics", headers={"Authorization": "Bearer not-the-right-token"})
    assert r.status_code == 401


def test_metrics_401_on_non_bearer_scheme() -> None:
    client = TestClient(create_app())
    r = client.get("/metrics", headers={"Authorization": f"Basic {_token()}"})
    assert r.status_code == 401


def test_metrics_200_with_correct_token() -> None:
    client = TestClient(create_app())
    r = client.get("/metrics", headers={"Authorization": f"Bearer {_token()}"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "ingest_webhook_total" in r.text
    assert "session_transitions_total" in r.text
    assert "db_pool_size" in r.text


# ---------------------------------------------------------------------------
# mediamtx-auth decision counter (route level, FakeConn)
# ---------------------------------------------------------------------------


def _auth_url() -> str:
    return f"/v1/ingest/mediamtx-auth/{get_settings().mediamtx_webhook_secret.get_secret_value()}"


def _auth_payload(path: str) -> dict[str, str]:
    return {
        "user": "",
        "password": "",
        "ip": "203.0.113.42",
        "action": "publish",
        "path": path,
        "protocol": "srt",
        "id": "deadbeef-dead-beef-dead-beefdeadbeef",
        "query": "",
    }


def _client(pool: FakePool) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: pool
    return TestClient(app)


def test_mediamtx_auth_allow_increments_counter() -> None:
    key = mint_stream_key()
    row = _session_row(
        session_id=uuid4(),
        state="approved",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    client = _client(FakePool(FakeConn({build_streamid(key): row})))

    before_allow = _sample("mediamtx_auth_total", {"decision": "allow"})
    before_ok = _sample("ingest_webhook_total", {"endpoint": "mediamtx-auth", "outcome": "ok"})
    r = client.post(_auth_url(), json=_auth_payload(key))
    assert r.status_code == 200
    assert _sample("mediamtx_auth_total", {"decision": "allow"}) == before_allow + 1
    assert (
        _sample("ingest_webhook_total", {"endpoint": "mediamtx-auth", "outcome": "ok"})
        == before_ok + 1
    )


def test_mediamtx_auth_deny_increments_counter() -> None:
    client = _client(FakePool(FakeConn()))  # no session rows -> deny

    before_deny = _sample("mediamtx_auth_total", {"decision": "deny"})
    before_err = _sample(
        "ingest_webhook_total", {"endpoint": "mediamtx-auth", "outcome": "client_error"}
    )
    r = client.post(_auth_url(), json=_auth_payload(mint_stream_key()))
    assert r.status_code == 401
    assert _sample("mediamtx_auth_total", {"decision": "deny"}) == before_deny + 1
    assert (
        _sample("ingest_webhook_total", {"endpoint": "mediamtx-auth", "outcome": "client_error"})
        == before_err + 1
    )


# ---------------------------------------------------------------------------
# Snapshot ingest counter (route level, SnapshotIngestFakeConn)
# ---------------------------------------------------------------------------


_JPEG_BYTES = b"\xff\xd8\xff" + b"x" * 1024  # ~1KB stub; never decoded


def _webhook_headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()}


def _post_ingest_snapshot(client: TestClient, session_id: UUID) -> Any:
    return client.post(
        "/v1/ingest/snapshots",
        data={
            "session_id": str(session_id),
            "captured_at": datetime.now(tz=UTC).isoformat(),
        },
        files={"file": ("frame.jpg", io.BytesIO(_JPEG_BYTES), "image/jpeg")},
        headers=_webhook_headers(),
    )


def test_ingest_snapshots_ok_increments_counter() -> None:
    session_id = uuid4()
    row = _session_row(
        session_id=session_id,
        state="streaming",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )
    client = _client(FakePool(SnapshotIngestFakeConn(row)))

    before_ok = _sample("ingest_webhook_total", {"endpoint": "snapshots", "outcome": "ok"})
    with patch("upstream_api.routers.snapshots.upload_bytes", new=AsyncMock(return_value=None)):
        r = _post_ingest_snapshot(client, session_id)
    assert r.status_code == 201, r.text
    assert (
        _sample("ingest_webhook_total", {"endpoint": "snapshots", "outcome": "ok"}) == before_ok + 1
    )


def test_ingest_snapshots_client_error_increments_counter() -> None:
    client = _client(FakePool(SnapshotIngestFakeConn(None)))  # no session row -> 404

    before_err = _sample(
        "ingest_webhook_total", {"endpoint": "snapshots", "outcome": "client_error"}
    )
    r = _post_ingest_snapshot(client, uuid4())
    assert r.status_code == 404
    assert (
        _sample("ingest_webhook_total", {"endpoint": "snapshots", "outcome": "client_error"})
        == before_err + 1
    )


# ---------------------------------------------------------------------------
# Session-transition counters + time-to-streaming histogram (service level).
# The mark_* helpers RETURN the transition facts; the caller records them
# after its transaction commits — these tests replay that caller pattern.
# ---------------------------------------------------------------------------


async def test_mark_streaming_records_transition_and_histogram() -> None:
    before_t = _transitions("approved", "streaming")
    before_count = _sample("session_start_to_streaming_seconds_count")
    before_sum = _sample("session_start_to_streaming_seconds_sum")

    conn = TransitionFakeConn(
        update_returns={
            "state": "streaming",
            "created_at": datetime.now(tz=UTC) - timedelta(seconds=3),
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    observed = {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 2980}
    state = await mark_streaming(
        conn, session_id=uuid4(), observed=observed, started_at=datetime.now(tz=UTC)
    )
    state.transition.record()  # what the router does after COMMIT

    assert _transitions("approved", "streaming") == before_t + 1
    assert _sample("session_start_to_streaming_seconds_count") == before_count + 1
    # ~3 s elapsed between the faked created_at and started_at.
    assert _sample("session_start_to_streaming_seconds_sum") >= before_sum + 2


async def test_mark_streaming_rollback_does_not_count_transition() -> None:
    """A transaction that fails AFTER mark_streaming returns must not move
    any counter: recording belongs to the caller, after COMMIT. Otherwise the
    rolled-back transition counts once now (a phantom — no state actually
    changed) and once more when the streaming server retries the webhook."""
    before_t = _transitions("approved", "streaming")
    before_count = _sample("session_start_to_streaming_seconds_count")

    conn = TransitionFakeConn(
        update_returns={
            "state": "streaming",
            "created_at": datetime.now(tz=UTC) - timedelta(seconds=3),
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    observed = {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 2980}
    with pytest.raises(RuntimeError):
        async with RollbackTx():
            await mark_streaming(
                conn, session_id=uuid4(), observed=observed, started_at=datetime.now(tz=UTC)
            )
            # The caller's `state.transition.record()` lives AFTER the
            # transaction block, so it is never reached on rollback.

    assert _transitions("approved", "streaming") == before_t
    assert _sample("session_start_to_streaming_seconds_count") == before_count


def test_validation_failed_rollback_records_no_transition() -> None:
    """Route-level record-after-commit: when the webhook's transaction fails
    on exit, /v1/ingest/validation-failed 500s (outcome=server_error) and
    session_transitions_total must not move — the retry will count it."""
    before_t = _transitions("approved", "failed")
    before_err = _sample(
        "ingest_webhook_total", {"endpoint": "validation-failed", "outcome": "server_error"}
    )

    conn = RollbackConn(
        update_returns={
            "state": "failed",
            "started_at": None,
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    client = TestClient(app, raise_server_exceptions=False)
    r = client.post(
        "/v1/ingest/validation-failed",
        json={
            "session_id": str(uuid4()),
            "mismatch_fields": ["codec"],
            "failure_code": "codec_mismatch",
            "failure_message": "expected h264, observed h265",
        },
        headers=_webhook_headers(),
    )

    assert r.status_code == 500
    assert _transitions("approved", "failed") == before_t
    after_err = _sample(
        "ingest_webhook_total", {"endpoint": "validation-failed", "outcome": "server_error"}
    )
    assert after_err == before_err + 1


async def test_mark_streaming_naive_created_at_skips_histogram() -> None:
    """A tz-naive ``created_at`` must not raise (aware/naive subtraction is a
    TypeError) inside the webhook handler — the observation is skipped, and
    the transition itself still succeeds and is counted."""
    before_t = _transitions("approved", "streaming")
    before_count = _sample("session_start_to_streaming_seconds_count")

    conn = TransitionFakeConn(
        update_returns={
            "state": "streaming",
            "created_at": datetime.now(tz=UTC).replace(tzinfo=None),  # tz-naive
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    observed = {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 2980}
    state = await mark_streaming(
        conn, session_id=uuid4(), observed=observed, started_at=datetime.now(tz=UTC)
    )
    state.transition.record()

    assert state == "streaming"
    assert _transitions("approved", "streaming") == before_t + 1
    assert _sample("session_start_to_streaming_seconds_count") == before_count


async def test_mark_streaming_reconnect_does_not_record_transition() -> None:
    before_t = _transitions("approved", "streaming")
    conn = TransitionFakeConn(update_returns=None, current_state="streaming")
    observed = {"codec": "h264", "width": 1280, "height": 720, "fps": 30, "bitrate_kbps": 2980}
    state = await mark_streaming(
        conn, session_id=uuid4(), observed=observed, started_at=datetime.now(tz=UTC)
    )
    state.transition.record()  # NO_TRANSITION on the idempotent path: a no-op
    assert _transitions("approved", "streaming") == before_t


async def test_mark_ended_records_streaming_to_ended() -> None:
    before_t = _transitions("streaming", "ended")
    conn = TransitionFakeConn(
        update_returns={
            "state": "ended",
            "started_at": datetime.now(tz=UTC) - timedelta(minutes=5),
            "ended_at": datetime.now(tz=UTC),
        }
    )
    state, _duration = await mark_ended(
        conn, session_id=uuid4(), ended_at=datetime.now(tz=UTC), reason="client_disconnect"
    )
    state.transition.record()
    assert _transitions("streaming", "ended") == before_t + 1


async def test_mark_failed_records_approved_to_failed() -> None:
    # started_at=None -> the session never reached streaming -> from "approved".
    before_t = _transitions("approved", "failed")
    conn = TransitionFakeConn(
        update_returns={
            "state": "failed",
            "started_at": None,
            "lim_width": 1280,
            "lim_height": 720,
            "lim_fps": 30,
            "lim_codec": "h264",
            "lim_bitrate_kbps": 3000,
            "lim_latency_mode": "balanced",
        }
    )
    state = await mark_failed(
        conn,
        session_id=uuid4(),
        observed=None,
        mismatch_fields=["codec"],
        failure_code="codec_mismatch",
        failure_message="expected h264, observed h265",
    )
    state.transition.record()
    assert _transitions("approved", "failed") == before_t + 1


async def test_track_ingest_records_server_error_and_propagates() -> None:
    """An unexpected exception inside a tracked handler is counted as
    outcome=server_error and still propagates to the framework."""
    before = _sample("ingest_webhook_total", {"endpoint": "boom-test", "outcome": "server_error"})

    @track_ingest("boom-test")
    async def handler() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await handler()

    assert (
        _sample("ingest_webhook_total", {"endpoint": "boom-test", "outcome": "server_error"})
        == before + 1
    )


async def test_start_session_returns_transition_recorded_only_by_caller() -> None:
    """start_session must NOT increment inside the (caller-owned)
    transaction; it returns the facts and the /start router records them
    after COMMIT."""
    before_t = _transitions("created", "approved")
    account_id = uuid4()
    conn = StartSessionFakeConn(account_id)
    requested = RequestedStream(
        width=1280, height=720, fps=30, codec="h264", bitrate_kbps=3000, latency_mode="balanced"
    )
    result = await start_session(
        conn,
        settings=get_settings(),
        account_id=account_id,
        camera_id=uuid4(),
        requested=requested,
    )
    # No increment inside the service (a rollback must not count).
    assert _transitions("created", "approved") == before_t
    assert result.transition.from_state == "created"
    assert result.transition.to_state == "approved"
    # The router's post-commit one-liner is what counts it.
    result.transition.record()
    assert _transitions("created", "approved") == before_t + 1
