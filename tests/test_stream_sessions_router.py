"""Integration tests for the `/v1/stream-sessions/start` router orchestration.

Lower layers — `start_session` (DB) and `provision_path` (HTTP) — already
have unit tests. These tests validate the **router glue**: that the
post-transaction MediaMTX provisioning is actually called with the right
arguments after `start_session` commits, and that a provisioning failure
triggers `mark_failed` + a 502 response (not a 5xx leak or a row left in
`approved` state).

Pattern mirrors `tests/test_whep_proxy.py`: TestClient + respx + a fake
asyncpg pool. No real DB, no real network.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, current_agent
from upstream_api.services import usage as usage_service

_MEDIAMTX_API = "http://mediamtx.test:9997"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Mirrors the relevant slice of asyncpg.Connection for `/start`.

    Captures `mark_failed` UPDATEs so the failure-path test can assert
    the session was actually flipped to `failed` after the MediaMTX
    provisioning call raised.
    """

    def __init__(
        self,
        *,
        camera_account_id: UUID,
        camera_agent_id: UUID,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.camera_agent_id = camera_agent_id
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        # Stable session_id so the post-txn handlers and the assertions
        # can reference the same UUID.
        self.session_id = uuid4()
        # Stable streamid so respx can match the MediaMTX URL deterministically.
        self.stream_key = "sess_0123456789abcdef"
        self.streamid = f"publish:{self.stream_key}"

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from cameras" in query:
            return {
                "id": args[0],
                "account_id": self.camera_account_id,
                "agent_id": self.camera_agent_id,
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
                "shared_inference_enabled": False,
            }
        if "insert into stream_sessions" in query:
            # Override the generated session_id with our stable one so the
            # respx URL match is deterministic. start_session passes
            # session_id as args[0]; we ignore that and use ours.
            self.session_id = args[0]
            self.stream_key = args[17].split("publish:")[1]  # streamid arg
            self.streamid = args[17]
            return {
                "id": self.session_id,
                "state": "approved",
                "expires_at": datetime.now(tz=UTC),
                "created_at": datetime.now(tz=UTC),
                "started_at": None,
                "ended_at": None,
            }
        if "update stream_sessions" in query and "set state = 'failed'" in query:
            # mark_failed path. Return enough to satisfy the helper.
            return {
                "state": "failed",
                "started_at": None,
                "lim_width": 1280,
                "lim_height": 720,
                "lim_fps": 30,
                "lim_codec": "h264",
                "lim_bitrate_kbps": 3000,
                "lim_latency_mode": "balanced",
            }
        if "from stream_sessions" in query:
            # SessionStateRow probe used by mark_failed's _explain_unexpected_state
            # fallback (not hit on the happy path; defensive return for negative
            # tests where mark_failed is invoked).
            return {
                "id": args[0],
                "account_id": self.camera_account_id,
                "agent_id": self.camera_agent_id,
                "camera_id": uuid4(),
                "state": "approved",
                "expires_at": datetime.now(tz=UTC),
                "session_token_jti": uuid4(),
                "streamid": self.streamid,
                "lim_width": 1280,
                "lim_height": 720,
                "lim_fps": 30,
                "lim_codec": "h264",
                "lim_bitrate_kbps": 3000,
                "lim_latency_mode": "balanced",
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
        self.executes.append((query, args))
        return "OK"


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


def _agent_auth(account_id: UUID) -> AgentAuth:
    return AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",))


def _build_client(conn: FakeConn, account_id: UUID) -> TestClient:
    """TestClient with overridden DB pool, agent auth, and Settings.

    The MediaMTX HTTP API base URL is overridden to a deterministic test
    host so respx can match the URL. `SettingsDep` resolves through
    `get_settings` (the cached factory), so overriding that one function
    in `dependency_overrides` is what swaps in the test URL.
    """
    app = create_app()
    pool = FakePool(conn)
    app.dependency_overrides[get_pool] = lambda: pool
    app.dependency_overrides[current_agent] = lambda: _agent_auth(account_id)

    base = get_settings()
    overridden: Settings = base.model_copy(update={"mediamtx_api_base_url": _MEDIAMTX_API})
    app.dependency_overrides[get_settings] = lambda: overridden

    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_usage_cache() -> None:
    """The session-started usage event probes `to_regclass` once and
    then caches the result module-globally. Reset before each test so
    a prior test that ran against a fixture returning `True` can't
    short-circuit our `False` here. Same pattern as
    test_session_lifecycle.py.
    """
    usage_service.reset_usage_table_cache()


def _start_body(camera_id: UUID) -> dict[str, Any]:
    return {
        "camera_id": str(camera_id),
        "requested": {
            "width": 1280,
            "height": 720,
            "fps": 30,
            "codec": "h264",
            "bitrate_kbps": 3000,
        },
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@respx.mock
def test_start_provisions_mediamtx_path_after_db_commit() -> None:
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(camera_account_id=account_id, camera_agent_id=uuid4())
    client = _build_client(conn, account_id)

    # respx matches by exact URL; the path segment depends on the
    # streamid the api-server mints. We use a regex match to allow any
    # `sess_<hex>` since the mint is non-deterministic.
    add_route = respx.post(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/sess_[0-9a-f]+$",
    ).mock(return_value=httpx.Response(200, json={}))

    r = client.post(
        "/v1/stream-sessions/start",
        json=_start_body(camera_id),
        headers={"Authorization": "Bearer test"},
    )

    assert r.status_code == 200, r.text
    body = r.json()
    # The returned URL must include the per-session passphrase + pbkeylen.
    assert "passphrase=" in body["srt_url"]
    assert "pbkeylen=32" in body["srt_url"]
    # The session is `approved`, not `failed` — provisioning succeeded.
    assert body["session"]["state"] == "approved"
    # MediaMTX was actually called.
    assert add_route.called, "provision_path must be called after the DB txn"
    # The PATCH endpoint MUST NOT be called when POST succeeds — that would
    # double the round-trip and could clobber a concurrent caller's path.
    request_body = add_route.calls.last.request.content.decode()
    assert '"source": "publisher"' in request_body or '"source":"publisher"' in request_body
    assert '"srtPublishPassphrase"' in request_body
    # Hooks must travel with the per-session path config since
    # `paths.all_others` was removed in Phase 2.
    assert "/hooks/on_ready.sh" in request_body
    assert "/hooks/on_not_ready.sh" in request_body


# ---------------------------------------------------------------------------
# Failure path: MediaMTX unreachable / errors
# ---------------------------------------------------------------------------


@respx.mock
def test_start_marks_session_failed_on_mediamtx_5xx() -> None:
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(camera_account_id=account_id, camera_agent_id=uuid4())
    client = _build_client(conn, account_id)

    respx.post(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/sess_[0-9a-f]+$",
    ).mock(return_value=httpx.Response(503, text="MediaMTX overloaded"))

    r = client.post(
        "/v1/stream-sessions/start",
        json=_start_body(camera_id),
        headers={"Authorization": "Bearer test"},
    )

    assert r.status_code == 502, r.text
    assert r.json()["error"]["code"] == "upstream_unavailable"

    # mark_failed must have run — the audit trail says "this session
    # was approved, then MediaMTX provisioning failed". An UPDATE that
    # sets state='failed' should appear in the captured executes.
    failed_updates = [
        q for q, _ in conn.executes if "update stream_sessions" in q and "'failed'" in q
    ]
    # mark_failed uses fetchrow (not execute) for the UPDATE, so
    # check the stream_validations insert it makes as a side-effect:
    failed_validations = [q for q, _ in conn.executes if "insert into stream_validations" in q]
    # Either the UPDATE or the validations insert proves mark_failed ran.
    assert failed_updates or failed_validations, (
        "mark_failed must have been invoked after MediaMTX 5xx"
    )


@respx.mock
def test_start_502s_on_mediamtx_transport_error() -> None:
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(camera_account_id=account_id, camera_agent_id=uuid4())
    client = _build_client(conn, account_id)

    respx.post(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/sess_[0-9a-f]+$",
    ).mock(side_effect=httpx.ConnectError("no route to host"))

    r = client.post(
        "/v1/stream-sessions/start",
        json=_start_body(camera_id),
        headers={"Authorization": "Bearer test"},
    )

    assert r.status_code == 502, r.text
    assert r.json()["error"]["code"] == "upstream_unavailable"


# ---------------------------------------------------------------------------
# POST→PATCH fallback: orphan path retry
# ---------------------------------------------------------------------------


@respx.mock
def test_start_falls_back_to_patch_when_path_already_exists() -> None:
    """If a previous `/start` succeeded with the same streamid (or a
    prior deprovision failed), POST add will 400. The router shouldn't
    care — it should still return 200. The provisioning module handles
    the POST→PATCH fallback transparently.
    """
    account_id = uuid4()
    camera_id = uuid4()
    conn = FakeConn(camera_account_id=account_id, camera_agent_id=uuid4())
    client = _build_client(conn, account_id)

    add_route = respx.post(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/sess_[0-9a-f]+$",
    ).mock(return_value=httpx.Response(400, json={"error": "path already exists"}))
    patch_route = respx.patch(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/patch/sess_[0-9a-f]+$",
    ).mock(return_value=httpx.Response(200, json={}))

    r = client.post(
        "/v1/stream-sessions/start",
        json=_start_body(camera_id),
        headers={"Authorization": "Bearer test"},
    )

    assert r.status_code == 200, r.text
    assert add_route.called
    assert patch_route.called, "PATCH fallback must run when POST returns 400"
