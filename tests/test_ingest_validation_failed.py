"""Route-level tests for the M5.2 enforcement endpoint.

POST /v1/ingest/validation-failed — the streaming-server calls this
(behind its own `UPSTREAM_VALIDATION_ENFORCE` flag) when the comparator
rejects a stream. Unlike the M5.1 report-only `/stream-validations`,
this endpoint MUST:

  - transition the session `approved|streaming` -> `failed`
  - set ended_at + failure_code + failure_message
  - insert a passed=false stream_validations row (mismatch_fields)
  - mirror cameras.status -> 'error'
  - best-effort deprovision the MediaMTX path (suppressing failures)

Pattern mirrors tests/test_ingest_stream_validations.py (TestClient +
fake asyncpg pool) and tests/test_stream_sessions_router.py (respx for
the post-commit MediaMTX DELETE). No real DB, no real network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import respx
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool

_MEDIAMTX_API = "http://mediamtx.test:9997"
_STREAM_KEY = "sess_0123456789abcdef"
_STREAMID = f"publish:{_STREAM_KEY}"


def _session_state_row(*, session_id: UUID, state: str) -> dict[str, Any]:
    """Shape returned by the `select ... from stream_sessions where id`
    query that backs `load_session_state` (used post-mark for deprovision
    and by `_explain_unexpected_state` on the negative paths)."""
    return {
        "id": session_id,
        "account_id": uuid4(),
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "state": state,
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


def _update_returning_row() -> dict[str, Any]:
    """Shape returned by the `update ... set state='failed' ... returning`
    query in `mark_failed` on the success path."""
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


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    """Records the mark_failed writes and serves the two stream_sessions
    reads (the failing UPDATE...RETURNING and the SessionStateRow SELECT).

    - ``update_returns=None`` simulates a no-op UPDATE (session not in
      ``approved|streaming``), forcing the `_explain_unexpected_state`
      branch.
    - ``session_row=None`` makes that branch resolve to 404; a non-None
      row with a terminal state resolves to 409.
    """

    def __init__(
        self,
        *,
        update_returns: dict[str, Any] | None,
        session_row: dict[str, Any] | None,
    ) -> None:
        self._update_returns = update_returns
        self._session_row = session_row
        self.executed_updates: list[tuple[str, tuple[Any, ...]]] = []
        self.executed_camera_updates: list[tuple[str, tuple[Any, ...]]] = []
        self.executed_inserts: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "update stream_sessions" in query and "set state = 'failed'" in query:
            self.executed_updates.append((query, args))
            return self._update_returns
        if "from stream_sessions where id" in query:
            return self._session_row
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        return None

    async def execute(self, query: str, *args: Any) -> str:
        if "update cameras" in query:
            self.executed_camera_updates.append((query, args))
        elif "insert into stream_validations" in query:
            self.executed_inserts.append((query, args))
        elif "update stream_sessions" in query:
            self.executed_updates.append((query, args))
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


def _client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    base = get_settings()
    overridden: Settings = base.model_copy(update={"mediamtx_api_base_url": _MEDIAMTX_API})
    app.dependency_overrides[get_settings] = lambda: overridden
    return TestClient(app)


def _webhook_headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().mediamtx_webhook_secret.get_secret_value()}


def _body(
    *,
    session_id: UUID,
    observed: dict[str, Any] | None = None,
    mismatch_fields: list[str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "session_id": str(session_id),
        "mismatch_fields": mismatch_fields if mismatch_fields is not None else ["width", "height"],
        "failure_code": "resolution_exceeds_limit",
        "failure_message": "width 1920 exceeds approved 1280; height 1080 exceeds approved 720",
    }
    if observed is not None:
        body["observed"] = observed
    return body


_OBSERVED = {"codec": "h264", "width": 1920, "height": 1080, "fps": 30.0, "bitrate_kbps": 0}


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------


@respx.mock
def test_validation_failed_marks_failed_and_deprovisions() -> None:
    session_id = uuid4()
    conn = FakeConn(
        update_returns=_update_returning_row(),
        session_row=_session_state_row(session_id=session_id, state="failed"),
    )
    delete_route = respx.delete(
        f"{_MEDIAMTX_API}/v3/config/paths/delete/{_STREAM_KEY}",
    ).mock(return_value=httpx.Response(200, json={}))

    client = _client(conn)
    r = client.post(
        "/v1/ingest/validation-failed",
        json=_body(session_id=session_id, observed=_OBSERVED),
        headers=_webhook_headers(),
    )

    assert r.status_code == 200
    assert r.json() == {"session_id": str(session_id), "state": "failed"}

    # State transition: the failing UPDATE ran with ended_at + failure_*.
    assert len(conn.executed_updates) == 1
    upd_query, upd_args = conn.executed_updates[0]
    assert "set state = 'failed'" in upd_query
    assert "ended_at = now()" in upd_query
    # Positional args: $1 session_id, $2 failure_code, $3 failure_message.
    assert upd_args[1] == "resolution_exceeds_limit"
    assert upd_args[2].startswith("width 1920 exceeds approved 1280")

    # cameras.status mirrored to 'error'.
    assert len(conn.executed_camera_updates) == 1
    cam_query, cam_args = conn.executed_camera_updates[0]
    assert "update cameras" in cam_query
    assert cam_args[1] == "error"

    # Exactly one passed=false stream_validations row with mismatch_fields.
    assert len(conn.executed_inserts) == 1
    ins_query, ins_args = conn.executed_inserts[0]
    assert "insert into stream_validations" in ins_query
    assert "false" in ins_query  # passed is the hard-coded literal in the VALUES
    assert json.loads(ins_args[1]) == _OBSERVED  # observed jsonb
    assert ins_args[3] == ["width", "height"]  # mismatch_fields

    # Post-commit MediaMTX path delete actually fired.
    assert delete_route.called


@respx.mock
def test_validation_failed_observed_optional_stores_empty_object() -> None:
    """`observed` is optional; mark_failed stores `{}` when omitted."""
    session_id = uuid4()
    conn = FakeConn(
        update_returns=_update_returning_row(),
        session_row=_session_state_row(session_id=session_id, state="failed"),
    )
    respx.delete(f"{_MEDIAMTX_API}/v3/config/paths/delete/{_STREAM_KEY}").mock(
        return_value=httpx.Response(200, json={})
    )

    client = _client(conn)
    r = client.post(
        "/v1/ingest/validation-failed",
        json=_body(session_id=session_id),  # no observed
        headers=_webhook_headers(),
    )

    assert r.status_code == 200
    assert r.json()["state"] == "failed"
    _ins_query, ins_args = conn.executed_inserts[0]
    assert json.loads(ins_args[1]) == {}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_validation_failed_requires_webhook_secret() -> None:
    conn = FakeConn(update_returns=_update_returning_row(), session_row=None)
    client = _client(conn)
    r = client.post("/v1/ingest/validation-failed", json=_body(session_id=uuid4()))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
    # Auth gate runs before any state mutation.
    assert conn.executed_updates == []


def test_validation_failed_rejects_wrong_webhook_secret() -> None:
    conn = FakeConn(update_returns=_update_returning_row(), session_row=None)
    client = _client(conn)
    r = client.post(
        "/v1/ingest/validation-failed",
        json=_body(session_id=uuid4()),
        headers={"X-Upstream-Webhook-Secret": "definitely-wrong"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"
    assert conn.executed_updates == []


# ---------------------------------------------------------------------------
# Negative state machine
# ---------------------------------------------------------------------------


def test_validation_failed_404_on_unknown_session() -> None:
    # UPDATE no-ops AND the SessionStateRow probe finds nothing -> 404.
    conn = FakeConn(update_returns=None, session_row=None)
    client = _client(conn)
    r = client.post(
        "/v1/ingest/validation-failed",
        json=_body(session_id=uuid4()),
        headers=_webhook_headers(),
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
    assert conn.executed_camera_updates == []
    assert conn.executed_inserts == []


def test_validation_failed_409_on_terminal_state() -> None:
    # UPDATE no-ops (already ended) but the row exists -> 409 conflict.
    session_id = uuid4()
    conn = FakeConn(
        update_returns=None,
        session_row=_session_state_row(session_id=session_id, state="ended"),
    )
    client = _client(conn)
    r = client.post(
        "/v1/ingest/validation-failed",
        json=_body(session_id=session_id),
        headers=_webhook_headers(),
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "stream_state_invalid"
    assert conn.executed_camera_updates == []
    assert conn.executed_inserts == []


# ---------------------------------------------------------------------------
# Best-effort deprovision
# ---------------------------------------------------------------------------


@respx.mock
def test_validation_failed_succeeds_even_if_deprovision_fails() -> None:
    """A MediaMTX deprovision 5xx must NOT fail the enforcement call —
    the session is already `failed` in the committed transaction."""
    session_id = uuid4()
    conn = FakeConn(
        update_returns=_update_returning_row(),
        session_row=_session_state_row(session_id=session_id, state="failed"),
    )
    delete_route = respx.delete(
        f"{_MEDIAMTX_API}/v3/config/paths/delete/{_STREAM_KEY}",
    ).mock(return_value=httpx.Response(503, text="MediaMTX overloaded"))

    client = _client(conn)
    r = client.post(
        "/v1/ingest/validation-failed",
        json=_body(session_id=session_id, observed=_OBSERVED),
        headers=_webhook_headers(),
    )

    assert r.status_code == 200
    assert r.json()["state"] == "failed"
    assert delete_route.called
    # The DB writes still happened (transaction committed before deprovision).
    assert len(conn.executed_inserts) == 1
    assert len(conn.executed_camera_updates) == 1
