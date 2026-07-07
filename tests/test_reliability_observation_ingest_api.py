"""Upstream Reliability M1a — observation ingest API tests.

Scope enforcement, tenant-from-token, body-spoof rejection, cross-account 404,
batch insert, idempotent replay, and detector-failure isolation — all against
hand-rolled fakes (no real DB), mirroring the M6 ingest/read test style.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.api_tokens import ALLOWED_SCOPES
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller

_NOW = datetime.now(tz=UTC)
_OBSERVED_AT = _NOW.isoformat()


class FakeTxn:
    async def __aenter__(self) -> FakeTxn:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class FakeIngestConn:
    def __init__(
        self,
        *,
        owned: tuple[UUID, ...] = (),
        existing_batch: dict[str, Any] | None = None,
    ) -> None:
        self.owned = set(owned)
        self.existing_batch = existing_batch
        self.new_batch_id = uuid4()
        self.executemany_calls: list[list[tuple[Any, ...]]] = []
        self.finalized: list[tuple[Any, ...]] = []

    def transaction(self) -> FakeTxn:
        return FakeTxn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from cameras" in query:
            ids = args[0]
            return [{"id": cid} for cid in ids if cid in self.owned]
        if "from cv_observations" in query:
            return []  # no history -> detectors do nothing
        if "from event_destinations" in query:
            return []
        return []

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "insert into cv_observation_batches" in query:
            return None if self.existing_batch is not None else self.new_batch_id
        return None

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "select id, observation_count" in query:
            return self.existing_batch
        return None

    async def executemany(self, query: str, args_list: list[tuple[Any, ...]]) -> None:
        assert "insert into cv_observations" in query
        self.executemany_calls.append(args_list)

    async def execute(self, query: str, *args: Any) -> str:
        if "update cv_observation_batches" in query:
            self.finalized.append(args)
        return "OK"


class AcquireCtx:
    def __init__(self, conn: FakeIngestConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeIngestConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeIngestConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _agent_caller(account_id: UUID, scopes: tuple[str, ...]) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=scopes), user=None
    )


def _user_caller(account_id: UUID) -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(user_id=uuid4(), memberships={account_id: "owner"}, aal="aal1", amr=()),
    )


def _client(conn: FakeIngestConn, caller: CallerAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


_URL = "/v1/reliability/observations"


def _body(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "client_batch_id": "batch_1",
        "source": "edge-box-4",
        "observations": [
            {
                "type": "event_observed",
                "external_camera_key": "dock-12",
                "observed_at": _OBSERVED_AT,
                "payload": {"event_name": "line_crossing", "track_id": "t1"},
            }
        ],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Scope + auth.
# ---------------------------------------------------------------------------


def test_ingest_rejects_token_without_agent_or_ingest_scope() -> None:
    # A token with neither `agent` nor `reliability:ingest` cannot push.
    account = uuid4()
    r = _client(FakeIngestConn(), _agent_caller(account, ("events:read",))).post(_URL, json=_body())
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_ingest_with_agent_scope_succeeds() -> None:
    # The managed agent's local collector reuses its `agent` token to ingest.
    account = uuid4()
    conn = FakeIngestConn()
    r = _client(conn, _agent_caller(account, ("agent",))).post(_URL, json=_body())
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == 1


def test_ingest_with_reliability_ingest_scope_succeeds() -> None:
    # External CV systems use a dedicated reliability:ingest token.
    account = uuid4()
    conn = FakeIngestConn()
    r = _client(conn, _agent_caller(account, ("reliability:ingest",))).post(_URL, json=_body())
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == 1
    assert UUID(body["batch_id"]) == conn.new_batch_id
    assert "server_time" in body
    assert len(conn.executemany_calls) == 1  # observations inserted


def test_jwt_user_can_ingest_without_token_scope() -> None:
    account = uuid4()
    r = _client(FakeIngestConn(), _user_caller(account)).post(_URL, json=_body())
    assert r.status_code == 200, r.text


def test_unauthenticated_401() -> None:
    r = _client(FakeIngestConn(), None).post(_URL, json=_body())
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Tenant integrity.
# ---------------------------------------------------------------------------


def test_cross_account_camera_returns_404() -> None:
    account = uuid4()
    foreign_camera = uuid4()  # not in `owned` -> belongs to nobody this caller owns
    conn = FakeIngestConn(owned=())
    body = _body(
        observations=[
            {
                "type": "detection_summary",
                "camera_id": str(foreign_camera),
                "observed_at": _OBSERVED_AT,
                "payload": {"counts_by_class": {"forklift": 2}},
            }
        ]
    )
    r = _client(conn, _agent_caller(account, ("reliability:ingest",))).post(_URL, json=body)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.executemany_calls == []  # nothing inserted on rejection


def test_owned_camera_passes_ownership_check() -> None:
    account = uuid4()
    camera = uuid4()
    conn = FakeIngestConn(owned=(camera,))
    body = _body(
        observations=[
            {
                "type": "detection_summary",
                "camera_id": str(camera),
                "observed_at": _OBSERVED_AT,
                "payload": {"counts_by_class": {"forklift": 2}},
            }
        ]
    )
    r = _client(conn, _agent_caller(account, ("reliability:ingest",))).post(_URL, json=body)
    assert r.status_code == 200, r.text


def test_spoofed_account_id_in_body_is_ignored() -> None:
    account = uuid4()
    conn = FakeIngestConn()
    # Extra identity fields in the body are not request schema fields; they are
    # silently dropped and the account comes from the credential.
    body = _body(account_id=str(uuid4()), batch_id=str(uuid4()), id=str(uuid4()))
    r = _client(conn, _agent_caller(account, ("reliability:ingest",))).post(_URL, json=body)
    assert r.status_code == 200, r.text
    assert UUID(r.json()["batch_id"]) == conn.new_batch_id  # server-generated id


# ---------------------------------------------------------------------------
# Idempotency.
# ---------------------------------------------------------------------------


def test_duplicate_client_batch_id_is_idempotent() -> None:
    account = uuid4()
    original_id = uuid4()
    conn = FakeIngestConn(existing_batch={"id": original_id, "observation_count": 7})
    r = _client(conn, _agent_caller(account, ("reliability:ingest",))).post(_URL, json=_body())
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == 7  # prior count, not len(observations)
    assert UUID(body["batch_id"]) == original_id
    assert conn.executemany_calls == []  # no re-insert on replay


# ---------------------------------------------------------------------------
# Validation.
# ---------------------------------------------------------------------------


def test_empty_observations_rejected() -> None:
    account = uuid4()
    r = _client(FakeIngestConn(), _agent_caller(account, ("reliability:ingest",))).post(
        _URL, json=_body(observations=[])
    )
    assert r.status_code == 422


def test_too_many_observations_rejected() -> None:
    account = uuid4()
    many = [
        {"type": "event_observed", "external_camera_key": "k", "observed_at": _OBSERVED_AT}
        for _ in range(501)
    ]
    r = _client(FakeIngestConn(), _agent_caller(account, ("reliability:ingest",))).post(
        _URL, json=_body(observations=many)
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Detector failure isolation.
# ---------------------------------------------------------------------------


def test_detector_failure_does_not_break_ingest(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("detector exploded")

    monkeypatch.setattr("upstream_api.routers.reliability_ingest.evaluate_observation_batch", _boom)
    account = uuid4()
    conn = FakeIngestConn()
    r = _client(conn, _agent_caller(account, ("reliability:ingest",))).post(_URL, json=_body())
    assert r.status_code == 200, r.text  # ingest survives a detector blow-up
    assert len(conn.executemany_calls) == 1  # observations still persisted


# ---------------------------------------------------------------------------
# Scope vocabulary (mintability).
# ---------------------------------------------------------------------------


def test_reliability_scopes_are_mintable() -> None:
    assert {"reliability:ingest", "reliability:read", "reliability:write"} <= ALLOWED_SCOPES
    # An unknown reliability scope is still rejected at mint time.
    assert {"reliability:admin"} - ALLOWED_SCOPES == {"reliability:admin"}
