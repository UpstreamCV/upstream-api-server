"""P5-A — route-level tests for GET /v1/cameras/{camera_id}/observations/recent.

Pattern mirrors tests/test_list_endpoints.py: TestClient + a fake asyncpg
pool, no real DB. The endpoint reads the append-only ``observations`` table
written by the processor (P4-B..P4-E). It is a debug/visibility read — the
fake ``fetch`` re-implements the documented WHERE/ORDER/LIMIT so filtering,
ordering and the limit are genuinely exercised; a separate test pins the
production SQL so the simulation cannot silently drift.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller
from upstream_api.services.processor import _RECENT_OBSERVATIONS_SQL

_P4_TYPES = (
    "processor.line_crossing",
    "processor.roi_occupancy",
    "processor.zone_entered",
    "processor.zone_exited",
    "processor.dwell_started",
    "processor.dwell_updated",
    "processor.dwell_exceeded",
)

_RESPONSE_FIELDS = {
    "id",
    "camera_id",
    "stream_session_id",
    "processor_id",
    "package_id",
    "type",
    "payload",
    "observed_at",
    "created_at",
}


# ---------------------------------------------------------------------------
# Fake pool/conn — handles the ownership fetchrow AND the list fetch.
# ---------------------------------------------------------------------------


class FakeConn:
    """Mirrors the slice of asyncpg.Connection the endpoint touches.

    ``camera_account_id=None`` simulates a camera that does not exist. The
    ``fetch`` simulation re-implements the SQL: scope by account_id +
    camera_id, optional type filter, observed_at desc, limit. ``fetch_calls``
    records every ``fetch`` arg tuple so a test can assert what the router
    forwarded.
    """

    def __init__(
        self,
        *,
        camera_account_id: UUID | None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.camera_account_id = camera_account_id
        self.rows = rows or []
        self.fetch_calls: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "from cameras" in query
        if self.camera_account_id is None:
            return None
        return {"account_id": self.camera_account_id}

    async def fetch(self, _query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append(args)
        account_id, camera_id, type_, limit = args
        result = [
            r
            for r in self.rows
            if r["account_id"] == account_id
            and r["camera_id"] == camera_id
            and (type_ is None or r["type"] == type_)
        ]
        result.sort(key=lambda r: r["observed_at"], reverse=True)
        return result[:limit]


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


def _client(conn: FakeConn, *, account_id: UUID) -> TestClient:
    """Client authed as an agent scoped to ``account_id``."""
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[current_caller] = lambda: CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )
    return TestClient(app)


def _client_no_auth(conn: FakeConn) -> TestClient:
    """Client with NO current_caller override — auth resolves naturally."""
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    return TestClient(app)


def _obs_row(
    *,
    account_id: UUID,
    camera_id: UUID,
    observed_at: datetime,
    type_: str = "processor.line_crossing",
    payload: dict[str, Any] | None = None,
    processor_id: str | None = "proc-1",
    package_id: str | None = "pkg.detect.v1",
    stream_session_id: UUID | None = None,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "camera_id": camera_id,
        "stream_session_id": stream_session_id or uuid4(),
        "processor_id": processor_id,
        "package_id": package_id,
        "type": type_,
        "payload": payload if payload is not None else {"count": 1},
        "observed_at": observed_at,
        "created_at": observed_at,
    }


_NOW = datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC)
_PATH = "/v1/cameras/{camera_id}/observations/recent"


# ---------------------------------------------------------------------------
# Happy path.
# ---------------------------------------------------------------------------


def test_owned_camera_returns_observations() -> None:
    account, camera = uuid4(), uuid4()
    rows = [
        _obs_row(account_id=account, camera_id=camera, observed_at=_NOW),
        _obs_row(
            account_id=account,
            camera_id=camera,
            observed_at=_NOW - timedelta(minutes=1),
        ),
    ]
    conn = FakeConn(camera_account_id=account, rows=rows)
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert len(r.json()["observations"]) == 2


def test_observations_newest_first() -> None:
    account, camera = uuid4(), uuid4()
    # Inserted oldest-first so a no-op router would fail the assertion.
    rows = [
        _obs_row(
            account_id=account,
            camera_id=camera,
            observed_at=_NOW - timedelta(minutes=10),
        ),
        _obs_row(account_id=account, camera_id=camera, observed_at=_NOW),
        _obs_row(
            account_id=account,
            camera_id=camera,
            observed_at=_NOW - timedelta(minutes=5),
        ),
    ]
    conn = FakeConn(camera_account_id=account, rows=rows)
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    stamps = [o["observed_at"] for o in r.json()["observations"]]
    assert stamps == sorted(stamps, reverse=True)


def test_payload_returned_intact() -> None:
    account, camera = uuid4(), uuid4()
    payload = {
        "zone_id": "lobby",
        "track_id": "t-42",
        "threshold_seconds": 60,
        "duration_seconds": 73.5,
        "nested": {"source": {"runtime": "deepstream"}},
    }
    conn = FakeConn(
        camera_account_id=account,
        rows=[
            _obs_row(
                account_id=account,
                camera_id=camera,
                observed_at=_NOW,
                type_="processor.dwell_exceeded",
                payload=payload,
            )
        ],
    )
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json()["observations"][0]["payload"] == payload


# ---------------------------------------------------------------------------
# Limit.
# ---------------------------------------------------------------------------


def test_default_limit_is_50() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=account, rows=[])
    _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert conn.fetch_calls[0][3] == 50


def test_explicit_limit_forwarded() -> None:
    account, camera = uuid4(), uuid4()
    rows = [
        _obs_row(
            account_id=account,
            camera_id=camera,
            observed_at=_NOW - timedelta(seconds=i),
        )
        for i in range(8)
    ]
    conn = FakeConn(camera_account_id=account, rows=rows)
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera), params={"limit": 3})
    assert r.status_code == 200, r.text
    assert conn.fetch_calls[0][3] == 3
    assert len(r.json()["observations"]) == 3


def test_limit_above_max_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=account, rows=[])
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera), params={"limit": 201})
    assert r.status_code == 422
    assert conn.fetch_calls == []


def test_limit_below_min_rejected() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=account, rows=[])
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera), params={"limit": 0})
    assert r.status_code == 422
    assert conn.fetch_calls == []


# ---------------------------------------------------------------------------
# Type filter.
# ---------------------------------------------------------------------------


def test_type_filter_narrows_results() -> None:
    account, camera = uuid4(), uuid4()
    rows = [
        _obs_row(
            account_id=account,
            camera_id=camera,
            observed_at=_NOW,
            type_="processor.line_crossing",
        ),
        _obs_row(
            account_id=account,
            camera_id=camera,
            observed_at=_NOW - timedelta(minutes=1),
            type_="processor.dwell_exceeded",
        ),
    ]
    conn = FakeConn(camera_account_id=account, rows=rows)
    r = _client(conn, account_id=account).get(
        _PATH.format(camera_id=camera),
        params={"type": "processor.dwell_exceeded"},
    )
    assert r.status_code == 200, r.text
    body = r.json()["observations"]
    assert len(body) == 1
    assert body[0]["type"] == "processor.dwell_exceeded"
    assert conn.fetch_calls[0][2] == "processor.dwell_exceeded"


def test_unknown_type_returns_empty() -> None:
    account, camera = uuid4(), uuid4()
    rows = [_obs_row(account_id=account, camera_id=camera, observed_at=_NOW)]
    conn = FakeConn(camera_account_id=account, rows=rows)
    r = _client(conn, account_id=account).get(
        _PATH.format(camera_id=camera),
        params={"type": "processor.does_not_exist"},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"observations": []}


# ---------------------------------------------------------------------------
# Empty / not-found.
# ---------------------------------------------------------------------------


def test_empty_camera_returns_empty_list() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=account, rows=[])
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json() == {"observations": []}


def test_cross_tenant_camera_returns_404() -> None:
    caller_account, other_account, camera = uuid4(), uuid4(), uuid4()
    # The camera exists but belongs to a different account.
    conn = FakeConn(camera_account_id=other_account, rows=[])
    r = _client(conn, account_id=caller_account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    # Ownership check fails before any observation query runs.
    assert conn.fetch_calls == []


def test_unknown_camera_returns_404() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=None, rows=[])
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "camera_not_found"
    assert conn.fetch_calls == []


def test_unauthenticated_returns_401() -> None:
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=account, rows=[])
    r = _client_no_auth(conn).get(_PATH.format(camera_id=camera))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_credentials"
    assert conn.fetch_calls == []


# ---------------------------------------------------------------------------
# Tenant scoping + response shape.
# ---------------------------------------------------------------------------


def test_query_is_scoped_to_caller_account() -> None:
    """The observation query's $1 account_id is the caller's scoped account,
    never a value taken from the path or payload."""
    account, camera = uuid4(), uuid4()
    conn = FakeConn(camera_account_id=account, rows=[])
    _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert conn.fetch_calls[0][0] == account
    assert conn.fetch_calls[0][1] == camera


def test_response_includes_expected_fields_and_no_account_id() -> None:
    account, camera = uuid4(), uuid4()
    # Nullable processor_id / package_id are exercised here.
    conn = FakeConn(
        camera_account_id=account,
        rows=[
            _obs_row(
                account_id=account,
                camera_id=camera,
                observed_at=_NOW,
                processor_id=None,
                package_id=None,
            )
        ],
    )
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    obs = r.json()["observations"][0]
    assert set(obs.keys()) == _RESPONSE_FIELDS
    assert "account_id" not in obs
    assert obs["processor_id"] is None
    assert obs["package_id"] is None


@pytest.mark.parametrize("obs_type", _P4_TYPES)
def test_all_p4_types_round_trip_as_plain_strings(obs_type: str) -> None:
    """The response model types ``type`` as a plain str, so every real P4
    observation type (and any future/older one) reads back unchanged."""
    account, camera = uuid4(), uuid4()
    conn = FakeConn(
        camera_account_id=account,
        rows=[
            _obs_row(
                account_id=account,
                camera_id=camera,
                observed_at=_NOW,
                type_=obs_type,
            )
        ],
    )
    r = _client(conn, account_id=account).get(_PATH.format(camera_id=camera))
    assert r.status_code == 200, r.text
    assert r.json()["observations"][0]["type"] == obs_type


# ---------------------------------------------------------------------------
# Pin the production SQL so the FakeConn simulation cannot drift.
# ---------------------------------------------------------------------------


def test_recent_observations_sql_pins_predicates() -> None:
    sql = _RECENT_OBSERVATIONS_SQL
    assert "where account_id = $1" in sql
    assert "and camera_id = $2" in sql
    assert "($3::text is null or type = $3)" in sql
    assert "order by observed_at desc" in sql
    assert "limit $4" in sql
    # account_id must never be selected onto the read response.
    assert "select\n    id, camera_id" in sql
