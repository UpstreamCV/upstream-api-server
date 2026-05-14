"""Tests for the WHEP proxy endpoints (POST/DELETE /v1/stream-sessions/{id}/whep).

Uses respx to mock MediaMTX so we never reach the network. The session row is
served from a FakeConn / FakePool that mirrors the asyncpg interface.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller
from upstream_api.services.whep_proxy import parse_mediamtx_location

# Match the test conftest default; override_settings is not necessary because
# we plumb the host directly into the respx mock URLs.
MEDIAMTX_BASE = "http://localhost:8889"

CANNED_SDP_OFFER = b"v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\n"
CANNED_SDP_ANSWER = b"v=0\r\no=mediamtx 1 1 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\n"
SESSION_PATH = "sess_abc123def456"
STREAMID = f"publish:{SESSION_PATH}"


class FakeConn:
    """asyncpg-connection stand-in keyed by session UUID."""

    def __init__(self, rows: dict[UUID, dict[str, Any]] | None = None) -> None:
        self.rows = rows or {}

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        # Both the POST and DELETE endpoints look up by session id with
        # `where id = $1`. Defensive grep keeps the test honest about which
        # query it's mocking.
        if "from stream_sessions where id = $1" not in query:
            return None
        return self.rows.get(args[0])


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


def _user_caller(account_id: UUID) -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(
            user_id=uuid4(),
            memberships={account_id: "owner"},
            aal="aal1",
            amr=(),
        ),
    )


def _agent_caller(account_id: UUID) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
        user=None,
    )


def _build_client(rows: dict[UUID, dict[str, Any]], caller: CallerAuth) -> TestClient:
    """Construct a TestClient with overridden pool + caller. No lifespan run."""
    app = create_app()
    pool = FakePool(FakeConn(rows))
    app.dependency_overrides[get_pool] = lambda: pool
    app.dependency_overrides[current_caller] = lambda: caller
    return TestClient(app)


def _session_row(*, account_id: UUID, state: str = "streaming", streamid: str = STREAMID) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "account_id": account_id,
        "state": state,
        "streamid": streamid,
    }


# -----------------------------------------------------------------------------
# parse_mediamtx_location — unit test the helper directly.
# -----------------------------------------------------------------------------


def test_parse_location_well_formed() -> None:
    assert parse_mediamtx_location("/sess_abc/whepsession/xyz-123") == "xyz-123"


def test_parse_location_trailing_slash() -> None:
    assert parse_mediamtx_location("/sess_abc/whepsession/xyz-123/") == "xyz-123"


def test_parse_location_absolute_url() -> None:
    assert (
        parse_mediamtx_location("http://localhost:8889/sess_abc/whepsession/xyz-123")
        == "xyz-123"
    )


def test_parse_location_none() -> None:
    assert parse_mediamtx_location(None) is None


def test_parse_location_empty() -> None:
    assert parse_mediamtx_location("") is None


def test_parse_location_no_resource() -> None:
    assert parse_mediamtx_location("/sess_abc/whepsession/") is None


def test_parse_location_unexpected_shape() -> None:
    assert parse_mediamtx_location("/totally/different/path") is None


def test_parse_location_rejects_dot_in_resource_id() -> None:
    # MediaMTX uses UUIDs (no dots) but historically _LOCATION_RE allowed
    # dots, which would have created a POST/DELETE inconsistency: POST
    # succeeds with Location: "abc.def" while the DELETE router regex
    # (_RESOURCE_ID_RE) rejects dots. Aligning both sides at "no dots"
    # collapses the mismatch into a single 502 outcome at parse time.
    assert parse_mediamtx_location("/sess_abc/whepsession/abc.def") is None


# -----------------------------------------------------------------------------
# POST /v1/stream-sessions/{id}/whep
# -----------------------------------------------------------------------------


@respx.mock
def test_whep_offer_happy_path() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))

    respx.post(f"{MEDIAMTX_BASE}/{SESSION_PATH}/whep").mock(
        return_value=httpx.Response(
            201,
            content=CANNED_SDP_ANSWER,
            headers={
                "Content-Type": "application/sdp",
                "Location": f"/{SESSION_PATH}/whepsession/abc-123",
            },
        )
    )

    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={
            "Content-Type": "application/sdp",
            "Authorization": "Bearer test",
        },
    )

    assert r.status_code == 201
    assert r.headers["content-type"].startswith("application/sdp")
    assert r.headers["location"] == "abc-123"
    assert r.content == CANNED_SDP_ANSWER


@respx.mock
def test_whep_offer_agent_caller_works() -> None:
    """Agents using their token (not a user JWT) can also play back streams.

    Proves the dual-auth via CurrentCaller is hooked up. Same shape as the
    happy path test; only the caller fixture differs.
    """
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _agent_caller(account_id))

    respx.post(f"{MEDIAMTX_BASE}/{SESSION_PATH}/whep").mock(
        return_value=httpx.Response(
            201,
            content=CANNED_SDP_ANSWER,
            headers={
                "Content-Type": "application/sdp",
                "Location": f"/{SESSION_PATH}/whepsession/abc-123",
            },
        )
    )

    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 201


@respx.mock
def test_whep_offer_cross_account_returns_404() -> None:
    """User in a different account must not see that the session exists."""
    session_owner = uuid4()
    other_user_account = uuid4()
    row = _session_row(account_id=session_owner)
    client = _build_client({row["id"]: row}, _user_caller(other_user_account))

    # respx route is intentionally NOT registered — if the router reached out,
    # the test would fail with a connection error (respx's default behaviour
    # when no route matches).
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
        params={"account_id": str(other_user_account)},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def test_whep_offer_unknown_session_returns_404() -> None:
    account_id = uuid4()
    client = _build_client({}, _user_caller(account_id))
    r = client.post(
        f"/v1/stream-sessions/{uuid4()}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


@pytest.mark.parametrize("state", ["approved", "ended", "expired", "failed"])
def test_whep_offer_non_streaming_state_returns_409(state: str) -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id, state=state)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "session_not_streaming"


def test_whep_offer_wrong_content_type_returns_415() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        json={"sdp": "v=0"},
        headers={"Authorization": "Bearer test"},
    )
    assert r.status_code == 415
    assert r.json()["error"]["code"] == "invalid_content_type"


def test_whep_offer_empty_body_returns_400() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=b"",
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_sdp"


@respx.mock
def test_whep_offer_mediamtx_404_returns_stream_not_ready() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    respx.post(f"{MEDIAMTX_BASE}/{SESSION_PATH}/whep").mock(
        return_value=httpx.Response(404)
    )
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "stream_not_ready"


@respx.mock
def test_whep_offer_mediamtx_500_returns_502() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    respx.post(f"{MEDIAMTX_BASE}/{SESSION_PATH}/whep").mock(
        return_value=httpx.Response(500, content=b"oops")
    )
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "upstream_unavailable"


@respx.mock
def test_whep_offer_mediamtx_timeout_returns_502() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    respx.post(f"{MEDIAMTX_BASE}/{SESSION_PATH}/whep").mock(
        side_effect=httpx.ConnectTimeout("timeout")
    )
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "upstream_unavailable"


@respx.mock
def test_whep_offer_missing_location_returns_502() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    respx.post(f"{MEDIAMTX_BASE}/{SESSION_PATH}/whep").mock(
        return_value=httpx.Response(
            201,
            content=CANNED_SDP_ANSWER,
            headers={"Content-Type": "application/sdp"},  # no Location
        )
    )
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "upstream_unavailable"


def test_whep_offer_malformed_streamid_returns_502() -> None:
    """If the session row's streamid doesn't start with `publish:`, fail safe.

    Real api-server-issued streamids always have the prefix. Data corruption
    surfaces as 502 rather than something the client can paper over.
    """
    account_id = uuid4()
    row = _session_row(account_id=account_id, streamid="garbage")
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    r = client.post(
        f"/v1/stream-sessions/{row['id']}/whep",
        content=CANNED_SDP_OFFER,
        headers={"Content-Type": "application/sdp", "Authorization": "Bearer test"},
    )
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "upstream_unavailable"


# -----------------------------------------------------------------------------
# DELETE /v1/stream-sessions/{id}/whep/{resource_id}
# -----------------------------------------------------------------------------


@respx.mock
def test_whep_delete_happy_path() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))

    route = respx.delete(
        f"{MEDIAMTX_BASE}/{SESSION_PATH}/whepsession/abc-123"
    ).mock(return_value=httpx.Response(200))

    r = client.delete(
        f"/v1/stream-sessions/{row['id']}/whep/abc-123",
        headers={"Authorization": "Bearer test"},
    )
    assert r.status_code == 204
    assert route.called


def test_whep_delete_invalid_resource_id_does_not_call_mediamtx() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))

    # Two attempts at path-traversal characters. Both should bounce at the
    # FastAPI router level before any handler runs; FastAPI's path parsing
    # also strips slashes from path params, so the regex provides
    # defense-in-depth.
    for bad in ["abc.123", "../etc/passwd"]:
        r = client.delete(
            f"/v1/stream-sessions/{row['id']}/whep/{bad}",
            headers={"Authorization": "Bearer test"},
        )
        # `../etc/passwd` may 404 (no route matches the path) or 400
        # (regex rejects). Either is fine — neither leaks data or reaches
        # MediaMTX. Dots are not in the allowed regex either.
        assert r.status_code in {400, 404}


@respx.mock
def test_whep_delete_cross_account_returns_404() -> None:
    """Cross-account DELETE attempts must 404, not touch MediaMTX."""
    session_owner = uuid4()
    other_user_account = uuid4()
    row = _session_row(account_id=session_owner)
    client = _build_client({row["id"]: row}, _user_caller(other_user_account))

    route = respx.delete(
        f"{MEDIAMTX_BASE}/{SESSION_PATH}/whepsession/abc-123"
    ).mock(return_value=httpx.Response(200))

    r = client.delete(
        f"/v1/stream-sessions/{row['id']}/whep/abc-123",
        headers={"Authorization": "Bearer test"},
        params={"account_id": str(other_user_account)},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"
    assert not route.called


@respx.mock
def test_whep_delete_mediamtx_404_still_returns_204() -> None:
    """Best-effort teardown: MediaMTX already cleaned up is still success."""
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    respx.delete(
        f"{MEDIAMTX_BASE}/{SESSION_PATH}/whepsession/abc-123"
    ).mock(return_value=httpx.Response(404))

    r = client.delete(
        f"/v1/stream-sessions/{row['id']}/whep/abc-123",
        headers={"Authorization": "Bearer test"},
    )
    assert r.status_code == 204


@respx.mock
def test_whep_delete_mediamtx_timeout_still_returns_204() -> None:
    account_id = uuid4()
    row = _session_row(account_id=account_id)
    client = _build_client({row["id"]: row}, _user_caller(account_id))
    respx.delete(
        f"{MEDIAMTX_BASE}/{SESSION_PATH}/whepsession/abc-123"
    ).mock(side_effect=httpx.ConnectTimeout("timeout"))

    r = client.delete(
        f"/v1/stream-sessions/{row['id']}/whep/abc-123",
        headers={"Authorization": "Bearer test"},
    )
    assert r.status_code == 204
