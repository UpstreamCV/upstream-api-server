"""Module 7 M1 — usage summary + monthly minutes quota enforcement tests.

Three layers:

1. ``usage_state`` / ``current_usage_period`` — pure helpers.
2. ``GET /v1/usage/summary`` — route level with a fake pool (shape, counts,
   states, plan label, tenancy, auth).
3. ``check_monthly_minutes_quota`` via ``check_session_start`` — quota
   enforcement without breaking the existing session-limit checks.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.db.pool import get_pool
from upstream_api.errors import LimitExceeded, NotFoundError
from upstream_api.schemas.usage import usage_state
from upstream_api.security.deps import UserAuth, current_user
from upstream_api.services.limits import RequestedStream, check_session_start
from upstream_api.services.usage import current_usage_period
from upstream_api.services.usage_summary import (
    _ACCOUNT_AND_LIMITS_SQL,
    _USAGE_COUNTERS_SQL,
)

# ---------------------------------------------------------------------------
# Pure helpers.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("used", "limit", "expected"),
    [
        (0, 10, "ok"),
        (7, 10, "ok"),  # 70% < 80%
        (8, 10, "near_limit"),  # exactly 80%
        (9, 10, "near_limit"),
        (10, 10, "at_limit"),
        (11, 10, "over_limit"),
        (0, None, "ok"),  # unlimited
        (10_000, None, "ok"),
        (0, 0, "at_limit"),  # zero quota: nothing allowed
        (1, 0, "over_limit"),
        (430, 600, "ok"),  # 71.6%
        (480, 600, "near_limit"),  # exactly 80%
    ],
)
def test_usage_state(used: int, limit: int | None, expected: str) -> None:
    assert usage_state(used, limit) == expected


def test_current_usage_period_utc_month() -> None:
    start, end = current_usage_period(datetime(2026, 6, 11, 15, 30, tzinfo=UTC))
    assert start == datetime(2026, 6, 1, tzinfo=UTC)
    assert end == datetime(2026, 7, 1, tzinfo=UTC)


def test_current_usage_period_december_wraps_year() -> None:
    start, end = current_usage_period(datetime(2026, 12, 31, 23, 59, tzinfo=UTC))
    assert start == datetime(2026, 12, 1, tzinfo=UTC)
    assert end == datetime(2027, 1, 1, tzinfo=UTC)


def test_current_usage_period_naive_treated_as_utc() -> None:
    start, _ = current_usage_period(datetime(2026, 6, 11, 12, 0))
    assert start == datetime(2026, 6, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Route level — GET /v1/usage/summary.
# ---------------------------------------------------------------------------


class FakeConn:
    def __init__(
        self,
        *,
        account_row: dict[str, Any] | None,
        usage_row: dict[str, Any],
        billing_row: dict[str, Any] | None = None,
    ) -> None:
        self.account_row = account_row
        self.usage_row = usage_row
        self.billing_row = billing_row
        self.usage_args: tuple[Any, ...] | None = None

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "from accounts a" in query:
            return self.account_row
        if "stream_seconds_completed" in query:
            self.usage_args = args
            return self.usage_row
        if "from account_billing" in query:
            return self.billing_row
        raise AssertionError(f"unexpected fetchrow: {query}")


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


def _account_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "plan": "free",
        "status": "active",
        "max_cameras": 5,
        "max_concurrent_sessions": 5,
        "monthly_minutes_quota": 600,
        "allowed_stream_types": ["rtsp", "webcam"],
    }
    row.update(overrides)
    return row


def _usage_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "cameras": 2,
        "active_streams": 1,
        "stream_seconds_completed": 25_800,  # 430 minutes
        "workflow_runs": 1204,
        "alerts": 86,
        "rules": 4,
        "workflows": 1,
    }
    row.update(overrides)
    return row


def _user(account_id: UUID, role: str = "viewer") -> UserAuth:
    return UserAuth(user_id=uuid4(), memberships={account_id: role}, aal="aal1", amr=("password",))


def _client(conn: FakeConn, user: UserAuth | None) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if user is not None:
        app.dependency_overrides[current_user] = lambda: user
    return TestClient(app)


def test_summary_shape_and_values() -> None:
    account = uuid4()
    conn = FakeConn(account_row=_account_row(), usage_row=_usage_row())
    r = _client(conn, _user(account)).get("/v1/usage/summary", params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body) == {"plan", "period", "limits", "usage", "states", "billing"}
    assert body["plan"] == {"key": "free", "label": "Free", "status": "active"}
    assert body["limits"] == {
        "max_cameras": 5,
        "max_concurrent_sessions": 5,
        "monthly_minutes_quota": 600,
        "allowed_stream_types": ["rtsp", "webcam"],
    }
    assert body["usage"] == {
        "cameras": 2,
        "active_streams": 1,
        "stream_minutes": 430,  # 25_800 seconds // 60
        "workflow_runs": 1204,
        "alerts": 86,
        "rules": 4,
        "workflows": 1,
    }
    assert body["states"] == {
        "cameras": "ok",  # 2/5 = 40%
        "active_streams": "ok",  # 1/5 = 20%
        "stream_minutes": "ok",  # 430/600 = 71.6%
    }


def test_summary_period_is_current_utc_month() -> None:
    account = uuid4()
    conn = FakeConn(account_row=_account_row(), usage_row=_usage_row())
    r = _client(conn, _user(account)).get("/v1/usage/summary", params={"account_id": str(account)})
    body = r.json()
    start = datetime.fromisoformat(body["period"]["start"])
    end = datetime.fromisoformat(body["period"]["end"])
    expected_start, expected_end = current_usage_period()
    assert start == expected_start
    assert end == expected_end
    # And the SAME boundaries were bound into the counters query.
    assert conn.usage_args is not None
    assert conn.usage_args[1] == expected_start
    assert conn.usage_args[2] == expected_end


@pytest.mark.parametrize(
    ("usage_overrides", "limit_overrides", "expected"),
    [
        ({"cameras": 4}, {"max_cameras": 5}, {"cameras": "near_limit"}),
        ({"cameras": 5}, {"max_cameras": 5}, {"cameras": "at_limit"}),
        ({"cameras": 7}, {"max_cameras": 5}, {"cameras": "over_limit"}),
        (
            {"active_streams": 5},
            {"max_concurrent_sessions": 5},
            {"active_streams": "at_limit"},
        ),
        (
            {"stream_seconds_completed": 480 * 60},
            {"monthly_minutes_quota": 600},
            {"stream_minutes": "near_limit"},
        ),
        (
            {"stream_seconds_completed": 700 * 60},
            {"monthly_minutes_quota": 600},
            {"stream_minutes": "over_limit"},
        ),
    ],
)
def test_summary_states(
    usage_overrides: dict[str, Any],
    limit_overrides: dict[str, Any],
    expected: dict[str, str],
) -> None:
    account = uuid4()
    conn = FakeConn(
        account_row=_account_row(**limit_overrides),
        usage_row=_usage_row(**usage_overrides),
    )
    r = _client(conn, _user(account)).get("/v1/usage/summary", params={"account_id": str(account)})
    for key, state in expected.items():
        assert r.json()["states"][key] == state, (key, r.json()["states"])


def test_summary_plan_label_title_cased() -> None:
    account = uuid4()
    conn = FakeConn(account_row=_account_row(plan="early_partner"), usage_row=_usage_row())
    r = _client(conn, _user(account)).get("/v1/usage/summary", params={"account_id": str(account)})
    assert r.json()["plan"] == {
        "key": "early_partner",
        "label": "Early Partner",
        "status": "active",
    }


def test_summary_viewer_member_can_read() -> None:
    account = uuid4()
    conn = FakeConn(account_row=_account_row(), usage_row=_usage_row())
    r = _client(conn, _user(account, role="viewer")).get(
        "/v1/usage/summary", params={"account_id": str(account)}
    )
    assert r.status_code == 200


def test_summary_cross_tenant_forbidden() -> None:
    member_of, requested = uuid4(), uuid4()
    conn = FakeConn(account_row=_account_row(), usage_row=_usage_row())
    r = _client(conn, _user(member_of)).get(
        "/v1/usage/summary", params={"account_id": str(requested)}
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden_account"


def test_summary_defaults_to_single_membership_account() -> None:
    account = uuid4()
    conn = FakeConn(account_row=_account_row(), usage_row=_usage_row())
    r = _client(conn, _user(account)).get("/v1/usage/summary")
    assert r.status_code == 200


def test_summary_unauthenticated_rejected() -> None:
    conn = FakeConn(account_row=_account_row(), usage_row=_usage_row())
    r = _client(conn, None).get("/v1/usage/summary")
    assert r.status_code == 401


def test_summary_missing_limits_row_404() -> None:
    account = uuid4()
    conn = FakeConn(account_row=None, usage_row=_usage_row())
    r = _client(conn, _user(account)).get("/v1/usage/summary", params={"account_id": str(account)})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "account_limits_missing"


# ---------------------------------------------------------------------------
# Pin the production SQL so the fakes cannot drift.
# ---------------------------------------------------------------------------


def test_counters_sql_pins_owning_predicates() -> None:
    sql = _USAGE_COUNTERS_SQL
    assert "status <> 'disabled'" in sql  # camera enforcement predicate
    assert "state in ('approved', 'streaming')" in sql  # session predicate
    assert "event_type = 'session_ended'" in sql  # the ONLY minutes source
    assert "stream_seconds" not in sql.replace("stream_seconds_completed", "")
    assert "from workflow_runs" in sql
    assert "from alerts" in sql
    assert "from rules" in sql
    assert "from workflows" in sql


def test_account_sql_joins_limits() -> None:
    assert "join account_limits l" in _ACCOUNT_AND_LIMITS_SQL


# ---------------------------------------------------------------------------
# Monthly minutes quota enforcement (check_session_start path).
# ---------------------------------------------------------------------------


class QuotaFakeConn:
    """Answers limits, active-count, and minutes-sum queries for
    check_session_start."""

    def __init__(self, *, quota: int, used_seconds: int, active: int = 0) -> None:
        self.quota = quota
        self.used_seconds = used_seconds
        self.active = active

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "from account_limits" in query
        return {
            "max_cameras": 10,
            "max_concurrent_sessions": 5,
            "max_fps": 60,
            "max_width": 1920,
            "max_height": 1080,
            "max_bitrate_kbps": 8000,
            "allowed_codecs": ["h264"],
            "allowed_stream_types": ["webcam", "rtsp"],
            "monthly_minutes_quota": self.quota,
            "snapshot_storage_mb": 1024,
            "session_ttl_seconds": None,
            "shared_inference_enabled": False,
        }

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "status from accounts" in query:
            return "active"
        if "from usage_events" in query:
            return self.used_seconds
        if "count(*)" in query:
            return self.active
        return None

    async def execute(self, query: str, *args: Any) -> str:
        return "OK"


_REQUESTED = RequestedStream(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=2500)


async def test_session_start_allowed_below_quota() -> None:
    conn = QuotaFakeConn(quota=600, used_seconds=300 * 60)
    snapshot = await check_session_start(
        conn,
        account_id=uuid4(),
        requested=_REQUESTED,
        camera_source_type="webcam",
    )
    assert snapshot.width == 1280


async def test_session_start_rejected_at_quota() -> None:
    conn = QuotaFakeConn(quota=600, used_seconds=600 * 60)
    with pytest.raises(LimitExceeded) as exc:
        await check_session_start(
            conn,
            account_id=uuid4(),
            requested=_REQUESTED,
            camera_source_type="webcam",
        )
    assert exc.value.code == "limit_exceeded.monthly_minutes_quota"
    details = exc.value.details
    assert details["limit"] == 600
    assert details["used"] == 600
    start, end = current_usage_period()
    assert details["period_start"] == start.isoformat()
    assert details["period_end"] == end.isoformat()


async def test_session_start_rejected_over_quota() -> None:
    conn = QuotaFakeConn(quota=600, used_seconds=900 * 60)
    with pytest.raises(LimitExceeded) as exc:
        await check_session_start(
            conn,
            account_id=uuid4(),
            requested=_REQUESTED,
            camera_source_type="webcam",
        )
    assert exc.value.code == "limit_exceeded.monthly_minutes_quota"


async def test_session_start_zero_quota_always_rejected() -> None:
    conn = QuotaFakeConn(quota=0, used_seconds=0)
    with pytest.raises(LimitExceeded) as exc:
        await check_session_start(
            conn,
            account_id=uuid4(),
            requested=_REQUESTED,
            camera_source_type="webcam",
        )
    assert exc.value.code == "limit_exceeded.monthly_minutes_quota"


async def test_session_start_partial_minutes_round_down() -> None:
    """599m59s used of a 600m quota is still below the cap (//60)."""
    conn = QuotaFakeConn(quota=600, used_seconds=600 * 60 - 1)
    snapshot = await check_session_start(
        conn,
        account_id=uuid4(),
        requested=_REQUESTED,
        camera_source_type="webcam",
    )
    assert snapshot.fps == 30


async def test_concurrent_session_limit_still_enforced() -> None:
    """Quota passes but all slots are busy -> the original error, unchanged."""
    conn = QuotaFakeConn(quota=600, used_seconds=0, active=5)
    with pytest.raises(LimitExceeded) as exc:
        await check_session_start(
            conn,
            account_id=uuid4(),
            requested=_REQUESTED,
            camera_source_type="webcam",
        )
    assert exc.value.code == "limit_exceeded.max_concurrent_sessions"


async def test_stream_type_check_still_enforced_before_quota() -> None:
    conn = QuotaFakeConn(quota=600, used_seconds=600 * 60)
    with pytest.raises(LimitExceeded) as exc:
        await check_session_start(
            conn,
            account_id=uuid4(),
            requested=_REQUESTED,
            camera_source_type="hdmi",
        )
    assert exc.value.code == "limit_exceeded.stream_type_not_allowed"


async def test_missing_limits_row_unchanged() -> None:
    class NoLimitsConn(QuotaFakeConn):
        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            return None

    with pytest.raises(NotFoundError):
        await check_session_start(
            NoLimitsConn(quota=600, used_seconds=0),
            account_id=uuid4(),
            requested=_REQUESTED,
            camera_source_type="webcam",
        )
