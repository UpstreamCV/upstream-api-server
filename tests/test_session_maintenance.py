"""Tests for the session-maintenance tick (expiry sweeper + FU-7 reconciler).

Pattern mirrors tests/test_stream_sessions_router.py: fake asyncpg pool +
respx for the MediaMTX HTTP API. No real DB, no real network.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.services import usage as usage_service
from upstream_api.services.session_maintenance import run_session_maintenance

_MEDIAMTX_API = "http://mediamtx.test:9997"


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


def _session_row(
    *,
    streaming: bool,
    stream_key: str,
    last_beat_minutes_ago: int | None = None,
) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    started = now - timedelta(hours=1) if streaming else None
    return {
        "id": uuid4(),
        "account_id": uuid4(),
        "camera_id": uuid4(),
        "streamid": f"publish:{stream_key}",
        "started_at": started,
        "ended_at": now,
        "state": "streaming" if streaming else "approved",
        "last_beat": (
            now - timedelta(minutes=last_beat_minutes_ago)
            if last_beat_minutes_ago is not None
            else started
        ),
        "srt_publish_passphrase": "p" * 24,
    }


class FakeConn:
    """The slice of asyncpg.Connection run_session_maintenance touches."""

    def __init__(
        self,
        *,
        lock_acquired: bool = True,
        expired_rows: list[dict[str, Any]] | None = None,
        stale_rows: list[dict[str, Any]] | None = None,
        active_rows: list[dict[str, Any]] | None = None,
        terminal_rows: list[dict[str, Any]] | None = None,
        session_still_live: bool = True,
        stale_update_matches: bool = True,
        execute_raises_on: str | None = None,
    ) -> None:
        self.lock_acquired = lock_acquired
        self.expired_rows = expired_rows or []
        self.stale_rows = stale_rows or []
        self.active_rows = active_rows or []
        # rows for the terminal-path cleanup query (streamid = any(...))
        self.terminal_rows = terminal_rows or []
        # the reconciler's post-provision liveness re-check
        self.session_still_live = session_still_live
        # False simulates a recovery heartbeat landing between the
        # candidate SELECT and the guarded UPDATE (NOT EXISTS matches)
        self.stale_update_matches = stale_update_matches
        self.execute_raises_on = execute_raises_on
        self.executes: list[tuple[str, tuple[Any, ...]]] = []
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrows: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "pg_try_advisory_xact_lock" in query:
            return self.lock_acquired
        if "to_regclass" in query:
            return False  # skip real usage_events inserts
        if "select exists" in query and "from stream_sessions" in query:
            return self.session_still_live
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetches.append((query, args))
        if "update stream_sessions" in query and "'expired'" in query:
            return self.expired_rows
        if "select id, state from stream_sessions" in query:
            # expiry clause, report mode
            return self.expired_rows
        if "left join lateral" in query:
            return self.stale_rows
        if "select streamid, srt_publish_passphrase" in query:
            return self.active_rows
        if "any($1::text[])" in query:
            return self.terminal_rows
        return []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrows.append((query, args))
        if "set state = 'ended'" in query:
            if not self.stale_update_matches:
                return None
            # staleness enforce path: echo the matching stale row
            for row in self.stale_rows:
                if row["id"] == args[0]:
                    return {
                        "id": row["id"],
                        "started_at": row["started_at"],
                        "ended_at": args[1],
                    }
            return None
        return None

    async def execute(self, query: str, *args: Any) -> str:
        if self.execute_raises_on and self.execute_raises_on in query:
            raise RuntimeError("injected failure")
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


def _settings(**overrides: Any) -> Any:
    base = get_settings()
    return base.model_copy(update={"mediamtx_api_base_url": _MEDIAMTX_API, **overrides})


def _mock_paths_list(names: list[str], *, page_count: int = 1) -> None:
    respx.get(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/list.*").mock(
        return_value=httpx.Response(
            200,
            json={
                "itemCount": len(names),
                "pageCount": page_count,
                "items": [{"name": n} for n in names],
            },
        )
    )


@pytest.fixture(autouse=True)
def _reset_usage_cache() -> None:
    usage_service.reset_usage_table_cache()


# ---------------------------------------------------------------------------
# Expiry clause
# ---------------------------------------------------------------------------


@respx.mock
async def test_expiry_enforce_sweeps_and_deprovisions() -> None:
    approved = _session_row(streaming=False, stream_key="sess_" + "a" * 16)
    streaming = _session_row(streaming=True, stream_key="sess_" + "b" * 16)
    conn = FakeConn(expired_rows=[approved, streaming])
    _mock_paths_list([])
    delete_route = respx.delete(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/sess_[ab]+$",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.expired_approved == 1
    assert stats.expired_streaming == 1
    assert not stats.skipped
    # Camera mirrored back to idle for each swept session.
    idle_updates = [e for e in conn.executes if "set status = 'idle'" in e[0]]
    assert len(idle_updates) == 2
    # Both swept paths deprovisioned (this is the LG-5 publisher eviction).
    assert delete_route.call_count == 2


@respx.mock
async def test_expiry_report_mode_counts_without_writing() -> None:
    row = _session_row(streaming=True, stream_key="sess_" + "c" * 16)
    conn = FakeConn(expired_rows=[row])
    _mock_paths_list([])

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(
            FakePool(conn), http, _settings(sweeper_expiry_mode="report")
        )

    assert stats.would_expire == 1
    assert stats.expired_approved == 0
    assert stats.expired_streaming == 0
    # Report mode must not write anything.
    assert not any("update" in q.lower() for q, _ in conn.executes)
    # The report-mode SELECT ran (not the UPDATE .. RETURNING).
    assert any("select id, state from stream_sessions" in q for q, _ in conn.fetches)


async def test_lock_contention_skips_tick() -> None:
    conn = FakeConn(
        lock_acquired=False,
        expired_rows=[_session_row(streaming=False, stream_key="sess_" + "d" * 16)],
    )

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.skipped
    # Nothing swept, nothing fetched beyond the lock probe, no HTTP traffic
    # (no respx mock active — an HTTP call would have raised).
    assert conn.fetches == []
    assert conn.executes == []


# ---------------------------------------------------------------------------
# Staleness clause
# ---------------------------------------------------------------------------


@respx.mock
async def test_stale_report_mode_flags_without_writing() -> None:
    stale = _session_row(streaming=True, stream_key="sess_" + "e" * 16, last_beat_minutes_ago=10)
    conn = FakeConn(stale_rows=[stale])
    _mock_paths_list([])

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.stale_reported == 1
    assert stats.stale_swept == 0
    assert not any("set state = 'ended'" in q for q, _ in conn.executes)


@respx.mock
async def test_stale_enforce_sweeps_with_last_beat_ended_at() -> None:
    stale = _session_row(streaming=True, stream_key="sess_" + "f" * 16, last_beat_minutes_ago=10)
    conn = FakeConn(stale_rows=[stale])
    _mock_paths_list([])
    delete_route = respx.delete(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/sess_f+$",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(
            FakePool(conn), http, _settings(sweeper_stale_mode="enforce")
        )

    assert stats.stale_swept == 1
    assert delete_route.call_count == 1
    # Camera mirrored to idle.
    assert any("set status = 'idle'" in q for q, _ in conn.executes)


@respx.mock
async def test_stale_mode_off_skips_clause() -> None:
    stale = _session_row(streaming=True, stream_key="sess_" + "0" * 16, last_beat_minutes_ago=10)
    conn = FakeConn(stale_rows=[stale])
    _mock_paths_list([])

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(
            FakePool(conn), http, _settings(sweeper_stale_mode="off")
        )

    assert stats.stale_reported == 0
    assert stats.stale_swept == 0
    assert not any("left join lateral" in q for q, _ in conn.fetches)


# ---------------------------------------------------------------------------
# Reconciler (FU-7)
# ---------------------------------------------------------------------------


@respx.mock
async def test_reconciler_reprovisions_missing_paths_only() -> None:
    key_missing = "sess_" + "1" * 16
    key_present = "sess_" + "2" * 16
    active = [
        _session_row(streaming=True, stream_key=key_missing),
        _session_row(streaming=True, stream_key=key_present),
    ]
    conn = FakeConn(active_rows=active)
    _mock_paths_list([key_present, "sess_" + "9" * 16, "not_ours"])
    add_route = respx.post(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/{key_missing}$",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.paths_reprovisioned == 1
    assert add_route.call_count == 1
    # The extra sess_* path is flagged; the non-namespace path is ignored.
    assert stats.paths_extra == 1
    # Extra paths are never deleted by the reconciler.
    assert stats.deprovision_errors == 0


@respx.mock
async def test_reconciler_reprovision_sends_stored_passphrase() -> None:
    key = "sess_" + "3" * 16
    row = _session_row(streaming=True, stream_key=key)
    conn = FakeConn(active_rows=[row])
    _mock_paths_list([])
    captured: dict[str, Any] = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        import json

        captured.update(json.loads(request.content))
        return httpx.Response(200, json={})

    respx.post(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/{key}$").mock(
        side_effect=_capture
    )

    async with httpx.AsyncClient() as http:
        await run_session_maintenance(FakePool(conn), http, _settings())

    assert captured["srtPublishPassphrase"] == row["srt_publish_passphrase"]
    assert captured["source"] == "publisher"


@respx.mock
async def test_reconciler_paginates_paths_list() -> None:
    key = "sess_" + "4" * 16
    conn = FakeConn(active_rows=[_session_row(streaming=True, stream_key=key)])
    page_two_key = "sess_" + "5" * 16

    def _page(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "0"))
        names = [key] if page == 0 else [page_two_key]
        return httpx.Response(
            200,
            json={"itemCount": 2, "pageCount": 2, "items": [{"name": n} for n in names]},
        )

    respx.get(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/list.*").mock(side_effect=_page)

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    # The active path was found on page 0 → nothing re-provisioned; the
    # page-1 extra was seen → pagination actually happened.
    assert stats.paths_reprovisioned == 0
    assert stats.paths_extra == 1


@respx.mock
async def test_reconciler_skips_gracefully_when_mediamtx_down() -> None:
    conn = FakeConn(active_rows=[_session_row(streaming=True, stream_key="sess_" + "6" * 16)])
    respx.get(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/list.*").mock(
        side_effect=httpx.ConnectError("boom")
    )

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.reconcile_skipped
    assert stats.paths_reprovisioned == 0


@respx.mock
async def test_reconciler_skips_null_passphrase_and_malformed_streamid() -> None:
    ok = _session_row(streaming=True, stream_key="sess_" + "7" * 16)
    no_pass = _session_row(streaming=True, stream_key="sess_" + "8" * 16)
    no_pass["srt_publish_passphrase"] = None
    malformed = _session_row(streaming=True, stream_key="sess_" + "a1" * 8)
    malformed["streamid"] = "garbage-no-prefix"
    conn = FakeConn(active_rows=[ok, no_pass, malformed])
    _mock_paths_list([])
    add_route = respx.post(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/sess_[0-9a-f]+$",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    # Only the well-formed row with a stored passphrase is re-provisioned.
    assert stats.paths_reprovisioned == 1
    assert add_route.call_count == 1


# ---------------------------------------------------------------------------
# Review-driven invariants
# ---------------------------------------------------------------------------


@respx.mock
async def test_expiry_bills_to_expires_at_not_now() -> None:
    """Never bill dead air: the expiry UPDATE must set ended_at = expires_at.

    If the sweeper was down for hours past a session's TTL, billing to now()
    would record the whole gap as streamed minutes — enough to exhaust a
    monthly quota and block the account's next /start.
    """
    row = _session_row(streaming=True, stream_key="sess_" + "1a" * 8)
    conn = FakeConn(expired_rows=[row])
    _mock_paths_list([])
    respx.delete(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/.*").mock(
        return_value=httpx.Response(200, json={})
    )

    async with httpx.AsyncClient() as http:
        await run_session_maintenance(FakePool(conn), http, _settings())

    update_sql = next(
        q for q, _ in conn.fetches if "update stream_sessions" in q and "'expired'" in q
    )
    assert "ended_at = expires_at" in update_sql
    assert "ended_at = now()" not in update_sql


@respx.mock
async def test_stale_enforce_sets_ended_at_to_last_beat() -> None:
    """Never bill dead air: swept-stale ended_at must be the last heartbeat."""
    stale = _session_row(streaming=True, stream_key="sess_" + "2b" * 8, last_beat_minutes_ago=10)
    conn = FakeConn(stale_rows=[stale])
    _mock_paths_list([])
    respx.delete(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/.*").mock(
        return_value=httpx.Response(200, json={})
    )

    async with httpx.AsyncClient() as http:
        await run_session_maintenance(FakePool(conn), http, _settings(sweeper_stale_mode="enforce"))

    sweep_call = next((q, a) for q, a in conn.fetchrows if "set state = 'ended'" in q)
    # $2 is ended_at and must be the candidate row's last_beat, not now().
    assert sweep_call[1][1] == stale["last_beat"]
    # The guarded UPDATE re-checks staleness (recovery-heartbeat guard).
    assert "not exists" in sweep_call[0]


@respx.mock
async def test_stale_enforce_skips_recovered_session() -> None:
    """A heartbeat landing between the candidate SELECT and the guarded
    UPDATE (simulated by the UPDATE matching nothing) must not sweep."""
    stale = _session_row(streaming=True, stream_key="sess_" + "3c" * 8, last_beat_minutes_ago=10)
    conn = FakeConn(stale_rows=[stale], stale_update_matches=False)
    _mock_paths_list([])
    delete_route = respx.delete(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/.*",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(
            FakePool(conn), http, _settings(sweeper_stale_mode="enforce")
        )

    assert stats.stale_swept == 0
    # No eviction for the recovered session.
    assert delete_route.call_count == 0


@respx.mock
async def test_reconciler_compensates_when_session_died_mid_reconcile() -> None:
    """TOCTOU guard: a session that ended between the DB snapshot and the
    provision call must get its freshly-resurrected path deleted again."""
    key = "sess_" + "4d" * 8
    conn = FakeConn(
        active_rows=[_session_row(streaming=True, stream_key=key)],
        session_still_live=False,
    )
    _mock_paths_list([])
    respx.post(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/{key}$").mock(
        return_value=httpx.Response(200, json={})
    )
    delete_route = respx.delete(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/{key}$",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.paths_reprovisioned == 0
    assert delete_route.call_count == 1


@respx.mock
async def test_reconciler_cleans_terminal_extra_paths_only() -> None:
    """Extra sess_* paths of TERMINAL sessions are deleted (self-healing
    for failed sweeper deprovisions); unknown orphans are logged only."""
    terminal_key = "sess_" + "5e" * 8
    orphan_key = "sess_" + "6f" * 8
    conn = FakeConn(terminal_rows=[{"streamid": f"publish:{terminal_key}"}])
    _mock_paths_list([terminal_key, orphan_key])
    delete_route = respx.delete(
        url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/{terminal_key}$",
    ).mock(return_value=httpx.Response(200, json={}))

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.paths_cleaned == 1
    assert delete_route.call_count == 1
    # Both were "extra"; only the terminal one was deleted.
    assert stats.paths_extra == 2


@respx.mock
async def test_reconciler_counts_provision_errors() -> None:
    key = "sess_" + "7a" * 8
    conn = FakeConn(active_rows=[_session_row(streaming=True, stream_key=key)])
    _mock_paths_list([])
    respx.post(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/add/{key}$").mock(
        return_value=httpx.Response(500, json={})
    )

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.provision_errors == 1
    assert stats.paths_reprovisioned == 0


@respx.mock
async def test_sweeper_counts_deprovision_errors() -> None:
    row = _session_row(streaming=True, stream_key="sess_" + "8b" * 8)
    conn = FakeConn(expired_rows=[row])
    _mock_paths_list([])
    respx.delete(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/.*").mock(
        return_value=httpx.Response(500, json={})
    )

    async with httpx.AsyncClient() as http:
        stats = await run_session_maintenance(FakePool(conn), http, _settings())

    assert stats.deprovision_errors == 1


async def test_sweep_failure_makes_no_mediamtx_calls() -> None:
    """Rollback-must-not-count: if the sweep transaction blows up, nothing
    may have touched MediaMTX (no respx mocks are active — any HTTP call
    would raise a distinct error instead of the injected one)."""
    row = _session_row(streaming=True, stream_key="sess_" + "9c" * 8)
    conn = FakeConn(expired_rows=[row], execute_raises_on="set status = 'idle'")

    async with httpx.AsyncClient() as http:
        with pytest.raises(RuntimeError, match="injected failure"):
            await run_session_maintenance(FakePool(conn), http, _settings())


# ---------------------------------------------------------------------------
# Internal run endpoint
# ---------------------------------------------------------------------------


def _internal_client(conn: FakeConn) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    app.dependency_overrides[get_settings] = lambda: _settings()
    return TestClient(app)


@respx.mock
def test_internal_run_requires_processor_secret() -> None:
    conn = FakeConn()
    client = _internal_client(conn)
    r = client.post(
        "/v1/internal/session-maintenance/run",
        headers={"X-Upstream-Webhook-Secret": "wrong"},
    )
    assert r.status_code == 401


@respx.mock
def test_internal_run_returns_stats() -> None:
    conn = FakeConn(expired_rows=[_session_row(streaming=False, stream_key="sess_" + "b2" * 8)])
    _mock_paths_list([])
    respx.delete(url__regex=rf"^{_MEDIAMTX_API}/v3/config/paths/delete/.*").mock(
        return_value=httpx.Response(200, json={})
    )
    client = _internal_client(conn)
    r = client.post(
        "/v1/internal/session-maintenance/run",
        headers={"X-Upstream-Webhook-Secret": "test-processor"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["expired_approved"] == 1
    assert body["skipped"] is False
