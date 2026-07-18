"""DB-backed integration tests for the dashboard SQL.

These run the REAL query strings -- `_FLEET_TREND_SQL` and `_SUMMARY_SQL`,
imported from the router, not copies -- against a real Postgres. They close the
gap documented in test_dashboard_fleet_trend.py: the FakeConn tests there cover
only the Python row->response mapping, so a broken query ships green. Here a
mutation to the SQL fails a test. Specifically covered:

  * fleet trend: 24/168 bucket count + generate_series zero-fill; last-heartbeat
    -per-camera-per-hour wins; critical/review/healthy partition of
    reporting_cameras; the count(*) FILTER over an empty (null-extended) hour
    yielding 0; account scoping (cross-tenant isolation); camera_id-null
    (agent-level) heartbeats excluded.
  * summary: online_cameras counts CAMERAS via a fresh-agent join (not agents),
    a stale agent's cameras excluded, and a null-agent_id (dashboard-created)
    camera excluded from online but present in total.

REQUIRES A POSTGRES. Set UPSTREAM_TEST_DATABASE_URL (falls back to
UPSTREAM_DATABASE_URL). If neither points at a reachable server every test here
SKIPS, so `make ci` -- which starts no database -- stays green exactly as
before. To run them against a throwaway container:

    docker run --rm -d -p 5433:5432 -e POSTGRES_PASSWORD=postgres \\
        --name ft-pg postgres:16
    UPSTREAM_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/postgres \\
        uv run pytest tests/test_dashboard_sql_integration.py -q
    docker rm -f ft-pg

...or `make test-db` once the URL is exported.

The schema below is a faithful MINIMAL mirror of supabase/migrations/ for the
columns these queries touch. The full migrations can't apply to a bare Postgres
(they need the supabase auth schema, roles, and RLS scaffolding), and the
queries never traverse the accounts FK -- they filter on the scalar account_id
-- so foreign keys and unrelated columns are dropped. What must stay in sync
with the migrations, and is copied verbatim:

  heartbeats.status  check in (online, streaming, error, reconnecting)  [20260513120000]
  cameras.status     check in (idle, streaming, error, disabled)        [20260512130200]
  cameras.agent_id   NULLABLE                                           [20260611120000]

Everything is created in a dedicated `ft_test` schema and torn down per test, so
this never touches an existing database's data even if pointed at a shared one.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

from upstream_api.routers.dashboard import _FLEET_TREND_SQL, _SUMMARY_SQL

asyncpg = pytest.importorskip("asyncpg")

_DB_URL = os.environ.get("UPSTREAM_TEST_DATABASE_URL") or os.environ.get("UPSTREAM_DATABASE_URL")

# Minimal faithful schema. No FKs (see module docstring): the queries filter on
# scalar account_id and never read the accounts table.
_SCHEMA_SQL = """
drop schema if exists ft_test cascade;
create schema ft_test;
set search_path = ft_test, public;

create table agents (
  id            uuid primary key,
  account_id    uuid not null,
  last_seen_at  timestamptz,
  status        text not null default 'active'
);

create table cameras (
  id          uuid primary key,
  account_id  uuid not null,
  agent_id    uuid,
  status      text not null default 'idle'
              check (status in ('idle', 'streaming', 'error', 'disabled'))
);

create table heartbeats (
  id           uuid primary key default gen_random_uuid(),
  account_id   uuid not null,
  agent_id     uuid not null,
  camera_id    uuid,
  status       text not null
               check (status in ('online', 'streaming', 'error', 'reconnecting')),
  reported_at  timestamptz not null
);

create table stream_sessions (
  id          uuid primary key,
  account_id  uuid not null,
  state       text not null default 'approved'
);

create table stream_validations (
  id          uuid primary key default gen_random_uuid(),
  session_id  uuid not null,
  passed      bool not null,
  created_at  timestamptz not null default now()
);
"""


@pytest.fixture
async def conn() -> AsyncIterator[object]:
    """A connection with a fresh `ft_test` schema on the search_path.

    Skips the whole module when no database is reachable, so this file is inert
    under `make ci`.
    """
    if not _DB_URL:
        pytest.skip(
            "no UPSTREAM_TEST_DATABASE_URL / UPSTREAM_DATABASE_URL -- see module "
            "docstring to run the dashboard SQL integration tests"
        )
    try:
        connection = await asyncpg.connect(_DB_URL, timeout=5)
    except (OSError, asyncpg.PostgresError) as exc:  # unreachable / auth / etc.
        pytest.skip(f"database at UPSTREAM_TEST_DATABASE_URL not reachable: {exc}")

    try:
        await connection.execute(_SCHEMA_SQL)
        await connection.execute("set search_path = ft_test, public")
        yield connection
    finally:
        await connection.execute("drop schema if exists ft_test cascade")
        await connection.close()


# --- seed helpers (all timestamps derived in-DB via now() to avoid clock skew) --


async def _add_agent(conn: object, account: uuid.UUID, *, seen_mins_ago: int) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await conn.execute(  # type: ignore[attr-defined]
        "insert into agents (id, account_id, last_seen_at) "
        "values ($1, $2, now() - make_interval(mins => $3::int))",
        agent_id,
        account,
        seen_mins_ago,
    )
    return agent_id


async def _add_camera(
    conn: object,
    account: uuid.UUID,
    *,
    agent: uuid.UUID | None,
    status: str = "streaming",
) -> uuid.UUID:
    camera_id = uuid.uuid4()
    await conn.execute(  # type: ignore[attr-defined]
        "insert into cameras (id, account_id, agent_id, status) values ($1, $2, $3, $4)",
        camera_id,
        account,
        agent,
        status,
    )
    return camera_id


async def _add_heartbeat(
    conn: object,
    account: uuid.UUID,
    *,
    camera: uuid.UUID | None,
    status: str,
    hours_ago: int,
    mins_into_hour: int = 10,
    agent: uuid.UUID | None = None,
) -> None:
    # reported_at lands in the bucket `hours_ago` hours before the current hour,
    # `mins_into_hour` minutes in. The current partial hour is hours_ago=0.
    await conn.execute(  # type: ignore[attr-defined]
        "insert into heartbeats (account_id, agent_id, camera_id, status, reported_at) "
        "values ($1, $2, $3, $4, "
        "  date_trunc('hour', now()) "
        "  - make_interval(hours => $5::int) "
        "  + make_interval(mins => $6::int))",
        account,
        agent or uuid.uuid4(),
        camera,
        status,
        hours_ago,
        mins_into_hour,
    )


async def _trend(conn: object, account: uuid.UUID, hours: int) -> list[dict[str, Any]]:
    # dict values are mixed: bucket_start/bucket_end are datetimes, the counts
    # are ints -- hence dict[str, Any], not dict[str, int].
    rows = await conn.fetch(_FLEET_TREND_SQL, account, hours)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


async def _summary(conn: object, account: uuid.UUID) -> dict[str, int]:
    row = await conn.fetchrow(_SUMMARY_SQL, account)  # type: ignore[attr-defined]
    assert row is not None
    return dict(row)


# --- fleet trend --------------------------------------------------------------


async def test_bucket_count_and_zero_fill(conn: object) -> None:
    account = uuid.uuid4()
    # No heartbeats at all: still a full, contiguous, all-zero series.
    assert len(await _trend(conn, account, 24)) == 24
    rows = await _trend(conn, account, 168)
    assert len(rows) == 168
    assert all(
        r["critical"] == 0
        and r["review"] == 0
        and r["healthy"] == 0
        and r["reporting_cameras"] == 0
        for r in rows
    )


async def test_last_heartbeat_in_hour_wins(conn: object) -> None:
    account = uuid.uuid4()
    camera = await _add_camera(conn, account, agent=None)
    # Same camera, same hour: an earlier 'error' then a later 'streaming'.
    await _add_heartbeat(
        conn, account, camera=camera, status="error", hours_ago=0, mins_into_hour=10
    )
    await _add_heartbeat(
        conn, account, camera=camera, status="streaming", hours_ago=0, mins_into_hour=40
    )

    current = (await _trend(conn, account, 24))[-1]
    # The later heartbeat wins: healthy, counted exactly once.
    assert current == {
        "bucket_start": current["bucket_start"],
        "bucket_end": current["bucket_end"],
        "critical": 0,
        "review": 0,
        "healthy": 1,
        "reporting_cameras": 1,
    }


async def test_partition_holds_across_statuses(conn: object) -> None:
    account = uuid.uuid4()
    c_err = await _add_camera(conn, account, agent=None)
    c_recon = await _add_camera(conn, account, agent=None)
    c_ok = await _add_camera(conn, account, agent=None)
    await _add_heartbeat(conn, account, camera=c_err, status="error", hours_ago=0)
    await _add_heartbeat(conn, account, camera=c_recon, status="reconnecting", hours_ago=0)
    await _add_heartbeat(conn, account, camera=c_ok, status="online", hours_ago=0)

    current = (await _trend(conn, account, 24))[-1]
    assert current["critical"] == 1
    assert current["review"] == 1
    assert current["healthy"] == 1
    # The invariant the whole design rests on.
    assert (
        current["critical"] + current["review"] + current["healthy"]
        == current["reporting_cameras"]
        == 3
    )


async def test_empty_hour_between_active_hours_is_zero(conn: object) -> None:
    account = uuid.uuid4()
    camera = await _add_camera(conn, account, agent=None)
    # Active 3h ago, silent since. The count(*) FILTER must not count the left
    # join's null-extended row in the empty hours.
    await _add_heartbeat(conn, account, camera=camera, status="online", hours_ago=3)

    rows = await _trend(conn, account, 24)
    three_ago, two_ago = rows[-4], rows[-3]
    assert three_ago["healthy"] == 1 and three_ago["reporting_cameras"] == 1
    assert two_ago["critical"] == 0 and two_ago["review"] == 0
    assert two_ago["healthy"] == 0 and two_ago["reporting_cameras"] == 0


async def test_account_scoping_isolates_tenants(conn: object) -> None:
    account_a = uuid.uuid4()
    account_b = uuid.uuid4()
    cam_a = await _add_camera(conn, account_a, agent=None)
    cam_b = await _add_camera(conn, account_b, agent=None)
    await _add_heartbeat(conn, account_a, camera=cam_a, status="online", hours_ago=0)
    await _add_heartbeat(conn, account_b, camera=cam_b, status="online", hours_ago=0)

    # Account A must see only its own camera -- dropping `account_id = $1` from
    # the CTE would leak B's heartbeat and make this 2.
    assert (await _trend(conn, account_a, 24))[-1]["reporting_cameras"] == 1


async def test_agent_level_heartbeat_excluded(conn: object) -> None:
    account = uuid.uuid4()
    camera = await _add_camera(conn, account, agent=None)
    await _add_heartbeat(conn, account, camera=camera, status="online", hours_ago=0)
    # An agent-level heartbeat carries no camera_id and must not be counted.
    await _add_heartbeat(conn, account, camera=None, status="online", hours_ago=0)

    assert (await _trend(conn, account, 24))[-1]["reporting_cameras"] == 1


# --- summary online_cameras join ---------------------------------------------


async def test_online_cameras_counts_cameras_not_agents(conn: object) -> None:
    account = uuid.uuid4()
    # ONE fresh agent owning THREE cameras.
    agent = await _add_agent(conn, account, seen_mins_ago=1)
    for _ in range(3):
        await _add_camera(conn, account, agent=agent, status="streaming")

    s = await _summary(conn, account)
    # The fix: cameras, not agents. Pre-fix this returned 1 (the agent count);
    # a cartesian `join on account_id` would over-count.
    assert s["online_cameras"] == 3
    assert s["online_agents"] == 1
    assert s["total_agents"] == 1
    assert s["total_cameras"] == 3


async def test_stale_agent_cameras_are_not_online(conn: object) -> None:
    account = uuid.uuid4()
    stale = await _add_agent(conn, account, seen_mins_ago=10)  # older than 5 min
    await _add_camera(conn, account, agent=stale, status="streaming")

    s = await _summary(conn, account)
    assert s["total_cameras"] == 1
    assert s["online_cameras"] == 0
    assert s["online_agents"] == 0


async def test_null_agent_camera_excluded_from_online_but_in_total(conn: object) -> None:
    account = uuid.uuid4()
    fresh = await _add_agent(conn, account, seen_mins_ago=1)
    await _add_camera(conn, account, agent=fresh, status="streaming")
    # A dashboard-created camera no agent has claimed (agent_id NULL): counts
    # toward total, but the inner join drops it from online (it reads offline).
    await _add_camera(conn, account, agent=None, status="idle")

    s = await _summary(conn, account)
    assert s["total_cameras"] == 2
    assert s["online_cameras"] == 1
