"""Upstream Reliability M1c — collector status summary.

A single bounded, account-scoped read over the M1a ``cv_*`` tables for the
dashboard's ``/agents/collector`` page. It answers "is data flowing, from which
sources, is the collector healthy, what recent activity/issues" entirely from
DB rows (the browser can't reach the collector's localhost ``/status``).

Posture: SQL lives here (module-level ``_*_SQL`` constants, mirroring
``services/reliability_issues.py``); the router passes the ``account_id`` it
resolved from the credential. Queries run sequentially on one connection
(asyncpg connections are not safe for concurrent queries). Windows are bounded
(last 24h; lists capped at 50).

Heartbeat plumbing is excluded from the "real data" views: the collector's own
``collector_heartbeat`` observations (batched under a ``collector-heartbeat:``
client_batch_id) are filtered out of the observation/source/activity aggregates
and surfaced only via ``latest_collector_heartbeat``.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.services.processor import coerce_jsonb, parse_iso_datetime

_RECENT_WINDOW = "24 hours"
_RECENT_BATCH_LIMIT = 50
_SOURCE_LIMIT = 50

# The collector's own heartbeat batches carry this client_batch_id (=dedupe_key)
# prefix; excluded from activity/source views so those reflect real CV data.
_HEARTBEAT_BATCH_PREFIX = "collector-heartbeat:%"
_HEARTBEAT_TYPE = "collector_heartbeat"

_LAST_RECEIVED_SQL = """
select max(received_at) as last_received_at
  from cv_observation_batches
 where account_id = $1
"""

_RECENT_BATCH_COUNT_SQL = f"""
select count(*) as n
  from cv_observation_batches
 where account_id = $1
   and received_at > now() - interval '{_RECENT_WINDOW}'
   and dedupe_key not like '{_HEARTBEAT_BATCH_PREFIX}'
"""

_RECENT_BATCHES_SQL = f"""
select id, source, external_camera_key, observation_count, status, received_at
  from cv_observation_batches
 where account_id = $1
   and received_at > now() - interval '{_RECENT_WINDOW}'
   and dedupe_key not like '{_HEARTBEAT_BATCH_PREFIX}'
 order by received_at desc
 limit {_RECENT_BATCH_LIMIT}
"""

_RECENT_OBSERVATION_COUNT_SQL = f"""
select count(*) as n
  from cv_observations
 where account_id = $1
   and observed_at > now() - interval '{_RECENT_WINDOW}'
   and type <> '{_HEARTBEAT_TYPE}'
"""

_SOURCES_SQL = f"""
select b.source                                                    as source,
       o.external_camera_key                                       as external_camera_key,
       max(o.observed_at)                                          as last_seen_at,
       count(*)                                                    as recent_observations,
       count(*) filter (where o.type = 'event_observed')          as recent_events,
       count(*) filter (where o.type = 'detection_summary')        as recent_detection_summaries
  from cv_observations o
  join cv_observation_batches b on b.id = o.batch_id
 where o.account_id = $1
   and o.observed_at > now() - interval '{_RECENT_WINDOW}'
   and o.type <> '{_HEARTBEAT_TYPE}'
 group by b.source, o.external_camera_key
 order by max(o.observed_at) desc
 limit {_SOURCE_LIMIT}
"""

# M2a: "sources active" reads the durable cv_sources registry rather than
# re-counting raw observation groups. Heartbeat-only collector rows are excluded
# (source_kind <> 'collector') to stay consistent with the heartbeat-excluded
# ``sources`` array. The detailed, stable source list lives at /v1/reliability/sources.
_ACTIVE_SOURCE_COUNT_SQL = f"""
select count(*) as n
  from cv_sources
 where account_id = $1
   and source_kind <> 'collector'
   and last_seen_at > now() - interval '{_RECENT_WINDOW}'
"""

# M2b: per-account source health rollup for the dashboard health-summary card.
_HEALTH_COUNTS_SQL = """
select health_status, count(*) as n
  from cv_sources
 where account_id = $1
 group by health_status
"""

_HEALTH_STATES = (
    "healthy",
    "delayed",
    "silent",
    "collector_offline",
    "unknown",
    "disabled",
)

_OPEN_ISSUES_TOTAL_SQL = """
select count(*) as n
  from cv_issues
 where account_id = $1
   and status = 'open'
"""

_OPEN_ISSUES_BY_KEY_SQL = """
select external_camera_key, count(*) as n
  from cv_issues
 where account_id = $1
   and status = 'open'
   and external_camera_key is not null
 group by external_camera_key
"""

_LATEST_HEARTBEAT_SQL = f"""
select o.observed_at as observed_at, o.payload as payload, b.source as source
  from cv_observations o
  join cv_observation_batches b on b.id = o.batch_id
 where o.account_id = $1
   and o.type = '{_HEARTBEAT_TYPE}'
 order by o.observed_at desc
 limit 1
"""

# Safe heartbeat payload fields surfaced to the dashboard. Anything else in the
# payload is dropped (defense-in-depth — the agent only sends counters, but we
# never echo an arbitrary payload dict).
_HEARTBEAT_FIELDS = (
    "version",
    "running",
    "cloud_reachable",
    "spooled_batches",
    "spooled_observations",
    "uploaded_batches",
    "uploaded_observations",
    "dropped_batches",
    "dropped_observations",
    "last_error_code",
)


def _int(value: Any) -> int:
    return int(value) if isinstance(value, int) else 0


def _heartbeat(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = coerce_jsonb(row.get("payload"))
    out: dict[str, Any] = {
        "observed_at": parse_iso_datetime(row.get("observed_at")),
        "source": row.get("source"),
        "last_upload_at": parse_iso_datetime(payload.get("last_upload_at")),
    }
    for field in _HEARTBEAT_FIELDS:
        if field in payload:
            out[field] = payload[field]
    return out


async def get_collector_status(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
) -> dict[str, Any]:
    """Summarize collector ingest health for ``account_id``.

    Returns a dict ready to validate into ``CollectorStatusResponse``. Empty
    account → empty lists, null timestamps, zero counts (never 404).
    """
    last_received = await conn.fetchrow(_LAST_RECEIVED_SQL, account_id)
    batch_count = await conn.fetchrow(_RECENT_BATCH_COUNT_SQL, account_id)
    batches = await conn.fetch(_RECENT_BATCHES_SQL, account_id)
    obs_count = await conn.fetchrow(_RECENT_OBSERVATION_COUNT_SQL, account_id)
    source_rows = await conn.fetch(_SOURCES_SQL, account_id)
    active_count = await conn.fetchrow(_ACTIVE_SOURCE_COUNT_SQL, account_id)
    open_total = await conn.fetchrow(_OPEN_ISSUES_TOTAL_SQL, account_id)
    open_by_key_rows = await conn.fetch(_OPEN_ISSUES_BY_KEY_SQL, account_id)
    health_rows = await conn.fetch(_HEALTH_COUNTS_SQL, account_id)
    heartbeat_row = await conn.fetchrow(_LATEST_HEARTBEAT_SQL, account_id)

    health_counts = dict.fromkeys(_HEALTH_STATES, 0)
    for r in health_rows:
        if r["health_status"] in health_counts:
            health_counts[r["health_status"]] = _int(r["n"])

    open_by_key = {
        r["external_camera_key"]: _int(r["n"]) for r in open_by_key_rows
    }

    sources = [
        {
            "source": r["source"],
            "external_camera_key": r["external_camera_key"],
            "last_seen_at": parse_iso_datetime(r["last_seen_at"]),
            "recent_observations": _int(r["recent_observations"]),
            "recent_events": _int(r["recent_events"]),
            "recent_detection_summaries": _int(r["recent_detection_summaries"]),
            "open_issues": open_by_key.get(r["external_camera_key"], 0),
        }
        for r in source_rows
    ]

    recent_batches = [
        {
            "id": r["id"],
            "source": r["source"],
            "external_camera_key": r["external_camera_key"],
            "observation_count": _int(r["observation_count"]),
            "status": r["status"],
            "received_at": parse_iso_datetime(r["received_at"]),
        }
        for r in batches
    ]

    return {
        "last_received_at": parse_iso_datetime(
            last_received["last_received_at"] if last_received else None
        ),
        "recent_batch_count": _int(batch_count["n"]) if batch_count else 0,
        "recent_observation_count": _int(obs_count["n"]) if obs_count else 0,
        "active_source_count": _int(active_count["n"]) if active_count else 0,
        "open_issue_count": _int(open_total["n"]) if open_total else 0,
        "latest_collector_heartbeat": _heartbeat(heartbeat_row),
        "sources": sources,
        "recent_batches": recent_batches,
        "source_health_counts": health_counts,
    }
