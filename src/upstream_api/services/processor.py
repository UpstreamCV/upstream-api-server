"""M8.0 Processor Readiness Gate — DB helpers.

Three concerns, kept out of the router the same way ingest webhooks keep
their state machine in ``services/stream_validation.py``:

- ``fetch_eligible_sessions``   — the safety gate. The WHERE clause is the
  single source of truth for "processable"; nothing is filtered in Python.
- ``upsert_processor_status``   — one current-status row per
  ``(processor_id, stream_session_id)``.
- ``insert_observation``        — append-only mock event row.

Tenant columns are never taken from the request; callers pass the
``account_id`` / ``camera_id`` they derived from the session row.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.processor import (
    PerceptionSnapshotIngestRequest,
    ProcessorObservationRequest,
    ProcessorStatusRequest,
)

# Inner-joined lateral picks the single latest validation row. A session
# with no validation row produces no lateral row and the INNER join drops
# it — that is how "streaming with no validation" is excluded. `v.passed`
# is checked on that latest row only, so an older passing validation cannot
# rescue a session whose newest validation failed.
_ELIGIBLE_SESSIONS_SQL = """
select
    ss.id               as session_id,
    ss.account_id       as account_id,
    ss.camera_id        as camera_id,
    ss.streamid         as streamid,
    ss.started_at       as started_at,
    ss.lim_codec        as lim_codec,
    ss.lim_width        as lim_width,
    ss.lim_height       as lim_height,
    ss.lim_fps          as lim_fps,
    ss.lim_bitrate_kbps as lim_bitrate_kbps,
    c.metadata          as camera_metadata,
    c.external_id       as external_id,
    v.passed            as v_passed,
    v.observed          as v_observed,
    v.expected          as v_expected,
    v.mismatch_fields   as v_mismatch_fields,
    v.created_at        as v_created_at,
    gc.revision         as gc_revision,
    gc.config           as gc_config,
    pc.revision         as pc_revision,
    pc.config           as pc_config
  from stream_sessions ss
  join cameras c on c.id = ss.camera_id
  join accounts a on a.id = ss.account_id
  join lateral (
         select passed, observed, expected, mismatch_fields, created_at
           from stream_validations
          where session_id = ss.id
          order by created_at desc
          limit 1
       ) v on true
  left join camera_geometry_configs gc on gc.camera_id = ss.camera_id
  left join camera_processing_configs pc on pc.camera_id = ss.camera_id
 where ss.state = 'streaming'
   and ss.expires_at > now()
   and v.passed = true
   -- M3 account-level processing master switch. Absent key = treated as
   -- true so every existing account continues to be processed exactly as
   -- before. Operators flip ``processing_enabled=false`` via the dashboard
   -- toggle (PATCH /v1/processing-config) to pause GPU work without taking
   -- down streams.
   and coalesce((a.metadata->>'processing_enabled')::boolean, true) = true
 order by ss.started_at desc nulls last
 limit $1
"""

_UPSERT_STATUS_SQL = """
insert into processor_statuses (
    account_id, camera_id, stream_session_id, processor_id, runtime,
    package_id, status, fps_input, fps_processed, last_frame_at,
    last_observation_at, error_code, error_message, payload
)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
on conflict (processor_id, stream_session_id) do update set
    account_id          = excluded.account_id,
    camera_id           = excluded.camera_id,
    runtime             = excluded.runtime,
    package_id          = excluded.package_id,
    status              = excluded.status,
    fps_input           = excluded.fps_input,
    fps_processed       = excluded.fps_processed,
    last_frame_at       = excluded.last_frame_at,
    last_observation_at = excluded.last_observation_at,
    error_code          = excluded.error_code,
    error_message       = excluded.error_message,
    payload             = excluded.payload
returning id, account_id, camera_id, stream_session_id, processor_id,
          runtime, package_id, status, created_at, updated_at
"""

_INSERT_OBSERVATION_SQL = """
insert into observations (
    account_id, camera_id, stream_session_id, processor_id, package_id,
    type, payload, observed_at
)
values ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
returning id
"""

# P5-A read path. Double-scoped by account_id AND camera_id — defense in
# depth on top of the router's separate camera-ownership check. account_id
# is NOT selected onto the response (the caller is already account-scoped).
# An unknown `type` filter simply matches zero rows; it is never an error.
_RECENT_OBSERVATIONS_SQL = """
select
    id, camera_id, stream_session_id, processor_id, package_id,
    type, payload, observed_at, created_at
  from observations
 where account_id = $1
   and camera_id = $2
   and ($3::text is null or type = $3)
 order by observed_at desc
 limit $4
"""

# P5-C latest perception snapshot. UPSERT keyed on camera_id alone — one
# latest-state row per camera, overwritten by each newer snapshot. Distinct
# from the append-only observations path.
_UPSERT_PERCEPTION_SNAPSHOT_SQL = """
insert into latest_perception_snapshots (
    account_id, camera_id, stream_session_id, processor_id, package_id,
    snapshot, observed_at
)
values ($1, $2, $3, $4, $5, $6::jsonb, $7)
on conflict (camera_id) do update set
    account_id        = excluded.account_id,
    stream_session_id = excluded.stream_session_id,
    processor_id      = excluded.processor_id,
    package_id        = excluded.package_id,
    snapshot          = excluded.snapshot,
    observed_at       = excluded.observed_at
returning id, camera_id, stream_session_id, processor_id, package_id,
          snapshot, observed_at, updated_at
"""

# P6-D read path for the latest processor_statuses row for a camera.
# Double-scoped by account_id AND camera_id; the optional stream_session_id
# narrows to a specific run (useful when multiple processor restarts have
# stamped multiple rows for the same camera — common in dev/verification).
# Ordering by updated_at desc + limit 1 picks the freshest row. The
# (processor_id, stream_session_id) unique key on the table means at most
# one row per (processor, session), so the narrow case returns the live row
# for that session directly.
_LATEST_PROCESSOR_STATUS_SQL = """
select
    id, camera_id, stream_session_id, processor_id, runtime, package_id,
    status, fps_input, fps_processed, last_frame_at, last_observation_at,
    error_code, error_message, payload, created_at, updated_at
  from processor_statuses
 where account_id = $1
   and camera_id = $2
   and ($3::uuid is null or stream_session_id = $3)
 order by updated_at desc
 limit 1
"""

# P5-C read path. Double-scoped by account_id AND camera_id — defense in
# depth on top of the router's separate camera-ownership check. account_id
# is NOT selected onto the response.
_LATEST_PERCEPTION_SNAPSHOT_SQL = """
select
    id, camera_id, stream_session_id, processor_id, package_id,
    snapshot, observed_at, updated_at
  from latest_perception_snapshots
 where account_id = $1
   and camera_id = $2
"""


async def fetch_eligible_sessions(
    conn: asyncpg.Connection,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_ELIGIBLE_SESSIONS_SQL, limit)
    return [dict(r) for r in rows]


async def upsert_processor_status(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    req: ProcessorStatusRequest,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        _UPSERT_STATUS_SQL,
        account_id,
        camera_id,
        req.stream_session_id,
        req.processor_id,
        req.runtime,
        req.package_id,
        req.status,
        req.fps_input,
        req.fps_processed,
        req.last_frame_at,
        req.last_observation_at,
        req.error_code,
        req.error_message,
        json.dumps(req.payload),
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return dict(row)


async def insert_observation(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    req: ProcessorObservationRequest,
) -> UUID:
    new_id = await conn.fetchval(
        _INSERT_OBSERVATION_SQL,
        account_id,
        camera_id,
        req.stream_session_id,
        req.processor_id,
        req.package_id,
        req.type,
        json.dumps(req.payload),
        req.observed_at,
    )
    assert isinstance(new_id, UUID)
    return new_id


async def fetch_recent_observations(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    type_: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        _RECENT_OBSERVATIONS_SQL,
        account_id,
        camera_id,
        type_,
        limit,
    )
    return [dict(r) for r in rows]


async def upsert_perception_snapshot(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    req: PerceptionSnapshotIngestRequest,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        _UPSERT_PERCEPTION_SNAPSHOT_SQL,
        account_id,
        camera_id,
        req.stream_session_id,
        req.processor_id,
        req.package_id,
        json.dumps(req.snapshot),
        req.observed_at,
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return dict(row)


async def fetch_latest_processor_status(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    stream_session_id: UUID | None,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        _LATEST_PROCESSOR_STATUS_SQL,
        account_id,
        camera_id,
        stream_session_id,
    )
    return dict(row) if row is not None else None


async def fetch_latest_perception_snapshot(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        _LATEST_PERCEPTION_SNAPSHOT_SQL,
        account_id,
        camera_id,
    )
    return dict(row) if row is not None else None


def coerce_jsonb(value: object) -> dict[str, Any]:
    """asyncpg returns jsonb as ``str`` unless a codec is registered; the
    fake pool in tests hands back ``dict``. Accept both, like the
    stream-sessions router's ``_as_dict``."""
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    parsed = json.loads(value)  # type: ignore[arg-type]
    assert isinstance(parsed, dict)
    return parsed


def parse_iso_datetime(value: object) -> datetime | None:
    """Coerce a started_at / created_at column to ``datetime``. asyncpg
    returns timezone-aware datetimes for timestamptz; the test fake may
    hand back an ISO string. ``None`` passes through."""
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
