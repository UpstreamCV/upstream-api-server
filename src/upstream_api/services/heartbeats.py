"""Heartbeat batch ingest.

Validates ownership of any referenced camera_id / session_id in one round-trip,
then inserts the batch with ``COPY``-style ``executemany``. Updates the
``last_seen_at`` columns on agents + cameras as a side effect so the dashboard
can render "online / offline" without a recent-heartbeat join.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg

from upstream_api.errors import ForbiddenError
from upstream_api.schemas.heartbeats import Heartbeat


@dataclass(frozen=True, slots=True)
class RecordBatchResult:
    accepted: int
    distinct_camera_ids: tuple[UUID, ...]


async def _verify_ownership(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    agent_id: UUID,
    camera_ids: set[UUID],
    session_ids: set[UUID],
) -> None:
    if camera_ids:
        rows = await conn.fetch(
            """
            select id from cameras
             where id = any($1::uuid[])
               and account_id = $2
               and agent_id = $3
            """,
            list(camera_ids),
            account_id,
            agent_id,
        )
        found = {r["id"] for r in rows}
        missing = camera_ids - found
        if missing:
            raise ForbiddenError(
                "Heartbeat references a camera not owned by this agent.",
                code="forbidden_resource",
                details={"missing_camera_ids": [str(c) for c in missing]},
            )

    if session_ids:
        rows = await conn.fetch(
            """
            select id from stream_sessions
             where id = any($1::uuid[])
               and account_id = $2
               and agent_id = $3
            """,
            list(session_ids),
            account_id,
            agent_id,
        )
        found = {r["id"] for r in rows}
        missing = session_ids - found
        if missing:
            raise ForbiddenError(
                "Heartbeat references a session not owned by this agent.",
                code="forbidden_resource",
                details={"missing_session_ids": [str(s) for s in missing]},
            )


async def record_batch(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    agent_id: UUID,
    items: list[Heartbeat],
) -> RecordBatchResult:
    camera_ids = {h.camera_id for h in items if h.camera_id is not None}
    session_ids = {h.session_id for h in items if h.session_id is not None}

    await _verify_ownership(
        conn,
        account_id=account_id,
        agent_id=agent_id,
        camera_ids=camera_ids,
        session_ids=session_ids,
    )

    args = [
        (
            account_id,
            agent_id,
            h.camera_id,
            h.session_id,
            h.status,
            h.fps,
            h.bitrate_kbps,
            h.width,
            h.height,
            h.codec,
            h.last_error,
            h.agent_version,
            h.reported_at,
        )
        for h in items
    ]

    await conn.executemany(
        """
        insert into heartbeats (
          account_id, agent_id, camera_id, session_id, status,
          fps, bitrate_kbps, width, height, codec,
          last_error, agent_version, reported_at
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """,
        args,
    )

    # Side-effect updates: agent + cameras' last_seen_at. Done in two cheap
    # statements rather than per-row to avoid hot-row contention.
    await conn.execute(
        "update agents set last_seen_at = now() where id = $1",
        agent_id,
    )
    if camera_ids:
        await conn.execute(
            "update cameras set updated_at = now() where id = any($1::uuid[])",
            list(camera_ids),
        )

    return RecordBatchResult(
        accepted=len(items),
        distinct_camera_ids=tuple(camera_ids),
    )


def latest_reported_at(items: list[Heartbeat]) -> datetime:
    return max(h.reported_at for h in items)
