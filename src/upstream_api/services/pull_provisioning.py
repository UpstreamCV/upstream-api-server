"""Pull-based MediaMTX path provisioning — DB helpers.

Companion to ``services/mediamtx_provisioning.py`` (the push path). Where that
module reaches OUT to the streaming box's MediaMTX API, this module records
provisioning intent and lets the box collect it on its own outbound poll.

Why this exists: the push path needs inbound reachability into the box's
network — a live DNS record, a router port-forward, and a stable WAN IP. When
the box's WAN IP lease rolled on 2026-08-05 all three broke at once and
every session start failed `mediamtx_provision_failed / reason=transport
status=None` until the DNS was corrected four days later. Nothing about that
failure was visible to the box, which was healthy the whole time.

State is carried by ``stream_sessions.provisioned_at`` rather than a new enum
value (see migration 20260810000000):

    state='approved' AND provisioned_at IS NULL   -> pending
    provisioned_at IS NOT NULL                    -> path exists on the box

A pending row that is never claimed simply ages out of the pending window and
the start handler fails it — the same observable outcome as a push-mode
transport failure, so callers and dashboards need no changes.
"""

from __future__ import annotations

from uuid import UUID

import asyncpg

# Only rows this recent are offered as pending work. A session whose start
# handler has already given up (see `provision_wait_seconds`) must not be
# provisioned later — that would create an orphan path on the box for a
# session no agent is going to publish to, which is exactly the leak that
# left ten stale `sess_*` paths behind during the 2026-08 incident.
#
# Keep comfortably above `provision_wait_seconds` so a slow-but-succeeding
# ack is never discarded, and well below the session TTL.
PENDING_WINDOW_SECONDS = 30

_PENDING_PROVISIONS_SQL = """
select
    ss.id                     as session_id,
    ss.account_id             as account_id,
    ss.camera_id              as camera_id,
    ss.streamid               as streamid,
    ss.srt_publish_passphrase as passphrase,
    ss.created_at             as created_at
  from stream_sessions ss
 where ss.provisioned_at is null
   and ss.state = 'approved'
   and ss.expires_at > now()
   and ss.created_at > now() - make_interval(secs => $1::double precision)
 order by ss.created_at
 limit $2
"""

# Ack is idempotent and single-writer-safe: the `provisioned_at is null`
# predicate means a duplicate ack (retry after a lost response) updates zero
# rows and reports not-applied rather than moving the timestamp forward.
_ACK_PROVISION_SQL = """
update stream_sessions
   set provisioned_at = now(),
       updated_at     = now()
 where id = $1
   and provisioned_at is null
   and state = 'approved'
returning id
"""

_PROVISION_STATE_SQL = """
select state::text as state, provisioned_at
  from stream_sessions
 where id = $1
"""


async def fetch_pending_provisions(
    conn: asyncpg.Connection,
    *,
    limit: int = 50,
    window_seconds: int = PENDING_WINDOW_SECONDS,
) -> list[asyncpg.Record]:
    """Approved sessions whose MediaMTX path has not been created yet.

    Ordered oldest-first so a backlog drains in the order agents asked, and
    windowed so abandoned rows are never resurrected.
    """
    rows: list[asyncpg.Record] = await conn.fetch(
        _PENDING_PROVISIONS_SQL, float(window_seconds), limit
    )
    return rows


async def ack_provision(conn: asyncpg.Connection, *, session_id: UUID) -> bool:
    """Record that the box created the path. True if this call applied it.

    False means the row was already acked, already failed, or has been
    deleted — all benign for a retrying caller.
    """
    row = await conn.fetchrow(_ACK_PROVISION_SQL, session_id)
    return row is not None


async def get_provision_state(
    conn: asyncpg.Connection, *, session_id: UUID
) -> tuple[str, bool] | None:
    """``(state, provisioned)`` for the start handler's wait loop.

    None when the row is gone. ``provisioned`` is True once the box has
    acked; ``state`` lets the waiter notice a row that went `failed`
    underneath it instead of blocking for the full budget.
    """
    row = await conn.fetchrow(_PROVISION_STATE_SQL, session_id)
    if row is None:
        return None
    return row["state"], row["provisioned_at"] is not None
