"""Upstream Reliability M1a — observation batch ingest.

Stores one ``cv_observation_batches`` header + N ``cv_observations`` rows. Same
posture as services/heartbeats.py and services/reliability_repo.py: SQL lives
here, the router passes the ``account_id`` it already resolved from the auth
credential (never the request body).

Idempotency: the batch header carries ``dedupe_key = client_batch_id`` under a
full unique index ``(account_id, dedupe_key)``. A replayed upload is absorbed by
``INSERT ... ON CONFLICT DO NOTHING`` — the second call inserts nothing, returns
the original batch id, and reports the originally-accepted count (NOT
``len(observations)``), so a retry after an uncertain network failure is safe.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg

if TYPE_CHECKING:
    from upstream_api.schemas.reliability_observations import ReliabilityObservationItem


@dataclass(frozen=True, slots=True)
class RecordObservationBatchResult:
    batch_id: UUID
    accepted: int
    # False when the batch was a dedupe replay (no observations inserted) — the
    # caller skips detector evaluation in that case.
    inserted: bool


# ON CONFLICT DO NOTHING is legal here because (account_id, dedupe_key) is a
# FULL unique index (unlike the partial open-issue index on cv_issues).
_INSERT_BATCH_SQL = """
insert into cv_observation_batches (account_id, source, dedupe_key)
values ($1, $2, $3)
on conflict (account_id, dedupe_key) do nothing
returning id
"""

_SELECT_EXISTING_BATCH_SQL = """
select id, observation_count
  from cv_observation_batches
 where account_id = $1
   and dedupe_key = $2
"""

_INSERT_OBSERVATION_SQL = """
insert into cv_observations (
    account_id, camera_id, batch_id, external_camera_key, type, payload, observed_at
)
values ($1, $2, $3, $4, $5, $6::jsonb, $7)
"""

_FINALIZE_BATCH_SQL = """
update cv_observation_batches
   set observation_count = $2,
       status            = 'completed',
       processed_at      = now()
 where id = $1
"""


async def record_observation_batch(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    client_batch_id: str,
    source: str,
    observations: list[ReliabilityObservationItem],
) -> RecordObservationBatchResult:
    """Insert the batch header + its observations in the caller's transaction.

    Returns the batch id and the accepted observation count. On a dedupe replay
    (same ``client_batch_id`` for the account) inserts nothing and returns the
    ORIGINAL batch id + its stored ``observation_count`` with ``inserted=False``.

    Must run inside the caller's transaction so the header + detail rows commit
    atomically.
    """
    batch_id = await conn.fetchval(_INSERT_BATCH_SQL, account_id, source, client_batch_id)
    if batch_id is None:
        # Replay: the (account_id, client_batch_id) row already exists. Return
        # the original id + its accepted count; do not re-insert observations.
        existing = await conn.fetchrow(_SELECT_EXISTING_BATCH_SQL, account_id, client_batch_id)
        assert existing is not None  # the ON CONFLICT proves the row exists.
        return RecordObservationBatchResult(
            batch_id=existing["id"],
            accepted=existing["observation_count"],
            inserted=False,
        )

    assert isinstance(batch_id, UUID)
    args = [
        (
            account_id,
            obs.camera_id,
            batch_id,
            obs.external_camera_key,
            obs.type,
            json.dumps(obs.payload, default=str),
            obs.observed_at,
        )
        for obs in observations
    ]
    await conn.executemany(_INSERT_OBSERVATION_SQL, args)
    await conn.execute(_FINALIZE_BATCH_SQL, batch_id, len(observations))
    return RecordObservationBatchResult(
        batch_id=batch_id,
        accepted=len(observations),
        inserted=True,
    )
