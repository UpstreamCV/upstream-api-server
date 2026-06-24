"""Upstream Reliability M2a — source registry projection + read.

``cv_sources`` is a materialized projection of the observation stream: one
account-scoped row per ``(source, external_camera_key)`` that durably answers
"which sources exist, when did each last send data, what data types, which
collector/source produced them, which have open issues." The ingest path calls
:func:`upsert_sources_from_observation_batch` after a batch's observations are
accepted (only for a freshly-inserted batch, so idempotent replays never
double-count), and ``GET /v1/reliability/sources`` reads the registry via
:func:`list_sources`.

Posture matches the other reliability services: SQL lives here as module-level
constants; the router passes the ``account_id`` it resolved from the credential.

Source-kind classification (a derived, mutable attribute — deliberately NOT part
of the unique key):

- keyed group (``external_camera_key`` present) → ``external_camera``
- keyless group with any ``collector_heartbeat`` → ``collector``
- keyless group otherwise → ``observation_source``

Because heartbeats arrive in their own all-heartbeat batch and normal
observations in user batches, a single batch's keyless group is never mixed. The
rare cross-batch collision (a collector hostname that also emits keyless CV
observations) merges into one row via the never-downgrade precedence in the
upsert, keeping one logical source as one row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg

if TYPE_CHECKING:
    from upstream_api.schemas.reliability_observations import ReliabilityObservationItem

_EVENT_TYPE = "event_observed"
_DETECTION_SUMMARY_TYPE = "detection_summary"
_HEARTBEAT_TYPE = "collector_heartbeat"
_CONFIG_CHANGED_TYPE = "config_changed"

_LIST_DEFAULT_LIMIT = 100
_LIST_MAX_LIMIT = 500


# The 19 INSERT columns; the ON CONFLICT target expression is byte-identical to
# cv_sources_account_source_external_key_uidx (migration 20260624120000). The
# UPDATE merges this batch's group into the existing row:
#   - last_*_at via greatest() (Postgres scalar greatest skips NULLs, so a batch
#     with none of a type preserves the stored timestamp; first sighting sets it)
#   - counters by addition (excluded.* are 0, never NULL, for absent types)
#   - model identity only from non-empty values (coalesce keeps the prior)
#   - source_kind never downgrades collector -> observation_source; a heartbeat
#     upgrades an existing observation_source row. (keyed/keyless never collide.)
#   - first_seen_at / display_name / created_at are insert-only (omitted here)
_UPSERT_SQL = """
insert into cv_sources (
    account_id, source, external_camera_key, display_name, source_kind,
    first_seen_at, last_seen_at, last_observation_at,
    last_event_at, last_detection_summary_at, last_collector_heartbeat_at,
    last_config_changed_at, last_model_name, last_model_version,
    observation_count_total, event_count_total, detection_summary_count_total,
    collector_heartbeat_count_total, config_changed_count_total
)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19)
on conflict (account_id, source, coalesce(external_camera_key, '')) do update set
    last_seen_at = greatest(cv_sources.last_seen_at, excluded.last_seen_at),
    last_observation_at = greatest(
        cv_sources.last_observation_at, excluded.last_observation_at),
    last_event_at = greatest(cv_sources.last_event_at, excluded.last_event_at),
    last_detection_summary_at = greatest(
        cv_sources.last_detection_summary_at, excluded.last_detection_summary_at),
    last_collector_heartbeat_at = greatest(
        cv_sources.last_collector_heartbeat_at, excluded.last_collector_heartbeat_at),
    last_config_changed_at = greatest(
        cv_sources.last_config_changed_at, excluded.last_config_changed_at),
    last_model_name = coalesce(excluded.last_model_name, cv_sources.last_model_name),
    last_model_version = coalesce(excluded.last_model_version, cv_sources.last_model_version),
    observation_count_total =
        cv_sources.observation_count_total + excluded.observation_count_total,
    event_count_total = cv_sources.event_count_total + excluded.event_count_total,
    detection_summary_count_total =
        cv_sources.detection_summary_count_total + excluded.detection_summary_count_total,
    collector_heartbeat_count_total =
        cv_sources.collector_heartbeat_count_total + excluded.collector_heartbeat_count_total,
    config_changed_count_total =
        cv_sources.config_changed_count_total + excluded.config_changed_count_total,
    source_kind = case
        when excluded.external_camera_key is not null then 'external_camera'
        when cv_sources.source_kind = 'collector' or excluded.source_kind = 'collector'
            then 'collector'
        else 'observation_source'
    end
"""

# Read: registry rows for the account + a lightweight derived status_hint and an
# open-issue count joined by external_camera_key. status_hint/source_kind filters
# and the limit are applied AFTER the derivation (outer query over the CTE) so a
# status_hint filter never under-fills the page. open_issue_count is 0 for
# keyless rows (the join key is null) — documented; camera-keyed issues are not
# joined (no camera mapping in M2a).
_LIST_SOURCES_SQL = """
with src as (
    select
        id, source, external_camera_key, display_name, source_kind,
        first_seen_at, last_seen_at, last_observation_at,
        last_event_at, last_detection_summary_at, last_collector_heartbeat_at,
        last_config_changed_at, last_model_name, last_model_version,
        observation_count_total, event_count_total, detection_summary_count_total,
        collector_heartbeat_count_total, config_changed_count_total,
        case
            when last_seen_at > now() - interval '5 minutes' then 'active'
            when last_seen_at > now() - interval '24 hours' then 'recent'
            else 'unknown'
        end as status_hint
    from cv_sources
    where account_id = $1
),
open_issues as (
    select external_camera_key, count(*) as n
        from cv_issues
        where account_id = $1
            and status = 'open'
            and external_camera_key is not null
        group by external_camera_key
)
select src.*, coalesce(oi.n, 0) as open_issue_count
    from src
    left join open_issues oi on oi.external_camera_key = src.external_camera_key
    where ($2::text is null or src.source_kind = $2)
        and ($3::text is null or src.status_hint = $3)
    order by src.last_seen_at desc
    limit $4
"""


@dataclass
class _SourceAgg:
    """Per-(external_camera_key) aggregation of one batch's observations."""

    external_camera_key: str | None
    count_total: int = 0
    event_count: int = 0
    detection_summary_count: int = 0
    heartbeat_count: int = 0
    config_changed_count: int = 0
    first_observed: datetime | None = None
    last_observed: datetime | None = None
    last_event_at: datetime | None = None
    last_detection_summary_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    last_config_changed_at: datetime | None = None
    # Latest non-empty model identity (tracked independently, latest wins).
    _model_name: str | None = None
    _model_name_at: datetime | None = None
    _model_version: str | None = None
    _model_version_at: datetime | None = None
    _seen_types: set[str] = field(default_factory=set)

    @property
    def source_kind(self) -> str:
        if self.external_camera_key is not None:
            return "external_camera"
        if _HEARTBEAT_TYPE in self._seen_types:
            return "collector"
        return "observation_source"


def _max(current: datetime | None, candidate: datetime) -> datetime:
    return candidate if current is None or candidate > current else current


def _min(current: datetime | None, candidate: datetime) -> datetime:
    return candidate if current is None or candidate < current else current


def _nonempty_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _aggregate(
    observations: list[ReliabilityObservationItem],
) -> list[_SourceAgg]:
    groups: dict[str | None, _SourceAgg] = {}
    for obs in observations:
        key = obs.external_camera_key
        agg = groups.get(key)
        if agg is None:
            agg = _SourceAgg(external_camera_key=key)
            groups[key] = agg
        when = obs.observed_at
        agg.count_total += 1
        agg._seen_types.add(obs.type)
        agg.first_observed = _min(agg.first_observed, when)
        agg.last_observed = _max(agg.last_observed, when)
        if obs.type == _EVENT_TYPE:
            agg.event_count += 1
            agg.last_event_at = _max(agg.last_event_at, when)
        elif obs.type == _DETECTION_SUMMARY_TYPE:
            agg.detection_summary_count += 1
            agg.last_detection_summary_at = _max(agg.last_detection_summary_at, when)
            name = _nonempty_str(obs.payload.get("model_name"))
            if name is not None and (agg._model_name_at is None or when >= agg._model_name_at):
                agg._model_name, agg._model_name_at = name, when
            version = _nonempty_str(obs.payload.get("model_version"))
            if version is not None and (
                agg._model_version_at is None or when >= agg._model_version_at
            ):
                agg._model_version, agg._model_version_at = version, when
        elif obs.type == _HEARTBEAT_TYPE:
            agg.heartbeat_count += 1
            agg.last_heartbeat_at = _max(agg.last_heartbeat_at, when)
        elif obs.type == _CONFIG_CHANGED_TYPE:
            agg.config_changed_count += 1
            agg.last_config_changed_at = _max(agg.last_config_changed_at, when)
        # Any other (unknown) type still advanced count_total + first/last
        # observed above, and does not fail ingest.
    return list(groups.values())


async def upsert_sources_from_observation_batch(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    source: str,
    observations: list[ReliabilityObservationItem],
) -> None:
    """Upsert one registry row per ``(source, external_camera_key)`` group.

    Aggregates the batch in Python (≤500 observations → ≤500 groups, bounded),
    then issues one ``INSERT ... ON CONFLICT DO UPDATE`` per group. Must run in
    the caller's transaction; the caller isolates it in a savepoint and gates it
    on a freshly-inserted batch so replays do not double-count.
    """
    aggs = _aggregate(observations)
    args = []
    for agg in aggs:
        assert agg.first_observed is not None and agg.last_observed is not None
        display_name = agg.external_camera_key if agg.external_camera_key else source
        args.append(
            (
                account_id,
                source,
                agg.external_camera_key,
                display_name,
                agg.source_kind,
                agg.first_observed,
                agg.last_observed,
                agg.last_observed,  # last_observation_at == most recent observation
                agg.last_event_at,
                agg.last_detection_summary_at,
                agg.last_heartbeat_at,
                agg.last_config_changed_at,
                agg._model_name,
                agg._model_version,
                agg.count_total,
                agg.event_count,
                agg.detection_summary_count,
                agg.heartbeat_count,
                agg.config_changed_count,
            )
        )
    if args:
        await conn.executemany(_UPSERT_SQL, args)


async def list_sources(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    source_kind: str | None = None,
    status_hint: str | None = None,
    limit: int = _LIST_DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """Account-scoped registry rows + derived status_hint + open_issue_count,
    most-recently-seen first. Bounded by ``limit``."""
    rows = await conn.fetch(
        _LIST_SOURCES_SQL, account_id, source_kind, status_hint, limit
    )
    return [dict(r) for r in rows]
