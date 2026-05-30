"""Detection-metrics retention + hourly rollup maintenance (reliability M6).

`snapshot_detection_metrics` (M5) is an append-only, ~1-row-per-second-per-
camera time series. It is ideal for SHORT-TERM drift detection (the M5 evaluator
reads the last ~30 minutes of raw rows) but must not grow forever. This module
rolls older raw rows up into compact HOURLY summaries
(`detection_confidence_hourly_rollups`) and then safely prunes the raw rows a
rollup already covers, plus ages out very old hourly rows.

Three rules, in this exact order, inside ONE transaction:

1. ROLL UP completed hours into ``detection_confidence_hourly_rollups`` (upsert,
   so a re-run recomputes/replaces a bucket — idempotent).
2. PRUNE RAW rows older than the (hour-aligned) raw-retention horizon, but ONLY
   where a covering hourly bucket already exists. Never delete un-rolled data.
3. PRUNE old hourly rollups past the long-term horizon.

Why this is safe (read before touching the SQL):

- The raw-prune cutoff is HOUR-ALIGNED (``date_trunc('hour', now - retention)``)
  so a bucket's raw rows are always deleted *atomically* — a whole hour at a
  time, never half. That is what lets the rollup safely re-scan a wider lookback
  window than the retention horizon: a bucket the rollup re-touches is either
  fully present (recomputed exactly) or fully pruned (produces no SELECT rows,
  so its existing summary is left untouched). A bucket is never recomputed from
  a half-pruned source.
- ``ROLLUP_LOOKBACK_HOURS`` > ``RAW_METRIC_RETENTION_HOURS`` on purpose: the
  extra margin is catch-up. If the job lapses and resumes, completed hours that
  are now prune-eligible still get a final rollup *this run* (rollup runs before
  prune) before their raw rows are deleted.
- The prune's coverage join (``DELETE ... USING`` the rollup table on
  account_id + camera_id + bucket_start) — not the age filter — is the
  load-bearing guarantee. A raw row with no covering bucket (e.g. the job never
  ran for that hour) is never deleted, no matter how old. ``DELETE`` removes
  each raw row at most once even if its hour holds several per-package buckets.

Compactness posture matches the raw table: ONLY numeric summaries move here,
never the raw snapshot / objects JSON, never secrets.

This module deliberately does NOT touch ``services/detection_metrics.py`` or the
M5 drift path: M5 drift keeps reading RAW metrics (its 30-min window sits well
inside the 72h raw-retention horizon). Hourly rollups are long-term history for
future Reliability-tab trend charts, not a drift input.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

# ---------------------------------------------------------------------------
# Retention policy. Named + centralised so the windows are easy to change and
# never duplicated as magic numbers across the SQL.
# ---------------------------------------------------------------------------

# Keep RAW high-resolution metrics this long. Comfortably exceeds the M5 drift
# window (~30 min) so pruning can never starve drift detection.
RAW_METRIC_RETENTION_HOURS = 72

# Keep compact HOURLY rollups this long (long-term trend history).
HOURLY_ROLLUP_RETENTION_DAYS = 90

# How far back the rollup re-scans raw rows each run. MUST stay greater than
# RAW_METRIC_RETENTION_HOURS — the surplus (here 24h) is the catch-up margin
# that lets a lapsed-then-resumed job roll up still-prunable hours before their
# raw rows are deleted. It is safe to exceed retention only because the raw
# prune is hour-aligned (see module docstring): older buckets are fully pruned,
# so re-scanning them yields no rows and leaves their summaries untouched.
ROLLUP_LOOKBACK_HOURS = 96

# Threshold matching detection_confidence.LOW_CONFIDENCE_THRESHOLD, kept here
# only for the pure-Python reference's documentation; the SQL reads the raw
# row's precomputed low_confidence_ratio, it does not re-threshold.

# ---------------------------------------------------------------------------
# SQL (string constants executed over asyncpg, mirroring the M5 writer/window
# queries in services/detection_metrics.py). $1 is a single captured `now`
# passed to ALL statements so the hour boundary cannot drift between them.
# ---------------------------------------------------------------------------

# Roll completed hours into hourly buckets, upserting so a re-run replaces the
# bucket (idempotent). Every raw frame in the hour is counted in metric_count
# (so every active hour yields a bucket and the prune can cover quiet/empty
# frames), while each weighted confidence aggregate is taken ONLY over rows that
# carry THAT field — its FILTER predicate is the field itself (avg_confidence /
# low_confidence_ratio / confidence_stddev), and a nullif() divisor yields NULL
# (not a divide-by-zero) when no row carries it. The writer invariant means all
# three are non-null together iff confidence_sample_count > 0, so for real data
# the three filters coincide; filtering per-field keeps the math correct (and
# matching the pure reference) even for a malformed non-writer row.
#
# Both the lookback lower bound and the current-hour upper bound are
# HOUR-ALIGNED via date_trunc, so the rollup only ever scans WHOLE hours — a
# bucket is never built from a partial slice of its hour (which, combined with
# the hour-aligned prune, is what keeps prune coverage honest).
#
# GROUP BY and the package_id projection both use coalesce(package_id, '') so a
# NULL and an empty-string package_id fold into ONE group (and ONE conflict key)
# rather than two rows that would collide on the unique index; the stored
# package_id is nullif(...,'') so no sentinel string is ever written.
#
# $1 = now (timestamptz); $2 = ROLLUP_LOOKBACK_HOURS (int).
_ROLLUP_HOURLY_SQL = """
insert into detection_confidence_hourly_rollups (
    account_id, camera_id, stream_session_id, package_id,
    bucket_start, bucket_end,
    metric_count, object_count_total, confidence_sample_count,
    avg_confidence_weighted, min_confidence, max_confidence,
    avg_low_confidence_ratio_weighted, avg_confidence_stddev_weighted,
    source
)
select
    m.account_id,
    m.camera_id,
    null::uuid                                              as stream_session_id,
    nullif(coalesce(m.package_id, ''), '')                  as package_id,
    date_trunc('hour', m.observed_at)                       as bucket_start,
    date_trunc('hour', m.observed_at) + interval '1 hour'   as bucket_end,
    count(*)                                                as metric_count,
    coalesce(sum(m.object_count), 0)                        as object_count_total,
    coalesce(sum(m.confidence_sample_count), 0)             as confidence_sample_count,
    sum(m.avg_confidence * m.confidence_sample_count)
        filter (where m.avg_confidence is not null)
      / nullif(sum(m.confidence_sample_count) filter (where m.avg_confidence is not null), 0)
                                                            as avg_confidence_weighted,
    min(m.min_confidence)                                   as min_confidence,
    max(m.max_confidence)                                   as max_confidence,
    sum(m.low_confidence_ratio * m.confidence_sample_count)
        filter (where m.low_confidence_ratio is not null)
      / nullif(sum(m.confidence_sample_count) filter (where m.low_confidence_ratio is not null), 0)
                                                            as avg_low_confidence_ratio_weighted,
    sum(m.confidence_stddev * m.confidence_sample_count)
        filter (where m.confidence_stddev is not null)
      / nullif(sum(m.confidence_sample_count) filter (where m.confidence_stddev is not null), 0)
                                                            as avg_confidence_stddev_weighted,
    'snapshot_detection_metrics_hourly'                     as source
  from snapshot_detection_metrics m
 where m.observed_at >= date_trunc('hour', $1::timestamptz - make_interval(hours => $2::int))
   and m.observed_at <  date_trunc('hour', $1::timestamptz)
 group by m.account_id, m.camera_id, coalesce(m.package_id, ''), date_trunc('hour', m.observed_at)
on conflict (account_id, camera_id, bucket_start, coalesce(package_id, '')) do update set
    bucket_end                        = excluded.bucket_end,
    metric_count                      = excluded.metric_count,
    object_count_total                = excluded.object_count_total,
    confidence_sample_count           = excluded.confidence_sample_count,
    avg_confidence_weighted           = excluded.avg_confidence_weighted,
    min_confidence                    = excluded.min_confidence,
    max_confidence                    = excluded.max_confidence,
    avg_low_confidence_ratio_weighted = excluded.avg_low_confidence_ratio_weighted,
    avg_confidence_stddev_weighted    = excluded.avg_confidence_stddev_weighted,
    source                            = excluded.source
"""

# Prune raw rows past the raw-retention horizon, but ONLY those whose hour is
# already covered by an hourly bucket. The coverage JOIN (DELETE ... USING the
# rollup table on account_id, camera_id, bucket_start) is the safety guarantee —
# never the age filter; DELETE removes each raw row at most once even if its hour
# has several per-package buckets. The cutoff is HOUR-ALIGNED so whole buckets
# are deleted atomically (see module docstring).
#
# $1 = now (timestamptz); $2 = RAW_METRIC_RETENTION_HOURS (int).
_PRUNE_RAW_SQL = """
delete from snapshot_detection_metrics m
 using detection_confidence_hourly_rollups r
 where r.account_id   = m.account_id
   and r.camera_id    = m.camera_id
   and r.bucket_start = date_trunc('hour', m.observed_at)
   and m.observed_at  < date_trunc('hour', $1::timestamptz - make_interval(hours => $2::int))
"""

# Age out very old hourly rollups.
#
# $1 = now (timestamptz); $2 = HOURLY_ROLLUP_RETENTION_DAYS (int).
_PRUNE_ROLLUPS_SQL = """
delete from detection_confidence_hourly_rollups
 where bucket_start < $1::timestamptz - make_interval(days => $2::int)
"""


# ---------------------------------------------------------------------------
# Result.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RollupRunResult:
    """Counts + timing for one maintenance run. ``rolled_up_buckets`` is the
    number of hourly buckets inserted or updated; the prune counts are raw /
    hourly rows deleted. Carries no row contents or secrets — safe to log/print."""

    rolled_up_buckets: int
    raw_rows_pruned: int
    hourly_rows_pruned: int
    started_at: datetime
    finished_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly form (timestamps as ISO-8601 strings)."""
        out = asdict(self)
        out["started_at"] = self.started_at.isoformat()
        out["finished_at"] = self.finished_at.isoformat()
        return out


def _affected_count(status: str) -> int:
    """Parse the affected-row count from an asyncpg command-status tag.

    asyncpg's ``Connection.execute`` returns the command tag of the last
    statement: ``"INSERT 0 12"`` (oid, count) for INSERT, ``"DELETE 5"`` for
    DELETE — the count is always the trailing integer.
    """
    try:
        return int(status.split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0


async def run_detection_metrics_rollup(
    conn: asyncpg.Connection,
    *,
    now: datetime | None = None,
    lookback_hours: int = ROLLUP_LOOKBACK_HOURS,
    raw_retention_hours: int = RAW_METRIC_RETENTION_HOURS,
    rollup_retention_days: int = HOURLY_ROLLUP_RETENTION_DAYS,
) -> RollupRunResult:
    """Run the hourly rollup + retention prune as one atomic maintenance pass.

    Rollup → prune-raw → prune-rollups, all in a single transaction so a crash
    can never delete raw rows whose bucket failed to materialise. A SINGLE
    captured ``now`` anchors every statement's hour boundary, so the cutoff
    cannot drift across an hour edge mid-run.

    Returns counts only — no row contents are read back, so nothing sensitive
    can be logged. Raises on a DB error (the caller/script surfaces it).
    """
    if now is None:
        now = datetime.now(tz=UTC)
    started_at = now

    async with conn.transaction():
        rollup_status = await conn.execute(_ROLLUP_HOURLY_SQL, now, lookback_hours)
        prune_raw_status = await conn.execute(_PRUNE_RAW_SQL, now, raw_retention_hours)
        prune_rollups_status = await conn.execute(_PRUNE_ROLLUPS_SQL, now, rollup_retention_days)

    return RollupRunResult(
        rolled_up_buckets=_affected_count(rollup_status),
        raw_rows_pruned=_affected_count(prune_raw_status),
        hourly_rows_pruned=_affected_count(prune_rollups_status),
        started_at=started_at,
        finished_at=datetime.now(tz=UTC),
    )


# ---------------------------------------------------------------------------
# Pure-Python reference implementation of the rollup math.
#
# This is a hand-maintained reference / executable specification of
# ``_ROLLUP_HOURLY_SQL`` — production rolls up set-based in SQL for scalability
# (M6 exists precisely because the raw table is huge), so this is not on the hot
# path. It exists so the weighted math, bucketing, hour-aligned windows and
# null/zero handling can be unit tested with real numeric assertions under the
# project's no-real-DB convention. NOTE: the tests pin the SQL by substring
# (clause/column PRESENCE) and exercise this reference numerically — they do NOT
# execute the SQL, so the two are kept in lockstep by review, not proven equal
# by an engine. Mirror any change to one in the other (and the substring pins).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HourlyRollupBucket:
    """One computed hourly bucket — the reference's output, mirroring the
    rollup table's aggregate columns."""

    account_id: UUID
    camera_id: UUID
    package_id: str | None
    bucket_start: datetime
    bucket_end: datetime
    metric_count: int
    object_count_total: int
    confidence_sample_count: int
    avg_confidence_weighted: float | None
    min_confidence: float | None
    max_confidence: float | None
    avg_low_confidence_ratio_weighted: float | None
    avg_confidence_stddev_weighted: float | None


def _hour_floor(value: datetime) -> datetime:
    """``date_trunc('hour', value)``."""
    return value.replace(minute=0, second=0, microsecond=0)


def _weighted(rows: list[Mapping[str, Any]], field: str) -> float | None:
    """sum(field * confidence_sample_count) / sum(confidence_sample_count),
    weighted over rows that actually carry ``field`` — the same per-field FILTER
    the SQL uses (filter (where <field> is not null)). A row missing ``field`` is
    excluded from BOTH numerator and denominator. ``None`` when no row carries it
    (the SQL's nullif divisor → NULL)."""
    num = 0.0
    den = 0
    for r in rows:
        value = r.get(field)
        weight = r.get("confidence_sample_count") or 0
        if value is None or weight == 0:
            continue
        num += float(value) * int(weight)
        den += int(weight)
    if den == 0:
        return None
    return num / den


def compute_hourly_rollup_buckets(
    rows: Iterable[Mapping[str, Any]],
    *,
    now: datetime,
    lookback_hours: int = ROLLUP_LOOKBACK_HOURS,
) -> list[HourlyRollupBucket]:
    """Reference implementation of ``_ROLLUP_HOURLY_SQL``.

    Groups raw ``snapshot_detection_metrics``-shaped rows into hourly buckets,
    excluding the current incomplete hour and anything older than
    ``lookback_hours``, and applies the exact weighted-aggregate formulae the
    SQL uses. Each ``row`` is a mapping with at least: account_id, camera_id,
    package_id, observed_at, object_count, confidence_sample_count,
    avg_confidence, min_confidence, max_confidence, low_confidence_ratio,
    confidence_stddev.
    """
    # Both window edges are hour-aligned, mirroring the SQL: the rollup only ever
    # scans WHOLE hours, so a boundary hour is never aggregated from a partial
    # slice of itself (which would let the prune later delete its never-rolled
    # rows). The upper edge excludes the current, still-filling hour.
    current_hour = _hour_floor(now)
    lookback_start = _hour_floor(now - timedelta(hours=lookback_hours))

    groups: dict[tuple[UUID, UUID, str | None, datetime], list[Mapping[str, Any]]] = defaultdict(
        list
    )
    for r in rows:
        observed_at = r["observed_at"]
        if observed_at >= current_hour or observed_at < lookback_start:
            continue  # exclude the current incomplete hour and rows past lookback
        # A NULL and an empty-string package_id fold to the same bucket (the SQL
        # groups on coalesce(package_id,'') and stores nullif(...,'')); both map
        # to None here so they never produce two colliding rows.
        package_id = r.get("package_id") or None
        key = (r["account_id"], r["camera_id"], package_id, _hour_floor(observed_at))
        groups[key].append(r)

    buckets: list[HourlyRollupBucket] = []
    for (account_id, camera_id, package_id, bucket_start), grouped in groups.items():
        mins = [float(g["min_confidence"]) for g in grouped if g.get("min_confidence") is not None]
        maxs = [float(g["max_confidence"]) for g in grouped if g.get("max_confidence") is not None]
        buckets.append(
            HourlyRollupBucket(
                account_id=account_id,
                camera_id=camera_id,
                package_id=package_id,
                bucket_start=bucket_start,
                bucket_end=bucket_start + timedelta(hours=1),
                metric_count=len(grouped),
                object_count_total=sum(int(g.get("object_count") or 0) for g in grouped),
                confidence_sample_count=sum(
                    int(g.get("confidence_sample_count") or 0) for g in grouped
                ),
                # Each weighted aggregate filters on ITS OWN field (per-field
                # FILTER in the SQL), so pass all grouped rows, not a pre-filtered
                # subset.
                avg_confidence_weighted=_weighted(grouped, "avg_confidence"),
                min_confidence=min(mins) if mins else None,
                max_confidence=max(maxs) if maxs else None,
                avg_low_confidence_ratio_weighted=_weighted(grouped, "low_confidence_ratio"),
                avg_confidence_stddev_weighted=_weighted(grouped, "confidence_stddev"),
            )
        )
    return buckets
