"""P5-J — processing runtime evidence assembly (read-only).

Composes existing read helpers (`build_package_config_response`,
`fetch_latest_processor_status`) with three small windowed/history queries into
one camera-level evidence summary. No writes, no evaluator, no recommendations.

Sourcing notes:
- Lineage is read STRAIGHT from the latest status payload via `coerce_jsonb` —
  `_derive_applied` (camera_package_config) does NOT expose `tracker_algorithm`,
  so reusing AppliedPackageConfig would silently drop it.
- Observation counts are WINDOWED from the `observations` table. The status
  payload's `*_observations_emitted` counters are per-session cumulative (not
  windowed) and are deliberately NOT used here.
- Inferred zone exits have no status counter — they are only visible as
  `processor.zone_exited` observations carrying `payload.inferred = true`
  (P5-Gap), so we count them from observation payloads.
- "Dwell closed by inferred exit" is not persisted anywhere → omitted, with a
  standing warning.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.processing_evidence import (
    EvidenceLifecycle,
    EvidenceLineage,
    EvidenceObservationCount,
    EvidencePackage,
    EvidencePackageHistoryItem,
    EvidenceStatus,
    EvidenceTracking,
    EvidenceWindow,
    ProcessingEvidenceResponse,
)
from upstream_api.services.camera_package_config import build_package_config_response
from upstream_api.services.processor import (
    coerce_jsonb,
    fetch_latest_processor_status,
)

# Stable warning codes — clients/tests switch on these, not the messages.
WARN_DWELL_CLOSURE_NOT_PERSISTED = "dwell_closure_reason_not_persisted"
WARN_TRACKING_SESSION_LEVEL = "tracking_metrics_are_session_level"
WARN_PACKAGE_HISTORY_UNAVAILABLE = "package_history_unavailable"

_PACKAGE_EVENT_TYPES = [
    "camera.processing_package_requested",
    "camera.processing_package_applied",
]
_HISTORY_LIMIT = 20

# Windowed observation count-by-type. RAW trailing window on `observed_at`,
# half-open [now - n hours, now). This is DELIBERATELY NOT hour-aligned (unlike
# reliability_trends, which reads pre-bucketed hourly rollups) — we want the
# literal last 1h/24h of raw events. Do not "fix" this to date_trunc('hour').
_OBSERVATION_COUNTS_SQL = """
select type, count(*) as count
  from observations
 where account_id = $1
   and camera_id = $2
   and observed_at >= $3::timestamptz - make_interval(hours => $4::int)
   and observed_at <  $3::timestamptz
 group by type
 order by type asc
"""

# Inferred / track-missing-timeout zone-exit counts over the same window. Only
# `processor.zone_exited` observations carry these (P5-Gap), and `inferred` is a
# JSON boolean so `payload->>'inferred'` yields the text 'true'.
_INFERRED_EXITS_SQL = """
select
    count(*) filter (where payload->>'inferred' = 'true')
        as inferred_zone_exits,
    count(*) filter (where payload->>'exit_reason' = 'track_missing_timeout')
        as track_missing_timeout_exits
  from observations
 where account_id = $1
   and camera_id = $2
   and type = 'processor.zone_exited'
   and observed_at >= $3::timestamptz - make_interval(hours => $4::int)
   and observed_at <  $3::timestamptz
"""

# Recent package request/apply audit events for this camera (both event types
# in one pass; rides the (account_id, resource_type, resource_id, created_at)
# index).
_PACKAGE_HISTORY_SQL = """
select id, actor_user_id, actor_type, event_type, summary, metadata, created_at
  from account_audit_logs
 where account_id = $1
   and event_type = any($2::text[])
   and resource_type = 'camera'
   and resource_id = $3
 order by created_at desc, id desc
 limit $4
"""

_AUDIT_TABLE_EXISTS_SQL = "select to_regclass('public.account_audit_logs') is not null"


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _tracking_from_payload(payload: dict[str, Any]) -> EvidenceTracking:
    count = payload.get("unique_tracker_ids_count")
    sample = payload.get("tracker_ids_sample")
    return EvidenceTracking(
        unique_tracker_ids_count=int(count) if isinstance(count, (int, float)) else None,
        tracker_ids_sample=[str(s) for s in sample] if isinstance(sample, list) else [],
        is_session_level=True,
    )


async def _fetch_observation_counts(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    now: datetime,
    window_hours: int,
) -> list[EvidenceObservationCount]:
    rows = await conn.fetch(
        _OBSERVATION_COUNTS_SQL, account_id, camera_id, now, window_hours
    )
    return [
        EvidenceObservationCount(type=r["type"], count=int(r["count"])) for r in rows
    ]


async def _fetch_lifecycle(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    now: datetime,
    window_hours: int,
) -> EvidenceLifecycle:
    row = await conn.fetchrow(
        _INFERRED_EXITS_SQL, account_id, camera_id, now, window_hours
    )
    if row is None:
        return EvidenceLifecycle()
    return EvidenceLifecycle(
        inferred_zone_exits=int(row["inferred_zone_exits"] or 0),
        track_missing_timeout_exits=int(row["track_missing_timeout_exits"] or 0),
    )


async def _fetch_package_history(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
) -> tuple[list[EvidencePackageHistoryItem], bool]:
    """Returns (history, table_available). Guarded by a to_regclass probe so a
    pre-migration deploy returns [] + a warning instead of 500-ing the read."""
    exists = await conn.fetchval(_AUDIT_TABLE_EXISTS_SQL)
    if not exists:
        return [], False
    rows = await conn.fetch(
        _PACKAGE_HISTORY_SQL,
        account_id,
        _PACKAGE_EVENT_TYPES,
        str(camera_id),
        _HISTORY_LIMIT,
    )
    items: list[EvidencePackageHistoryItem] = []
    for r in rows:
        meta = coerce_jsonb(r["metadata"])
        package_id = meta.get("package_id")
        items.append(
            EvidencePackageHistoryItem(
                id=r["id"],
                event_type=r["event_type"],
                package_id=package_id if isinstance(package_id, str) else None,
                actor_type=r["actor_type"],
                actor_user_id=r["actor_user_id"],
                summary=r["summary"],
                created_at=r["created_at"],
            )
        )
    return items, True


async def build_processing_evidence(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    window_hours: int,
    now: datetime,
) -> ProcessingEvidenceResponse:
    # Package + state (reuse the P5-G request/apply view).
    pkg_config = await build_package_config_response(
        conn, account_id=account_id, camera_id=camera_id
    )
    package = EvidencePackage(
        requested_package_id=(
            pkg_config.requested.package_id if pkg_config.requested else None
        ),
        applied_package_id=(
            pkg_config.applied.package_id if pkg_config.applied else None
        ),
        state=pkg_config.state,
        requested_revision=(
            pkg_config.requested.revision if pkg_config.requested else None
        ),
    )

    # Latest status → lineage (from payload directly) + status + tracking.
    latest = await fetch_latest_processor_status(
        conn, account_id=account_id, camera_id=camera_id, stream_session_id=None
    )
    payload = coerce_jsonb(latest.get("payload")) if latest else {}
    lineage = EvidenceLineage(
        model_id=_str_or_none(payload.get("model_id")),
        tracker_profile_id=_str_or_none(payload.get("tracker_profile_id")),
        tracker_algorithm=_str_or_none(payload.get("tracker_algorithm")),
        analytics_profile_id=_str_or_none(payload.get("analytics_profile_id")),
    )
    status = EvidenceStatus(
        status=_str_or_none(latest.get("status")) if latest else None,
        last_seen_at=(
            (latest.get("last_frame_at") or latest.get("last_observation_at"))
            if latest
            else None
        ),
        last_error_code=_str_or_none(latest.get("error_code")) if latest else None,
        updated_at=latest.get("updated_at") if latest else None,
    )
    tracking = _tracking_from_payload(payload)

    observation_counts = await _fetch_observation_counts(
        conn, account_id=account_id, camera_id=camera_id, now=now, window_hours=window_hours
    )
    lifecycle = await _fetch_lifecycle(
        conn, account_id=account_id, camera_id=camera_id, now=now, window_hours=window_hours
    )
    history, history_available = await _fetch_package_history(
        conn, account_id=account_id, camera_id=camera_id
    )

    warnings = [WARN_DWELL_CLOSURE_NOT_PERSISTED]
    if tracking.unique_tracker_ids_count is not None or tracking.tracker_ids_sample:
        warnings.append(WARN_TRACKING_SESSION_LEVEL)
    if not history_available:
        warnings.append(WARN_PACKAGE_HISTORY_UNAVAILABLE)

    return ProcessingEvidenceResponse(
        camera_id=camera_id,
        window=EvidenceWindow(
            range="1h" if window_hours == 1 else "24h",
            started_at=now - timedelta(hours=window_hours),
            ended_at=now,
        ),
        package=package,
        lineage=lineage,
        status=status,
        observation_counts=observation_counts,
        lifecycle=lifecycle,
        tracking=tracking,
        package_history=history,
        warnings=warnings,
    )
