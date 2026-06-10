from __future__ import annotations

import json
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.platform_admin import (
    AdminHeartbeat,
    AdminProcessorStatus,
    AdminReliabilityFinding,
    AdminStreamDetailResponse,
    AdminStreamItem,
    AdminStreamListResponse,
    AdminStreamsSummaryResponse,
    AdminStreamValidation,
)
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep

router = APIRouter()

_SESSION_ITEM_SELECT = """
    select s.id, s.account_id, a.name as account_name, a.slug::text as account_slug,
           s.agent_id, s.camera_id, c.name as camera_name,
           s.state::text as state, s.created_at, s.started_at, s.ended_at,
           s.expires_at, s.end_reason, s.failure_code,
           hb.status as hb_status, hb.fps as hb_fps,
           hb.bitrate_kbps as hb_bitrate_kbps, hb.reported_at as hb_reported_at,
           v.passed as latest_validation_passed,
           ps.status as processor_status
"""

_SESSION_ITEM_JOINS = """
      from stream_sessions s
      join accounts a on a.id = s.account_id
      left join cameras c on c.id = s.camera_id
      left join lateral (
        select status, fps, bitrate_kbps, reported_at
          from heartbeats
         where session_id = s.id
         order by reported_at desc
         limit 1
      ) hb on true
      left join lateral (
        select passed
          from stream_validations
         where session_id = s.id
         order by created_at desc
         limit 1
      ) v on true
      left join lateral (
        select status
          from processor_statuses
         where stream_session_id = s.id
         order by updated_at desc
         limit 1
      ) ps on true
"""


def _item_from_row(row: dict[str, Any]) -> AdminStreamItem:
    heartbeat = None
    if row["hb_reported_at"] is not None:
        heartbeat = AdminHeartbeat(
            status=row["hb_status"],
            fps=row["hb_fps"],
            bitrate_kbps=row["hb_bitrate_kbps"],
            reported_at=row["hb_reported_at"],
        )
    return AdminStreamItem(
        id=row["id"],
        account_id=row["account_id"],
        account_name=row["account_name"],
        account_slug=row["account_slug"],
        agent_id=row["agent_id"],
        camera_id=row["camera_id"],
        camera_name=row["camera_name"],
        state=row["state"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        expires_at=row["expires_at"],
        end_reason=row["end_reason"],
        failure_code=row["failure_code"],
        latest_heartbeat=heartbeat,
        latest_validation_passed=row["latest_validation_passed"],
        processor_status=row["processor_status"],
    )


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return json.loads(value) if value else {}


@router.get("/streams", response_model=AdminStreamListResponse)
async def list_streams(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    state: Annotated[
        Literal["approved", "streaming", "ended", "expired", "failed"] | None, Query()
    ] = None,
    account_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AdminStreamListResponse:
    """Cross-account session list, newest first, with each session's latest
    heartbeat / validation / processor status pulled via lateral joins."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _SESSION_ITEM_SELECT
            + ", count(*) over()::int as total"
            + _SESSION_ITEM_JOINS
            + """
             where ($1::stream_session_state is null or s.state = $1)
               and ($2::uuid is null or s.account_id = $2)
             order by s.created_at desc
             limit $3 offset $4
            """,
            state,
            account_id,
            limit,
            offset,
        )
    return AdminStreamListResponse(
        items=[_item_from_row(dict(r)) for r in rows],
        total=rows[0]["total"] if rows else 0,
    )


@router.get("/streams/summary", response_model=AdminStreamsSummaryResponse)
async def streams_summary(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminStreamsSummaryResponse:
    async with pool.acquire() as conn:
        state_rows = await conn.fetch(
            """
            select state::text as state, count(*)::int as count
              from stream_sessions
             where state in ('approved', 'streaming')
                or created_at > now() - interval '24 hours'
             group by state
            """
        )
        row = await conn.fetchrow(
            """
            select
              (select count(*)::int from stream_sessions
                where started_at > now() - interval '24 hours') as started_24h,
              (select count(*)::int from stream_validations
                where passed = false
                  and created_at > now() - interval '24 hours') as validation_failures_24h,
              (select count(distinct account_id)::int from stream_sessions
                where state in ('approved', 'streaming')) as accounts_streaming,
              (select count(*)::int from reliability_findings
                where status = 'open') as open_reliability_findings
            """
        )
    assert row is not None
    return AdminStreamsSummaryResponse(
        by_state={r["state"]: r["count"] for r in state_rows},
        started_24h=row["started_24h"],
        validation_failures_24h=row["validation_failures_24h"],
        accounts_streaming=row["accounts_streaming"],
        open_reliability_findings=row["open_reliability_findings"],
    )


@router.get("/streams/{session_id}", response_model=AdminStreamDetailResponse)
async def get_stream(
    session_id: UUID,
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminStreamDetailResponse:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            _SESSION_ITEM_SELECT
            + """,
            s.req_width, s.req_height, s.req_fps, s.req_codec, s.req_bitrate_kbps,
            s.lim_width, s.lim_height, s.lim_fps, s.lim_codec, s.lim_bitrate_kbps,
            s.validated_width, s.validated_height, s.validated_fps,
            s.validated_codec, s.validated_bitrate_kbps
            """
            + _SESSION_ITEM_JOINS
            + " where s.id = $1",
            session_id,
        )
        if row is None:
            raise NotFoundError("Stream session not found.", code="session_not_found")

        heartbeat_rows = await conn.fetch(
            """
            select status, fps, bitrate_kbps, width, height, codec,
                   last_error, reported_at
              from heartbeats
             where session_id = $1
             order by reported_at desc
             limit 50
            """,
            session_id,
        )
        validation_rows = await conn.fetch(
            """
            select passed, observed, expected, mismatch_fields, created_at
              from stream_validations
             where session_id = $1
             order by created_at desc
             limit 20
            """,
            session_id,
        )
        finding_rows = await conn.fetch(
            """
            select finding_type, category, severity, status, title, message,
                   last_seen_at
              from reliability_findings
             where camera_id = $1
             order by last_seen_at desc
             limit 20
            """,
            row["camera_id"],
        )
        processor_rows = await conn.fetch(
            """
            select processor_id, status, fps_input, fps_processed,
                   last_frame_at, error_code, error_message, updated_at
              from processor_statuses
             where stream_session_id = $1
             order by updated_at desc
            """,
            session_id,
        )

    data = dict(row)
    return AdminStreamDetailResponse(
        session=_item_from_row(data),
        requested={
            "width": data["req_width"],
            "height": data["req_height"],
            "fps": data["req_fps"],
            "codec": data["req_codec"],
            "bitrate_kbps": data["req_bitrate_kbps"],
        },
        approved={
            "width": data["lim_width"],
            "height": data["lim_height"],
            "fps": data["lim_fps"],
            "codec": data["lim_codec"],
            "bitrate_kbps": data["lim_bitrate_kbps"],
        },
        validated={
            "width": data["validated_width"],
            "height": data["validated_height"],
            "fps": data["validated_fps"],
            "codec": data["validated_codec"],
            "bitrate_kbps": data["validated_bitrate_kbps"],
        },
        heartbeats=[
            AdminHeartbeat(
                status=r["status"],
                fps=r["fps"],
                bitrate_kbps=r["bitrate_kbps"],
                width=r["width"],
                height=r["height"],
                codec=r["codec"],
                last_error=r["last_error"],
                reported_at=r["reported_at"],
            )
            for r in heartbeat_rows
        ],
        validations=[
            AdminStreamValidation(
                passed=r["passed"],
                observed=_as_dict(r["observed"]),
                expected=_as_dict(r["expected"]),
                mismatch_fields=list(r["mismatch_fields"]),
                created_at=r["created_at"],
            )
            for r in validation_rows
        ],
        reliability_findings=[
            AdminReliabilityFinding(
                finding_type=r["finding_type"],
                category=r["category"],
                severity=r["severity"],
                status=r["status"],
                title=r["title"],
                message=r["message"],
                last_seen_at=r["last_seen_at"],
            )
            for r in finding_rows
        ],
        processor_statuses=[
            AdminProcessorStatus(
                processor_id=r["processor_id"],
                status=r["status"],
                fps_input=r["fps_input"],
                fps_processed=r["fps_processed"],
                last_frame_at=r["last_frame_at"],
                error_code=r["error_code"],
                error_message=r["error_message"],
                updated_at=r["updated_at"],
            )
            for r in processor_rows
        ],
    )
