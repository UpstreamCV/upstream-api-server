from __future__ import annotations

import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ConflictError, NotFoundError
from upstream_api.schemas.cameras import (
    CameraListResponse,
    CameraRegisterRequest,
    CameraResponse,
)
from upstream_api.schemas.geometry import (
    GeometryConfigResponse,
    GeometryConfigUpdateRequest,
)
from upstream_api.schemas.processor import (
    LatestPerceptionSnapshotResponse,
    LatestProcessorStatusResponse,
    ObservationRecord,
    PerceptionSnapshotRecord,
    ProcessorStatusRecord,
    RecentObservationsResponse,
)
from upstream_api.security.deps import CurrentAgent, CurrentCaller, PoolDep
from upstream_api.services.geometry import (
    default_geometry_config_record,
    fetch_geometry_config,
    upsert_geometry_config,
)
from upstream_api.services.limits import (
    RequestedStream,
    check_camera_defaults,
    check_camera_slot_available,
    load_account_limits,
)
from upstream_api.services.processor import (
    coerce_jsonb,
    fetch_latest_perception_snapshot,
    fetch_latest_processor_status,
    fetch_recent_observations,
)

router = APIRouter(prefix="/v1/cameras", tags=["cameras"])


def _camera_to_response(row: dict) -> CameraResponse:  # type: ignore[type-arg]
    return CameraResponse(
        id=row["id"],
        account_id=row["account_id"],
        agent_id=row["agent_id"],
        name=row["name"],
        external_id=row["external_id"],
        device_path=row["device_path"],
        default_width=row["default_width"],
        default_height=row["default_height"],
        default_fps=row["default_fps"],
        default_codec=row["default_codec"],
        default_bitrate_kbps=row["default_bitrate_kbps"],
        default_latency_mode=row["default_latency_mode"],
        metadata=row["metadata"]
        if isinstance(row["metadata"], dict)
        else json.loads(row["metadata"]),
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


_CAMERA_COLUMNS = (
    "id, account_id, agent_id, name, external_id, device_path, "
    "default_width, default_height, default_fps, default_codec, "
    "default_bitrate_kbps, default_latency_mode, metadata, status, "
    "created_at, updated_at"
)


@router.post("/register", response_model=CameraResponse)
async def register_camera(
    body: CameraRegisterRequest,
    auth: CurrentAgent,
    pool: PoolDep,
) -> CameraResponse:
    async with pool.acquire() as conn, conn.transaction():
        # Confirm agent belongs to this account.
        agent_row = await conn.fetchrow(
            "select account_id from agents where id = $1",
            body.agent_id,
        )
        if agent_row is None:
            raise NotFoundError("Agent not found.", code="agent_not_found")
        if agent_row["account_id"] != auth.account_id:
            raise ConflictError(
                "Agent does not belong to this account.", code="agent_account_mismatch"
            )

        # Validate defaults against limits.
        limits = await load_account_limits(conn, auth.account_id)
        check_camera_defaults(
            limits,
            RequestedStream(
                width=body.default_width,
                height=body.default_height,
                fps=body.default_fps,
                codec=body.default_codec,
                bitrate_kbps=body.default_bitrate_kbps,
            ),
        )

        # Look up by (agent_id, external_id) for idempotent re-register.
        existing = None
        if body.external_id is not None:
            existing = await conn.fetchrow(
                "select id from cameras where agent_id = $1 and external_id = $2",
                body.agent_id,
                body.external_id,
            )

        # New-camera path enforces max_cameras.
        await check_camera_slot_available(
            conn,
            account_id=auth.account_id,
            is_new_camera=existing is None,
        )

        if existing is None:
            row = await conn.fetchrow(
                f"""
                insert into cameras (
                  account_id, agent_id, name, external_id, device_path,
                  default_width, default_height, default_fps, default_codec, default_bitrate_kbps,
                  default_latency_mode, metadata
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
                returning {_CAMERA_COLUMNS}
                """,
                auth.account_id,
                body.agent_id,
                body.name,
                body.external_id,
                body.device_path,
                body.default_width,
                body.default_height,
                body.default_fps,
                body.default_codec,
                body.default_bitrate_kbps,
                body.default_latency_mode,
                json.dumps(body.metadata),
            )
        else:
            row = await conn.fetchrow(
                f"""
                update cameras
                   set name = $3,
                       device_path = $4,
                       default_width = $5,
                       default_height = $6,
                       default_fps = $7,
                       default_codec = $8,
                       default_bitrate_kbps = $9,
                       default_latency_mode = $10,
                       metadata = $11::jsonb
                 where id = $1 and account_id = $2
                returning {_CAMERA_COLUMNS}
                """,
                existing["id"],
                auth.account_id,
                body.name,
                body.device_path,
                body.default_width,
                body.default_height,
                body.default_fps,
                body.default_codec,
                body.default_bitrate_kbps,
                body.default_latency_mode,
                json.dumps(body.metadata),
            )

    assert row is not None
    return _camera_to_response(row)


@router.get("", response_model=CameraListResponse)
async def list_cameras(
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    agent_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[str | None, Query(max_length=32)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> CameraListResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            select {_CAMERA_COLUMNS}
              from cameras
             where account_id = $1
               and ($2::uuid is null or agent_id = $2)
               and ($3::text is null or status = $3)
             order by created_at desc
             limit $4
            """,
            scope_account,
            agent_id,
            status,
            limit,
        )
    return CameraListResponse(cameras=[_camera_to_response(r) for r in rows])


@router.get("/{camera_id}", response_model=CameraResponse)
async def get_camera(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraResponse:
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"select {_CAMERA_COLUMNS} from cameras where id = $1",
            camera_id,
        )
    if row is None or row["account_id"] != scope_account:
        raise NotFoundError("Camera not found.", code="camera_not_found")
    return _camera_to_response(row)


@router.get(
    "/{camera_id}/observations/recent",
    response_model=RecentObservationsResponse,
)
async def list_recent_observations(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    type: Annotated[str | None, Query(max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> RecentObservationsResponse:
    """Most recent processor observations for one camera. Newest first.

    A debug/visibility read (P5-A) so operators can see processor output
    without direct DB access. Scoped to the caller's account both by an
    explicit camera-ownership check and by the account-scoped query.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        rows = await fetch_recent_observations(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            type_=type,
            limit=limit,
        )
    observations = [
        ObservationRecord(
            id=r["id"],
            camera_id=r["camera_id"],
            stream_session_id=r["stream_session_id"],
            processor_id=r["processor_id"],
            package_id=r["package_id"],
            type=r["type"],
            payload=coerce_jsonb(r["payload"]),
            observed_at=r["observed_at"],
            created_at=r["created_at"],
        )
        for r in rows
    ]
    return RecentObservationsResponse(observations=observations)


@router.get(
    "/{camera_id}/perception/latest",
    response_model=LatestPerceptionSnapshotResponse,
)
async def get_latest_perception_snapshot(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> LatestPerceptionSnapshotResponse:
    """Latest perception snapshot for one camera (P5-C).

    Current visual state — detected objects + configured geometry — for a
    dashboard overlay. Returns 404 ``perception_snapshot_not_found`` when the
    processor has not posted a snapshot yet (same missing-resource convention
    as latest-frame). Scoped to the caller's account both by an explicit
    camera-ownership check and by the account-scoped query.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        row = await fetch_latest_perception_snapshot(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
        )
    if row is None:
        raise NotFoundError(
            "No perception snapshot for this camera yet.",
            code="perception_snapshot_not_found",
        )
    return LatestPerceptionSnapshotResponse(
        snapshot=PerceptionSnapshotRecord(
            id=row["id"],
            camera_id=row["camera_id"],
            stream_session_id=row["stream_session_id"],
            processor_id=row["processor_id"],
            package_id=row["package_id"],
            snapshot=coerce_jsonb(row["snapshot"]),
            observed_at=row["observed_at"],
            updated_at=row["updated_at"],
        )
    )


@router.get(
    "/{camera_id}/processor-status/latest",
    response_model=LatestProcessorStatusResponse,
)
async def get_latest_processor_status(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    stream_session_id: Annotated[UUID | None, Query()] = None,
) -> LatestProcessorStatusResponse:
    """Latest processor status for one camera (P6-D).

    The full ``processor_statuses`` row including the ``payload`` JSONB —
    where the P6-C analytics worker stamps the ``applied_geometry`` lineage
    block (``source``, ``revision``, ``line_count``, ``roi_count``). This is
    the read counterpart that lets a dashboard chip / verification script
    confirm the processor actually picked up the operator's saved geometry.

    Returns 404 ``processor_status_not_found`` when no row matches — same
    missing-resource convention as ``perception/latest`` (no synthetic
    default to invent for a processor that has never reported in). Pass
    ``stream_session_id`` to narrow to a specific run when multiple
    processor restarts have stamped multiple rows for the same camera.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        row = await fetch_latest_processor_status(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            stream_session_id=stream_session_id,
        )
    if row is None:
        raise NotFoundError(
            "No processor status for this camera yet.",
            code="processor_status_not_found",
        )
    return LatestProcessorStatusResponse(
        status=ProcessorStatusRecord(
            id=row["id"],
            camera_id=row["camera_id"],
            stream_session_id=row["stream_session_id"],
            processor_id=row["processor_id"],
            runtime=row["runtime"],
            package_id=row["package_id"],
            status=row["status"],
            fps_input=row["fps_input"],
            fps_processed=row["fps_processed"],
            last_frame_at=row["last_frame_at"],
            last_observation_at=row["last_observation_at"],
            error_code=row["error_code"],
            error_message=row["error_message"],
            payload=coerce_jsonb(row["payload"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
    )


@router.get(
    "/{camera_id}/geometry-config",
    response_model=GeometryConfigResponse,
)
async def get_geometry_config(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> GeometryConfigResponse:
    """Dashboard-authored ROI/line geometry for one camera (P6-A).

    Returns the saved config, or the probe-default config with
    ``is_default = true`` / ``revision = 0`` when the camera has none yet.
    Scoped to the caller's account by an explicit camera-ownership check
    and by the account-scoped query.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        record = await fetch_geometry_config(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
        )
    if record is None:
        record = default_geometry_config_record(camera_id)
    return GeometryConfigResponse(geometry_config=record)


@router.put(
    "/{camera_id}/geometry-config",
    response_model=GeometryConfigResponse,
)
async def put_geometry_config(
    camera_id: UUID,
    body: GeometryConfigUpdateRequest,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> GeometryConfigResponse:
    """Replace the ROI/line geometry config for one camera (P6-A).

    Full replacement — the new config overwrites any prior one and the row
    revision bumps by one. ``account_id`` / ``camera_id`` are derived from
    the authenticated caller and path, never trusted from the request body.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        record = await upsert_geometry_config(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            config=body.config,
        )
    return GeometryConfigResponse(geometry_config=record)
