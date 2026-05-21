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
from upstream_api.schemas.processor import (
    ObservationRecord,
    RecentObservationsResponse,
)
from upstream_api.security.deps import CurrentAgent, CurrentCaller, PoolDep
from upstream_api.services.limits import (
    RequestedStream,
    check_camera_defaults,
    check_camera_slot_available,
    load_account_limits,
)
from upstream_api.services.processor import coerce_jsonb, fetch_recent_observations

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
