from __future__ import annotations

import json

from fastapi import APIRouter

from upstream_api.errors import ConflictError, NotFoundError
from upstream_api.schemas.cameras import CameraRegisterRequest, CameraResponse
from upstream_api.security.deps import CurrentAgent, PoolDep
from upstream_api.services.limits import (
    RequestedStream,
    check_camera_defaults,
    check_camera_slot_available,
    load_account_limits,
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
        metadata=row["metadata"] if isinstance(row["metadata"], dict)
        else json.loads(row["metadata"]),
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
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
                """
                insert into cameras (
                  account_id, agent_id, name, external_id, device_path,
                  default_width, default_height, default_fps, default_codec, default_bitrate_kbps,
                  metadata
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                returning id, account_id, agent_id, name, external_id, device_path,
                          default_width, default_height, default_fps, default_codec,
                          default_bitrate_kbps, metadata, status, created_at, updated_at
                """,
                auth.account_id, body.agent_id, body.name, body.external_id, body.device_path,
                body.default_width, body.default_height, body.default_fps, body.default_codec,
                body.default_bitrate_kbps, json.dumps(body.metadata),
            )
        else:
            row = await conn.fetchrow(
                """
                update cameras
                   set name = $3,
                       device_path = $4,
                       default_width = $5,
                       default_height = $6,
                       default_fps = $7,
                       default_codec = $8,
                       default_bitrate_kbps = $9,
                       metadata = $10::jsonb
                 where id = $1 and account_id = $2
                returning id, account_id, agent_id, name, external_id, device_path,
                          default_width, default_height, default_fps, default_codec,
                          default_bitrate_kbps, metadata, status, created_at, updated_at
                """,
                existing["id"], auth.account_id, body.name, body.device_path,
                body.default_width, body.default_height, body.default_fps, body.default_codec,
                body.default_bitrate_kbps, json.dumps(body.metadata),
            )

    assert row is not None
    return _camera_to_response(row)
