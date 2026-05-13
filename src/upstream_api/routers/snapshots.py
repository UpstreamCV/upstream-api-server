"""POST /v1/snapshots: latest-frame JPEG upload.

The agent (or, optionally, the streaming server) uploads a still image as a
multipart form. The bytes go to Supabase Storage; metadata + a `storage_key`
go into the `snapshots` table. Quota is enforced against
`account_limits.snapshot_storage_mb` across the trailing 30-day window.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, File, Form, Query, UploadFile

from upstream_api.errors import ApiError, ConflictError, LimitExceeded, NotFoundError
from upstream_api.observability.context import agent_id_var
from upstream_api.schemas.snapshots import LatestFrameResponse, SnapshotUploadResponse
from upstream_api.security.deps import CurrentAgent, CurrentCaller, PoolDep, SettingsDep
from upstream_api.services.storage import create_signed_url, upload_bytes
from upstream_api.services.usage import record_event

router = APIRouter(prefix="/v1/snapshots", tags=["snapshots"])

# Lives at /v1/cameras/{id}/latest-frame to match the design doc; mounted
# separately so this file owns all snapshot-related handlers.
camera_router = APIRouter(tags=["snapshots"])

_SIGNED_URL_TTL_SECONDS = 60

_ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/jpg"}


def _build_storage_key(account_id: UUID, camera_id: UUID, snapshot_id: UUID) -> str:
    day = datetime.now(tz=UTC).strftime("%Y%m%d")
    return f"{account_id}/{camera_id}/{day}/{snapshot_id}.jpg"


@router.post("", response_model=SnapshotUploadResponse, status_code=201)
async def upload_snapshot(
    auth: CurrentAgent,
    pool: PoolDep,
    settings: SettingsDep,
    camera_id: Annotated[UUID, Form()],
    captured_at: Annotated[datetime, Form()],
    file: Annotated[UploadFile, File()],
    agent_id: Annotated[UUID, Form()],
    session_id: Annotated[UUID | None, Form()] = None,
    width: Annotated[int | None, Form(ge=1, le=7680)] = None,
    height: Annotated[int | None, Form(ge=1, le=4320)] = None,
) -> SnapshotUploadResponse:
    content_type = (file.content_type or "").lower()
    if content_type not in _ALLOWED_CONTENT_TYPES:
        raise ApiError(
            "Only image/jpeg is accepted.",
            code="invalid_content_type",
            status_code=415,
            details={"received": content_type, "allowed": sorted(_ALLOWED_CONTENT_TYPES)},
        )

    data = await file.read()
    size = len(data)
    if size == 0:
        raise ApiError("Empty upload.", code="empty_upload", status_code=400)
    if size > settings.snapshot_max_bytes:
        raise ApiError(
            f"Snapshot exceeds maximum size of {settings.snapshot_max_bytes} bytes.",
            code="snapshot_too_large",
            status_code=413,
            details={"size": size, "max_bytes": settings.snapshot_max_bytes},
        )

    async with pool.acquire() as conn, conn.transaction():
        camera = await conn.fetchrow(
            "select id, account_id, agent_id from cameras where id = $1",
            camera_id,
        )
        if camera is None:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        if camera["account_id"] != auth.account_id or camera["agent_id"] != agent_id:
            raise ConflictError(
                "Camera does not belong to this agent/account.",
                code="agent_account_mismatch",
            )

        # Quota: trailing 30 days against snapshot_storage_mb.
        used_bytes = await conn.fetchval(
            """
            select coalesce(sum(bytes), 0)::bigint
              from snapshots
             where account_id = $1
               and created_at > now() - interval '30 days'
            """,
            auth.account_id,
        )
        limit_row = await conn.fetchrow(
            "select snapshot_storage_mb from account_limits where account_id = $1",
            auth.account_id,
        )
        if limit_row is None:
            raise NotFoundError(
                "Account limits not configured.",
                code="account_limits_missing",
                details={"account_id": str(auth.account_id)},
            )
        quota_bytes = int(limit_row["snapshot_storage_mb"]) * 1024 * 1024
        if int(used_bytes) + size > quota_bytes:
            raise LimitExceeded(
                "Snapshot storage quota exceeded.",
                code="snapshot_quota_exceeded",
                status_code=429,
                details={
                    "used_bytes": int(used_bytes),
                    "incoming_bytes": size,
                    "quota_bytes": quota_bytes,
                },
            )

        snapshot_id = uuid4()
        storage_key = _build_storage_key(auth.account_id, camera_id, snapshot_id)

        # Upload first; if it fails, the transaction rolls back and no row
        # references a nonexistent object. We accept the small risk of an
        # orphaned object on rare crashes — a janitor job sweeps Storage
        # against the snapshots table.
        await upload_bytes(
            settings=settings,
            bucket=settings.supabase_storage_bucket,
            storage_key=storage_key,
            data=data,
            content_type="image/jpeg",
        )

        row = await conn.fetchrow(
            """
            insert into snapshots (
              id, account_id, agent_id, camera_id, session_id,
              storage_key, content_type, width, height, bytes, captured_at
            ) values ($1, $2, $3, $4, $5, $6, 'image/jpeg', $7, $8, $9, $10)
            returning id, camera_id, session_id, storage_key, bytes, width, height,
                      captured_at, created_at
            """,
            snapshot_id,
            auth.account_id,
            agent_id,
            camera_id,
            session_id,
            storage_key,
            width,
            height,
            size,
            captured_at,
        )
        assert row is not None

        await record_event(
            conn,
            account_id=auth.account_id,
            event_type="snapshot_bytes",
            session_id=session_id,
            quantity=size,
            unit="bytes",
            metadata={"camera_id": str(camera_id)},
        )

    agent_id_var.set(agent_id)
    return SnapshotUploadResponse(
        id=row["id"],
        camera_id=row["camera_id"],
        session_id=row["session_id"],
        storage_key=row["storage_key"],
        bytes=row["bytes"],
        width=row["width"],
        height=row["height"],
        captured_at=row["captured_at"],
        created_at=row["created_at"],
    )


@camera_router.get(
    "/v1/cameras/{camera_id}/latest-frame",
    response_model=LatestFrameResponse,
)
async def get_latest_frame(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> LatestFrameResponse:
    """Most recent snapshot for a camera + a short-lived signed URL.

    Replaces the dashboard's direct Supabase Storage + table query. The
    signed URL expires after ~60s; clients re-fetch this endpoint to refresh.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        row = await conn.fetchrow(
            """
            select storage_key, bytes, width, height, captured_at, created_at
              from snapshots
             where camera_id = $1
             order by captured_at desc
             limit 1
            """,
            camera_id,
        )
    if row is None:
        raise NotFoundError(
            "No frames captured for this camera yet.",
            code="snapshot_not_found",
        )

    signed_url = await create_signed_url(
        settings=settings,
        bucket=settings.supabase_storage_bucket,
        storage_key=row["storage_key"],
        expires_in=_SIGNED_URL_TTL_SECONDS,
    )
    return LatestFrameResponse(
        storage_key=row["storage_key"],
        bytes=row["bytes"],
        width=row["width"],
        height=row["height"],
        captured_at=row["captured_at"],
        created_at=row["created_at"],
        signed_url=signed_url,
        expires_at=datetime.now(tz=UTC) + timedelta(seconds=_SIGNED_URL_TTL_SECONDS),
    )
