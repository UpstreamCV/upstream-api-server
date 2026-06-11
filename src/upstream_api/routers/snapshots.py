"""Latest-frame JPEG upload + read.

Two producers, one pipeline:

* ``POST /v1/snapshots`` — agent-token authed. The agent supplies
  ``camera_id`` + ``agent_id`` and ownership is checked against ``cameras``.
* ``POST /v1/ingest/snapshots`` — webhook-secret authed (for the streaming
  server). It supplies only ``session_id``; ``account_id`` / ``agent_id`` /
  ``camera_id`` are derived from the session row and never trusted from the
  request. Only ``streaming`` sessions are accepted.

Both go through the same file-validation → quota → Supabase Storage upload →
``snapshots`` insert → usage-event pipeline (``_read_validated_jpeg`` /
``_enforce_snapshot_quota`` / ``_persist_snapshot``). Quota is enforced
against ``account_limits.snapshot_storage_mb`` across the trailing 30-day
window.

``GET /v1/cameras/{id}/latest-frame`` returns the most recent snapshot plus a
short-lived signed URL.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID, uuid4

import asyncpg
from fastapi import APIRouter, File, Form, Query, UploadFile

from upstream_api.config import Settings
from upstream_api.errors import ApiError, ConflictError, LimitExceeded, NotFoundError
from upstream_api.observability.context import agent_id_var, session_id_var
from upstream_api.observability.metrics import track_ingest
from upstream_api.schemas.snapshots import LatestFrameResponse, SnapshotUploadResponse
from upstream_api.security.deps import (
    CurrentAgent,
    CurrentCaller,
    CurrentIngest,
    PoolDep,
    SettingsDep,
)
from upstream_api.services.storage import create_signed_url, upload_bytes
from upstream_api.services.stream_validation import load_session_state
from upstream_api.services.usage import record_event

router = APIRouter(prefix="/v1/snapshots", tags=["snapshots"])

# Lives at /v1/cameras/{id}/latest-frame to match the design doc; mounted
# separately so this file owns all snapshot-related handlers.
camera_router = APIRouter(tags=["snapshots"])

# Lives at /v1/ingest/snapshots (webhook-secret authed; for the streaming
# server). Mounted separately for the same reason as camera_router.
ingest_router = APIRouter(tags=["snapshots"])

_SIGNED_URL_TTL_SECONDS = 60

_ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/jpg"}


def _build_storage_key(account_id: UUID, camera_id: UUID, snapshot_id: UUID) -> str:
    day = datetime.now(tz=UTC).strftime("%Y%m%d")
    return f"{account_id}/{camera_id}/{day}/{snapshot_id}.jpg"


async def _read_validated_jpeg(file: UploadFile, settings: Settings) -> bytes:
    """Content-type / non-empty / size checks shared by both upload routes.

    Runs before any DB access (same order as the original handler). Raises
    ``invalid_content_type`` (415), ``empty_upload`` (400), or
    ``snapshot_too_large`` (413).
    """
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
    return data


async def _enforce_snapshot_quota(
    conn: asyncpg.Connection, account_id: UUID, incoming_bytes: int
) -> None:
    """Trailing-30-day byte sum vs ``account_limits.snapshot_storage_mb``.

    Raises ``account_limits_missing`` (404) or ``snapshot_quota_exceeded``
    (429). Must run inside the caller's transaction.
    """
    used_bytes = await conn.fetchval(
        """
        select coalesce(sum(bytes), 0)::bigint
          from snapshots
         where account_id = $1
           and created_at > now() - interval '30 days'
        """,
        account_id,
    )
    limit_row = await conn.fetchrow(
        "select snapshot_storage_mb from account_limits where account_id = $1",
        account_id,
    )
    if limit_row is None:
        raise NotFoundError(
            "Account limits not configured.",
            code="account_limits_missing",
            details={"account_id": str(account_id)},
        )
    quota_bytes = int(limit_row["snapshot_storage_mb"]) * 1024 * 1024
    if int(used_bytes) + incoming_bytes > quota_bytes:
        raise LimitExceeded(
            "Snapshot storage quota exceeded.",
            code="snapshot_quota_exceeded",
            status_code=429,
            details={
                "used_bytes": int(used_bytes),
                "incoming_bytes": incoming_bytes,
                "quota_bytes": quota_bytes,
            },
        )


async def _persist_snapshot(
    conn: asyncpg.Connection,
    *,
    settings: Settings,
    account_id: UUID,
    agent_id: UUID,
    camera_id: UUID,
    session_id: UUID | None,
    width: int | None,
    height: int | None,
    data: bytes,
    captured_at: datetime,
) -> SnapshotUploadResponse:
    """Upload to Storage, insert the row, record the usage event.

    Identity (account/agent/camera) is resolved by the caller and passed in;
    this helper never reads it from the request. Must run inside the caller's
    transaction.
    """
    size = len(data)
    snapshot_id = uuid4()
    storage_key = _build_storage_key(account_id, camera_id, snapshot_id)

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
        account_id,
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
        account_id=account_id,
        event_type="snapshot_bytes",
        session_id=session_id,
        quantity=size,
        unit="bytes",
        metadata={"camera_id": str(camera_id)},
    )

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
    """Agent-token authed upload. Ownership checked against ``cameras``."""
    data = await _read_validated_jpeg(file, settings)

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

        await _enforce_snapshot_quota(conn, auth.account_id, len(data))
        response = await _persist_snapshot(
            conn,
            settings=settings,
            account_id=auth.account_id,
            agent_id=agent_id,
            camera_id=camera_id,
            session_id=session_id,
            width=width,
            height=height,
            data=data,
            captured_at=captured_at,
        )

    agent_id_var.set(agent_id)
    return response


@ingest_router.post(
    "/v1/ingest/snapshots",
    response_model=SnapshotUploadResponse,
    status_code=201,
)
@track_ingest("snapshots")
async def ingest_upload_snapshot(
    _ingest: CurrentIngest,
    pool: PoolDep,
    settings: SettingsDep,
    session_id: Annotated[UUID, Form()],
    captured_at: Annotated[datetime, Form()],
    file: Annotated[UploadFile, File()],
    width: Annotated[int | None, Form(ge=1, le=7680)] = None,
    height: Annotated[int | None, Form(ge=1, le=4320)] = None,
) -> SnapshotUploadResponse:
    """Streaming-server upload, webhook-secret authed (no agent token).

    ``account_id`` / ``agent_id`` / ``camera_id`` are derived from the session
    row via ``load_session_state`` — never trusted from the multipart body.
    Any extra ``camera_id`` / ``agent_id`` / ``account_id`` / ``storage_key``
    form fields are silently ignored (they are not declared parameters), so a
    caller cannot write into another tenant's path. Only ``streaming``
    sessions are accepted.
    """
    data = await _read_validated_jpeg(file, settings)

    async with pool.acquire() as conn, conn.transaction():
        session = await load_session_state(conn, session_id)
        if session is None:
            raise NotFoundError("Session not found.", code="session_not_found")
        if session.state != "streaming":
            raise ConflictError(
                f"Session is in state '{session.state}', expected 'streaming'.",
                code="stream_state_invalid",
                details={"current_state": session.state, "expected": "streaming"},
            )

        await _enforce_snapshot_quota(conn, session.account_id, len(data))
        response = await _persist_snapshot(
            conn,
            settings=settings,
            account_id=session.account_id,
            agent_id=session.agent_id,
            camera_id=session.camera_id,
            session_id=session.id,
            width=width,
            height=height,
            data=data,
            captured_at=captured_at,
        )

    session_id_var.set(session.id)
    return response


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
