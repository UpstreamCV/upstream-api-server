from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ApiError, ConflictError, ForbiddenError, NotFoundError
from upstream_api.schemas.cameras import (
    CameraClaimRequest,
    CameraCreateRequest,
    CameraListResponse,
    CameraRegisterRequest,
    CameraResponse,
    CameraUpdateRequest,
)
from upstream_api.schemas.geometry import (
    GeometryConfigResponse,
    GeometryConfigUpdateRequest,
)
from upstream_api.schemas.processing_config_camera import (
    CameraPackageConfigResponse,
    CameraPackageRequest,
    CameraProcessingConfigResponse,
    CameraProcessingConfigUpdateRequest,
)
from upstream_api.schemas.processor import (
    LatestPerceptionSnapshotResponse,
    LatestProcessorStatusResponse,
    ObservationRecord,
    PerceptionSnapshotRecord,
    PerceptionStreamTokenResponse,
    ProcessorStatusRecord,
    RecentObservationsResponse,
)
from upstream_api.security.deps import (
    CurrentAgent,
    CurrentCaller,
    PoolDep,
    SettingsDep,
    require_account_role,
    require_agent_access,
    require_caller_agent_access,
)
from upstream_api.security.fanout_token import mint_fanout_token
from upstream_api.services.account_audit import try_record_account_audit_log
from upstream_api.services.camera_package_config import (
    build_package_config_response,
    store_requested_package,
)
from upstream_api.services.geometry import (
    default_geometry_config_record,
    fetch_geometry_config,
    upsert_geometry_config,
)
from upstream_api.services.limits import (
    RequestedStream,
    check_camera_defaults,
    check_camera_slot_available,
    check_execution_mode_allowed,
    check_stream_type_allowed,
    load_account_limits,
)
from upstream_api.services.models import get_model
from upstream_api.services.processing_config_camera import (
    default_camera_processing_config_record,
    fetch_camera_processing_config,
    upsert_camera_processing_config,
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
        source_type=row["source_type"],
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
    "id, account_id, agent_id, name, external_id, device_path, source_type, "
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
    # Device-scoped tokens may only attach cameras to their bound agent.
    require_agent_access(auth, body.agent_id)
    async with pool.acquire() as conn, conn.transaction():
        # Confirm agent belongs to this account (and read the account plan,
        # used by the counting-package auto-apply below).
        agent_row = await conn.fetchrow(
            "select ag.account_id, a.plan "
            "from agents ag join accounts a on a.id = ag.account_id "
            "where ag.id = $1",
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
        check_stream_type_allowed(limits, body.source_type)

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

        # Auto-apply the CrowdCount counting package: a device on the comped
        # CrowdCount fleet plan that declares the counting workload gets the
        # package stamped onto its camera metadata, so the processor picks it up
        # with no dashboard step. The account plan is the authorization; the
        # device-declared workload is only a hint (a device cannot grant itself
        # a package its account is not entitled to). Folded into the written
        # metadata so it survives the re-register overwrite.
        metadata = dict(body.metadata)
        if (
            agent_row["plan"] == "crowdcount"
            and metadata.get("workload") == "overhead-people-counter-line-crossing"
        ):
            metadata["package_id"] = "overhead_people_counter.v1"

        if existing is None:
            row = await conn.fetchrow(
                f"""
                insert into cameras (
                  account_id, agent_id, name, external_id, device_path, source_type,
                  default_width, default_height, default_fps, default_codec, default_bitrate_kbps,
                  default_latency_mode, metadata
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
                returning {_CAMERA_COLUMNS}
                """,
                auth.account_id,
                body.agent_id,
                body.name,
                body.external_id,
                body.device_path,
                body.source_type,
                body.default_width,
                body.default_height,
                body.default_fps,
                body.default_codec,
                body.default_bitrate_kbps,
                body.default_latency_mode,
                json.dumps(metadata),
            )
            assert row is not None
            await try_record_account_audit_log(
                conn,
                account_id=auth.account_id,
                actor_type="system",
                event_type="camera.created",
                resource_type="camera",
                resource_id=str(row["id"]),
                summary=f"Camera {body.name!r} registered by agent.",
                metadata={"name": body.name, "source_type": body.source_type},
            )
        else:
            # coalesce keeps a previously declared source_type when an older
            # agent re-registers without sending one.
            row = await conn.fetchrow(
                f"""
                update cameras
                   set name = $3,
                       device_path = $4,
                       source_type = coalesce($5, source_type),
                       default_width = $6,
                       default_height = $7,
                       default_fps = $8,
                       default_codec = $9,
                       default_bitrate_kbps = $10,
                       default_latency_mode = $11,
                       metadata = $12::jsonb
                 where id = $1 and account_id = $2
                returning {_CAMERA_COLUMNS}
                """,
                existing["id"],
                auth.account_id,
                body.name,
                body.device_path,
                body.source_type,
                body.default_width,
                body.default_height,
                body.default_fps,
                body.default_codec,
                body.default_bitrate_kbps,
                body.default_latency_mode,
                json.dumps(metadata),
            )

    assert row is not None
    return _camera_to_response(row)


@router.post("", response_model=CameraResponse, status_code=201)
async def create_camera(
    body: CameraCreateRequest,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraResponse:
    """Dashboard-authored camera. Owner/admin users only.

    Created with ``agent_id = NULL`` — an agent claims it via
    ``POST /v1/cameras/{id}/claim`` before it can stream. Defaults are
    validated against account limits exactly like agent registration, so a
    camera can never be configured beyond what the account allows.
    """
    if caller.user is None:
        raise ForbiddenError(
            "Camera creation requires a user session; agents register cameras "
            "via /v1/cameras/register.",
            code="forbidden.user_required",
        )
    scope_account = caller.resolve_account(account_id)
    require_account_role(caller.user, scope_account, "owner", "admin")

    async with pool.acquire() as conn, conn.transaction():
        limits = await load_account_limits(conn, scope_account)
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
        check_stream_type_allowed(limits, body.source_type)
        await check_camera_slot_available(
            conn,
            account_id=scope_account,
            is_new_camera=True,
        )
        row = await conn.fetchrow(
            f"""
            insert into cameras (
              account_id, agent_id, name, device_path, source_type,
              default_width, default_height, default_fps, default_codec, default_bitrate_kbps,
              default_latency_mode, metadata
            )
            values ($1, null, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            returning {_CAMERA_COLUMNS}
            """,
            scope_account,
            body.name,
            body.device_path,
            body.source_type,
            body.default_width,
            body.default_height,
            body.default_fps,
            body.default_codec,
            body.default_bitrate_kbps,
            body.default_latency_mode,
            json.dumps(body.metadata),
        )
        assert row is not None
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            actor_user_id=caller.user.user_id if caller.user else None,
            actor_type="user" if caller.user else "system",
            event_type="camera.created",
            resource_type="camera",
            resource_id=str(row["id"]),
            summary=f"Camera {body.name!r} created.",
            metadata={"name": body.name, "source_type": body.source_type},
        )
    assert row is not None
    return _camera_to_response(row)


@router.patch("/{camera_id}", response_model=CameraResponse)
async def update_camera(
    camera_id: UUID,
    body: CameraUpdateRequest,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraResponse:
    """Partial dashboard edit of a camera's config. Owner/admin users only.

    The merged result (current row + provided fields) is re-validated
    against account limits, so an edit can't push a camera over caps.
    """
    if caller.user is None:
        raise ForbiddenError(
            "Camera editing requires a user session.",
            code="forbidden.user_required",
        )
    scope_account = caller.resolve_account(account_id)
    require_account_role(caller.user, scope_account, "owner", "admin")

    async with pool.acquire() as conn, conn.transaction():
        current = await conn.fetchrow(
            f"select {_CAMERA_COLUMNS} from cameras where id = $1",
            camera_id,
        )
        if current is None or current["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        merged_width = body.default_width or current["default_width"] or 1280
        merged_height = body.default_height or current["default_height"] or 720
        merged_fps = body.default_fps or current["default_fps"] or 30
        merged_codec = body.default_codec or current["default_codec"] or "h264"
        merged_bitrate = body.default_bitrate_kbps or current["default_bitrate_kbps"] or 3000
        merged_source_type = body.source_type or current["source_type"]

        limits = await load_account_limits(conn, scope_account)
        check_camera_defaults(
            limits,
            RequestedStream(
                width=merged_width,
                height=merged_height,
                fps=merged_fps,
                codec=merged_codec,
                bitrate_kbps=merged_bitrate,
            ),
        )
        check_stream_type_allowed(limits, merged_source_type)

        row = await conn.fetchrow(
            f"""
            update cameras
               set name = coalesce($3, name),
                   device_path = coalesce($4, device_path),
                   source_type = coalesce($5, source_type),
                   default_width = coalesce($6, default_width),
                   default_height = coalesce($7, default_height),
                   default_fps = coalesce($8, default_fps),
                   default_codec = coalesce($9, default_codec),
                   default_bitrate_kbps = coalesce($10, default_bitrate_kbps),
                   default_latency_mode = coalesce($11, default_latency_mode)
             where id = $1 and account_id = $2
            returning {_CAMERA_COLUMNS}
            """,
            camera_id,
            scope_account,
            body.name,
            body.device_path,
            body.source_type,
            body.default_width,
            body.default_height,
            body.default_fps,
            body.default_codec,
            body.default_bitrate_kbps,
            body.default_latency_mode,
        )
        assert row is not None
        changed = sorted(k for k, v in body.model_dump(exclude_unset=True).items() if v is not None)
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            actor_user_id=caller.user.user_id if caller.user else None,
            actor_type="user" if caller.user else "system",
            event_type="camera.updated",
            resource_type="camera",
            resource_id=str(camera_id),
            summary=f"Camera {row['name']!r} updated.",
            metadata={"name": row["name"], "changed_fields": changed},
        )
    assert row is not None
    return _camera_to_response(row)


@router.post("/{camera_id}/claim", response_model=CameraResponse)
async def claim_camera(
    camera_id: UUID,
    body: CameraClaimRequest,
    auth: CurrentAgent,
    pool: PoolDep,
) -> CameraResponse:
    """Assign an unassigned (dashboard-created) camera to an agent.

    Idempotent when the camera is already assigned to the same agent;
    409 ``camera_claimed_by_other_agent`` when another agent holds it —
    reassignment is a dashboard/operator decision, not an agent race.
    """
    # Device-scoped tokens may only claim cameras for their bound agent.
    require_agent_access(auth, body.agent_id)
    async with pool.acquire() as conn, conn.transaction():
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

        camera = await conn.fetchrow(
            f"select {_CAMERA_COLUMNS} from cameras where id = $1",
            camera_id,
        )
        if camera is None or camera["account_id"] != auth.account_id:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        if camera["agent_id"] is not None and camera["agent_id"] != body.agent_id:
            raise ConflictError(
                "Camera is already claimed by another agent.",
                code="camera_claimed_by_other_agent",
            )
        if camera["agent_id"] == body.agent_id:
            return _camera_to_response(camera)

        row = await conn.fetchrow(
            f"""
            update cameras
               set agent_id = $3
             where id = $1 and account_id = $2 and agent_id is null
            returning {_CAMERA_COLUMNS}
            """,
            camera_id,
            auth.account_id,
            body.agent_id,
        )
        if row is not None:
            await try_record_account_audit_log(
                conn,
                account_id=auth.account_id,
                actor_type="system",
                event_type="camera.claimed",
                resource_type="camera",
                resource_id=str(camera_id),
                summary=f"Camera {row['name']!r} claimed by agent.",
                metadata={"name": row["name"], "agent_id": str(body.agent_id)},
            )
    if row is None:
        # Lost a claim race between the read and the conditional update.
        raise ConflictError(
            "Camera is already claimed by another agent.",
            code="camera_claimed_by_other_agent",
        )
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


class PerceptionStreamNotConfiguredError(ApiError):
    status_code = 503
    code = "perception_stream_not_configured"


@router.post(
    "/{camera_id}/perception/stream-token",
    response_model=PerceptionStreamTokenResponse,
)
async def mint_perception_stream_token(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> PerceptionStreamTokenResponse:
    """Mint a short-lived token for the perception-fanout WS hot path.

    The perception-fanout sidecar (upstream-streaming-server) relays
    processor snapshots to browsers at 10-15 Hz, bypassing the DB; this
    endpoint is where its subscriber auth happens. Supabase JWT auth +
    the same camera-ownership check as ``perception/latest`` run HERE,
    then the browser presents the resulting HMAC token to the sidecar,
    which only verifies signature + expiry (shared
    ``UPSTREAM_FANOUT_TOKEN_SECRET``). Membership is re-checked at every
    mint, so revocation exposure is bounded by the token TTL.

    Returns 503 ``perception_stream_not_configured`` when the fanout
    settings are unset (same convention as ``billing_not_configured``) —
    the dashboard then stays on its Realtime + polling fallback. The
    token is returned once and never logged.
    """
    # An empty-string env var yields SecretStr("") (not None), and the sidecar
    # fail-louds on the same empty value — treat empty as unconfigured here too
    # so the operator gets the clean 503 instead of unverifiable tokens.
    if (
        settings.fanout_token_secret is None
        or not settings.fanout_token_secret.get_secret_value()
        or not settings.fanout_public_ws_url
    ):
        raise PerceptionStreamNotConfiguredError(
            "Perception streaming is not configured for this deployment."
        )

    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id, agent_id from cameras where id = $1",
            camera_id,
        )
    if owner_row is None or owner_row["account_id"] != scope_account:
        raise NotFoundError("Camera not found.", code="camera_not_found")
    # Device-scoped tokens may only mint stream tokens for their own camera.
    require_caller_agent_access(caller, owner_row["agent_id"])

    if caller.user is not None:
        subject = str(caller.user.user_id)
    else:
        assert caller.agent is not None
        subject = f"agent-token:{caller.agent.token_id}"

    now = int(time.time())
    expires_at = now + settings.fanout_token_ttl_seconds
    token = mint_fanout_token(
        camera_id=str(camera_id),
        account_id=str(scope_account),
        subject=subject,
        issued_at=now,
        expires_at=expires_at,
        secret=settings.fanout_token_secret.get_secret_value(),
    )
    return PerceptionStreamTokenResponse(
        token=token,
        ws_url=f"{settings.fanout_public_ws_url.rstrip('/')}/v1/subscribe",
        expires_at=datetime.fromtimestamp(expires_at, tz=UTC),
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
            "select account_id, agent_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        # Device-scoped tokens may only write their own camera's geometry.
        require_caller_agent_access(caller, owner_row["agent_id"])

        record = await upsert_geometry_config(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            config=body.config,
        )
    return GeometryConfigResponse(geometry_config=record)


@router.get(
    "/{camera_id}/processing-config",
    response_model=CameraProcessingConfigResponse,
)
async def get_processing_config(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraProcessingConfigResponse:
    """Dashboard-authored model/processing config for one camera.

    Returns the saved config, or the default config with
    ``is_default = true`` / ``revision = 0`` when the camera has none yet.
    Scoped to the caller's account by an explicit camera-ownership check
    and by the account-scoped query. The processor does not consume this
    config yet.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        record = await fetch_camera_processing_config(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
        )
    if record is None:
        record = default_camera_processing_config_record(
            account_id=scope_account,
            camera_id=camera_id,
        )
    return CameraProcessingConfigResponse(processing_config=record)


@router.put(
    "/{camera_id}/processing-config",
    response_model=CameraProcessingConfigResponse,
)
async def put_processing_config(
    camera_id: UUID,
    body: CameraProcessingConfigUpdateRequest,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraProcessingConfigResponse:
    """Replace the model/processing config for one camera.

    Full replacement — the new config overwrites any prior one and the row
    revision bumps by one. ``account_id`` / ``camera_id`` are derived from
    the authenticated caller and path, never trusted from the request body.
    The config is validated against the model registry before write.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id, agent_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        # Device-scoped tokens may only write their own camera's config.
        require_caller_agent_access(caller, owner_row["agent_id"])

        # execution_mode = shared_inference is an account-gated capability; the
        # config schema can only validate it structurally (no DB access), so the
        # entitlement is enforced here before the write. per_session is a no-op.
        limits = await load_account_limits(conn, scope_account)
        check_execution_mode_allowed(limits, body.config.execution_mode)

        record = await upsert_camera_processing_config(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            config=body.config,
        )

        # Package assignment v1: the chosen model maps to a processor
        # package (see services/models.py). Stamp it into cameras.metadata
        # — the field the processor's eligible-sessions payload reads — so
        # the model picker genuinely switches the processing engine. Takes
        # effect on the camera's NEXT stream session. ``get_model`` cannot
        # return None here: the request schema already validated model_key.
        model_entry = get_model(body.config.model_key)
        if model_entry is not None and model_entry.package_id is not None:
            await conn.execute(
                """
                update cameras
                   set metadata = jsonb_set(
                           coalesce(metadata, '{}'::jsonb),
                           '{package_id}',
                           to_jsonb($2::text)
                       ),
                       updated_at = now()
                 where id = $1
                """,
                camera_id,
                model_entry.package_id,
            )

        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            event_type="camera.processing_config_updated",
            summary=f"Processing config updated (model {body.config.model_key}).",
            actor_user_id=caller.user.user_id if caller.user else None,
            actor_type="user" if caller.user else "system",
            resource_type="camera",
            resource_id=str(camera_id),
            metadata={
                "model_key": body.config.model_key,
                "package_id": model_entry.package_id if model_entry else None,
                "revision": record.revision,
            },
        )
    return CameraProcessingConfigResponse(processing_config=record)


# --- P5-G: package-centric request/apply -------------------------------------


@router.get(
    "/{camera_id}/processing-config/package",
    response_model=CameraPackageConfigResponse,
)
async def get_package_config(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraPackageConfigResponse:
    """P5-G: the camera's requested-vs-applied processing PACKAGE state.

    ``requested`` is the operator's package selection (stored in
    ``cameras.metadata``); ``applied`` is derived from the latest processor
    status (what actually ran); ``state`` compares them
    (unset/active/pending_apply/pending_restart/failed_config/
    experimental_selected). Catalog only — this does not assign or restart a
    running session; a selection takes effect on the camera's next session.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        return await build_package_config_response(
            conn, account_id=scope_account, camera_id=camera_id
        )


@router.put(
    "/{camera_id}/processing-config/package",
    response_model=CameraPackageConfigResponse,
)
async def put_package_config(
    camera_id: UUID,
    body: CameraPackageRequest,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraPackageConfigResponse:
    """P5-G: request a processing package for one camera. Validated against the
    package catalog (exists/visible; ``experimental`` requires
    ``allow_experimental``; ``deprecated`` rejected; selected classes + settings
    within the package's bounds). Stores the requested config (bumping its
    revision) and stamps ``cameras.metadata.package_id``; takes effect on the
    camera's next session start."""
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        await store_requested_package(
            conn,
            camera_id=camera_id,
            package_id=body.package_id,
            settings=body.settings,
            updated_by=(f"user:{caller.user.user_id}" if caller.user else None),
            now=datetime.now(UTC),
        )
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            event_type="camera.processing_package_requested",
            summary=f"Processing package requested ({body.package_id}).",
            actor_user_id=caller.user.user_id if caller.user else None,
            actor_type="user" if caller.user else "system",
            resource_type="camera",
            resource_id=str(camera_id),
            metadata={"package_id": body.package_id},
        )
        return await build_package_config_response(
            conn, account_id=scope_account, camera_id=camera_id
        )


@router.post(
    "/{camera_id}/processing-config/apply",
    response_model=CameraPackageConfigResponse,
)
async def apply_package_config(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> CameraPackageConfigResponse:
    """P5-G: confirm the requested package as the active intent and return the
    derived state. Re-stamps ``metadata.package_id`` from the requested config
    (idempotent); the processor applies it on the next session start — no live
    restart (Option A), so the state is ``pending_apply`` / ``pending_restart``
    until then. 409 if nothing has been requested."""
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")
        current = await build_package_config_response(
            conn, account_id=scope_account, camera_id=camera_id
        )
        if current.requested is None:
            raise ConflictError("No requested package config to apply.", code="no_requested_config")
        # Idempotent re-stamp of metadata.package_id from the requested config
        # (does not bump the requested revision). The next session picks it up.
        await conn.execute(
            """
            update cameras
               set metadata = jsonb_set(
                       coalesce(metadata, '{}'::jsonb),
                       '{package_id}', to_jsonb($2::text)
                   ),
                   updated_at = now()
             where id = $1
            """,
            camera_id,
            current.requested.package_id,
        )
        await try_record_account_audit_log(
            conn,
            account_id=scope_account,
            event_type="camera.processing_package_applied",
            summary=f"Processing package apply confirmed ({current.requested.package_id}).",
            actor_user_id=caller.user.user_id if caller.user else None,
            actor_type="user" if caller.user else "system",
            resource_type="camera",
            resource_id=str(camera_id),
            metadata={"package_id": current.requested.package_id},
        )
        return await build_package_config_response(
            conn, account_id=scope_account, camera_id=camera_id
        )
