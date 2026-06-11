"""User-centric admin operations: recovery links + the platform_admins
allowlist itself."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter

from upstream_api.errors import BadRequestError, ConflictError, NotFoundError
from upstream_api.schemas.platform_admin import (
    AdminPlatformAdminAdd,
    AdminPlatformAdminEntry,
    AdminPlatformAdminsResponse,
    AdminRecoveryLinkRequest,
    AdminRecoveryLinkResponse,
)
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep, SettingsDep
from upstream_api.services.admin_audit import record_admin_action
from upstream_api.services.http_client import HttpClientDep
from upstream_api.services.supabase_admin import generate_recovery_link

router = APIRouter()


@router.post("/users/recovery-link", response_model=AdminRecoveryLinkResponse)
async def create_recovery_link(
    body: AdminRecoveryLinkRequest,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
) -> AdminRecoveryLinkResponse:
    """Mint a one-time password-recovery link for any auth user (support
    action; the admin delivers it out of band). The link itself is shown
    once and is NOT written to the audit log."""
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "select 1 from auth.users where email = $1",
            body.email,
        )
    if exists is None:
        raise NotFoundError("No auth user with that email.", code="user_not_found")
    link = await generate_recovery_link(http, settings, email=body.email)
    async with pool.acquire() as conn:
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="user.recovery_link.create",
            target_type="auth_user",
            target_id=None,
            after={"email": body.email},
        )
    return AdminRecoveryLinkResponse(email=body.email, action_link=link)


# ---------------------------------------------------------------------------
# platform_admins allowlist CRUD
# ---------------------------------------------------------------------------


@router.get("/platform-admins", response_model=AdminPlatformAdminsResponse)
async def list_platform_admins(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminPlatformAdminsResponse:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select p.user_id, p.note, p.created_at, u.email
              from platform_admins p
              left join auth.users u on u.id = p.user_id
             order by p.created_at
            """
        )
    return AdminPlatformAdminsResponse(
        admins=[
            AdminPlatformAdminEntry(
                user_id=r["user_id"],
                email=r["email"],
                note=r["note"],
                created_at=r["created_at"],
            )
            for r in rows
        ]
    )


@router.post(
    "/platform-admins",
    response_model=AdminPlatformAdminEntry,
    status_code=201,
)
async def add_platform_admin(
    body: AdminPlatformAdminAdd,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminPlatformAdminEntry:
    """Grant /v1/admin/* access to an EXISTING auth user by email. Deliberately
    does not create users — sign the person up first, then grant."""
    async with pool.acquire() as conn, conn.transaction():
        user_id: UUID | None = await conn.fetchval(
            "select id from auth.users where email = $1",
            body.email,
        )
        if user_id is None:
            raise BadRequestError(
                "No auth user with that email. They must sign in once first.",
                code="user_not_found",
            )
        inserted = await conn.fetchrow(
            """
            insert into platform_admins (user_id, note, created_by)
            values ($1, $2, $3)
            on conflict (user_id) do nothing
            returning created_at
            """,
            user_id,
            body.note,
            admin.user_id,
        )
        if inserted is None:
            raise ConflictError(
                "That user is already a platform admin.",
                code="already_admin",
            )
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="platform_admin.add",
            target_type="platform_admin",
            target_id=user_id,
            after={"email": body.email, "note": body.note},
        )
    return AdminPlatformAdminEntry(
        user_id=user_id,
        email=body.email,
        note=body.note,
        created_at=inserted["created_at"],
    )


@router.delete("/platform-admins/{user_id}", status_code=204)
async def remove_platform_admin(
    user_id: UUID,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> None:
    """Revoke admin access. You cannot remove yourself — prevents the panel
    from locking everyone out (the last remaining admin can't self-delete
    either, by the same rule)."""
    if user_id == admin.user_id:
        raise ConflictError(
            "You cannot remove your own admin access.",
            code="cannot_remove_self",
        )
    async with pool.acquire() as conn, conn.transaction():
        deleted = await conn.fetchrow(
            "delete from platform_admins where user_id = $1 returning note",
            user_id,
        )
        if deleted is None:
            raise NotFoundError("Not a platform admin.", code="admin_not_found")
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="platform_admin.remove",
            target_type="platform_admin",
            target_id=user_id,
            before={"note": deleted["note"]},
        )
