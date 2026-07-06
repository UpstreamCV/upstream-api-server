from __future__ import annotations

import re
from typing import Annotated, Any
from uuid import UUID, uuid4

import asyncpg
from fastapi import APIRouter, Query

from upstream_api.errors import BadRequestError, ConflictError, NotFoundError
from upstream_api.schemas.platform_admin import (
    AdminAccountCreate,
    AdminAccountDetailResponse,
    AdminAccountItem,
    AdminAccountLimits,
    AdminAccountLimitsUpdate,
    AdminAccountListResponse,
    AdminAccountMember,
    AdminAccountMembersResponse,
    AdminAccountUpdate,
    AdminApiToken,
    AdminApiTokenCreate,
    AdminApiTokenCreateResponse,
    AdminApiTokensResponse,
    AdminMemberAdd,
    AdminMemberUpdate,
    AdminUsageTotal,
)
from upstream_api.security.api_tokens import issue_token_async
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep, SettingsDep
from upstream_api.services.admin_audit import record_admin_action
from upstream_api.services.http_client import HttpClientDep
from upstream_api.services.supabase_admin import create_auth_user

router = APIRouter()

_ACCOUNT_ITEM_SELECT = """
    select a.id, a.name, a.slug::text as slug, a.plan, a.status, a.created_at,
           (select count(*)::int from account_members m
             where m.account_id = a.id) as member_count,
           (select count(*)::int from agents g
             where g.account_id = a.id) as agent_count,
           (select count(*)::int from cameras c
             where c.account_id = a.id) as camera_count,
           (select count(*)::int from stream_sessions s
             where s.account_id = a.id
               and s.state in ('approved', 'streaming')) as active_sessions
      from accounts a
"""


def _account_item(row: dict[str, Any]) -> AdminAccountItem:
    return AdminAccountItem(
        id=row["id"],
        name=row["name"],
        slug=row["slug"],
        plan=row["plan"],
        status=row["status"],
        created_at=row["created_at"],
        member_count=row["member_count"],
        agent_count=row["agent_count"],
        camera_count=row["camera_count"],
        active_sessions=row["active_sessions"],
    )


def _limits_from_row(row: dict[str, Any] | None) -> AdminAccountLimits | None:
    if row is None:
        return None
    return AdminAccountLimits(
        max_cameras=row["max_cameras"],
        max_concurrent_sessions=row["max_concurrent_sessions"],
        max_fps=row["max_fps"],
        max_width=row["max_width"],
        max_height=row["max_height"],
        max_bitrate_kbps=row["max_bitrate_kbps"],
        allowed_codecs=list(row["allowed_codecs"]),
        monthly_minutes_quota=row["monthly_minutes_quota"],
        snapshot_storage_mb=row["snapshot_storage_mb"],
        session_ttl_seconds=row["session_ttl_seconds"],
        updated_at=row["updated_at"],
    )


@router.get("/accounts", response_model=AdminAccountListResponse)
async def list_accounts(
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
    search: Annotated[str | None, Query(max_length=200)] = None,
    status: Annotated[str | None, Query()] = None,
    plan: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AdminAccountListResponse:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _ACCOUNT_ITEM_SELECT.replace(
                "from accounts a",
                ", count(*) over()::int as total from accounts a",
            )
            + """
             where ($1::text is null
                    or a.name ilike '%' || $1 || '%'
                    or a.slug::text ilike '%' || $1 || '%')
               and ($2::text is null or a.status = $2)
               and ($3::text is null or a.plan = $3)
             order by a.created_at desc
             limit $4 offset $5
            """,
            search,
            status,
            plan,
            limit,
            offset,
        )
    return AdminAccountListResponse(
        items=[_account_item(dict(r)) for r in rows],
        total=rows[0]["total"] if rows else 0,
    )


async def _fetch_account_item(conn: Any, account_id: UUID) -> dict[str, Any]:
    row = await conn.fetchrow(_ACCOUNT_ITEM_SELECT + " where a.id = $1", account_id)
    if row is None:
        raise NotFoundError("Account not found.", code="account_not_found")
    return dict(row)


@router.get("/accounts/{account_id}", response_model=AdminAccountDetailResponse)
async def get_account(
    account_id: UUID,
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminAccountDetailResponse:
    async with pool.acquire() as conn:
        account_row = await _fetch_account_item(conn, account_id)
        limits_row = await conn.fetchrow(
            """
            select max_cameras, max_concurrent_sessions, max_fps, max_width,
                   max_height, max_bitrate_kbps, allowed_codecs,
                   monthly_minutes_quota, snapshot_storage_mb,
                   session_ttl_seconds, updated_at
              from account_limits
             where account_id = $1
            """,
            account_id,
        )
        usage_rows = await conn.fetch(
            """
            select event_type, unit, sum(quantity)::bigint as total_quantity
              from usage_events
             where account_id = $1
               and occurred_at > now() - interval '30 days'
             group by event_type, unit
             order by total_quantity desc
            """,
            account_id,
        )
    return AdminAccountDetailResponse(
        account=_account_item(account_row),
        limits=_limits_from_row(dict(limits_row) if limits_row else None),
        usage_30d=[
            AdminUsageTotal(
                event_type=r["event_type"],
                unit=r["unit"],
                total_quantity=r["total_quantity"],
            )
            for r in usage_rows
        ],
    )


@router.post("/accounts", response_model=AdminAccountItem, status_code=201)
async def create_account(
    body: AdminAccountCreate,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
) -> AdminAccountItem:
    """Provision a tenant directly: accounts + account_limits (same permissive
    defaults as the signup trigger), optionally attaching a first owner.

    Owner resolution: existing auth user by email, else created (confirmed)
    via the GoTrue Admin API when `owner_password` is given. A newly created
    user also gets its own auto-provisioned workspace from the signup trigger
    — the membership added here is a second, intended one.
    """
    account_id = uuid4()
    slug = body.slug or _derive_slug(body.name, account_id)

    owner_user_id: UUID | None = None
    if body.owner_email:
        async with pool.acquire() as conn:
            owner_user_id = await conn.fetchval(
                "select id from auth.users where email = $1",
                body.owner_email,
            )
        if owner_user_id is None:
            if not body.owner_password:
                raise BadRequestError(
                    "No auth user with that email; provide owner_password to create one.",
                    code="owner_not_found",
                )
            created = await create_auth_user(
                http,
                settings,
                email=body.owner_email,
                password=body.owner_password,
                workspace_name=body.name,
            )
            owner_user_id = created.user_id

    async with pool.acquire() as conn, conn.transaction():
        try:
            await conn.execute(
                """
                insert into accounts (id, name, slug, plan, status)
                values ($1, $2, $3, $4, 'active')
                """,
                account_id,
                body.name,
                slug,
                body.plan,
            )
        except asyncpg.UniqueViolationError as exc:
            raise ConflictError(
                "That slug is already taken.",
                code="slug_taken",
                details={"slug": slug},
            ) from exc
        await conn.execute(
            """
            insert into account_limits (
                account_id, max_cameras, max_concurrent_sessions, max_fps,
                max_width, max_height, max_bitrate_kbps, allowed_codecs
            ) values ($1, 5, 5, 60, 1920, 1080, 8000, array['h264','h265'])
            """,
            account_id,
        )
        if owner_user_id is not None:
            await conn.execute(
                """
                insert into account_members (account_id, user_id, role)
                values ($1, $2, 'owner')
                on conflict do nothing
                """,
                account_id,
                owner_user_id,
            )
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.create",
            target_type="account",
            target_id=account_id,
            after={
                "name": body.name,
                "slug": slug,
                "plan": body.plan,
                "owner_email": body.owner_email,
            },
        )
        account_row = await _fetch_account_item(conn, account_id)
    return _account_item(account_row)


def _derive_slug(name: str, account_id: UUID) -> str:
    """Same recipe as the signup trigger: dashed lowercase + uuid suffix."""
    base = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "workspace"
    return f"{base}-{account_id.hex[:6]}"


@router.patch("/accounts/{account_id}", response_model=AdminAccountItem)
async def patch_account(
    account_id: UUID,
    body: AdminAccountUpdate,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminAccountItem:
    """Update identity (name/slug) and/or plan/status. Audited.
    `deleted` is not settable here."""
    changes = body.model_dump(exclude_none=True)
    if not changes:
        raise BadRequestError(
            "Provide at least one of: name, slug, plan, status.",
            code="empty_update",
        )

    async with pool.acquire() as conn, conn.transaction():
        before_row = await conn.fetchrow(
            "select name, slug::text as slug, plan, status from accounts where id = $1 for update",
            account_id,
        )
        if before_row is None:
            raise NotFoundError("Account not found.", code="account_not_found")

        try:
            await conn.execute(
                """
                update accounts
                   set name = coalesce($2, name),
                       slug = coalesce($3, slug),
                       plan = coalesce($4, plan),
                       status = coalesce($5, status)
                 where id = $1
                """,
                account_id,
                changes.get("name"),
                changes.get("slug"),
                changes.get("plan"),
                changes.get("status"),
            )
        except asyncpg.UniqueViolationError as exc:
            raise ConflictError(
                "That slug is already taken.",
                code="slug_taken",
                details={"slug": changes.get("slug")},
            ) from exc

        changed_keys = sorted(changes)
        action = f"account.{changed_keys[0]}.update" if len(changed_keys) == 1 else "account.update"
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action=action,
            target_type="account",
            target_id=account_id,
            before=dict(before_row),
            after={**dict(before_row), **changes},
        )

        account_row = await _fetch_account_item(conn, account_id)
    return _account_item(account_row)


@router.patch("/accounts/{account_id}/limits", response_model=AdminAccountLimits)
async def patch_account_limits(
    account_id: UUID,
    body: AdminAccountLimitsUpdate,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminAccountLimits:
    """Partial update of account_limits columns. Audited.

    Changes apply to FUTURE sessions only — in-flight sessions keep the
    lim_* snapshot taken at approval time.
    """
    changes = body.model_dump(exclude_none=True)
    if not changes:
        raise BadRequestError(
            "Provide at least one limits field.",
            code="empty_update",
        )

    columns = [
        "max_cameras",
        "max_concurrent_sessions",
        "max_fps",
        "max_width",
        "max_height",
        "max_bitrate_kbps",
        "allowed_codecs",
        "monthly_minutes_quota",
        "snapshot_storage_mb",
        "session_ttl_seconds",
    ]
    async with pool.acquire() as conn, conn.transaction():
        before_row = await conn.fetchrow(
            f"""
            select {", ".join(columns)}
              from account_limits
             where account_id = $1
               for update
            """,
            account_id,
        )
        if before_row is None:
            raise NotFoundError(
                "Account limits not configured.",
                code="account_limits_missing",
            )

        set_clauses = []
        params: list[Any] = [account_id]
        for column in columns:
            if column in changes:
                params.append(changes[column])
                set_clauses.append(f"{column} = ${len(params)}")
        updated_row = await conn.fetchrow(
            f"""
            update account_limits
               set {", ".join(set_clauses)}
             where account_id = $1
            returning {", ".join(columns)}, updated_at
            """,
            *params,
        )
        assert updated_row is not None

        before = {k: list(v) if isinstance(v, list) else v for k, v in dict(before_row).items()}
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.limits.update",
            target_type="account_limits",
            target_id=account_id,
            before=before,
            after={**before, **changes},
        )
    return _limits_from_row(dict(updated_row))  # type: ignore[return-value]


@router.get("/accounts/{account_id}/members", response_model=AdminAccountMembersResponse)
async def list_account_members(
    account_id: UUID,
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminAccountMembersResponse:
    async with pool.acquire() as conn:
        member_rows = await conn.fetch(
            """
            select user_id, role, created_at
              from account_members
             where account_id = $1
             order by created_at
            """,
            account_id,
        )
        # Emails live in auth.users. The local `postgres` role can read it;
        # if the production role can't, degrade to user_id-only rather than
        # failing the page.
        emails: dict[UUID, str] = {}
        if member_rows:
            try:
                email_rows = await conn.fetch(
                    "select id, email from auth.users where id = any($1::uuid[])",
                    [r["user_id"] for r in member_rows],
                )
                emails = {r["id"]: r["email"] for r in email_rows}
            except Exception:
                emails = {}
    return AdminAccountMembersResponse(
        members=[
            AdminAccountMember(
                user_id=r["user_id"],
                role=r["role"],
                email=emails.get(r["user_id"]),
                created_at=r["created_at"],
            )
            for r in member_rows
        ]
    )


@router.get("/accounts/{account_id}/api-tokens", response_model=AdminApiTokensResponse)
async def list_account_api_tokens(
    account_id: UUID,
    _admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminApiTokensResponse:
    """Token metadata only — `hash` is never selected."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select id, name, prefix, last_four, scopes, last_used_at,
                   expires_at, revoked_at, created_at
              from api_tokens
             where account_id = $1
             order by created_at desc
            """,
            account_id,
        )
    return AdminApiTokensResponse(
        tokens=[
            AdminApiToken(
                id=r["id"],
                name=r["name"],
                prefix=r["prefix"],
                last_four=r["last_four"],
                scopes=list(r["scopes"]),
                last_used_at=r["last_used_at"],
                expires_at=r["expires_at"],
                revoked_at=r["revoked_at"],
                created_at=r["created_at"],
            )
            for r in rows
        ]
    )


# ---------------------------------------------------------------------------
# Member management
# ---------------------------------------------------------------------------


async def _count_owners(conn: Any, account_id: UUID) -> int:
    return int(
        await conn.fetchval(
            "select count(*)::int from account_members where account_id = $1 and role = 'owner'",
            account_id,
        )
    )


@router.post(
    "/accounts/{account_id}/members",
    response_model=AdminAccountMember,
    status_code=201,
)
async def add_account_member(
    account_id: UUID,
    body: AdminMemberAdd,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
    settings: SettingsDep,
    http: HttpClientDep,
) -> AdminAccountMember:
    """Attach a member by email; creates the auth user (confirmed) via the
    GoTrue Admin API when the email is unknown and a password is supplied."""
    async with pool.acquire() as conn:
        if await conn.fetchval("select 1 from accounts where id = $1", account_id) is None:
            raise NotFoundError("Account not found.", code="account_not_found")
        user_id: UUID | None = await conn.fetchval(
            "select id from auth.users where email = $1",
            body.email,
        )

    if user_id is None:
        if not body.password:
            raise BadRequestError(
                "No auth user with that email; provide password to create one.",
                code="member_user_not_found",
            )
        created = await create_auth_user(http, settings, email=body.email, password=body.password)
        user_id = created.user_id

    async with pool.acquire() as conn, conn.transaction():
        inserted = await conn.fetchrow(
            """
            insert into account_members (account_id, user_id, role)
            values ($1, $2, $3)
            on conflict (account_id, user_id) do nothing
            returning created_at
            """,
            account_id,
            user_id,
            body.role,
        )
        if inserted is None:
            raise ConflictError(
                "User is already a member of this account.",
                code="member_exists",
            )
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.member.add",
            target_type="account",
            target_id=account_id,
            after={"user_id": str(user_id), "email": body.email, "role": body.role},
        )
    return AdminAccountMember(
        user_id=user_id,
        role=body.role,
        email=body.email,
        created_at=inserted["created_at"],
    )


@router.patch(
    "/accounts/{account_id}/members/{user_id}",
    response_model=AdminAccountMember,
)
async def update_account_member(
    account_id: UUID,
    user_id: UUID,
    body: AdminMemberUpdate,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminAccountMember:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            select role, created_at from account_members
             where account_id = $1 and user_id = $2
               for update
            """,
            account_id,
            user_id,
        )
        if row is None:
            raise NotFoundError("Membership not found.", code="member_not_found")
        if (
            row["role"] == "owner"
            and body.role != "owner"
            and await _count_owners(conn, account_id) <= 1
        ):
            raise ConflictError(
                "Cannot demote the only owner of an account.",
                code="last_owner",
            )
        await conn.execute(
            "update account_members set role = $3 where account_id = $1 and user_id = $2",
            account_id,
            user_id,
            body.role,
        )
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.member.role.update",
            target_type="account",
            target_id=account_id,
            before={"user_id": str(user_id), "role": row["role"]},
            after={"user_id": str(user_id), "role": body.role},
        )
        email = None
        try:
            email = await conn.fetchval("select email from auth.users where id = $1", user_id)
        except Exception:  # pragma: no cover — auth schema unreadable
            email = None
    return AdminAccountMember(
        user_id=user_id,
        role=body.role,
        email=email,
        created_at=row["created_at"],
    )


@router.delete("/accounts/{account_id}/members/{user_id}", status_code=204)
async def remove_account_member(
    account_id: UUID,
    user_id: UUID,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> None:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            select role from account_members
             where account_id = $1 and user_id = $2
               for update
            """,
            account_id,
            user_id,
        )
        if row is None:
            raise NotFoundError("Membership not found.", code="member_not_found")
        if row["role"] == "owner" and await _count_owners(conn, account_id) <= 1:
            raise ConflictError(
                "Cannot remove the only owner of an account.",
                code="last_owner",
            )
        await conn.execute(
            "delete from account_members where account_id = $1 and user_id = $2",
            account_id,
            user_id,
        )
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.member.remove",
            target_type="account",
            target_id=account_id,
            before={"user_id": str(user_id), "role": row["role"]},
        )


# ---------------------------------------------------------------------------
# API tokens (issue / revoke)
# ---------------------------------------------------------------------------


@router.post(
    "/accounts/{account_id}/api-tokens",
    response_model=AdminApiTokenCreateResponse,
    status_code=201,
)
async def create_account_api_token(
    account_id: UUID,
    body: AdminApiTokenCreate,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
    settings: SettingsDep,
) -> AdminApiTokenCreateResponse:
    """Issue an agent token for any account. The plaintext is returned exactly
    once and is never stored or written to the audit log."""
    env_label = "live" if settings.is_production else "test"
    issued = await issue_token_async(environment=env_label)

    async with pool.acquire() as conn, conn.transaction():
        if await conn.fetchval("select 1 from accounts where id = $1", account_id) is None:
            raise NotFoundError("Account not found.", code="account_not_found")
        row = await conn.fetchrow(
            """
            insert into api_tokens
                (account_id, name, prefix, hash, last_four, scopes, created_by)
            values ($1, $2, $3, $4, $5, '{agent}'::text[], $6)
            returning id, name, prefix, last_four, scopes,
                      last_used_at, expires_at, revoked_at, created_at
            """,
            account_id,
            body.name,
            issued.prefix,
            issued.hash,
            issued.last_four,
            admin.user_id,
        )
        assert row is not None
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.api_token.create",
            target_type="api_token",
            target_id=row["id"],
            after={
                "account_id": str(account_id),
                "name": body.name,
                "prefix": row["prefix"],
            },
        )
    return AdminApiTokenCreateResponse(
        token=AdminApiToken(
            id=row["id"],
            name=row["name"],
            prefix=row["prefix"],
            last_four=row["last_four"],
            scopes=list(row["scopes"]),
            last_used_at=row["last_used_at"],
            expires_at=row["expires_at"],
            revoked_at=row["revoked_at"],
            created_at=row["created_at"],
        ),
        plaintext=issued.plaintext,
    )


@router.delete("/accounts/{account_id}/api-tokens/{token_id}", status_code=204)
async def revoke_account_api_token(
    account_id: UUID,
    token_id: UUID,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> None:
    """Soft-revoke (idempotent). Audited."""
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            "select revoked_at, prefix from api_tokens where id = $1 and account_id = $2",
            token_id,
            account_id,
        )
        if row is None:
            raise NotFoundError("Token not found.", code="token_not_found")
        if row["revoked_at"] is not None:
            return
        await conn.execute(
            "update api_tokens set revoked_at = now() where id = $1",
            token_id,
        )
        await record_admin_action(
            conn,
            admin_user_id=admin.user_id,
            action="account.api_token.revoke",
            target_type="api_token",
            target_id=token_id,
            before={"account_id": str(account_id), "prefix": row["prefix"]},
        )
