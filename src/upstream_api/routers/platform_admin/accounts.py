from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import BadRequestError, NotFoundError
from upstream_api.schemas.platform_admin import (
    AdminAccountDetailResponse,
    AdminAccountItem,
    AdminAccountLimits,
    AdminAccountLimitsUpdate,
    AdminAccountListResponse,
    AdminAccountMember,
    AdminAccountMembersResponse,
    AdminAccountUpdate,
    AdminApiToken,
    AdminApiTokensResponse,
    AdminUsageTotal,
)
from upstream_api.security.deps import CurrentPlatformAdmin, PoolDep
from upstream_api.services.admin_audit import record_admin_action

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
                   monthly_minutes_quota, snapshot_storage_mb, updated_at
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


@router.patch("/accounts/{account_id}", response_model=AdminAccountItem)
async def patch_account(
    account_id: UUID,
    body: AdminAccountUpdate,
    admin: CurrentPlatformAdmin,
    pool: PoolDep,
) -> AdminAccountItem:
    """Update plan and/or status. Audited. `deleted` is not settable here."""
    changes = body.model_dump(exclude_none=True)
    if not changes:
        raise BadRequestError(
            "Provide at least one of: plan, status.",
            code="empty_update",
        )

    async with pool.acquire() as conn, conn.transaction():
        before_row = await conn.fetchrow(
            "select plan, status from accounts where id = $1 for update",
            account_id,
        )
        if before_row is None:
            raise NotFoundError("Account not found.", code="account_not_found")

        await conn.execute(
            """
            update accounts
               set plan = coalesce($2, plan),
                   status = coalesce($3, status)
             where id = $1
            """,
            account_id,
            changes.get("plan"),
            changes.get("status"),
        )

        action = "account.plan.update" if "plan" in changes else "account.status.update"
        if "plan" in changes and "status" in changes:
            action = "account.update"
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
