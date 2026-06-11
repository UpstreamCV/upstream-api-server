"""Module 7 M1 — usage summary read endpoint.

Dashboard-only (Supabase user auth). Any account member can read — usage
visibility isn't privileged. Same account-resolution posture as the
workflows/api-tokens routers.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ForbiddenError
from upstream_api.schemas.usage import UsageSummaryResponse
from upstream_api.security.deps import CurrentUser, PoolDep
from upstream_api.services.usage_summary import build_usage_summary

router = APIRouter(prefix="/v1/usage", tags=["usage"])


def _resolve_account_for_user(user: CurrentUser, account_id: UUID | None) -> UUID:
    """Pick the account scope for a user-only endpoint (mirrors
    `CallerAuth.resolve_account`; same helper shape as routers/workflows.py)."""
    if account_id is not None:
        if account_id not in user.memberships:
            raise ForbiddenError(
                "User is not a member of this account.",
                code="forbidden_account",
            )
        return account_id
    if len(user.memberships) == 1:
        return next(iter(user.memberships))
    raise ForbiddenError(
        "account_id query parameter is required for users with multiple accounts.",
        code="account_id_required",
    )


@router.get("/summary", response_model=UsageSummaryResponse)
async def get_usage_summary(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> UsageSummaryResponse:
    scope_account = _resolve_account_for_user(user, account_id)
    async with pool.acquire() as conn:
        return await build_usage_summary(conn, account_id=scope_account)
