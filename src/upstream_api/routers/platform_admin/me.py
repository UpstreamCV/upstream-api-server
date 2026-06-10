from __future__ import annotations

from fastapi import APIRouter

from upstream_api.schemas.platform_admin import AdminMeResponse
from upstream_api.security.deps import CurrentPlatformAdmin

router = APIRouter()


@router.get("/me", response_model=AdminMeResponse)
async def get_me(admin: CurrentPlatformAdmin) -> AdminMeResponse:
    """The panel's gate check: 200 means 'logged in AND on the allowlist'.

    A signed-in non-admin gets the dep's 403 ``not_platform_admin``, which
    the panel renders as its not-authorized page.
    """
    return AdminMeResponse(user_id=admin.user_id, email=admin.email)
