"""Cross-tenant /v1/admin/* surface for the platform admin panel.

Every handler authenticates with ``CurrentPlatformAdmin`` (Supabase JWT +
``platform_admins`` allowlist). Mutations are recorded in ``admin_audit_log``
via ``services.admin_audit``.
"""

from __future__ import annotations

from fastapi import APIRouter

from upstream_api.routers.platform_admin import (
    accounts,
    audit,
    me,
    streams,
    system,
    usage,
)

router = APIRouter(prefix="/v1/admin", tags=["platform-admin"])
router.include_router(me.router)
router.include_router(system.router)
router.include_router(streams.router)
router.include_router(accounts.router)
router.include_router(usage.router)
router.include_router(audit.router)
