"""Module 7 M2 — plan key -> account limits mapping.

Deliberately code, not a plans table: three plans with static limits don't
justify a table yet (the M1/M2 audits both deferred it). When a
subscription becomes active the mapped limits OVERWRITE the account_limits
row, so hand-tuned per-account limits do not survive a plan change — the
plan is the source of truth for paying accounts.

Limit values are PLACEHOLDERS — final pricing/limits are a product
decision; the code only cares about plan keys and price-id wiring. 'free'
mirrors the signup-trigger defaults so a downgrade lands exactly where a
fresh account starts.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from upstream_api.config import Settings


@dataclass(frozen=True, slots=True)
class PlanLimits:
    max_cameras: int
    max_concurrent_sessions: int
    max_fps: int
    max_width: int
    max_height: int
    max_bitrate_kbps: int
    allowed_codecs: tuple[str, ...]
    allowed_stream_types: tuple[str, ...]
    monthly_minutes_quota: int
    snapshot_storage_mb: int


PLAN_LIMITS: dict[str, PlanLimits] = {
    # Mirrors supabase/migrations/20260513140000_signup_trigger.sql.
    "free": PlanLimits(
        max_cameras=5,
        max_concurrent_sessions=5,
        max_fps=60,
        max_width=1920,
        max_height=1080,
        max_bitrate_kbps=8000,
        allowed_codecs=("h264", "h265"),
        allowed_stream_types=("webcam", "rtsp"),
        monthly_minutes_quota=600,
        snapshot_storage_mb=1024,
    ),
    "starter": PlanLimits(
        max_cameras=10,
        max_concurrent_sessions=5,
        max_fps=60,
        max_width=1920,
        max_height=1080,
        max_bitrate_kbps=8000,
        allowed_codecs=("h264", "h265"),
        allowed_stream_types=("webcam", "rtsp"),
        monthly_minutes_quota=3000,
        snapshot_storage_mb=2048,
    ),
    "pro": PlanLimits(
        max_cameras=25,
        max_concurrent_sessions=15,
        max_fps=60,
        max_width=3840,
        max_height=2160,
        max_bitrate_kbps=12000,
        allowed_codecs=("h264", "h265"),
        allowed_stream_types=("webcam", "rtsp"),
        monthly_minutes_quota=15000,
        snapshot_storage_mb=10240,
    ),
    # Comped fleet plan for the CrowdCount 24/7 counter fleet. NOT in
    # PAID_PLAN_KEYS — it is applied by admin/ops, never sold through Stripe
    # Checkout. 250 camera slots for the ~100-counter target with headroom.
    # monthly_minutes_quota is account-wide: 250 cameras x 50,000 min/mo each =
    # 12,500,000, which covers a fully-24/7 fleet (250 x ~43,200 = 10.8M/mo)
    # with room to spare. Snapshot storage is left "unlimited" (int32-max
    # sentinel). session_ttl_seconds and shared_inference_enabled are
    # per-account settings this plan mapping does NOT overwrite (see
    # _APPLY_PLAN_LIMITS_SQL), so the appliance TTL and shared-inference gate
    # survive a plan (re)apply.
    "crowdcount": PlanLimits(
        max_cameras=250,
        max_concurrent_sessions=250,
        max_fps=60,
        max_width=3840,
        max_height=2160,
        max_bitrate_kbps=12000,
        allowed_codecs=("h264", "h265"),
        allowed_stream_types=("webcam", "rtsp"),
        monthly_minutes_quota=12500000,  # 250 cams x 50,000 min/mo
        snapshot_storage_mb=2147483647,  # int32-max = "unlimited"
    ),
}

# Plans purchasable through Checkout ('free' is the downgrade target only;
# 'crowdcount' is a comped fleet plan applied by admin, never sold).
PAID_PLAN_KEYS: tuple[str, ...] = ("starter", "pro")


def price_id_for_plan(settings: Settings, plan_key: str) -> str | None:
    return {
        "starter": settings.stripe_price_starter_monthly,
        "pro": settings.stripe_price_pro_monthly,
    }.get(plan_key)


def plan_for_price_id(settings: Settings, price_id: str) -> str | None:
    for plan_key in PAID_PLAN_KEYS:
        if price_id_for_plan(settings, plan_key) == price_id:
            return plan_key
    return None


_APPLY_PLAN_LIMITS_SQL = """
update account_limits
   set max_cameras = $2,
       max_concurrent_sessions = $3,
       max_fps = $4,
       max_width = $5,
       max_height = $6,
       max_bitrate_kbps = $7,
       allowed_codecs = $8,
       allowed_stream_types = $9,
       monthly_minutes_quota = $10,
       snapshot_storage_mb = $11
 where account_id = $1
"""


async def apply_plan_to_account(
    conn: asyncpg.Connection, *, account_id: UUID, plan_key: str
) -> None:
    """Set accounts.plan and overwrite account_limits from the mapping.

    Leaves accounts.status untouched — that column is account LIFECYCLE
    state (active/suspended/deleted), not billing state.
    """
    limits = PLAN_LIMITS[plan_key]
    await conn.execute("update accounts set plan = $2 where id = $1", account_id, plan_key)
    await conn.execute(
        _APPLY_PLAN_LIMITS_SQL,
        account_id,
        limits.max_cameras,
        limits.max_concurrent_sessions,
        limits.max_fps,
        limits.max_width,
        limits.max_height,
        limits.max_bitrate_kbps,
        list(limits.allowed_codecs),
        list(limits.allowed_stream_types),
        limits.monthly_minutes_quota,
        limits.snapshot_storage_mb,
    )
