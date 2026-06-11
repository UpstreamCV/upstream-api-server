"""Module 7 M1 — usage summary response models.

One read model for the dashboard billing/usage page: plan, current calendar
period, the account's limits, reliable usage counters, and computed
near/at-limit states. Deliberately absent: Stripe/billing links (M2) and
processor/compute minutes (not tracked anywhere yet — never invent them).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

UsageState = Literal["ok", "near_limit", "at_limit", "over_limit"]

# near_limit kicks in at >= 80% of the limit.
NEAR_LIMIT_RATIO = 0.8


class PlanSummary(BaseModel):
    # Open vocabulary — accounts.plan is a free-form text column ('free'
    # today); label is a display-cased copy until a plans table exists.
    key: str
    label: str
    status: str


class UsagePeriod(BaseModel):
    start: datetime
    end: datetime


class UsageLimits(BaseModel):
    max_cameras: int
    max_concurrent_sessions: int
    monthly_minutes_quota: int
    allowed_stream_types: list[str]


class UsageCounters(BaseModel):
    cameras: int
    active_streams: int
    # COMPLETED stream minutes this period (usage_events 'session_ended'
    # only). Currently-active sessions are not counted until they end.
    stream_minutes: int
    workflow_runs: int
    alerts: int
    rules: int
    workflows: int


class UsageStates(BaseModel):
    cameras: UsageState
    active_streams: UsageState
    stream_minutes: UsageState


class BillingSummary(BaseModel):
    """Module 7 M2. Stripe identifiers are deliberately NOT exposed here —
    the dashboard only needs states and capabilities."""

    provider: Literal["stripe"] = "stripe"
    # False when the deployment has no Stripe keys: the dashboard renders
    # its "coming soon" fallback instead of Upgrade buttons.
    configured: bool
    # Derived: none | active | trialing | past_due | canceled.
    status: str
    # Raw Stripe subscription status (null before first checkout).
    subscription_status: str | None
    current_period_start: datetime | None
    current_period_end: datetime | None
    cancel_at_period_end: bool
    # True when a Stripe customer exists -> Manage Billing can open the Portal.
    management_available: bool


class UsageSummaryResponse(BaseModel):
    plan: PlanSummary
    period: UsagePeriod
    limits: UsageLimits
    usage: UsageCounters
    states: UsageStates
    billing: BillingSummary


def usage_state(used: int, limit: int | None) -> UsageState:
    """Classify usage against a limit.

    ``None`` limit means unlimited -> always ``ok``. A zero/negative limit
    (possible by direct DB edit; the schema allows it) means nothing is
    allowed: any usage is ``over_limit``, zero usage is ``at_limit`` —
    consistent with how max_cameras=0 would behave at enforcement.
    """
    if limit is None:
        return "ok"
    if limit <= 0:
        return "over_limit" if used > 0 else "at_limit"
    if used > limit:
        return "over_limit"
    if used == limit:
        return "at_limit"
    if used >= limit * NEAR_LIMIT_RATIO:
        return "near_limit"
    return "ok"
