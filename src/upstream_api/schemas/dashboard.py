from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

FleetTrendRange = Literal["24h", "7d"]


class DashboardSummaryResponse(BaseModel):
    total_cameras: int
    online_cameras: int
    offline_cameras: int
    streaming_cameras: int
    cameras_with_errors: int
    active_streams: int
    validation_errors_24h: int


class FleetTrendPoint(BaseModel):
    """One hourly bucket of fleet health.

    Each camera is counted at most once per bucket, by the status of its LAST
    heartbeat in that hour. The three buckets partition ``reporting_cameras``:
    critical = ``error``, review = ``reconnecting``, healthy = ``online`` or
    ``streaming`` (the heartbeats CHECK constraint admits exactly these four).

    A camera that sent no heartbeat in the hour is in none of them, so an hour
    with no traffic is all-zero rather than absent -- the dashboard draws a
    continuous line and a genuinely quiet fleet reads as quiet, not as a gap.
    """

    bucket_start: datetime
    bucket_end: datetime
    critical: int
    review: int
    healthy: int
    reporting_cameras: int


class FleetTrendSummary(BaseModel):
    """Headline counts for the legend. These read the LATEST bucket, so the
    legend and the plotted lines can never disagree -- deriving them from a
    separate current-state query is what let the old legend drift out of sync
    with the chart above it."""

    point_count: int
    latest_bucket_start: datetime | None
    latest_critical: int
    latest_review: int
    latest_healthy: int
    total_cameras: int


class FleetTrendResponse(BaseModel):
    """Account-scoped fleet health over time.

    Scope note: this reports cameras that HEARTBEAT. ``total_cameras`` is the
    account's full camera count, so a caller can tell "3 healthy of 40" from
    "3 healthy of 3" -- an idle or disconnected camera emits nothing and is
    therefore absent from the three series by construction.
    """

    range: FleetTrendRange
    bucket_size: Literal["1h"]
    points: list[FleetTrendPoint]
    summary: FleetTrendSummary
