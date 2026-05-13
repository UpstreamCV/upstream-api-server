from __future__ import annotations

from pydantic import BaseModel


class DashboardSummaryResponse(BaseModel):
    total_cameras: int
    online_cameras: int
    offline_cameras: int
    streaming_cameras: int
    cameras_with_errors: int
    active_streams: int
    validation_errors_24h: int
