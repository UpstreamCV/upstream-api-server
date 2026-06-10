"""Rules/Triggers M1 — alerts, the user-facing records matched rules produce.

Distinct from reliability findings (system-health verdicts): alerts come from
user-authored rules and have a read/resolve lifecycle driven by the dashboard
inbox.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

AlertStatus = Literal["open", "resolved"]


class AlertSummary(BaseModel):
    id: UUID
    camera_id: UUID
    rule_id: UUID | None
    observation_id: UUID | None
    # M2: the workflow run whose action step created this alert. Nullable —
    # alerts written before the workflow_runs migration have no run.
    workflow_run_id: UUID | None
    severity: str
    status: str
    title: str
    message: str
    payload: dict[str, Any]
    created_at: datetime
    read_at: datetime | None
    resolved_at: datetime | None


class AlertListResponse(BaseModel):
    alerts: list[AlertSummary]
