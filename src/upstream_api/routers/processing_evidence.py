"""Processing runtime evidence — read-side router (P5-J).

One endpoint under ``/v1/cameras/{camera_id}/processing/evidence``. A pure,
read-only VISIBILITY surface: it summarizes recent observation counts + lifecycle
signals + package/lineage/status over a 1h/24h window. NOT a recommendation
engine — no verdicts, no auto-apply.

Auth + scoping follow the same pattern as ``reliability.get_reliability_summary``
(``CurrentCaller.resolve_account()`` + a separate camera-ownership check → 404).
It lives in its own router (mounted in ``app.py``) because the dashboard reads it
as a feature-namespaced surface, not a detail of cameras CRUD. Unlike the
reliability reads it is a pure read — no transaction (it never writes).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.processing_evidence import ProcessingEvidenceResponse
from upstream_api.security.deps import CurrentCaller, PoolDep
from upstream_api.services.processing_evidence import build_processing_evidence

router = APIRouter(tags=["processing"])


@router.get(
    "/v1/cameras/{camera_id}/processing/evidence",
    response_model=ProcessingEvidenceResponse,
)
async def get_processing_evidence(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
    range_: Annotated[Literal["1h", "24h"], Query(alias="range")] = "24h",
) -> ProcessingEvidenceResponse:
    """Read-only processing runtime evidence for one camera over a recent window.

    ``range`` is ``1h`` or ``24h`` (default ``24h``); a bogus value returns 422
    (Pydantic ``Literal``). Returns 404 ``camera_not_found`` when the caller
    doesn't own the camera. Observation counts are windowed; tracking metrics are
    session-level (flagged). This is visibility/evidence, not recommendations.
    """
    scope_account = caller.resolve_account(account_id)
    window_hours = 1 if range_ == "1h" else 24
    async with pool.acquire() as conn:
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        return await build_processing_evidence(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            window_hours=window_hours,
            now=datetime.now(UTC),
        )
