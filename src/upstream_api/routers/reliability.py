"""Reliability findings — read-side router.

Two endpoints, both under ``/v1/cameras/{camera_id}/reliability/``. Both are
"evaluate on read": the request runs the rule set, persists upserts/resolves,
and returns the resulting state in a single transaction. No background worker,
no cron, no queue — M0 chooses the least invasive surface.

Auth + scoping follow the same pattern as ``cameras.get_latest_processor_status``
(``routers/cameras.py``): ``CurrentCaller.resolve_account()`` derives the
tenant, then a separate camera-ownership check on the ``cameras`` row produces
a 404 if the caller doesn't own the camera. The evaluator never sees the
request body — tenant columns are passed in explicitly.

The router lives in its own file rather than under ``cameras.py`` because the
dashboard reads it as a feature-namespaced surface ("reliability"), not a
detail of the cameras CRUD; it's mounted as a separate router in ``app.py`` the
same way ``snapshots.camera_router`` is.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.reliability import (
    ConfidenceTrendByConfigResponse,
    ConfidenceTrendResponse,
    ProcessingConfigStatus,
    ReliabilityFindingsResponse,
    ReliabilitySummaryResponse,
)
from upstream_api.security.deps import CurrentCaller, PoolDep, SettingsDep
from upstream_api.services.reliability_service import (
    get_findings_for_camera,
    get_summary_for_camera,
)
from upstream_api.services.reliability_trends import (
    get_confidence_trend_by_config_for_camera,
    get_confidence_trend_for_camera,
)

router = APIRouter(tags=["reliability"])

# Findings/ accepts a limit. Capped so a noisy camera can't blow up the
# response; the summary endpoint uses its own internal cap.
_FINDINGS_DEFAULT_LIMIT = 50
_FINDINGS_MAX_LIMIT = 200


@router.get(
    "/v1/cameras/{camera_id}/reliability/summary",
    response_model=ReliabilitySummaryResponse,
)
async def get_reliability_summary(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ReliabilitySummaryResponse:
    """Categorical reliability verdict for one camera, evaluated on read.

    Returns 404 ``camera_not_found`` if the caller doesn't own ``camera_id``.
    The verdict is one of {healthy, info, needs_review, critical, unknown};
    see schemas/reliability.py for the contract.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        return await get_summary_for_camera(
            conn,
            settings=settings,
            account_id=scope_account,
            camera_id=camera_id,
        )


@router.get(
    "/v1/cameras/{camera_id}/reliability/findings",
    response_model=ReliabilityFindingsResponse,
)
async def get_reliability_findings(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=_FINDINGS_MAX_LIMIT)] = _FINDINGS_DEFAULT_LIMIT,
    status: Annotated[Literal["open", "resolved", "suppressed", "ignored"], Query()] = "open",
) -> ReliabilityFindingsResponse:
    """Reliability findings for one camera filtered by ``status``.

    Default ``status=open`` preserves the M0 contract: severity-first ordering,
    same shape the Monitor card consumes. ``status=resolved`` drives the new
    M4 Reliability tab's "Recently resolved" section -- newest-resolved first.

    Re-runs the evaluator on read so callers that hit findings/ without first
    hitting summary/ still see fresh state. Returns 404 ``camera_not_found``
    when ownership doesn't match. Pydantic ``Literal`` enforces the status
    enum; a bogus value returns 422 automatically.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        return await get_findings_for_camera(
            conn,
            settings=settings,
            status=status,
            account_id=scope_account,
            camera_id=camera_id,
            limit=limit,
        )


@router.get(
    "/v1/cameras/{camera_id}/reliability/confidence-trend",
    response_model=ConfidenceTrendResponse,
)
async def get_reliability_confidence_trend(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
    range_: Annotated[Literal["24h", "7d"], Query(alias="range")] = "24h",
) -> ConfidenceTrendResponse:
    """Hourly detection-confidence history for one camera (reliability M7).

    Reads the M6 ``detection_confidence_hourly_rollups`` table, aggregating
    across packages to one camera-level point per hour, for the requested
    ``range`` (``24h`` default, ``7d``). A bogus ``range`` returns 422 (Pydantic
    ``Literal``). Returns 404 ``camera_not_found`` when ownership doesn't match.

    Pure read — it does NOT run the evaluator and does NOT touch raw metrics or
    M5 drift; these rollups are long-term chart history only.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        return await get_confidence_trend_for_camera(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            range_=range_,
        )


@router.get(
    "/v1/cameras/{camera_id}/reliability/confidence-trend/by-config",
    response_model=ConfidenceTrendByConfigResponse,
)
async def get_reliability_confidence_trend_by_config(
    camera_id: UUID,
    caller: CurrentCaller,
    pool: PoolDep,
    settings: SettingsDep,
    account_id: Annotated[UUID | None, Query()] = None,
    range_: Annotated[Literal["24h", "7d"], Query(alias="range")] = "24h",
    processing_config_revision: Annotated[int | None, Query(ge=0)] = None,
    processing_config_status: Annotated[ProcessingConfigStatus | None, Query()] = None,
    applied_selected_classes_key: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
    applied_confidence_threshold: Annotated[float | None, Query(ge=0.0, le=1.0)] = None,
    warning: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    applied_processing_fps: Annotated[int | None, Query(ge=1, le=30)] = None,
) -> ConfidenceTrendByConfigResponse:
    """Hourly confidence history split per applied processing config (M9).

    Same data/window as ``confidence-trend``, but instead of blending every
    rollup row of an hour into one camera-level point, the M9 dimension columns
    stay grouped: one series per distinct applied-processing-config group.
    Optional query params filter to matching groups only:

    - ``processing_config_revision`` — the saved config's revision counter.
    - ``processing_config_status`` — e.g. ``applied`` / ``partially_applied``.
    - ``applied_selected_classes_key`` — canonical class key (``all``,
      ``person``, ``car,person``...).
    - ``applied_confidence_threshold`` — the threshold actually applied.
    - ``warning`` — only groups whose warnings include this code, e.g.
      ``selected_classes_analytics_counts_not_filtered``.
    - ``applied_processing_fps`` — the output-cadence FPS actually applied
      (M7E; cadence only, not inference FPS).

    History recorded before the processor stamped applied_processing_config
    (pre-M8) appears as the all-``null``-dimension group. This is a pure read
    of the M6/M9 rollups — no evaluator, no raw metrics, and the existing
    ``confidence-trend`` endpoint's blended response is untouched.
    """
    scope_account = caller.resolve_account(account_id)
    async with pool.acquire() as conn, conn.transaction():
        owner_row = await conn.fetchrow(
            "select account_id from cameras where id = $1",
            camera_id,
        )
        if owner_row is None or owner_row["account_id"] != scope_account:
            raise NotFoundError("Camera not found.", code="camera_not_found")

        return await get_confidence_trend_by_config_for_camera(
            conn,
            account_id=scope_account,
            camera_id=camera_id,
            range_=range_,
            processing_config_revision=processing_config_revision,
            processing_config_status=processing_config_status,
            applied_selected_classes_key=applied_selected_classes_key,
            applied_confidence_threshold=applied_confidence_threshold,
            warning=warning,
            applied_processing_fps=applied_processing_fps,
        )
