"""Model registry — read-only catalog endpoint.

``GET /v1/models`` returns the static model catalog so the dashboard can
offer a per-camera model picker. Authenticated (any caller) but NOT
account-scoped: the catalog is platform-global static data, no DB access.
"""

from __future__ import annotations

from fastapi import APIRouter

from upstream_api.schemas.models import ModelsListResponse
from upstream_api.security.deps import CurrentCaller
from upstream_api.services.models import list_models

router = APIRouter(prefix="/v1/models", tags=["models"])


@router.get("", response_model=ModelsListResponse)
async def list_models_endpoint(caller: CurrentCaller) -> ModelsListResponse:
    """The platform's static detection-model catalog."""
    return ModelsListResponse(models=list_models())
