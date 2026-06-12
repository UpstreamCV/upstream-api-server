"""``GET /metrics`` — Prometheus scrape endpoint (M9.3, design doc 01 §3).

Gating contract:

- ``UPSTREAM_PROMETHEUS_AUTH_TOKEN`` unset (or empty — config.py coerces
  ``""`` to None) → 404: the endpoint is not configured for this deployment.
- set → require exactly ``Authorization: Bearer <token>``, compared in
  constant time (same primitive as the webhook-secret checks in
  ``security/shared_secret.py``).
- wrong/missing token or wrong scheme → 401.

The asyncpg pool gauges are refreshed inline on each scrape — cheap, and it
keeps them truthful without a background sampler. In production the route
is additionally firewalled to the monitoring box's IP (doc 01 §8).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Header, Request
from fastapi.responses import Response
from fastapi.security.utils import get_authorization_scheme_param
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from upstream_api.errors import NotFoundError, UnauthorizedError
from upstream_api.observability.metrics import db_pool_in_use, db_pool_size, registry
from upstream_api.security.deps import SettingsDep
from upstream_api.security.shared_secret import verify_shared_secret

router = APIRouter(tags=["observability"])


@router.get("/metrics")
async def metrics(
    request: Request,
    settings: SettingsDep,
    authorization: Annotated[str | None, Header()] = None,
) -> Response:
    token = settings.prometheus_auth_token
    if token is None:
        # Unconfigured deployments report not-found; unauthenticated callers
        # cannot retrieve metrics either way.
        raise NotFoundError("Not found.", code="not_found")

    scheme, presented = get_authorization_scheme_param(authorization or "")
    if scheme.lower() != "bearer" or not presented:
        raise UnauthorizedError("Expected 'Bearer <token>'.", code="invalid_auth_scheme")
    if not verify_shared_secret(presented, token.get_secret_value()):
        raise UnauthorizedError("Invalid token.", code="invalid_token")

    # Refresh pool-saturation gauges from the live pool when reachable
    # (absent under TestClient without lifespan — gauges just keep 0).
    pool = getattr(request.app.state, "db_pool", None)
    if pool is not None:
        size = pool.get_size()
        db_pool_size.set(size)
        db_pool_in_use.set(size - pool.get_idle_size())

    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


__all__ = ["router"]
