"""Processing package catalog — read-only endpoints (P5-F).

``GET /v1/processing/packages`` and ``GET /v1/processing/packages/{package_id}``
return the platform's processing-package catalog so the dashboard can build
package cards. Authenticated (any caller) but NOT account-scoped — the catalog
is platform-global static data with a conservative visibility gate (no DB
access). This is **catalog only**: it does not assign packages to cameras and
does not expose requested/applied state (P5-G).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from upstream_api.errors import NotFoundError
from upstream_api.schemas.processing_packages import (
    ProcessingPackage,
    ProcessingPackageCatalogResponse,
)
from upstream_api.security.deps import CurrentCaller, SettingsDep
from upstream_api.services.package_catalog import (
    catalog_warnings,
    categories,
    get_package,
    list_packages,
)

router = APIRouter(prefix="/v1/processing", tags=["processing"])


@router.get("/packages", response_model=ProcessingPackageCatalogResponse)
async def list_packages_endpoint(
    caller: CurrentCaller,
    settings: SettingsDep,
    category: Annotated[str | None, Query(max_length=64)] = None,
    status: Annotated[str | None, Query(max_length=32)] = None,
    runtime: Annotated[str | None, Query(max_length=32)] = None,
    test_mode_safe: Annotated[bool | None, Query()] = None,
) -> ProcessingPackageCatalogResponse:
    """The processing-package catalog, optionally filtered, with grouping.

    P5-K: when ``catalog_first_party_only`` is on (go-live), non-first-party
    (AGPL/restrictive) packages are hidden from the catalog."""
    packages = list_packages(
        category=category,
        status=status,
        runtime=runtime,
        test_mode_safe=test_mode_safe,
        first_party_only=settings.catalog_first_party_only,
    )
    return ProcessingPackageCatalogResponse(
        packages=packages,
        categories=categories(packages),
        warnings=catalog_warnings(),
    )


@router.get("/packages/{package_id}", response_model=ProcessingPackage)
async def get_package_endpoint(
    package_id: str, caller: CurrentCaller, settings: SettingsDep
) -> ProcessingPackage:
    """One processing package by id (full detail)."""
    pkg = get_package(package_id, first_party_only=settings.catalog_first_party_only)
    if pkg is None:
        raise NotFoundError("Package not found.", code="package_not_found")
    return pkg
