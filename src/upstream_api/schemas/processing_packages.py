"""Processing package catalog — request/response models (P5-F).

A read-only, dashboard-friendly catalog of the **processing packages** the
platform knows about: what each package does, what it composes (model /
tracker / analytics), what it can emit, its readiness status, and whether it
is safe/recommended. This powers future dashboard "package cards" and the
P5-G camera package request/apply flow.

This is **catalog only** — it does NOT assign packages to cameras and does NOT
expose a per-camera requested/applied state (that is P5-G). The data is a
curated API-side snapshot of the processor's package registry (the processor
repo deploys independently; there is no runtime cross-repo dependency). The
models use ``extra="forbid"`` so a stray field is a 422, not a silent store.
Raw internal DeepStream knobs are deliberately NOT exposed.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

# Honest readiness ladder (mirrors the processor's component/package statuses).
# ``probe`` = proven in a probe milestone; ``experimental`` = runnable but
# unverified (never "recommended"); ``supported`` = production-supported;
# ``deprecated`` = retired.
PackageStatus = Literal["probe", "experimental", "supported", "deprecated"]


class ProcessingPackage(BaseModel):
    """One processing package the platform knows about."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    package_id: str
    display_name: str
    description: str | None
    category: str
    status: PackageStatus
    # ``production_ready`` may only be true on a ``supported`` package; the
    # dashboard must never present a non-supported package as recommended.
    production_ready: bool
    test_mode_safe: bool
    runtime: str
    model_id: str | None
    tracker_profile_id: str | None
    analytics_profile_id: str | None
    supported_classes: list[str]
    # Observation/event types this package can emit (the package's declared
    # outputs); empty for diagnostic probes that write status only.
    supported_outputs: list[str]
    supported_shapes: list[str]
    supported_triggers: list[str]
    # Curated, user-facing defaults (NOT raw DeepStream config). e.g.
    # ``{"confidence_threshold": 0.4, "target_fps": 30}``.
    default_settings: dict[str, Any]
    # Which processing-config fields an operator may override for this package
    # (the processor's genuine apply seams). Catalog facts only — assignment
    # is P5-G.
    allowed_overrides: list[str]
    compute_tier: str | None
    pricing_tier: str | None
    observation_volume_risk: str | None
    warnings: list[str]
    limitations: list[str]
    best_for: list[str]
    not_recommended_for: list[str]
    # Derived: does the package need operator-drawn geometry (ROI/line) to be
    # useful, and does selecting it require a processor restart to take effect?
    requires_geometry: bool
    requires_restart: bool
    # Detail-friendly extras (populated where meaningful; safe in list too).
    readiness_notes: str | None = None
    related_packages: list[str] = []


class ProcessingPackageCategory(BaseModel):
    """A grouping the dashboard can build package cards around."""

    model_config = ConfigDict(extra="forbid")
    id: str
    display_name: str


class ProcessingPackageCatalogResponse(BaseModel):
    """``GET /v1/processing/packages`` — list + grouping + catalog-level notes."""

    model_config = ConfigDict(extra="forbid")
    packages: list[ProcessingPackage]
    categories: list[ProcessingPackageCategory]
    warnings: list[str]
