"""Model registry — request/response models.

A read-only catalog of the detection models the platform knows about,
exposed so the dashboard can offer a per-camera model picker. This is
**facts only**: each entry describes a model's capabilities (task type,
runtime, supported classes/trackers, defaults) so the UI can validate a
camera's processing config against it.

Presentation (colors, badges, CSS) is deliberately NOT modelled here:
overlay/styling stays frontend-owned. The models use ``extra="forbid"``
so a stray field is a 422, not a silent store.

The registry is static, in-process data (see ``services/models.py``) —
there is no DB table. The processor does NOT yet read this registry or
per-camera config; it runs its hard-coded detector today.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ModelRegistryEntry(BaseModel):
    """One detection model the platform knows about."""

    # ``protected_namespaces=()`` so ``model_key`` / ``model_version`` don't
    # collide with Pydantic's reserved ``model_`` namespace.
    model_config = ConfigDict(extra="forbid", protected_namespaces=())
    model_key: str
    display_name: str
    description: str | None
    task_type: str
    runtime: str
    default_confidence_threshold: float | None
    supported_classes: list[str]
    supported_trackers: list[str]
    model_version: str | None
    detector_version: str | None
    resource_profile: str | None
    enabled: bool
    notes: str | None
    limitations: list[str] | None


class ModelsListResponse(BaseModel):
    models: list[ModelRegistryEntry]
