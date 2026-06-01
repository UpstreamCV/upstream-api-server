"""Per-camera Processing Config — request/response models.

Dashboard-authored model/processing selection for one camera. This is
**facts / config only** — which model to run, which classes to keep, the
tracker, confidence threshold and processing FPS. Presentation (colors,
stroke, fill, opacity, CSS, overlay styling) is deliberately NOT modelled
here: that stays frontend-owned. The models use ``extra="forbid"`` so a
stray style field is a 422, not a silent store.

Two distinct version-like numbers, never conflated:

- ``CameraProcessingConfig.version`` — the JSON **schema version**
  (``Literal[1]``). Bumps only when the config *shape* changes.
- ``CameraProcessingConfigRecord.revision`` — the DB **row revision**. 0
  for the unsaved default; 1 on first save; +1 on every subsequent PUT.

The base ``CameraProcessingConfig`` does **structural validation only** —
no registry checks — so a stored row always re-validates on read even if
the model registry later changes. Registry validation (model exists +
enabled, classes/tracker supported) lives in the *request* wrapper
``CameraProcessingConfigUpdateRequest`` so it gates writes but not reads.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from upstream_api.services.models import get_model


class CameraProcessingConfig(BaseModel):
    """The processing config payload stored in ``camera_processing_configs.config``.

    STRUCTURAL VALIDATION ONLY — bounds/types, no model-registry checks, so
    a stored row always re-validates on read.
    """

    # ``protected_namespaces=()`` so ``model_key`` doesn't collide with
    # Pydantic's reserved ``model_`` namespace.
    model_config = ConfigDict(extra="forbid", protected_namespaces=())
    version: Literal[1]
    model_key: str = Field(min_length=1, max_length=128)
    # [] means all supported classes.
    selected_classes: list[str] = Field(default_factory=list, max_length=64)
    tracker: str | None = Field(default=None, max_length=64)
    confidence_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    processing_fps: int | None = Field(default=None, ge=1, le=30)


class CameraProcessingConfigUpdateRequest(BaseModel):
    """``PUT /v1/cameras/{camera_id}/processing-config`` request body.

    Beyond the structural validation in ``CameraProcessingConfig``, this
    wrapper validates the config against the static model registry so a
    write referencing an unknown/disabled model — or unsupported classes /
    tracker — is rejected with a 422.
    """

    config: CameraProcessingConfig

    @model_validator(mode="after")
    def _validate_against_registry(self) -> CameraProcessingConfigUpdateRequest:
        model = get_model(self.config.model_key)
        if model is None:
            raise ValueError(f"unknown model_key: {self.config.model_key}")
        if not model.enabled:
            raise ValueError(f"model_key is disabled: {self.config.model_key}")
        # Empty selected_classes is allowed and means "all supported classes".
        supported = set(model.supported_classes)
        for cls in self.config.selected_classes:
            if cls not in supported:
                raise ValueError(f"class not supported by model: {cls}")
        if self.config.tracker is not None and self.config.tracker not in model.supported_trackers:
            raise ValueError(f"tracker not supported by model: {self.config.tracker}")
        return self


class CameraProcessingConfigRecord(BaseModel):
    """One camera's processing config, shaped for a read response.

    ``id`` / timestamps are ``None`` and ``revision`` is 0 when this is the
    unsaved default (``is_default = True``).
    """

    id: UUID | None
    account_id: UUID
    camera_id: UUID
    revision: int
    config: CameraProcessingConfig
    is_default: bool
    created_at: datetime | None
    updated_at: datetime | None


class CameraProcessingConfigResponse(BaseModel):
    processing_config: CameraProcessingConfigRecord
