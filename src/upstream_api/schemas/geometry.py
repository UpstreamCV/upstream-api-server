"""P6-A Camera Geometry Config — request/response models.

Dashboard-authored ROI/line geometry for one camera. This is **processing
geometry only** — the shapes the processor's analytics uses. Presentation
(colors, stroke width, fill, opacity, CSS) is deliberately NOT modelled
here: overlay styling stays frontend-owned. The geometry models use
``extra="forbid"`` so a stray style field is a 422, not a silent store.

Two distinct version-like numbers, never conflated:

- ``GeometryConfig.version`` — the JSON **schema version** (``Literal[1]``).
  Bumps only when the config *shape* changes.
- ``GeometryConfigRecord.revision`` — the DB **row revision**. 0 for the
  unsaved probe default; 1 on first save; +1 on every subsequent PUT.

Coordinates are normalized floats in ``[0, 1]`` so a config is resolution
independent — the processor scales to pixels against the approved frame
size (P6-C). ``frame`` records the reference resolution the geometry was
authored against, for provenance only.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Slug-ish stable identifier. IDs are embedded in processor observation
# idempotency keys, so they must be stable + URL/log safe.
GeometrySlug = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")]

_MAX_SHAPES = 20
_MAX_ROI_POINTS = 64
_MAX_LABEL_LEN = 80


class GeometryPoint(BaseModel):
    """One normalized vertex; origin top-left, ``x``/``y`` in ``[0, 1]``."""

    model_config = ConfigDict(extra="forbid")
    x: float = Field(ge=0.0, le=1.0)
    y: float = Field(ge=0.0, le=1.0)


class GeometryFrame(BaseModel):
    """Reference frame the geometry was authored against (provenance only).

    Coordinates are normalized, so ``width``/``height`` are not needed to
    interpret them — they record what the dashboard drew on.
    """

    model_config = ConfigDict(extra="forbid")
    width: int | None = Field(default=None, ge=1, le=7680)
    height: int | None = Field(default=None, ge=1, le=4320)
    coordinate_space: Literal["normalized"] = "normalized"


class GeometryRoi(BaseModel):
    """One analytics ROI as an ordered polygon (≥ 3 vertices)."""

    model_config = ConfigDict(extra="forbid")
    id: GeometrySlug
    label: str | None = Field(default=None, max_length=_MAX_LABEL_LEN)
    type: Literal["polygon"] = "polygon"
    points: list[GeometryPoint] = Field(min_length=3, max_length=_MAX_ROI_POINTS)
    enabled: bool = True


class GeometryLine(BaseModel):
    """One crossing line — exactly two segment endpoints."""

    model_config = ConfigDict(extra="forbid")
    id: GeometrySlug
    label: str | None = Field(default=None, max_length=_MAX_LABEL_LEN)
    points: list[GeometryPoint] = Field(min_length=2, max_length=2)
    direction: Literal["any", "in", "out"] = "any"
    enabled: bool = True


class GeometryConfig(BaseModel):
    """The full geometry config payload stored in ``camera_geometry_configs.config``."""

    model_config = ConfigDict(extra="forbid")
    version: Literal[1]
    frame: GeometryFrame
    rois: list[GeometryRoi] = Field(default_factory=list, max_length=_MAX_SHAPES)
    lines: list[GeometryLine] = Field(default_factory=list, max_length=_MAX_SHAPES)

    @model_validator(mode="after")
    def _reject_duplicate_ids(self) -> GeometryConfig:
        roi_ids = [r.id for r in self.rois]
        if len(roi_ids) != len(set(roi_ids)):
            raise ValueError("duplicate ROI id")
        line_ids = [line.id for line in self.lines]
        if len(line_ids) != len(set(line_ids)):
            raise ValueError("duplicate line id")
        return self


class GeometryConfigUpdateRequest(BaseModel):
    """``PUT /v1/cameras/{camera_id}/geometry-config`` request body."""

    config: GeometryConfig


class GeometryConfigRecord(BaseModel):
    """One camera's geometry config, shaped for a read response.

    ``id`` / timestamps are ``None`` and ``revision`` is 0 when this is the
    unsaved probe default (``is_default = True``).
    """

    id: UUID | None
    camera_id: UUID
    revision: int
    config: GeometryConfig
    is_default: bool
    created_at: datetime | None
    updated_at: datetime | None


class GeometryConfigResponse(BaseModel):
    geometry_config: GeometryConfigRecord
