"""P6-A Camera Geometry Config — DB helpers + probe default.

Kept out of the router the same way the processor helpers live in
``services/processor.py``. Tenant columns (``account_id`` / ``camera_id``)
are never taken from the request body; callers pass the ``account_id`` they
resolved from the authenticated caller and the ``camera_id`` they already
ownership-checked.

``revision`` is the DB row revision: 1 on first insert, +1 on every save.
The probe default (no stored row) reports ``revision = 0`` so a reader can
tell "never saved" from "saved once".
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.geometry import (
    GeometryConfig,
    GeometryConfigRecord,
    GeometryFrame,
    GeometryLine,
    GeometryPoint,
    GeometryRoi,
)
from upstream_api.services.processor import coerce_jsonb, parse_iso_datetime

# The probe-default geometry — the normalized form of the processor's
# hard-coded ``compute_analytics_geometry()`` output (near-full-frame ROI
# at a 5% margin + a horizontal mid-frame line). Returned by GET when a
# camera has no saved config; it is NOT written to the DB.
DEFAULT_GEOMETRY_CONFIG = GeometryConfig(
    version=1,
    frame=GeometryFrame(width=1280, height=720, coordinate_space="normalized"),
    rois=[
        GeometryRoi(
            id="entry-roi",
            label="Entry ROI",
            type="polygon",
            points=[
                GeometryPoint(x=0.05, y=0.05),
                GeometryPoint(x=0.95, y=0.05),
                GeometryPoint(x=0.95, y=0.95),
                GeometryPoint(x=0.05, y=0.95),
            ],
            enabled=True,
        )
    ],
    lines=[
        GeometryLine(
            id="entry-line",
            label="Entry Line",
            points=[GeometryPoint(x=0.25, y=0.5), GeometryPoint(x=0.75, y=0.5)],
            direction="any",
            enabled=True,
        )
    ],
)


def default_geometry_config_record(camera_id: UUID) -> GeometryConfigRecord:
    """The GET response for a camera with no saved config: the probe default,
    ``is_default = True``, ``revision = 0``, no DB id / timestamps."""
    return GeometryConfigRecord(
        id=None,
        camera_id=camera_id,
        revision=0,
        config=DEFAULT_GEOMETRY_CONFIG,
        is_default=True,
        created_at=None,
        updated_at=None,
    )


# Double-scoped by account_id AND camera_id — defense in depth on top of
# the router's separate camera-ownership check.
_SELECT_GEOMETRY_CONFIG_SQL = """
select id, camera_id, revision, config, created_at, updated_at
  from camera_geometry_configs
 where account_id = $1
   and camera_id = $2
"""

# Full-replacement upsert. On a re-save the row revision bumps by one;
# `config->>'version'` (the schema version) is untouched and stays 1.
_UPSERT_GEOMETRY_CONFIG_SQL = """
insert into camera_geometry_configs (account_id, camera_id, revision, config)
values ($1, $2, 1, $3::jsonb)
on conflict (camera_id) do update set
    account_id = excluded.account_id,
    config     = excluded.config,
    revision   = camera_geometry_configs.revision + 1
returning id, camera_id, revision, config, created_at, updated_at
"""


def _row_to_record(row: dict[str, Any]) -> GeometryConfigRecord:
    return GeometryConfigRecord(
        id=row["id"],
        camera_id=row["camera_id"],
        revision=row["revision"],
        config=GeometryConfig.model_validate(coerce_jsonb(row["config"])),
        is_default=False,
        created_at=parse_iso_datetime(row["created_at"]),
        updated_at=parse_iso_datetime(row["updated_at"]),
    )


async def fetch_geometry_config(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
) -> GeometryConfigRecord | None:
    """The saved geometry config for one camera, or ``None`` if never saved."""
    row = await conn.fetchrow(_SELECT_GEOMETRY_CONFIG_SQL, account_id, camera_id)
    return _row_to_record(dict(row)) if row is not None else None


async def upsert_geometry_config(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    config: GeometryConfig,
) -> GeometryConfigRecord:
    """Insert or full-replace the geometry config for one camera, bumping
    the row revision. Returns the stored record (``is_default = False``)."""
    row = await conn.fetchrow(
        _UPSERT_GEOMETRY_CONFIG_SQL,
        account_id,
        camera_id,
        json.dumps(config.model_dump()),
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return _row_to_record(dict(row))
