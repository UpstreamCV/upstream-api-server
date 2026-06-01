"""Per-camera Processing Config — DB helpers + default.

Kept out of the router the same way the geometry helpers live in
``services/geometry.py``. Tenant columns (``account_id`` / ``camera_id``)
are never taken from the request body; callers pass the ``account_id`` they
resolved from the authenticated caller and the ``camera_id`` they already
ownership-checked.

``revision`` is the DB row revision: 1 on first insert, +1 on every save.
The unsaved default (no stored row) reports ``revision = 0`` so a reader
can tell "never saved" from "saved once".
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.processing_config_camera import (
    CameraProcessingConfig,
    CameraProcessingConfigRecord,
)
from upstream_api.services.processor import coerce_jsonb, parse_iso_datetime

# The unsaved default — returned by GET when a camera has no saved config.
# It is NOT written to the DB. ``model_key`` is the platform default
# detector; an empty ``selected_classes`` means "all supported classes".
DEFAULT_CAMERA_PROCESSING_CONFIG = CameraProcessingConfig(
    version=1,
    model_key="trafficcamnet_v1",
    selected_classes=[],
    tracker=None,
    confidence_threshold=None,
    processing_fps=None,
)


def default_camera_processing_config_record(
    *,
    account_id: UUID,
    camera_id: UUID,
) -> CameraProcessingConfigRecord:
    """The GET response for a camera with no saved config: the default,
    ``is_default = True``, ``revision = 0``, no DB id / timestamps."""
    return CameraProcessingConfigRecord(
        id=None,
        account_id=account_id,
        camera_id=camera_id,
        revision=0,
        config=DEFAULT_CAMERA_PROCESSING_CONFIG,
        is_default=True,
        created_at=None,
        updated_at=None,
    )


# Double-scoped by account_id AND camera_id — defense in depth on top of
# the router's separate camera-ownership check.
_SELECT_CAMERA_PROCESSING_CONFIG_SQL = """
select id, account_id, camera_id, revision, config, created_at, updated_at
  from camera_processing_configs
 where account_id = $1
   and camera_id = $2
"""

# Full-replacement upsert. On a re-save the row revision bumps by one;
# `config->>'version'` (the schema version) is untouched and stays 1.
_UPSERT_CAMERA_PROCESSING_CONFIG_SQL = """
insert into camera_processing_configs (account_id, camera_id, revision, config)
values ($1, $2, 1, $3::jsonb)
on conflict (camera_id) do update set
    account_id = excluded.account_id,
    config     = excluded.config,
    revision   = camera_processing_configs.revision + 1
returning id, account_id, camera_id, revision, config, created_at, updated_at
"""


def _row_to_record(row: dict[str, Any]) -> CameraProcessingConfigRecord:
    return CameraProcessingConfigRecord(
        id=row["id"],
        account_id=row["account_id"],
        camera_id=row["camera_id"],
        revision=row["revision"],
        config=CameraProcessingConfig.model_validate(coerce_jsonb(row["config"])),
        is_default=False,
        created_at=parse_iso_datetime(row["created_at"]),
        updated_at=parse_iso_datetime(row["updated_at"]),
    )


async def fetch_camera_processing_config(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
) -> CameraProcessingConfigRecord | None:
    """The saved processing config for one camera, or ``None`` if never saved."""
    row = await conn.fetchrow(_SELECT_CAMERA_PROCESSING_CONFIG_SQL, account_id, camera_id)
    return _row_to_record(dict(row)) if row is not None else None


async def upsert_camera_processing_config(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    config: CameraProcessingConfig,
) -> CameraProcessingConfigRecord:
    """Insert or full-replace the processing config for one camera, bumping
    the row revision. Returns the stored record (``is_default = False``)."""
    row = await conn.fetchrow(
        _UPSERT_CAMERA_PROCESSING_CONFIG_SQL,
        account_id,
        camera_id,
        json.dumps(config.model_dump()),
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return _row_to_record(dict(row))
