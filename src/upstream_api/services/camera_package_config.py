"""P5-G: camera package request/apply state.

The REQUESTED package config is stored in ``cameras.metadata`` (JSONB, no
migration) under ``processing_config_requested``; ``cameras.metadata.package_id``
is stamped alongside so the processor's eligible-sessions payload serves it.
The APPLIED config is DERIVED from the latest ``processor_statuses`` row (what
the processor actually ran). The state machine compares requested vs applied.

This is **request/apply only** — selecting a package does not assign/restart a
running session; it takes effect on the camera's next session start (P5-G
Option A).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.processing_config_camera import (
    AppliedPackageConfig,
    CameraPackageConfigResponse,
    CameraPackageSettings,
    CameraPackageState,
    RequestedPackageConfig,
)
from upstream_api.services.package_catalog import get_package
from upstream_api.services.processor import (
    coerce_jsonb,
    fetch_latest_processor_status,
    parse_iso_datetime,
)


async def _fetch_metadata(conn: asyncpg.Connection, camera_id: UUID) -> dict[str, Any]:
    row = await conn.fetchrow("select metadata from cameras where id = $1", camera_id)
    return coerce_jsonb(row["metadata"]) if row is not None else {}


_SETTING_KEYS = ("confidence_threshold", "target_fps", "selected_classes")

# The ``cameras.metadata`` keys the SERVER owns: the operator's package
# selection and the ``package_id`` the processor's eligible-sessions payload
# resolves from it. A device never supplies these — ``register_camera`` strips
# them from the request body and carries the stored values across a
# re-register.
SERVER_OWNED_METADATA_KEYS = ("package_id", "processing_config_requested")


def requested_envelope(
    *,
    package_id: str,
    settings: CameraPackageSettings,
    revision: int,
    updated_by: str | None,
    now: datetime,
) -> dict[str, Any]:
    """The stored ``processing_config_requested`` shape. Shared by
    ``store_requested_package`` and register-time auto-apply so every writer
    produces the envelope ``_requested_from_metadata`` reads back."""
    return {
        "package_id": package_id,
        "settings": settings.model_dump(),
        "revision": revision,
        "updated_at": now.isoformat(),
        "updated_by": updated_by,
    }


def _requested_from_metadata(meta: dict[str, Any]) -> RequestedPackageConfig | None:
    """Read the stored requested config, tolerant of drift: unknown settings
    keys are dropped and a malformed revision falls back to 1, so a GET never
    500s on a config written by a future/older schema."""
    raw = meta.get("processing_config_requested")
    if not isinstance(raw, dict) or not isinstance(raw.get("package_id"), str):
        return None
    settings_raw = raw.get("settings")
    clean = (
        {k: v for k, v in settings_raw.items() if k in _SETTING_KEYS}
        if isinstance(settings_raw, dict)
        else {}
    )
    try:
        settings = CameraPackageSettings.model_validate(clean)
    except ValueError:
        settings = CameraPackageSettings()
    try:
        revision = int(raw.get("revision", 1))
    except (TypeError, ValueError):
        revision = 1
    updated_at = raw.get("updated_at")
    return RequestedPackageConfig(
        package_id=raw["package_id"],
        settings=settings,
        revision=revision,
        updated_at=parse_iso_datetime(updated_at) if isinstance(updated_at, str) else None,
        updated_by=raw.get("updated_by") if isinstance(raw.get("updated_by"), str) else None,
    )


async def store_requested_package(
    conn: asyncpg.Connection,
    *,
    camera_id: UUID,
    package_id: str,
    settings: CameraPackageSettings,
    updated_by: str | None,
    now: datetime,
) -> RequestedPackageConfig:
    """Persist the requested package into ``cameras.metadata`` (bumping the
    revision) and stamp ``metadata.package_id`` so eligible-sessions serves it.
    Migration-free: a single ``jsonb_set`` of the two keys."""
    meta = await _fetch_metadata(conn, camera_id)
    prior = _requested_from_metadata(meta)
    revision = (prior.revision + 1) if prior is not None else 1
    requested = requested_envelope(
        package_id=package_id,
        settings=settings,
        revision=revision,
        updated_by=updated_by,
        now=now,
    )
    await conn.execute(
        """
        update cameras
           set metadata = jsonb_set(
                   jsonb_set(
                       coalesce(metadata, '{}'::jsonb),
                       '{package_id}', to_jsonb($2::text)
                   ),
                   '{processing_config_requested}', $3::jsonb
               ),
               updated_at = now()
         where id = $1
        """,
        camera_id,
        package_id,
        json.dumps(requested),
    )
    return RequestedPackageConfig.model_validate(requested)


def _derive_applied(latest: dict[str, Any] | None) -> AppliedPackageConfig | None:
    if latest is None:
        return None
    payload = coerce_jsonb(latest.get("payload"))
    return AppliedPackageConfig(
        package_id=latest.get("package_id"),
        model_id=payload.get("model_id") if isinstance(payload, dict) else None,
        tracker_profile_id=payload.get("tracker_profile_id") if isinstance(payload, dict) else None,
        analytics_profile_id=(
            payload.get("analytics_profile_id") if isinstance(payload, dict) else None
        ),
        status=latest.get("status"),
        applied_at=latest.get("updated_at"),
    )


def _derive_state(
    requested: RequestedPackageConfig | None,
    latest: dict[str, Any] | None,
) -> tuple[CameraPackageState, list[str]]:
    warnings: list[str] = []
    if requested is None:
        return "unset", warnings
    pkg = get_package(requested.package_id)
    experimental = pkg is not None and pkg.status == "experimental"
    if experimental:
        warnings.append(
            f"Package {requested.package_id!r} is experimental and not recommended for production."
        )
    if pkg is not None and not pkg.production_ready:
        warnings.append(f"Package {requested.package_id!r} is not production-ready (test mode).")
    if latest is None:
        return "pending_apply", warnings
    if latest.get("package_id") != requested.package_id:
        return "pending_restart", warnings
    # Any failed run of the REQUESTED package is a failure — not just
    # package_resolution. The processor stamps other codes (e.g.
    # geometry_invalid) or none at all; reporting "active" there would show a
    # down camera as healthy.
    if latest.get("status") == "failed":
        if latest.get("error_code"):
            warnings.append(f"Latest processor run failed: {latest['error_code']}.")
        return "failed_config", warnings
    if experimental:
        return "experimental_selected", warnings
    return "active", warnings


async def build_package_config_response(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
) -> CameraPackageConfigResponse:
    """The full requested-vs-applied package view for one camera."""
    meta = await _fetch_metadata(conn, camera_id)
    requested = _requested_from_metadata(meta)
    latest = await fetch_latest_processor_status(
        conn, account_id=account_id, camera_id=camera_id, stream_session_id=None
    )
    applied = _derive_applied(latest)
    state, warnings = _derive_state(requested, latest)
    return CameraPackageConfigResponse(
        camera_id=camera_id,
        requested=requested,
        applied=applied,
        state=state,
        warnings=warnings,
    )
