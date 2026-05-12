"""Constant-time header comparison for service-to-service webhook auth."""

from __future__ import annotations

import secrets


def verify_shared_secret(presented: str | None, expected: str) -> bool:
    if presented is None:
        return False
    return secrets.compare_digest(presented.encode("utf-8"), expected.encode("utf-8"))
