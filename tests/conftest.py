from __future__ import annotations

import os

import pytest


def _set_dummy_env() -> None:
    defaults = {
        "UPSTREAM_DATABASE_URL": "postgres://postgres:postgres@127.0.0.1:54322/postgres",
        "UPSTREAM_SUPABASE_URL": "http://127.0.0.1:54321",
        "UPSTREAM_SUPABASE_ANON_KEY": "test-anon",
        "UPSTREAM_SUPABASE_SERVICE_ROLE_KEY": "test-service",
        "UPSTREAM_SUPABASE_JWT_SECRET": "test-supabase-jwt-secret",
        "UPSTREAM_SESSION_JWT_SECRET": "test-session-jwt-secret-32bytes-min!!",
        "UPSTREAM_STREAMID_HMAC_KEY": "test-hmac-32-bytes-minimum-please-!!",
        "UPSTREAM_MEDIAMTX_WEBHOOK_SECRET": "test-mediamtx",
        "UPSTREAM_PROCESSOR_WEBHOOK_SECRET": "test-processor",
        "UPSTREAM_PROMETHEUS_AUTH_TOKEN": "test-prom",
        "UPSTREAM_ENVIRONMENT": "development",
    }
    for k, v in defaults.items():
        os.environ.setdefault(k, v)


_set_dummy_env()


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    from upstream_api.config import get_settings

    get_settings.cache_clear()
