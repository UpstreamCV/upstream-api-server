"""Confirm the AccessLogMiddleware redacts secret-bearing URL paths.

The M4.1 MediaMTX authHTTP gate puts the shared webhook secret in the URL
path because MediaMTX 1.9.3 has no ``authHTTPHeaders`` support. The
middleware logs ``path=request.url.path`` — if that path isn't redacted,
every authHTTP call leaks the secret into the access log.
"""

from __future__ import annotations

from upstream_api.observability.middleware import _redact_path


def test_redact_path_masks_mediamtx_auth_secret() -> None:
    assert (
        _redact_path("/v1/ingest/mediamtx-auth/super-secret-value")
        == "/v1/ingest/mediamtx-auth/<redacted>"
    )


def test_redact_path_masks_secret_with_query_and_special_chars() -> None:
    # Path itself is just the segment; query strings live in url.query and
    # aren't part of the middleware's path log. But path can contain hex,
    # dashes, etc — verify the regex catches all.
    assert (
        _redact_path("/v1/ingest/mediamtx-auth/abcd1234-5678-EF")
        == "/v1/ingest/mediamtx-auth/<redacted>"
    )


def test_redact_path_does_not_touch_unrelated_routes() -> None:
    # Other paths should be passed through unchanged so we don't accidentally
    # mask legitimate identifiers (session ids, etc) in other endpoints.
    for path in (
        "/v1/ingest/validate-stream",
        "/v1/ingest/stream-started",
        "/v1/health",
        "/v1/stream-sessions/abc-123/whep",
    ):
        assert _redact_path(path) == path


def test_redact_path_is_idempotent() -> None:
    redacted = _redact_path("/v1/ingest/mediamtx-auth/whatever")
    assert _redact_path(redacted) == redacted
