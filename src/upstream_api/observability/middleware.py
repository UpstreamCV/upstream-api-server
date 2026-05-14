import re
import time
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from upstream_api.observability.context import request_id_var
from upstream_api.observability.logging import get_logger

_logger = get_logger("http")

# Paths whose trailing segment is a shared secret (M4.1 MediaMTX authHTTP
# uses path-segment auth because MediaMTX 1.9.3 has no authHTTPHeaders).
# Strip the secret before access-logging.
_SECRET_PATH_PATTERN = re.compile(r"(/v1/ingest/mediamtx-auth)/[^/]+")


def _redact_path(path: str) -> str:
    """Mask the secret in known path-segment-auth routes."""
    return _SECRET_PATH_PATTERN.sub(r"\1/<redacted>", path)


class RequestIdMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, *, header_name: str = "X-Request-ID") -> None:
        super().__init__(app)
        self._header_name = header_name

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        incoming = request.headers.get(self._header_name)
        request_id = incoming or uuid4().hex
        token = request_id_var.set(request_id)
        try:
            response: Response = await call_next(request)
        finally:
            request_id_var.reset(token)
        response.headers[self._header_name] = request_id
        return response


class AccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        started = time.perf_counter()
        path = _redact_path(request.url.path)
        try:
            response: Response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000
            _logger.exception(
                "request_failed",
                method=request.method,
                path=path,
                duration_ms=round(elapsed_ms, 2),
            )
            raise
        elapsed_ms = (time.perf_counter() - started) * 1000
        _logger.info(
            "request_completed",
            method=request.method,
            path=path,
            status_code=response.status_code,
            duration_ms=round(elapsed_ms, 2),
        )
        return response
