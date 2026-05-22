from typing import Any

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from upstream_api.observability.context import request_id_var


class ApiError(Exception):
    """Base for API-level errors that should be surfaced with stable error codes."""

    status_code: int = 400
    code: str = "api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code
        self.details = details or {}


class NotFoundError(ApiError):
    status_code = 404
    code = "not_found"


class UnauthorizedError(ApiError):
    status_code = 401
    code = "unauthorized"


class ForbiddenError(ApiError):
    status_code = 403
    code = "forbidden"


class ConflictError(ApiError):
    status_code = 409
    code = "conflict"


class LimitExceeded(ApiError):
    status_code = 403
    code = "limit_exceeded"


class UnsupportedMediaTypeError(ApiError):
    status_code = 415
    code = "unsupported_media_type"


class BadGatewayError(ApiError):
    status_code = 502
    code = "upstream_unavailable"


class BadRequestError(ApiError):
    status_code = 400
    code = "bad_request"


def _envelope(
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    status_code: int = 400,
) -> JSONResponse:
    body = {
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
            "request_id": request_id_var.get(""),
        }
    }
    return JSONResponse(status_code=status_code, content=body)


# FastAPI dispatches by exception class so the cast inside each handler is
# always safe; we accept ``Exception`` only to satisfy Starlette's signature.
async def api_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    err = exc if isinstance(exc, ApiError) else ApiError(str(exc))
    return _envelope(
        code=err.code,
        message=err.message,
        details=err.details,
        status_code=err.status_code,
    )


async def http_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    err = exc if isinstance(exc, StarletteHTTPException) else StarletteHTTPException(500, str(exc))
    return _envelope(
        code=f"http_{err.status_code}",
        message=str(err.detail) if err.detail else "",
        status_code=err.status_code,
    )


async def validation_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, RequestValidationError):
        # ``jsonable_encoder`` so a custom validator that raises ``ValueError``
        # (its error object lands in ``ctx``) does not break JSON encoding —
        # already-serializable field errors pass through unchanged.
        details = {"errors": jsonable_encoder(exc.errors())}
    else:
        details = {"errors": [{"msg": str(exc)}]}
    return _envelope(
        code="validation_error",
        message="Request body failed validation.",
        details=details,
        status_code=422,
    )


async def unhandled_exception_handler(_request: Request, _exc: Exception) -> JSONResponse:
    return _envelope(
        code="internal_error",
        message="An unexpected error occurred.",
        status_code=500,
    )
