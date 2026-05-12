import logging
import sys

import structlog
from structlog.types import EventDict, Processor

from upstream_api.observability.context import (
    account_id_var,
    agent_id_var,
    request_id_var,
    session_id_var,
)

_REDACT_KEYS = frozenset(
    {
        "authorization",
        "api_token",
        "session_token",
        "password",
        "secret",
        "service_role_key",
        "anon_key",
        "jwt_secret",
        "hmac_key",
    }
)


def _redact_secrets(_logger: object, _name: str, event_dict: EventDict) -> EventDict:
    for key in list(event_dict.keys()):
        if key.lower() in _REDACT_KEYS:
            event_dict[key] = "<redacted>"
    return event_dict


def _bind_context(_logger: object, _name: str, event_dict: EventDict) -> EventDict:
    if rid := request_id_var.get(""):
        event_dict.setdefault("request_id", rid)
    if aid := account_id_var.get(None):
        event_dict.setdefault("account_id", str(aid))
    if gid := agent_id_var.get(None):
        event_dict.setdefault("agent_id", str(gid))
    if sid := session_id_var.get(None):
        event_dict.setdefault("session_id", str(sid))
    return event_dict


def configure_logging(*, level: str = "INFO", json_logs: bool = True) -> None:
    logging.basicConfig(
        level=level.upper(),
        stream=sys.stdout,
        format="%(message)s",
    )

    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _bind_context,
        _redact_secrets,
    ]

    if json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
