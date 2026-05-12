"""SRT streamid carrier.

Format: ``publish:<session_jwt>`` for ingest connections. The streaming server
hands the streamid as-is to ``/v1/ingest/validate-stream``; we strip the
``publish:`` prefix and decode the JWT.
"""

from __future__ import annotations

_PUBLISH_PREFIX = "publish:"


def build_streamid(session_token: str) -> str:
    return f"{_PUBLISH_PREFIX}{session_token}"


def extract_session_token(streamid: str) -> str:
    """Return the JWT portion of an incoming streamid.

    Raises ``ValueError`` if the streamid does not start with ``publish:``.
    """
    if not streamid.startswith(_PUBLISH_PREFIX):
        raise ValueError("streamid must start with 'publish:'")
    return streamid[len(_PUBLISH_PREFIX) :]
