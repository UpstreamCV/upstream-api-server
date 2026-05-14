"""SRT streamid carrier.

Format: ``publish:<stream_key>`` for ingest connections. The ``stream_key``
is a short opaque token (e.g. ``sess_<hex>``) minted by the API and stored
on the ``stream_sessions`` row; the streaming server hands the full streamid
as-is to ``/v1/ingest/validate-stream``, which looks the session up by the
exact streamid value.

libsrt caps the streamid query parameter at ~512 bytes, so we deliberately
keep this short — embedding a signed JWT here exceeds that limit and FFmpeg
rejects the URL with "Query string option 'streamid' value is too long".
"""

from __future__ import annotations

import secrets

_PUBLISH_PREFIX = "publish:"
_STREAM_KEY_PREFIX = "sess_"


def mint_stream_key() -> str:
    """Return a fresh opaque stream key like ``sess_<16 hex chars>``."""
    return f"{_STREAM_KEY_PREFIX}{secrets.token_hex(8)}"


def build_streamid(stream_key: str) -> str:
    return f"{_PUBLISH_PREFIX}{stream_key}"


def extract_stream_key(streamid: str) -> str:
    """Return the stream_key portion of an incoming streamid.

    Raises ``ValueError`` if the streamid does not start with ``publish:``.
    """
    if not streamid.startswith(_PUBLISH_PREFIX):
        raise ValueError("streamid must start with 'publish:'")
    return streamid[len(_PUBLISH_PREFIX) :]
