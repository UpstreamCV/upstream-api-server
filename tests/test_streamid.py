from __future__ import annotations

import re

import pytest

from upstream_api.security.streamid import (
    build_streamid,
    extract_stream_key,
    mint_stream_key,
)


def test_build_and_extract_round_trip() -> None:
    streamid = build_streamid("sess_abc123")
    assert streamid == "publish:sess_abc123"
    assert extract_stream_key(streamid) == "sess_abc123"


def test_mint_stream_key_shape_and_uniqueness() -> None:
    a = mint_stream_key()
    b = mint_stream_key()
    assert a != b
    assert re.fullmatch(r"sess_[0-9a-f]{16}", a) is not None
    assert re.fullmatch(r"sess_[0-9a-f]{16}", b) is not None


def test_built_streamid_fits_libsrt_limit() -> None:
    # libsrt rejects streamid query values over ~512 bytes; ours must stay
    # well under that regardless of latency suffix appended downstream.
    streamid = build_streamid(mint_stream_key())
    assert len(streamid.encode("utf-8")) < 64


@pytest.mark.parametrize("bad", ["", "subscribe:abc", "abc"])
def test_extract_rejects_unknown_prefix(bad: str) -> None:
    with pytest.raises(ValueError):
        extract_stream_key(bad)
