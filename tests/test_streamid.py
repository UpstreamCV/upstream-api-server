from __future__ import annotations

import pytest

from upstream_api.security.streamid import build_streamid, extract_session_token


def test_build_and_extract_round_trip() -> None:
    streamid = build_streamid("abc.def.ghi")
    assert streamid == "publish:abc.def.ghi"
    assert extract_session_token(streamid) == "abc.def.ghi"


@pytest.mark.parametrize("bad", ["", "subscribe:abc", "abc"])
def test_extract_rejects_unknown_prefix(bad: str) -> None:
    with pytest.raises(ValueError):
        extract_session_token(bad)
