"""Image brightness primitive — unit tests.

Tiny in-memory JPEGs built with Pillow so the assertions don't depend on
external fixture files. The contract under test is intentionally narrow:

- valid image bytes (any mode) → ``ImageBrightness`` with a sane mean
- everything that isn't a valid image → ``None`` (the function never raises)

The reliability evaluator relies on the never-raises promise to keep the
summary endpoint from 500ing on a malformed snapshot, so the negative cases
are as important as the positive ones.
"""

from __future__ import annotations

import io

from PIL import Image

from upstream_api.services.image_analysis import (
    ImageBrightness,
    compute_image_brightness,
)


def _jpeg(color: tuple[int, int, int] | int, size: tuple[int, int] = (8, 8)) -> bytes:
    """Build an in-memory JPEG of a single solid color.

    ``color`` may be an ``(R, G, B)`` triple (mode RGB) or a single 0..255 int
    (mode L) so the test covers both code paths through the function's
    grayscale conversion.
    """
    mode = "RGB" if isinstance(color, tuple) else "L"
    img = Image.new(mode, size, color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def test_black_image_reports_near_zero_mean() -> None:
    """All-black input must read as very low brightness — the whole reason
    the dark-scene rule can fire."""
    out = compute_image_brightness(_jpeg((0, 0, 0)))
    assert out is not None
    assert isinstance(out, ImageBrightness)
    # JPEG isn't lossless; allow tiny encoder drift on a tiny image.
    assert out.mean < 5.0
    assert out.width == 8
    assert out.height == 8


def test_white_image_reports_near_max_mean() -> None:
    """All-white input must read at the top of the scale — the resolve path
    for the dark-scene rule."""
    out = compute_image_brightness(_jpeg((255, 255, 255)))
    assert out is not None
    assert out.mean > 250.0


def test_midgray_image_reports_midscale_mean() -> None:
    out = compute_image_brightness(_jpeg((128, 128, 128)))
    assert out is not None
    # Around 128, with JPEG slop.
    assert 120.0 < out.mean < 136.0


def test_grayscale_mode_image_works() -> None:
    """The function must accept already-grayscale images; the dashboard's
    snapshot pipeline doesn't guarantee colour space, and a probe-default
    'L' image must produce a meaningful reading instead of None."""
    out = compute_image_brightness(_jpeg(20))  # L mode, dark gray
    assert out is not None
    assert out.mean < 25.0


def test_non_uniform_color_image_reports_mean() -> None:
    """A real scene won't be one colour. Build a half-black-half-white JPEG
    and assert the mean lands around 128 — confirms the function averages
    rather than sampling a corner."""
    img = Image.new("RGB", (16, 16), (0, 0, 0))
    for x in range(8, 16):
        for y in range(16):
            img.putpixel((x, y), (255, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    out = compute_image_brightness(buf.getvalue())
    assert out is not None
    assert 110.0 < out.mean < 145.0  # JPEG chroma subsampling smooths the edge


def test_dimensions_round_trip() -> None:
    out = compute_image_brightness(_jpeg((32, 32, 32), size=(640, 480)))
    assert out is not None
    assert out.width == 640
    assert out.height == 480


# ---------------------------------------------------------------------------
# Negative paths — the never-raises promise.
# ---------------------------------------------------------------------------


def test_empty_bytes_returns_none() -> None:
    assert compute_image_brightness(b"") is None


def test_random_bytes_returns_none() -> None:
    """Random non-image bytes must not raise — the evaluator treats ``None``
    as 'skip', and a 500 here would crash the whole reliability summary
    endpoint for the camera."""
    assert compute_image_brightness(b"\x00\x01\x02not-an-image\xff\xfe") is None


def test_truncated_jpeg_returns_none() -> None:
    """A truncated upload is the realistic failure case (network blip mid-
    download). Must produce ``None``, not crash."""
    full = _jpeg((100, 100, 100), size=(32, 32))
    truncated = full[: len(full) // 2]
    assert compute_image_brightness(truncated) is None


def test_text_bytes_returns_none() -> None:
    """Pillow's identify guess can misfire on certain text starts; pin that
    it still doesn't propagate an exception."""
    assert compute_image_brightness(b"<html><body>not an image</body></html>") is None
