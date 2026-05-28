"""Image analysis primitives for reliability evaluators.

One concern: turn ``bytes`` into a brightness measurement, **safely**. The
function is called on the reliability summary read path, so the caller has
already accepted a small image-decode cost for a single small snapshot — but
the call must never raise, must never lock up on a malformed buffer, and must
not load arbitrarily large images.

Pillow is the right size of dependency for this: it ships its own type stubs,
it's mature, and it's the smallest library that gives us grayscale conversion
without a numpy/opencv detour. ``ImageStat`` returns ``stat.mean`` per band,
so the work is a few lines.

The module level ``MAX_IMAGE_PIXELS`` guard pins Pillow's decompression-bomb
warning into an exception rather than just a warning. Snapshots are already
bounded at 5 MB by the upload pipeline (see ``settings.snapshot_max_bytes``),
so a pathological file would have to be extremely tall-and-narrow to fit;
the guard is cheap insurance.
"""

from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image, ImageStat, UnidentifiedImageError

# Pillow defaults to ~89M pixels before emitting a DecompressionBombWarning.
# Upstream snapshots aren't anywhere near that — cap at 50M pixels (a 8000x6250
# image), which is well above any real surveillance frame and well below the
# point where decode time stops being predictable. ``MAX_IMAGE_PIXELS = None``
# would disable the check entirely; the explicit number gets both the warning
# and the eventual ``DecompressionBombError`` (with a ``LOAD_TRUNCATED_IMAGES``
# discipline this is the safer posture).
Image.MAX_IMAGE_PIXELS = 50_000_000


@dataclass(frozen=True)
class ImageBrightness:
    """One brightness reading from a single image.

    ``mean`` is the mean of the grayscale (mode ``L``) representation on a
    0..255 scale. ``width`` / ``height`` are the source image dimensions in
    pixels.
    """

    mean: float
    width: int
    height: int


def compute_image_brightness(image_bytes: bytes) -> ImageBrightness | None:
    """Mean grayscale brightness of ``image_bytes`` on a 0..255 scale.

    Returns ``None`` (never raises) on:

    - empty / unidentified / malformed buffers
    - decompression-bomb-sized images (``Image.DecompressionBombError``)
    - any other OS-level decode failure

    The caller is expected to skip the brightness check when this returns
    ``None``; the reliability evaluator treats that as "insufficient data"
    rather than "scene is bright" so a transient decode blip doesn't silently
    flip a finding closed.

    Accepts any Pillow-readable mode (RGB / RGBA / L / P / etc.) by converting
    to ``L`` first.
    """
    if not image_bytes:
        return None
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            # Force the decode now so a truncated buffer raises here rather
            # than later inside ``ImageStat`` with a less-clear traceback.
            img.load()
            width, height = img.size
            grayscale = img.convert("L")
            stat = ImageStat.Stat(grayscale)
            mean = float(stat.mean[0])
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
        # OSError covers Pillow's truncated-image errors; ValueError covers
        # the rare case of a recognised header pointing at nonsense pixels.
        return None
    return ImageBrightness(mean=mean, width=width, height=height)
