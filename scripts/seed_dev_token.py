"""Deprecated shim. Use `scripts/seed_dev_account.py` instead.

Kept for backwards-compat with the README's older quickstart. Runs the
new seeder in --token-only mode, which mints a fresh API token for the
dev account (creating the account first if it doesn't yet exist).
"""

from __future__ import annotations

import sys
import warnings

from scripts.seed_dev_account import main as _seed_main


def main() -> None:
    warnings.warn(
        "seed_dev_token.py is deprecated; use `make dev-account` "
        "(or `python scripts/seed_dev_account.py --token-only`) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    sys.argv = [sys.argv[0], "--token-only"]
    _seed_main()


if __name__ == "__main__":
    main()
