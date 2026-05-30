"""Manual maintenance runner for detection-metrics retention + rollup (M6).

Rolls completed hours of ``snapshot_detection_metrics`` into compact hourly
buckets (``detection_confidence_hourly_rollups``), prunes raw rows older than
the raw-retention horizon that a rollup already covers, and ages out very old
hourly rollups — all in one atomic transaction. See
``services/detection_metrics_rollup`` for the safety reasoning.

This is a maintenance operation, NOT a public/dashboard endpoint. There is no
pg_cron in this project; run it manually for M6 (or wire it into a future
cron/systemd timer using the same command):

    uv run python scripts/run_detection_metrics_rollup.py

It connects with the same ``UPSTREAM_DATABASE_URL`` the API uses (service-role,
RLS-exempt) and prints a JSON summary of counts to stdout. The retention
windows default to the named constants in the service and can be overridden for
ad-hoc/backfill runs:

    uv run python scripts/run_detection_metrics_rollup.py \\
        --lookback-hours 96 --raw-retention-hours 72 --hourly-retention-days 90

Output (counts only — never row contents or secrets):

    {"rolled_up_buckets": 12, "raw_rows_pruned": 12345, "hourly_rows_pruned": 0,
     "started_at": "...", "finished_at": "..."}
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import asyncpg

from upstream_api.config import get_settings
from upstream_api.services.detection_metrics_rollup import (
    HOURLY_ROLLUP_RETENTION_DAYS,
    RAW_METRIC_RETENTION_HOURS,
    ROLLUP_LOOKBACK_HOURS,
    run_detection_metrics_rollup,
)


def _eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


async def main_async(args: argparse.Namespace) -> int:
    settings = get_settings()
    conn = await asyncpg.connect(settings.database_url.get_secret_value())
    try:
        result = await run_detection_metrics_rollup(
            conn,
            lookback_hours=args.lookback_hours,
            raw_retention_hours=args.raw_retention_hours,
            rollup_retention_days=args.hourly_retention_days,
        )
    except Exception as exc:  # surface the failure clearly, without leaking secrets
        _eprint(f"[error] detection metrics rollup failed: {type(exc).__name__}: {exc}")
        return 1
    finally:
        await conn.close()

    print(json.dumps(result.to_dict()))
    _eprint(
        f"[ok] rolled_up_buckets={result.rolled_up_buckets} "
        f"raw_rows_pruned={result.raw_rows_pruned} "
        f"hourly_rows_pruned={result.hourly_rows_pruned}"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Roll up + prune detection-confidence metrics (reliability M6)."
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=ROLLUP_LOOKBACK_HOURS,
        help=f"How far back to re-scan raw rows for rollup (default {ROLLUP_LOOKBACK_HOURS}).",
    )
    parser.add_argument(
        "--raw-retention-hours",
        type=int,
        default=RAW_METRIC_RETENTION_HOURS,
        help=f"Prune covered raw rows older than this (default {RAW_METRIC_RETENTION_HOURS}).",
    )
    parser.add_argument(
        "--hourly-retention-days",
        type=int,
        default=HOURLY_ROLLUP_RETENTION_DAYS,
        help=f"Prune hourly rollups older than this (default {HOURLY_ROLLUP_RETENTION_DAYS}).",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
