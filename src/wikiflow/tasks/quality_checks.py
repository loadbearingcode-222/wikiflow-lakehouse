"""Run the four quality checks, record every result, and fail once naming every check that failed.

Failing on the first problem would mean fixing it, re-running, and discovering the next one,
which turns one incident into three (wikiflow's pageviews_quality DAG, same rule). A skipped
check is recorded and does not fail the run.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta

from wikiflow.plan_hours import DEFAULT_START, parse_hour
from wikiflow.quality import (
    FAIL,
    CheckResult,
    DataQualityError,
    Warehouse,
    check_freshness,
    check_gaps,
    check_mart_lag,
    check_volume,
    load_warehouse,
    write_results,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="quality_checks")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--job-run-id", default="local")
    parser.add_argument("--as-of", default="", help="e.g. 2026-09-16T15; empty means now")
    parser.add_argument("--gap-window-hours", type=int, default=72)
    #: Publication lags up to ~4 h past the hour and prod ingests every 3 h (ADR 0010), so an
    #: hour can legitimately be ~7 h old before it lands. wikiflow's threshold was 8 h.
    parser.add_argument("--freshness-hours", type=int, default=8)
    #: Loose on purpose: traffic really does spike on news. This catches a truncated or
    #: half-written hour, not something interesting happening in the world.
    parser.add_argument("--sigma", type=float, default=4.0)
    parser.add_argument("--min-history", type=int, default=3)
    #: A run writes bronze first and refreshes the marts at its end; a large catch-up run takes
    #: under an hour. Two hours without the fact carrying a write means the refresh stopped.
    parser.add_argument("--mart-lag-minutes", type=int, default=120)
    return parser.parse_args(argv)


def evaluate(args: argparse.Namespace, as_of: datetime, warehouse: Warehouse) -> list[CheckResult]:
    """The four checks over what `load_warehouse` read; pure, so it is unit-tested directly."""
    freshness = timedelta(hours=args.freshness_hours)
    return [
        check_gaps(
            set(warehouse.totals),
            as_of=as_of,
            window=timedelta(hours=args.gap_window_hours),
            freshness=freshness,
            epoch=parse_hour(DEFAULT_START),
        ),
        check_freshness(max(warehouse.totals, default=None), as_of=as_of, max_age=freshness),
        check_volume(warehouse.totals, sigma_threshold=args.sigma, min_history=args.min_history),
        check_mart_lag(
            warehouse.bronze_written,
            warehouse.mart_written,
            now_micros=warehouse.now_micros,
            max_lag=timedelta(minutes=args.mart_lag_minutes),
        ),
    ]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    checked_at = datetime.now(UTC)
    as_of = parse_hour(args.as_of) if args.as_of else checked_at

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = evaluate(args, as_of, load_warehouse(spark, args.catalog, as_of=as_of))
    write_results(
        spark, args.catalog, results, job_run_id=args.job_run_id, checked_at=checked_at, as_of=as_of
    )

    for result in results:
        print(f"{result.status.upper():8s}{result.check}: {result.detail}")
    print(
        "QUALITY "
        + json.dumps({r.check: r.status for r in results} | {"as_of": f"{as_of:%Y-%m-%dT%H:%M}"})
    )
    failures = [r for r in results if r.status == FAIL]
    if failures:
        raise DataQualityError(
            f"{len(failures)} of {len(results)} checks failed:\n  "
            + "\n  ".join(f"{r.check}: {r.detail}" for r in failures)
        )


if __name__ == "__main__":
    main()
