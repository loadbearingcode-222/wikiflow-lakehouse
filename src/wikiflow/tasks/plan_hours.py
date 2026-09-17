"""Plan a run of ingest_hours: publish the hours to ingest as the task value `hours`.

The job's `for_each` task fans out over `{{tasks.plan_hours.values.hours}}`, and `verify_run`
checks the same list afterwards.

Planning is also where the run's tables are created or migrated, once, BEFORE the fan-out.
Measured in sandbox (run 744903363204838): two iterations that both found the audit table
missing a column both ran ALTER TABLE, one lost with DELTA_METADATA_CHANGED, and a first run
against empty catalogs would race the same way on CREATE TABLE and ADD CONSTRAINT. After this
task the iterations find nothing to change and only write data.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wikiflow import reprocess
from wikiflow.audit import ensure_audit_table
from wikiflow.extract_spark import ensure_bronze_tables
from wikiflow.plan_hours import (
    DEFAULT_MAX_HOURS,
    DEFAULT_START,
    fetch_published,
    format_hour,
    hours_between,
    last_ended_hour,
    parse_hour,
    plan,
    present_hours,
)
from wikiflow.quality import ensure_results_table
from wikiflow.reference import load_current


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="plan_hours")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--start", default=DEFAULT_START, help="first hour, e.g. 2026-09-14T00")
    parser.add_argument(
        "--end", default="", help="last hour; empty means the last hour that has ended"
    )
    parser.add_argument("--max-hours", type=int, default=DEFAULT_MAX_HOURS)
    parser.add_argument(
        "--replace-present",
        default="false",
        choices=["true", "false"],
        help="also re-ingest hours bronze already holds (the dev smoke test)",
    )
    parser.add_argument(
        "--source",
        default="wikimedia",
        choices=["wikimedia", "raw"],
        help="where an hour counts as available: Wikimedia's listing, or the raw volume (reprocess)",
    )
    return parser.parse_args(argv)


def set_task_value(key: str, value: Any) -> None:
    from databricks.sdk.runtime import dbutils

    dbutils.jobs.taskValues.set(key=key, value=value)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    start = parse_hour(args.start)
    end = parse_hour(args.end) if args.end else last_ended_hour(datetime.now(UTC))

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # Refused here, before any download: every planned hour would fail on it anyway.
    if load_current(spark, args.catalog) is None:
        raise RuntimeError(
            f"no company filter is published in {args.catalog}; run company_pages_refresh first"
        )
    ensure_bronze_tables(spark, args.catalog)
    ensure_audit_table(spark, args.catalog)
    ensure_results_table(spark, args.catalog)

    available = (
        reprocess.raw_hours(Path(f"/Volumes/{args.catalog}/raw/dumps"))
        if args.source == "raw"
        else fetch_published(start, end)
    )
    result = plan(
        hours_between(start, end),
        available,
        present_hours(spark, args.catalog, start, end),
        max_hours=args.max_hours,
        replace_present=args.replace_present == "true",
    )
    planned = [format_hour(hour) for hour in result.hours]
    set_task_value("hours", planned)
    # A condition task can only compare numbers, strings and booleans, not the list (measured).
    set_task_value("count", len(planned))
    print(
        "PLANNED "
        + json.dumps(
            {"start": args.start, "end": format_hour(end), "source": args.source} | result.summary()
        )
    )


if __name__ == "__main__":
    main()
