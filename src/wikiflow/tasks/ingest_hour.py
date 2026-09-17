"""Ingest one hour: land it verified, extract both bronze grains, and audit the attempt.

One task per hour rather than a `land_hour` task followed by an `extract_hour` task. That way an
attempt is one audit row carrying both the download's and the extraction's numbers, as in
wikiflow, and M1-S5's `for_each` can fan out over hours with this task as its single nested
task.

The audit row is written whether the attempt succeeds or fails, and a failure is then re-raised,
so the job run still fails and its retry policy still applies.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wikiflow import download, extract_spark
from wikiflow.audit import IngestAudit, RunContext, write_audit
from wikiflow.source import HourlyDump


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ingest_hour")
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--hour", required=True, help="the hour the data describes, UTC, e.g. 2026-09-15T11"
    )
    # Filled from dynamic value references by the job; the defaults mark a run from a laptop.
    parser.add_argument("--job-run-id", default="local")
    parser.add_argument("--task-run-id", default="local")
    parser.add_argument("--task-key", default="ingest_hour")
    parser.add_argument("--attempt", type=int, default=1, help="{{task.execution_count}}")
    return parser.parse_args(argv)


def ingest(spark: Any, catalog: str, dump: HourlyDump, run: RunContext) -> IngestAudit:
    """Land, extract, audit. Returns the success record; on failure audits and re-raises."""
    record = IngestAudit.begin(run, dump)
    try:
        landed = download.land_hour(dump, Path(f"/Volumes/{catalog}/raw/dumps"))
        pageviews = landed["pageviews"].download
        record = record.landed(pageviews)
        hour = extract_spark.extract_hour(spark, catalog, dump, expected_lines=pageviews.line_count)
        record = record.extracted(hour)
    except Exception as exc:
        write_audit(spark, catalog, record.failed(exc))
        raise
    record = record.succeeded()
    write_audit(spark, catalog, record)
    return record


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    interval_start = datetime.strptime(args.hour, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    run = RunContext(args.job_run_id, args.task_run_id, args.task_key, args.attempt)

    from pyspark.sql import SparkSession

    record = ingest(
        SparkSession.builder.getOrCreate(),
        args.catalog,
        HourlyDump.for_interval(interval_start),
        run,
    )
    print(
        "INGESTED "
        + json.dumps(
            {
                "hour": args.hour,
                "attempt": record.attempt,
                "filter_snapshot": record.filter_snapshot,
                "line_count": record.line_count,
                "rows_malformed": record.rows_malformed,
                "company_rows": record.company_rows,
                "company_views": record.company_views,
                "domain_rows": record.domain_rows,
                "total_views": record.total_views,
                "company_share": record.company_share,
                "duration_seconds": round(record.duration_seconds, 1),
            }
        )
    )


if __name__ == "__main__":
    main()
