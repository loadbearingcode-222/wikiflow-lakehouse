"""Ingest one hour: land it verified, extract both bronze grains, and audit the attempt.

One task per hour rather than a `land_hour` task followed by an `extract_hour` task. That way an
attempt is one audit row carrying both the download's and the extraction's numbers, as in
wikiflow, and M1-S5's `for_each` can fan out over hours with this task as its single nested
task.

The audit row is written whether the attempt succeeds or fails, and a failure is then re-raised,
so the job run still fails and its retry policy still applies. Writing a FAILED attempt's row
never raises: an audit error there would replace the real failure in the task's result, hiding
the thing the row exists to explain (wikiflow learned this once already).

After extraction the hour is reconciled against the projectviews file landed beside it. A
mismatch is recorded, not raised: `verify_run` fails the run on it, after every hour is audited.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wikiflow import download, extract_spark, reprocess
from wikiflow.audit import IngestAudit, RunContext, parse_projectviews, write_audit
from wikiflow.source import HourlyDump

log = logging.getLogger(__name__)


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
    parser.add_argument(
        "--source",
        default="wikimedia",
        choices=["wikimedia", "raw"],
        help="raw: rebuild from files already landed, without downloading (M3-S3 reprocess)",
    )
    return parser.parse_args(argv)


def ingest(
    spark: Any, catalog: str, dump: HourlyDump, run: RunContext, *, source: str = "wikimedia"
) -> IngestAudit:
    """Land (or re-verify), extract, audit. Returns the success record; on failure audits and
    re-raises."""
    record = IngestAudit.begin(run, dump)
    raw_root = Path(f"/Volumes/{catalog}/raw/dumps")
    try:
        if source == "raw":
            landed = reprocess.verify_landed(dump, raw_root)
        else:
            landed = download.land_hour(dump, raw_root)
        pageviews = landed["pageviews"].download
        record = record.landed(pageviews)
        hour = extract_spark.extract_hour(spark, catalog, dump, expected_lines=pageviews.line_count)
        record = record.extracted(hour)
        theirs = parse_projectviews(landed["projectviews"].path.read_text(encoding="utf-8"))
        record = record.reconciled(hour.grains.domain_views, theirs)
    except Exception as exc:
        _audit_failure(spark, catalog, record.failed(exc))
        raise
    record = record.succeeded()
    try:
        write_audit(spark, catalog, record)
    except Exception as exc:
        # The data is committed, but the task fails and retries, so this attempt is recorded as
        # failed - with the audit error - rather than leaving an attempt with no row at all.
        _audit_failure(spark, catalog, record.failed(exc))
        raise
    return record


def _audit_failure(spark: Any, catalog: str, record: IngestAudit) -> None:
    """Write a failed attempt's row. Never raises: that would replace the failure being recorded."""
    try:
        write_audit(spark, catalog, record)
    except Exception:
        log.exception("could not audit the failed attempt; raising the original failure")


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
        source=args.source,
    )
    print(
        "INGESTED "
        + json.dumps(
            {
                "hour": args.hour,
                "source": args.source,
                "attempt": record.attempt,
                "filter_snapshot": record.filter_snapshot,
                "line_count": record.line_count,
                "rows_malformed": record.rows_malformed,
                "company_rows": record.company_rows,
                "company_views": record.company_views,
                "domain_rows": record.domain_rows,
                "total_views": record.total_views,
                "company_share": record.company_share,
                "projectviews_views": record.projectviews_views,
                "domains_mismatched": record.domains_mismatched,
                "duration_seconds": round(record.duration_seconds, 1),
            }
        )
    )


if __name__ == "__main__":
    main()
