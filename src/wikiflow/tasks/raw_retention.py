"""Raw retention, in two steps of one job run: plan, then apply exactly the plan.

    --step plan    list raw partitions, refuse a sweep that expires everything, record the
                   partitions to delete in ops.retention_manifest (step = planned)
    --step apply   when --mode apply, delete exactly what this run planned, re-checking each
                   partition, and record what went (step = deleted); in dry_run, delete nothing

Retention is its own job, not a task of ingest_hours. It depends on wall-clock time only, and a
backfill of old hours must never be able to trigger it (wikiflow's reason, unchanged).
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from wikiflow.retention import (
    DEFAULT_RETENTION_DAYS,
    SweepPlan,
    delete_partition,
    find_partitions,
    load_planned,
    manifest_rows,
    plan_sweep,
    raw_root,
    write_manifest,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="raw_retention")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--step", required=True, choices=["plan", "apply"])
    parser.add_argument("--job-run-id", default="local")
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--mode", default="dry_run", choices=["dry_run", "apply"])
    return parser.parse_args(argv)


def plan(args: argparse.Namespace, now: datetime) -> SweepPlan:
    partitions, unparseable = find_partitions(raw_root(args.catalog))
    return plan_sweep(partitions, unparseable, now=now, retention_days=args.retention_days)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    now = datetime.now(UTC)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    if args.step == "plan":
        sweep = plan(args, now)  # raises before anything is recorded if the sweep looks wrong
        print(sweep.manifest(dry_run=args.mode == "dry_run"))
        write_manifest(
            spark,
            args.catalog,
            manifest_rows(sweep, job_run_id=args.job_run_id, recorded_at=now, step="planned"),
        )
        print(f"PLANNED {len(sweep.expired)} partition(s) to delete, cutoff {sweep.cutoff}")
        return

    planned = load_planned(spark, args.catalog, args.job_run_id)
    if args.mode == "dry_run":
        print(f"DRY RUN: {len(planned)} planned partition(s) kept; mode=apply deletes them")
        return

    deleted_rows = []
    total = 0
    for row in planned:
        files = delete_partition(
            raw_root(args.catalog), row["dataset"], row["dt"], row["hour"], cutoff=row["cutoff"]
        )
        total += files
        deleted_rows.append(
            (args.job_run_id, datetime.now(UTC), "deleted", row["cutoff"], row["dataset"],
             row["dt"], row["hour"], files, row["bytes"])
        )  # fmt: skip
        print(f"deleted {row['dataset']}/dt={row['dt']}/hour={row['hour']:02d}: {files} file(s)")
    write_manifest(spark, args.catalog, deleted_rows)
    print(f"DELETED {len(planned)} partition(s), {total} file(s)")


if __name__ == "__main__":
    main()
