"""The last task of ingest_hours: fail the run unless every planned hour is verifiably in bronze.

Each `for_each` iteration already fails on its own errors. This catches what a green iteration can
still hide: an hour that does not reconcile with projectviews, or a re-ingested hour that did not
reproduce its earlier totals. See `wikiflow.audit.verify_attempts`.
"""

from __future__ import annotations

import argparse
import json
import sys

from wikiflow.audit import RunVerificationError, load_attempts, verify_attempts


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="verify_run")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--job-run-id", required=True)
    parser.add_argument("--hours", required=True, help="JSON list from plan_hours")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    hours: list[str] = json.loads(args.hours)

    from pyspark.sql import SparkSession

    attempts = load_attempts(SparkSession.builder.getOrCreate(), args.catalog, hours)
    problems = verify_attempts(hours, attempts, args.job_run_id)
    verdict = {
        "job_run_id": args.job_run_id,
        "hours": len(hours),
        "attempts_in_run": sum(1 for a in attempts if a["job_run_id"] == args.job_run_id),
        "earlier_attempts_compared": sum(
            1 for a in attempts if a["job_run_id"] != args.job_run_id and a["status"] == "success"
        ),
        "problems": problems,
    }
    print("VERIFIED " + json.dumps(verdict))
    if problems:
        raise RunVerificationError(f"{len(problems)} problem(s):\n" + "\n".join(problems))


if __name__ == "__main__":
    main()
