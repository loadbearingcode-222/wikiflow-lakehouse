"""Resolve the tracked companies into articles and redirects, and publish the filter.

A refused publish (the shrink guard) fails the task on purpose: the current filter stays in
use, and a failed daily refresh costs only staleness, which for article titles is weeks.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from wikiflow.redirects import resolve_filter
from wikiflow.reference import FilterCounts, publish


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="company_pages_refresh")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--seed", required=True, type=Path, help="seeds/companies.csv")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    from pyspark.sql import SparkSession

    pages = resolve_filter(args.seed)
    key = publish(SparkSession.builder.getOrCreate(), args.catalog, pages)
    counts = FilterCounts.of(pages)
    print(
        "PUBLISHED "
        + json.dumps(
            {
                "snapshot_key": key,
                "articles": counts.articles,
                "rows": counts.total,
                "redirects": counts.total - counts.articles,
                "companies": len({p.qid for p in pages}),
            }
        )
    )


if __name__ == "__main__":
    main()
