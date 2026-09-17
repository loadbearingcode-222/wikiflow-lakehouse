"""Apply Unity Catalog tags and bronze column comments to one catalog, as a diff (M4-S3)."""

from __future__ import annotations

import argparse
import json
import sys

from wikiflow.governance import apply


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="apply_governance")
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    from pyspark.sql import SparkSession

    applied = apply(SparkSession.builder.getOrCreate(), args.catalog)
    for statement in applied:
        print(statement)
    print("GOVERNANCE " + json.dumps({"catalog": args.catalog, "statements_applied": len(applied)}))


if __name__ == "__main__":
    main()
