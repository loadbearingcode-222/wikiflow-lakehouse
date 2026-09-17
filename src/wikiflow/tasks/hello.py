"""The first job: proves the whole deploy path, from wheel to serverless task to catalog.

It reads nothing and writes nothing. It prints what the task actually got (Python, Spark and
the catalog's schemas), because a deploy path is only proven by a run that shows its inputs.
"""

from __future__ import annotations

import argparse
import platform
import sys

EXPECTED_SCHEMAS = ("bronze", "gold", "ops", "raw", "reference", "silver")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="hello")
    parser.add_argument("--catalog", required=True)
    return parser.parse_args(argv)


def missing_schemas(found: list[str]) -> list[str]:
    return sorted(set(EXPECTED_SCHEMAS) - set(found))


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    from pyspark.sql import SparkSession  # available on the cluster, not a local dependency

    spark = SparkSession.builder.getOrCreate()
    schemas = [row[0] for row in spark.sql(f"SHOW SCHEMAS IN `{args.catalog}`").collect()]

    print(f"python   {platform.python_version()}")
    print(f"spark    {spark.version}")
    print(f"catalog  {args.catalog}")
    print(f"schemas  {', '.join(sorted(schemas))}")

    missing = missing_schemas(schemas)
    if missing:
        raise SystemExit(f"hello: {args.catalog} is missing schemas: {', '.join(missing)}")
    print("hello: the deploy path works")


if __name__ == "__main__":
    main()
