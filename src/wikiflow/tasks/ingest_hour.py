"""Land one hour: download, verify, publish into the raw volume. The first task of ingest_hours."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from wikiflow.download import land_hour
from wikiflow.source import HourlyDump


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="land_hour")
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--hour", required=True, help="the hour the data describes, UTC, e.g. 2026-09-15T11"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    interval_start = datetime.strptime(args.hour, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    dump = HourlyDump.for_interval(interval_start)
    raw_root = Path(f"/Volumes/{args.catalog}/raw/dumps")

    for dataset, landed in land_hour(dump, raw_root).items():
        partition = sorted(p.name for p in landed.path.parent.iterdir())
        print(
            "LANDED "
            + json.dumps(
                {
                    "dataset": dataset,
                    "hour": args.hour,
                    "path": str(landed.path),
                    "bytes": landed.download.compressed_bytes,
                    "lines": landed.download.line_count,
                    "sha256": landed.download.sha256,
                    "files_in_partition": partition,
                }
            )
        )


if __name__ == "__main__":
    main()
