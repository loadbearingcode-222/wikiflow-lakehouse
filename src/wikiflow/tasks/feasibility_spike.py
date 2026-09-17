"""M0-S4 spike: can this workspace's serverless compute reach Wikimedia, and what does an hour cost?

Free Edition restricts outbound internet to "a limited set of trusted domains", with no public
list, so whether ingestion can run inside Databricks at all is measured here, not assumed.

For one hour (label 2026-09-15T12:00 by default, an hour wikiflow already holds):

1. Probe the three hosts the pipeline needs: the dumps server, Wikidata and a Wikipedia API.
2. If the dumps server answers, download the hour's pageviews .gz into the raw volume, check the
   bytes against Content-Length, and decompress it end to end (which checks the gzip CRC).
3. Extract domain totals with Spark and reconcile them against Wikimedia's own projectviews file
   for the same hour, to the view.

Every number is printed as one `SPIKE-RESULT {json}` line, which ADR 0010 cites.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

USER_AGENT = (
    "wikiflow-lakehouse/0.1 (portfolio project; "
    "https://github.com/loadbearingcode-222/wikiflow-lakehouse)"
)
DUMPS = "https://dumps.wikimedia.org/other/pageviews"


@dataclass(frozen=True)
class Probe:
    name: str
    url: str
    ok: bool
    status: int | None
    elapsed_ms: int
    detail: str


def dump_urls(label: datetime) -> tuple[str, str]:
    """(pageviews .gz, projectviews) URLs for a filename label hour - the END of the hour covered."""
    directory = f"{DUMPS}/{label:%Y}/{label:%Y-%m}"
    stamp = f"{label:%Y%m%d}-{label:%H}0000"
    return f"{directory}/pageviews-{stamp}.gz", f"{directory}/projectviews-{stamp}"


def probe(name: str, url: str, timeout: float = 20.0) -> Probe:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(1024)
            status = int(response.status)
            return Probe(name, url, 200 <= status < 300, status, _ms(started), "")
    except urllib.error.HTTPError as exc:
        return Probe(name, url, False, exc.code, _ms(started), f"HTTPError: {exc.reason}")
    except Exception as exc:  # the class of failure IS the measurement (DNS, proxy, timeout)
        return Probe(name, url, False, None, _ms(started), f"{type(exc).__name__}: {exc}")


def parse_projectviews(text: str) -> dict[str, int]:
    """projectviews lines are `domain_code - views 0`; the domain code itself may be empty-ish."""
    totals: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split(" ")
        if len(parts) == 4:
            totals[parts[0]] = totals.get(parts[0], 0) + int(parts[2])
    return totals


def reconcile(ours: dict[str, int], theirs: dict[str, int]) -> dict[str, int]:
    domains = set(ours) | set(theirs)
    mismatched = sum(1 for d in domains if ours.get(d) != theirs.get(d))
    return {
        "domains": len(domains),
        "mismatched_domains": mismatched,
        "our_total_views": sum(ours.values()),
        "their_total_views": sum(theirs.values()),
    }


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _download(url: str, target: Path) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    started = time.monotonic()
    target.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(request, timeout=120) as response, target.open("wb") as out:
        expected = int(response.headers.get("Content-Length", -1))
        written = 0
        while chunk := response.read(1024 * 1024):
            out.write(chunk)
            written += len(chunk)
    download_ms = _ms(started)

    started = time.monotonic()
    lines = 0
    with gzip.open(target, "rb") as stream:
        for _ in stream:
            lines += 1
    return {
        "content_length": expected,
        "bytes_written": written,
        "length_matches": expected == written,
        "download_ms": download_ms,
        "gzip_lines": lines,
        "gzip_verify_ms": _ms(started),
    }


def _spark_domain_totals(path: str) -> tuple[dict[str, int], dict[str, Any]]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    started = time.monotonic()
    parts = spark.read.text(path).select(F.split("value", " ").alias("p"))
    parsed = parts.where(F.size("p") == 4).select(
        F.col("p")[0].alias("domain_code"), F.col("p")[2].cast("bigint").alias("views")
    )
    totals = {
        row["domain_code"]: int(row["views"])
        for row in parsed.groupBy("domain_code").agg(F.sum("views").alias("views")).collect()
    }
    rows = parts.count()
    return totals, {"spark_rows": rows, "spark_ms": _ms(started), "spark_version": spark.version}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="feasibility_spike")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--label", default="2026-09-15T12", help="dump filename hour, UTC")
    args = parser.parse_args(argv)

    label = datetime.strptime(args.label, "%Y-%m-%dT%H")
    pageviews_url, projectviews_url = dump_urls(label)
    probes = [
        probe("dumps.wikimedia.org", projectviews_url),
        probe("www.wikidata.org", "https://www.wikidata.org/w/api.php?action=query&meta=siteinfo&format=json"),
        probe("en.wikipedia.org", "https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&format=json"),
    ]  # fmt: skip
    result: dict[str, Any] = {"label": args.label, "probes": [asdict(p) for p in probes]}

    if probes[0].ok:
        volume = Path(f"/Volumes/{args.catalog}/raw/dumps/spike")
        target = volume / pageviews_url.rsplit("/", 1)[1]
        result["download"] = _download(pageviews_url, target)

        with urllib.request.urlopen(
            urllib.request.Request(projectviews_url, headers={"User-Agent": USER_AGENT}), timeout=60
        ) as response:
            theirs = parse_projectviews(response.read().decode("utf-8"))

        ours, spark_stats = _spark_domain_totals(str(target))
        result["spark"] = spark_stats
        result["reconciliation"] = reconcile(ours, theirs)
        target.unlink()  # the spike keeps its numbers, not the 50 MB
    else:
        result["download"] = "skipped: dumps.wikimedia.org unreachable from this compute"

    line = json.dumps(result, sort_keys=True)
    print(f"SPIKE-RESULT {line}")


if __name__ == "__main__":
    main()
