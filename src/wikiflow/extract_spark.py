"""Turning one landed hour into its two bronze grains, from a single Spark scan.

The pageviews file is ~7M lines, but almost none of them are about the question. One scan keeps
two grains (wikiflow's design, unchanged):

  company_pageviews  every row about a tracked company (~400 rows): the fact
  domain_totals      views per domain code (~1,950 rows): the denominator, and the key the
                     hour reconciles on against Wikimedia's own projectviews file

What changes on Spark is how "one scan" is kept honest. Spark is lazy, and serverless compute
cannot cache, so computing the two grains as two DataFrames would read the 62 MB gzip twice.
Instead, a single aggregation groups every line by (valid, domain_code, tracked title):

- untracked lines of a domain collapse into one group,
- each tracked title keeps its own group,
- every malformed line falls into one group.

That is ~2,400 rows, collected once. Both grains, the malformed count and the line count are
then built from those rows in plain Python (`build_grains`), which is also what makes all of it
unit-testable without Spark.

Lines are split on single spaces with no CSV quoting, because page titles legally contain `"`
(wikiflow's `quote=''` lesson; ADR 0010 measured Spark's `split` reconciling exactly).
Spark SQL runs with ANSI mode on, so fields are read with `get` and `try_cast`, which yield NULL
where a strict `p[2]` or `CAST` would fail the whole scan on one bad line.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from wikiflow.contracts import (
    BRONZE_CONTRACTS,
    columns,
    delta_constraints,
    delta_statements,
    validate_rows,
)
from wikiflow.parsing import UnparseableDomainCode, parse_domain_code
from wikiflow.reference import load_current
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

#: The most lines per hour that may fail to parse before extraction refuses the file.
#:
#: Measured in wikiflow: across 40 real hours (~270M lines) exactly zero lines failed to parse,
#: while a download cut at 30% and read leniently silently lost 72% of the hour. So this is
#: strict: 1 in 100,000 tolerates a stray garbage line upstream, while truncation-scale loss
#: fails loudly.
MAX_MALFORMED_RATIO = 1e-5


class MalformedRowsError(ValueError):
    """Too many lines failed to parse, or the scan did not see the lines landing verified."""


def check_malformed(
    *, rows_scanned: int, expected_lines: int, max_ratio: float = MAX_MALFORMED_RATIO
) -> int:
    """Return the malformed-line count, raising if it exceeds the tolerated ratio."""
    malformed = max(0, expected_lines - rows_scanned)
    if expected_lines and malformed / expected_lines > max_ratio:
        raise MalformedRowsError(
            f"{malformed:,} of {expected_lines:,} lines did not parse "
            f"({malformed / expected_lines:.4%}, limit {max_ratio:.4%}). Refusing to publish "
            "an hour that may be missing rows - a truncated file looks exactly like this."
        )
    return malformed


def raw_path(catalog: str, dump: HourlyDump, dataset: str) -> str:
    """Where `land_hour` published a dataset's file for this hour, as Spark reads it."""
    return f"/Volumes/{catalog}/raw/dumps/{dump.raw_key(dataset)}"


@dataclass(frozen=True, slots=True)
class LineGroup:
    """One row of the scan's aggregation."""

    valid: bool
    """False for lines that did not split into four fields with two integer counts."""
    domain_code: str | None
    tracked_title: str | None
    """The page title when (domain_code, title) is in the company filter; else None."""
    lines: int
    views: int | None
    response_bytes: int | None


@dataclass(frozen=True, slots=True)
class HourGrains:
    """Both grains for one hour, as rows in contract column order, plus the audit counts."""

    company_rows: list[tuple[Any, ...]]
    domain_rows: list[tuple[Any, ...]]
    line_count: int
    rows_scanned: int
    rows_malformed: int
    company_views: int
    total_views: int

    @property
    def company_share(self) -> float:
        return self.company_views / self.total_views if self.total_views else 0.0

    @property
    def domain_views(self) -> dict[str, int]:
        """Views per domain code: the side of the hour that reconciles with projectviews."""
        names = columns(BRONZE_CONTRACTS["domain_totals"])
        code, views = names.index("domain_code"), names.index("views")
        return {row[code]: row[views] for row in self.domain_rows}


def build_grains(
    groups: Iterable[LineGroup],
    pages: Sequence[CompanyPage],
    dump: HourlyDump,
    *,
    source_url: str,
    ingested_at: datetime,
    expected_lines: int | None = None,
    max_ratio: float = MAX_MALFORMED_RATIO,
) -> HourGrains:
    """Build both grains from the scan's groups. Refuses the hour before building any row if the
    malformed gate trips, or if the scan saw a different number of lines than landing verified.
    """
    groups = list(groups)
    line_count = sum(g.lines for g in groups)
    if expected_lines is not None and line_count != expected_lines:
        raise MalformedRowsError(
            f"Spark read {line_count:,} lines from {source_url}, but landing verified "
            f"{expected_lines:,}. The file changed or was read short; refusing the hour."
        )
    valid = [g for g in groups if g.valid]
    rows_scanned = sum(g.lines for g in valid)
    malformed = check_malformed(
        rows_scanned=rows_scanned, expected_lines=line_count, max_ratio=max_ratio
    )

    # Partition keys come from the hour the data describes, never the filename (ADR 0001).
    keys = (
        dump.interval_start.date(),
        dump.interval_start.hour,
        dump.interval_start.replace(tzinfo=None),
        source_url,
        ingested_at,
    )

    per_domain: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for group in valid:
        assert group.domain_code is not None and group.views is not None
        totals = per_domain[group.domain_code]
        totals[0] += group.views
        totals[1] += group.response_bytes or 0
        totals[2] += group.lines

    domain_rows = []
    for code in sorted(per_domain):
        views, response_bytes, pages_seen = per_domain[code]
        try:
            parsed = parse_domain_code(code)
        except UnparseableDomainCode:
            # Kept, not dropped: Wikimedia counts these views, so the reconciliation does too.
            attributes: tuple[Any, ...] = (None, None, None, False)
        else:
            attributes = (parsed.project, parsed.language, parsed.access_method, True)
        domain_rows.append((*keys, code, *attributes, views, response_bytes, pages_seen))

    # A title can belong to more than one company (a redirect shared by Facebook and Meta, say).
    # The scan joined on DISTINCT titles, so domain totals counted it once; here it becomes one
    # company row per company, which is what the narrow grain means.
    by_title: dict[tuple[str, str], list[CompanyPage]] = defaultdict(list)
    for page in pages:
        by_title[(page.domain_code, page.page_title)].append(page)

    company_rows = []
    for group in sorted(valid, key=lambda g: (g.domain_code or "", g.tracked_title or "")):
        if group.tracked_title is None:
            continue
        assert group.domain_code is not None
        for page in sorted(by_title[(group.domain_code, group.tracked_title)], key=_page_order):
            company_rows.append(
                (
                    *keys,
                    page.qid,
                    page.company_name,
                    page.language,
                    page.project,
                    page.access_method,
                    page.match_type,
                    page.canonical_title or page.page_title,
                    group.domain_code,
                    group.tracked_title,
                    group.views,
                    group.response_bytes,
                )
            )

    return HourGrains(
        company_rows=company_rows,
        domain_rows=domain_rows,
        line_count=line_count,
        rows_scanned=rows_scanned,
        rows_malformed=malformed,
        # Per title, not per company row: a shared title counts once, so the share of all
        # traffic can never exceed the truth.
        company_views=sum(g.views or 0 for g in valid if g.tracked_title is not None),
        total_views=sum(totals[0] for totals in per_domain.values()),
    )


def _page_order(page: CompanyPage) -> tuple[str, str]:
    return (page.qid, page.match_type)


_BIGINT = re.compile(r"[+-]?[0-9]+")
_BIGINT_RANGE = range(-(2**63), 2**63)


def _try_bigint(field: str | None) -> int | None:
    """Python's twin of Spark's `try_cast(... AS BIGINT)` for the fields a dump line holds."""
    if field is None or not _BIGINT.fullmatch(field):
        return None
    value = int(field)
    return value if value in _BIGINT_RANGE else None


def group_lines(lines: Iterable[str], pages: Sequence[CompanyPage]) -> list[LineGroup]:
    """The scan's grouping in plain Python: the specification `scan_hour` is tested against.

    Fine for a fixture; far too slow for a real 7M-line hour, which is what Spark is for.
    """
    tracked = {(page.domain_code, page.page_title) for page in pages}
    totals: dict[tuple[bool, str | None, str | None], list[int]] = defaultdict(lambda: [0, 0, 0])
    for line in lines:
        fields = line.rstrip("\r\n").split(" ")
        views = _try_bigint(fields[2] if len(fields) > 2 else None)
        response_bytes = _try_bigint(fields[3] if len(fields) > 3 else None)
        valid = len(fields) == 4 and views is not None and response_bytes is not None
        if valid:
            title = fields[1] if (fields[0], fields[1]) in tracked else None
            key: tuple[bool, str | None, str | None] = (True, fields[0], title)
        else:
            key = (False, None, None)
        group = totals[key]
        group[0] += 1
        group[1] += views or 0
        group[2] += response_bytes or 0
    return [
        LineGroup(
            valid=valid,
            domain_code=domain_code,
            tracked_title=title,
            lines=lines_,
            views=views_ if valid else None,
            response_bytes=bytes_ if valid else None,
        )
        for (valid, domain_code, title), (lines_, views_, bytes_) in totals.items()
    ]


# --- Spark side ---------------------------------------------------------------------------


def scan_hour(spark: Any, path: str, pages: Sequence[CompanyPage]) -> list[LineGroup]:
    """The one scan: every line of the file, grouped as described in the module docstring."""
    from pyspark.sql import functions as F

    titles = sorted({(page.domain_code, page.page_title) for page in pages})
    tracked = spark.createDataFrame(
        titles, schema="domain_code STRING NOT NULL, page_title STRING NOT NULL"
    ).withColumn("tracked", F.lit(True))

    fields = spark.read.text(path).selectExpr("split(value, ' ') AS p")
    typed = fields.selectExpr(
        "get(p, 0) AS domain_code",
        "get(p, 1) AS page_title",
        "try_cast(get(p, 2) AS BIGINT) AS views",
        "try_cast(get(p, 3) AS BIGINT) AS response_bytes",
        "size(p) = 4 AS four_fields",
    ).withColumn(
        "valid",
        F.col("four_fields") & F.col("views").isNotNull() & F.col("response_bytes").isNotNull(),
    )

    valid = F.col("valid")
    grouped = (
        typed.join(F.broadcast(tracked), ["domain_code", "page_title"], "left")
        .groupBy(
            valid.alias("valid"),
            F.when(valid, F.col("domain_code")).alias("domain_code"),
            F.when(valid & F.col("tracked"), F.col("page_title")).alias("tracked_title"),
        )
        .agg(
            F.count(F.lit(1)).alias("lines"),
            F.sum(F.when(valid, F.col("views"))).alias("views"),
            F.sum(F.when(valid, F.col("response_bytes"))).alias("response_bytes"),
        )
    )
    return [
        LineGroup(
            valid=bool(row["valid"]),
            domain_code=row["domain_code"],
            tracked_title=row["tracked_title"],
            lines=int(row["lines"]),
            views=None if row["views"] is None else int(row["views"]),
            response_bytes=None if row["response_bytes"] is None else int(row["response_bytes"]),
        )
        for row in grouped.collect()
    ]


REDACTED = "*********(redacted)"


def constraints_drifted(found: dict[str, str], expected: dict[str, str]) -> bool:
    """Whether a table's CHECK constraints differ from the contract's.

    Names are compared case-insensitively (Delta stores them lower-cased). Unity Catalog redacts
    some property values in `SHOW TBLPROPERTIES` - measured: `source_url_valid` reads back as
    `*********(redacted)` - so a redacted expression is trusted by name alone.
    """
    wanted = {name.lower(): check for name, check in expected.items()}
    if set(found) != set(wanted):
        return True
    return any(found[name] not in (REDACTED, check) for name, check in wanted.items())


def bronze_table(catalog: str, name: str) -> str:
    return f"{catalog}.bronze.{name}"


def ensure_bronze_tables(spark: Any, catalog: str) -> None:
    """Create both bronze tables from the contract, or re-apply constraints that drifted.

    Checked on every run but applied only when needed: each DROP/ADD CONSTRAINT is a commit, and
    re-applying ~15 of them per hour would bury the one replace commit per hour in history.
    """
    for name, model in BRONZE_CONTRACTS.items():
        table = bronze_table(catalog, name)
        if spark.catalog.tableExists(table):
            found = {
                row["key"].removeprefix("delta.constraints."): row["value"]
                for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
                if row["key"].startswith("delta.constraints.")
            }
            if not constraints_drifted(found, delta_constraints(model)):
                continue
        for statement in delta_statements(model, table):
            spark.sql(statement)


def write_hour(spark: Any, catalog: str, dump: HourlyDump, grains: HourGrains) -> None:
    """Validate both grains against their contracts, then replace the hour in each table.

    Both are validated before either is written. The two replaces are separate commits: a failure
    between them leaves the grains from different runs, which the task's retry (or a re-run)
    repairs, and the audit row - written last - records success only once both committed.
    """
    grains_by_table = {
        "company_pageviews": grains.company_rows,
        "domain_totals": grains.domain_rows,
    }
    for name, rows in grains_by_table.items():
        model = BRONZE_CONTRACTS[name]
        validate_rows(columns(model), rows, model, label=f"{name} {dump.partition_path}")

    ensure_bronze_tables(spark, catalog)
    predicate = f"dt = DATE'{dump.partition_date}' AND hour = {int(dump.partition_hour)}"
    for name, rows in grains_by_table.items():
        table = bronze_table(catalog, name)
        frame = spark.createDataFrame(rows, schema=spark.table(table).schema)
        frame.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(
            table
        )


@dataclass(frozen=True, slots=True)
class ExtractedHour:
    grains: HourGrains
    filter_snapshot: str
    source_url: str


def extract_hour(
    spark: Any,
    catalog: str,
    dump: HourlyDump,
    *,
    expected_lines: int | None = None,
    ingested_at: datetime | None = None,
) -> ExtractedHour:
    """Scan the hour's landed pageviews file and replace its two bronze grains."""
    current = load_current(spark, catalog)
    if current is None:
        raise RuntimeError(
            f"no company filter is published in {catalog}; run company_pages_refresh first"
        )
    source_url = raw_path(catalog, dump, "pageviews")
    grains = build_grains(
        scan_hour(spark, source_url, current.pages),
        current.pages,
        dump,
        source_url=source_url,
        ingested_at=ingested_at or datetime.now(UTC),
        expected_lines=expected_lines,
    )
    write_hour(spark, catalog, dump, grains)
    return ExtractedHour(grains, current.snapshot_key, source_url)
