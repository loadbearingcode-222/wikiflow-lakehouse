"""The company filter as versioned reference data in Unity Catalog.

Resolving the filter takes one Wikidata call and a few hundred MediaWiki requests (one per wiki,
serial, honouring maxlag). Ingestion runs many times a day, and the inputs change over weeks, so
the filter is resolved once a day by `company_pages_refresh` and published here:

    <catalog>.reference.company_pages_snapshots   append-only; one snapshot per refresh
    <catalog>.reference.company_pages_current     what ingestion reads; replaced in one commit

Every row carries its `snapshot_key` (e.g. `20260917T040517Z`), so the exact filter an hour was
extracted with can be cited in that hour's audit record.

Publishing refuses a snapshot that is much SMALLER than the current one. A MediaWiki outage
leaves the Wikidata articles intact and loses only the redirects, which looks healthy by
article count while every later hour silently undercounts (wikiflow ADR 0007). A sudden large
loss is more likely a failed lookup than a real change, and a refused refresh costs only a day
of staleness.

Ported from wikiflow, where the same filter lived as Parquet objects in S3. The guard is
unchanged. Delta adds the two properties the object layout had to arrange by write ordering:
the snapshot table is append-only by table property, and replacing `current` is atomic.
"""

from __future__ import annotations

from dataclasses import astuple, dataclass, fields
from datetime import UTC, datetime
from typing import Any

from wikiflow.wikidata import CompanyPage

#: The largest drop, in either article rows or all rows, a new snapshot may show against the
#: current one before publishing is refused.
MAX_SHRINK_RATIO = 0.10

PAGE_COLUMNS = tuple(f.name for f in fields(CompanyPage))
TABLE_SCHEMA = ", ".join(
    [
        *(f"{name} STRING" + ("" if name == "language" else " NOT NULL") for name in PAGE_COLUMNS),
        "snapshot_key STRING NOT NULL",
        "resolved_at TIMESTAMP NOT NULL",
    ]
)


class ReferenceShrinkError(RuntimeError):
    """A new filter snapshot lost too many pages to be trusted."""


@dataclass(frozen=True, slots=True)
class FilterCounts:
    articles: int
    total: int

    @classmethod
    def of(cls, pages: list[CompanyPage]) -> FilterCounts:
        return cls(
            articles=sum(1 for p in pages if p.match_type == "sitelink"),
            total=len(pages),
        )


@dataclass(frozen=True, slots=True)
class PublishedFilter:
    snapshot_key: str
    pages: list[CompanyPage]


def tables(catalog: str) -> tuple[str, str]:
    """(snapshots, current) table names in `catalog`."""
    return (
        f"{catalog}.reference.company_pages_snapshots",
        f"{catalog}.reference.company_pages_current",
    )


def snapshot_key(resolved_at: datetime) -> str:
    """UTC, second precision, sorts chronologically: `20260917T040517Z`."""
    return f"{resolved_at.astimezone(UTC):%Y%m%dT%H%M%SZ}"


def check_not_shrunk(
    previous: FilterCounts | None, new: FilterCounts, *, max_ratio: float = MAX_SHRINK_RATIO
) -> None:
    """Raise if `new` has lost more than `max_ratio` of the articles or rows in `previous`."""
    if new.articles == 0:
        raise ReferenceShrinkError("refusing to publish a filter with no articles in it")
    if previous is None:
        return
    for label, before, after in (
        ("article rows", previous.articles, new.articles),
        ("rows", previous.total, new.total),
    ):
        if before and (before - after) / before > max_ratio:
            raise ReferenceShrinkError(
                f"refusing to publish: {label} fell from {before:,} to {after:,} "
                f"({(before - after) / before:.1%}, limit {max_ratio:.0%}). A resolution "
                "that lost this much is more likely a failed lookup than a real change; "
                "the current filter stays in use."
            )


def to_rows(pages: list[CompanyPage], *, key: str, resolved_at: datetime) -> list[tuple[Any, ...]]:
    """Rows in TABLE_SCHEMA order, sorted by the filter's join key."""
    ordered = sorted(pages, key=lambda p: (p.domain_code, p.page_title))
    return [(*astuple(page), key, resolved_at) for page in ordered]


def from_rows(rows: list[Any]) -> PublishedFilter | None:
    """Pages from rows read back in TABLE_SCHEMA order; None when there are no rows."""
    if not rows:
        return None
    keys = {row[len(PAGE_COLUMNS)] for row in rows}
    if len(keys) != 1:
        raise ValueError(f"current filter mixes snapshots: {sorted(keys)}")
    pages = [CompanyPage(*row[: len(PAGE_COLUMNS)]) for row in rows]
    return PublishedFilter(snapshot_key=keys.pop(), pages=pages)


# --- Spark side ---------------------------------------------------------------------------


def ensure_tables(spark: Any, catalog: str) -> None:
    snapshots, current = tables(catalog)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {snapshots} ({TABLE_SCHEMA}) "
        "COMMENT 'Every published company filter, one snapshot per refresh (append-only)' "
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {current} ({TABLE_SCHEMA}) "
        "COMMENT 'The company filter ingestion reads; replaced whole by each publish'"
    )


def load_current(spark: Any, catalog: str) -> PublishedFilter | None:
    _, current = tables(catalog)
    columns = ", ".join([*PAGE_COLUMNS, "snapshot_key"])
    return from_rows(spark.sql(f"SELECT {columns} FROM {current}").collect())


def publish(
    spark: Any,
    catalog: str,
    pages: list[CompanyPage],
    *,
    resolved_at: datetime | None = None,
    max_ratio: float = MAX_SHRINK_RATIO,
) -> str:
    """Refuse a shrunken filter; otherwise append the snapshot, then replace current.

    The guard runs before either write. The snapshot is appended first, so a failure between the
    two writes leaves the previous current filter in place, and never a current filter that no
    snapshot records.
    """
    ensure_tables(spark, catalog)
    previous = load_current(spark, catalog)
    check_not_shrunk(
        FilterCounts.of(previous.pages) if previous is not None else None,
        FilterCounts.of(pages),
        max_ratio=max_ratio,
    )

    resolved_at = resolved_at or datetime.now(UTC)
    key = snapshot_key(resolved_at)
    frame = spark.createDataFrame(
        to_rows(pages, key=key, resolved_at=resolved_at), schema=TABLE_SCHEMA
    )
    snapshots, current = tables(catalog)
    frame.write.mode("append").saveAsTable(snapshots)
    frame.write.mode("overwrite").saveAsTable(current)
    return key
