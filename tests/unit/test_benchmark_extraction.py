"""The benchmark's DuckDB scan answers exactly what Spark's does, or fails loudly.

Spark's `scan_hour` is held to `group_lines` in tests/integration/test_extract_hour.py, on the
committed sample and on awkward lines. The DuckDB twin is held to the same `group_lines` here, on
the same inputs plus the counts DuckDB's own cast would read differently, so the benchmark
compares two engines answering one question. On real hours, the benchmark itself compares the two
engines' output digests.
"""

from __future__ import annotations

import csv
import gzip
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from benchmark_extraction import digest, grains_digest, groups_digest, scan_hour_duckdb
from wikiflow.extract_spark import LineGroup, build_grains, group_lines
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

FIXTURES = Path(__file__).parents[1] / "fixtures"
SAMPLE = FIXTURES / "pageviews-sample-20260914-120000.gz"

AWKWARD_LINES = [
    "en Apple_Inc. 100 0",
    '"" Some_Page 4321 0',
    'he אלפבת_בע"מ 3 0',
    "ja アップル 12 0",
    "en this line has far too many fields 1 0",
    "en Trailing_Space 1 0 ",
    "en Not_A_Number x 0",
    "en Underscored_Int 1_000 0",
    "en Too_Big 99999999999999999999 0",
    "",
    "en.m Apple_Inc. 250 0",
    # DuckDB's CAST accepts the next two; Spark's try_cast and the contract do not.
    "en Decimal 3882.0 0",
    "en Scientific 1e3 0",
    "en Signed +5 0",
    "en Padded 007 0",
    "en Tab\tInside 1 0",
]
PAGES = [
    CompanyPage("Q312", "Apple Inc.", "enwiki", "en", "wikipedia", "Apple_Inc.", "en", "desktop"),
    CompanyPage("Q312", "Apple Inc.", "hewiki", "he", "wikipedia", 'אלפבת_בע"מ', "he", "desktop"),
]  # fmt: skip


def write_gz(tmp_path: Path, lines: list[str]) -> str:
    path = tmp_path / "hour.gz"
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
    return str(path)


def fixture_pages() -> list[CompanyPage]:
    with (FIXTURES / "company_pages.csv").open(newline="", encoding="utf-8") as handle:
        return [
            CompanyPage(
                qid=row["qid"],
                company_name=row["company_name"],
                sitelink_key=row["sitelink_key"],
                language=row["language"] or None,
                project=row["project"],
                page_title=row["page_title"],
                domain_code=row["domain_code"],
                access_method=row["access_method"],
            )
            for row in csv.DictReader(handle)
        ]


def as_set(groups: list[LineGroup]) -> set[LineGroup]:
    assert len(set(groups)) == len(groups), "a group appeared twice"
    return set(groups)


def test_awkward_lines_group_the_same_in_duckdb_and_python(tmp_path: Path) -> None:
    in_duckdb = as_set(scan_hour_duckdb(duckdb.connect(), write_gz(tmp_path, AWKWARD_LINES), PAGES))

    assert in_duckdb == as_set(group_lines(AWKWARD_LINES, PAGES))
    (malformed,) = [g for g in in_duckdb if not g.valid]
    assert malformed.lines == 8, "the 6 Spark's test counts, plus a decimal and an exponent"


def test_the_committed_sample_gives_wikiflows_numbers_through_duckdb() -> None:
    pages = fixture_pages()
    groups = scan_hour_duckdb(duckdb.connect(), str(SAMPLE), pages)
    with gzip.open(SAMPLE, "rt", encoding="utf-8") as lines:
        assert as_set(groups) == as_set(group_lines(lines, pages))

    grains = build_grains(
        groups,
        pages,
        HourlyDump.for_interval(datetime(2026, 9, 14, 11, tzinfo=UTC)),
        source_url="sample",
        ingested_at=datetime(2026, 9, 14, 12, tzinfo=UTC),
        expected_lines=4_379,
    )
    counts = (len(grains.company_rows), grains.company_views, len(grains.domain_rows))
    assert (counts, grains.total_views) == ((379, 5_138, 320), 15_255)


@pytest.mark.parametrize(
    "line", ["en Delimiter\x01Inside 1 0", "en Bare\rReturn 1 0"], ids=["delimiter", "bare-cr"]
)
def test_a_line_duckdb_would_split_differently_from_spark_fails_the_scan(
    tmp_path: Path, line: str
) -> None:
    path = write_gz(tmp_path, ["en Apple_Inc. 100 0", line])
    with pytest.raises(duckdb.Error):
        scan_hour_duckdb(duckdb.connect(), path, PAGES)


def test_digests_ignore_order_and_see_a_single_view() -> None:
    groups = group_lines(AWKWARD_LINES, PAGES)
    assert groups_digest(groups) == groups_digest(list(reversed(groups)))

    changed = [g if g.tracked_title != "Apple_Inc." else _plus_one(g) for g in groups]
    assert groups_digest(changed) != groups_digest(groups)
    assert digest([(1, "a"), (2, "b")]) == digest([(2, "b"), (1, "a")])


def test_grains_digest_is_stable_for_a_fixed_ingestion_time() -> None:
    pages = fixture_pages()
    with gzip.open(SAMPLE, "rt", encoding="utf-8") as lines:
        groups = group_lines(lines, pages)
    dump = HourlyDump.for_interval(datetime(2026, 9, 14, 11, tzinfo=UTC))
    at = datetime(2026, 9, 14, 12, tzinfo=UTC)

    def build(order: list[LineGroup]) -> str:
        return grains_digest(build_grains(order, pages, dump, source_url="s", ingested_at=at))

    assert build(groups) == build(list(reversed(groups)))


def _plus_one(group: LineGroup) -> LineGroup:
    assert group.views is not None
    return LineGroup(
        group.valid,
        group.domain_code,
        group.tracked_title,
        group.lines,
        group.views + 1,
        group.response_bytes,
    )
