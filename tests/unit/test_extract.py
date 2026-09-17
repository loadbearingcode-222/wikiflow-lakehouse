"""Both bronze grains from one hour's line groups: the counting rules, the gates and the contract.

The Spark scan itself runs in tests/integration/test_extract_hour.py, where it is also checked
line group for line group against `group_lines`, the plain-Python grouping these tests use.
"""

from __future__ import annotations

import csv
import gzip
from datetime import UTC, datetime
from pathlib import Path

import pytest

from wikiflow.contracts import (
    CompanyPageviewRecord,
    DomainTotalRecord,
    columns,
    delta_constraints,
    validate_rows,
)
from wikiflow.extract_spark import (
    REDACTED,
    HourGrains,
    LineGroup,
    MalformedRowsError,
    build_grains,
    check_malformed,
    constraints_drifted,
    group_lines,
    raw_path,
)
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

FIXTURES = Path(__file__).parents[1] / "fixtures"
DUMP = HourlyDump.for_interval(datetime(2026, 9, 15, 11, tzinfo=UTC))
INGESTED = datetime(2026, 9, 17, 12, 30, tzinfo=UTC)
SOURCE = raw_path("wikiflow_sbx", DUMP, "pageviews")


def page(
    qid: str, name: str, domain: str, title: str, match: str = "sitelink", canonical: str = ""
) -> CompanyPage:
    return CompanyPage(
        qid=qid,
        company_name=name,
        sitelink_key="enwiki",
        language="en",
        project="wikipedia",
        page_title=title,
        domain_code=domain,
        access_method="mobile-web" if domain.endswith(".m") else "desktop",
        match_type=match,
        canonical_title=canonical or title,
    )


PAGES = [
    page("Q95", "Google", "en", "Google"),
    page("Q95", "Google", "en.m", "Google"),
    # One redirect title that resolves for two tracked companies.
    page("Q380", "Meta Platforms", "en", "Facebook,_Inc.", "redirect", "Meta_Platforms"),
    page("Q355", "Facebook", "en", "Facebook,_Inc.", "redirect", "Facebook"),
]


def valid(domain: str, title: str | None = None, *, lines: int = 1, views: int) -> LineGroup:
    return LineGroup(True, domain, title, lines, views, 0)


def malformed(lines: int) -> LineGroup:
    return LineGroup(False, None, None, lines, None, None)


GROUPS = [
    valid("en", lines=100_000, views=500_000),
    valid("en", "Google", views=120),
    valid("en", "Facebook,_Inc.", views=7),
    valid("en.m", lines=80_000, views=400_000),
    valid("en.m", "Google", views=300),
    valid('""', lines=3, views=5_383),
]


def grains(groups: list[LineGroup] = GROUPS, *, expected_lines: int | None = None) -> HourGrains:
    return build_grains(
        groups, PAGES, DUMP, source_url=SOURCE, ingested_at=INGESTED, expected_lines=expected_lines
    )


def by_column(model: type, rows: list[tuple[object, ...]]) -> list[dict[str, object]]:
    return [dict(zip(columns(model), row, strict=True)) for row in rows]


def test_domain_totals_count_every_line_of_a_domain_tracked_or_not() -> None:
    domains = {
        row["domain_code"]: row for row in by_column(DomainTotalRecord, grains().domain_rows)
    }

    assert domains["en"]["views"] == 500_000 + 120 + 7
    assert domains["en"]["distinct_pages"] == 100_002
    assert domains["en.m"]["views"] == 400_300
    assert grains().total_views == 500_127 + 400_300 + 5_383


def test_unparseable_domain_codes_are_kept_so_the_hour_still_reconciles() -> None:
    domains = {
        row["domain_code"]: row for row in by_column(DomainTotalRecord, grains().domain_rows)
    }
    attributes = ("project", "language", "access_method", "is_parseable", "views")

    assert [domains['""'][name] for name in attributes] == [None, None, None, False, 5_383]
    assert [domains["en.m"][name] for name in attributes] == [
        "wikipedia",
        "en",
        "mobile-web",
        True,
        400_300,
    ]


def test_a_title_shared_by_two_companies_is_a_row_each_but_counted_once() -> None:
    result = grains()
    shared = [
        row
        for row in by_column(CompanyPageviewRecord, result.company_rows)
        if row["page_title"] == "Facebook,_Inc."
    ]

    assert [(r["qid"], r["canonical_title"], r["count_views"]) for r in shared] == [
        ("Q355", "Facebook", 7),
        ("Q380", "Meta_Platforms", 7),
    ]
    assert len(result.company_rows) == 4
    assert result.company_views == 120 + 7 + 300, "the shared title's views count once"


def test_partition_keys_are_the_hour_described_not_the_filename_hour() -> None:
    row = by_column(DomainTotalRecord, grains().domain_rows)[0]

    assert DUMP.pageviews_filename == "pageviews-20260915-120000.gz"
    assert (row["dt"], row["hour"], row["interval_start_utc"]) == (
        datetime(2026, 9, 15).date(),
        11,
        datetime(2026, 9, 15, 11),
    )
    assert row["source_url"] == (
        "/Volumes/wikiflow_sbx/raw/dumps/pageviews/dt=2026-09-15/hour=11/"
        "pageviews-20260915-120000.gz"
    )


def test_every_row_satisfies_its_contract() -> None:
    result = grains()
    assert validate_rows(
        columns(DomainTotalRecord), result.domain_rows, DomainTotalRecord, label="d"
    )
    assert validate_rows(
        columns(CompanyPageviewRecord), result.company_rows, CompanyPageviewRecord, label="c"
    )


def test_a_few_malformed_lines_are_counted_and_tolerated() -> None:
    result = grains([*GROUPS, malformed(1)])
    assert (result.rows_malformed, result.line_count) == (1, 180_007)
    assert result.rows_scanned == 180_006


def test_truncation_scale_malformed_lines_refuse_the_hour() -> None:
    with pytest.raises(MalformedRowsError, match="did not parse"):
        grains([*GROUPS, malformed(50)])


def test_a_scan_that_saw_other_lines_than_landing_verified_refuses_the_hour() -> None:
    assert grains(expected_lines=180_006).line_count == 180_006
    with pytest.raises(MalformedRowsError, match="landing verified 7,118,105"):
        grains(expected_lines=7_118_105)


def test_malformed_ratio_boundary() -> None:
    """Ported from wikiflow: exactly at the limit is tolerated, one line over is not."""
    assert check_malformed(rows_scanned=99_999, expected_lines=100_000) == 1
    with pytest.raises(MalformedRowsError):
        check_malformed(rows_scanned=99_998, expected_lines=100_000)
    assert check_malformed(rows_scanned=7_451_970, expected_lines=7_451_970) == 0


# --- the committed sample, through the plain-Python grouping ----------------------------------


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


def test_the_committed_sample_gives_wikiflows_measured_numbers() -> None:
    """tests/fixtures/README.md: the numbers wikiflow's DuckDB extraction produced for this file."""
    pages = fixture_pages()
    with gzip.open(FIXTURES / "pageviews-sample-20260914-120000.gz", "rt", encoding="utf-8") as f:
        groups = group_lines(f, pages)
    sample = HourlyDump.for_interval(datetime(2026, 9, 14, 11, tzinfo=UTC))
    result = build_grains(
        groups, pages, sample, source_url="sample", ingested_at=INGESTED, expected_lines=4_379
    )

    assert (
        result.line_count,
        len(result.company_rows),
        result.company_views,
        len(result.domain_rows),
        result.total_views,
    ) == (4_379, 379, 5_138, 320, 15_255)
    titles = [row["page_title"] for row in by_column(CompanyPageviewRecord, result.company_rows)]
    assert any(not str(title).isascii() for title in titles), "non-ASCII titles survive"


def test_group_lines_mirrors_the_scan_on_awkward_lines() -> None:
    lines = [
        "en Apple_Inc. 100 0\n",
        '"" Some_Page 4321 0\n',
        'he אלפבת_בע"מ 3 0\n',
        "en this line has far too many fields 1 0\n",
        "en Not_A_Number x 0\n",
        "en Underscored_Int 1_000 0\n",
        "\n",
    ]
    pages = [page("Q312", "Apple Inc.", "he", 'אלפבת_בע"מ', "redirect", "אפל")]

    groups = {(g.valid, g.domain_code, g.tracked_title): g for g in group_lines(lines, pages)}

    assert groups[(False, None, None)].lines == 4, "extra fields, non-integers and blank lines"
    assert groups[(True, "he", 'אלפבת_בע"מ')].views == 3
    assert groups[(True, '""', None)].views == 4321
    assert groups[(True, "en", None)].views == 100


# --- constraint drift ---------------------------------------------------------------------------


def test_constraints_matching_the_contract_are_left_alone_even_when_redacted() -> None:
    expected = delta_constraints(DomainTotalRecord)
    found = {name.lower(): check for name, check in expected.items()} | {
        "source_url_valid": REDACTED
    }
    assert not constraints_drifted(found, expected)


def test_a_missing_or_changed_constraint_is_drift() -> None:
    expected = delta_constraints(DomainTotalRecord)
    found = {name.lower(): check for name, check in expected.items()}

    assert constraints_drifted({k: v for k, v in found.items() if k != "views_valid"}, expected)
    assert constraints_drifted(found | {"views_valid": "views >= 0"}, expected)
    assert constraints_drifted(found | {"retired_rule": "1 = 1"}, expected)
