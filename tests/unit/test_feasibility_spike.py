"""The spike's pure parts: URLs from a label hour, projectviews parsing, reconciliation."""

from __future__ import annotations

from datetime import datetime

from wikiflow.tasks.feasibility_spike import dump_urls, parse_projectviews, reconcile


def test_urls_follow_the_label_hour_and_its_month_directory() -> None:
    pageviews, projectviews = dump_urls(datetime(2026, 10, 1, 0))
    assert pageviews.endswith("/2026/2026-10/pageviews-20261001-000000.gz")
    assert projectviews.endswith("/2026/2026-10/projectviews-20261001-000000")


def test_projectviews_lines_sum_by_domain() -> None:
    text = "en - 100 0\nen.m - 40 0\nen - 5 0\nbroken line\n"
    assert parse_projectviews(text) == {"en": 105, "en.m": 40}


def test_reconciliation_counts_domains_missing_on_either_side_as_mismatches() -> None:
    result = reconcile({"en": 105, "de": 7}, {"en": 105, "fr": 3})
    assert result == {
        "domains": 3,
        "mismatched_domains": 2,
        "our_total_views": 112,
        "their_total_views": 108,
    }


def test_identical_totals_reconcile_exactly() -> None:
    assert reconcile({"en": 1}, {"en": 1})["mismatched_domains"] == 0
