"""The shrink guard against the real sandbox filter: a redirect outage is refused, current untouched.

Replays wikiflow ADR 0007's drill. Wikidata answered but every redirect lookup came back empty,
which is exactly the published filter with its redirect rows removed. Requires a filter already
published to wikiflow_sbx (run `company_pages_refresh` in sandbox first).
"""

from __future__ import annotations

from typing import Any

import pytest

from wikiflow.reference import ReferenceShrinkError, load_current, publish, tables

pytestmark = pytest.mark.integration
CATALOG = "wikiflow_sbx"


def latest_version(spark: Any, table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def test_a_filter_that_lost_its_redirects_is_refused_and_nothing_is_written(spark: Any) -> None:
    current = load_current(spark, CATALOG)
    if current is None:
        pytest.skip("no filter published to wikiflow_sbx yet - run company_pages_refresh first")

    articles_only = [page for page in current.pages if page.match_type == "sitelink"]
    assert len(articles_only) < len(current.pages), "the published filter has no redirects"

    snapshots, current_table = tables(CATALOG)
    before = {t: latest_version(spark, t) for t in (snapshots, current_table)}

    with pytest.raises(ReferenceShrinkError, match="rows fell from") as refused:
        publish(spark, CATALOG, articles_only)
    print(f"REFUSED: {refused.value}")

    after = {t: latest_version(spark, t) for t in (snapshots, current_table)}
    assert after == before, "a refused publish must not commit to either table"
    assert load_current(spark, CATALOG) == current
