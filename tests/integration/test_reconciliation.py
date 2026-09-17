"""ops.reconciliation in sandbox: every hour held on both sides is compared, and none mismatches.

Reads what the last pipeline update published; run `ingest_hours` (which refreshes the pipeline)
in sandbox first. The planted-difference drill that shows the check can fail is recorded in the
M2-S2 PR, because it needs a pipeline update, not a query.
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.integration

CATALOG = "wikiflow_sbx"


def test_every_hour_on_both_sides_is_compared(spark: Any) -> None:
    both = spark.sql(
        f"""
        SELECT count(*) FROM (
          SELECT DISTINCT dt, hour FROM {CATALOG}.bronze.domain_totals
          INTERSECT
          SELECT DISTINCT dt, hour FROM {CATALOG}.raw.projectviews
        )
        """
    ).first()[0]
    compared = spark.sql(f"SELECT count(*) FROM {CATALOG}.ops.reconciliation").first()[0]
    if both == 0:
        pytest.skip("no hour is on both sides yet - run ingest_hours in sandbox first")
    assert compared == both


def test_no_hour_mismatches_by_a_single_view(spark: Any) -> None:
    row = spark.sql(
        f"""
        SELECT count(*) AS hours, sum(domains) AS domains, sum(mismatched_domains) AS mismatched,
               sum(our_total_views) AS ours, sum(their_total_views) AS theirs
        FROM {CATALOG}.ops.reconciliation
        """
    ).first()
    print(f"RECONCILIATION {row.asDict()}")
    assert row["mismatched"] == 0
    assert row["ours"] == row["theirs"]
