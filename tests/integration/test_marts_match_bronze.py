"""Every hour in silver and the gold fact equals bronze, write time included (ADR 0011).

With no watermark, what keeps the marts honest is that each hour's rows, views and `ingested_at`
match bronze after a refresh. A rewritten old hour that failed to reach the marts would show here
as a stale `ingested_at`. Run after `ingest_hours` (which refreshes the pipeline) in sandbox.
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.integration

CATALOG = "wikiflow_sbx"

PAIRS = {
    "company grain": (
        f"SELECT dt AS d, hour AS h, count(*) AS n, sum(count_views) AS v, max(ingested_at) AS w "
        f"FROM {CATALOG}.bronze.company_pageviews GROUP BY dt, hour",
        [
            f"SELECT view_date AS d, view_hour AS h, count(*) AS n, sum(views) AS v, max(ingested_at) AS w "
            f"FROM {CATALOG}.{table} GROUP BY view_date, view_hour"
            for table in ("silver.stg_company_pageviews", "gold.fct_company_pageviews_hourly")
        ],
    ),
    "domain grain": (
        f"SELECT dt AS d, hour AS h, count(*) AS n, sum(views) AS v, max(ingested_at) AS w "
        f"FROM {CATALOG}.bronze.domain_totals GROUP BY dt, hour",
        [
            f"SELECT view_date AS d, view_hour AS h, count(*) AS n, sum(views) AS v, max(ingested_at) AS w "
            f"FROM {CATALOG}.silver.stg_domain_totals GROUP BY view_date, view_hour"
        ],
    ),
}


def by_hour(spark: Any, sql: str) -> dict[tuple[Any, int], tuple[int, int, Any]]:
    return {(r["d"], r["h"]): (r["n"], r["v"], r["w"]) for r in spark.sql(sql).collect()}


@pytest.mark.parametrize("grain", list(PAIRS))
def test_each_hour_of_the_marts_equals_bronze_including_write_time(spark: Any, grain: str) -> None:
    bronze_sql, mart_sqls = PAIRS[grain]
    bronze = by_hour(spark, bronze_sql)
    if not bronze:
        pytest.skip("bronze is empty - run ingest_hours in sandbox first")
    for mart_sql in mart_sqls:
        mart = by_hour(spark, mart_sql)
        stale = {
            hour: (mart.get(hour), expected)
            for hour, expected in bronze.items()
            if mart.get(hour) != expected
        }
        print(f"{grain}: {len(bronze)} hours, {len(stale)} differ")
        assert not stale
        assert set(mart) == set(bronze)
