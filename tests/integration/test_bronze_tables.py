"""Delta enforces the bronze contract itself: the tables are generated from src/wikiflow/contracts.py.

Creates (or re-applies) the real bronze tables in the sandbox catalog, then plants the defects the
contract exists for and watches Delta refuse each one. Planted rows use an hour far outside any
real data, and the one accepted planted row is deleted before the test ends.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from wikiflow.contracts import (
    BRONZE_CONTRACTS,
    DomainTotalRecord,
    columns,
    delta_constraints,
    delta_statements,
)

pytestmark = pytest.mark.integration

TABLE = "wikiflow_sbx.bronze.domain_totals"
PLANT_DT = date(2000, 1, 1)
PLANTED = "source_url = 'planted-by-integration-test'"


def row(**overrides: Any) -> dict[str, Any]:
    return {
        "dt": PLANT_DT,
        "hour": 0,
        "interval_start_utc": datetime(2000, 1, 1, 0),
        "source_url": "planted-by-integration-test",
        "ingested_at": datetime(2026, 9, 17, tzinfo=UTC),
        "domain_code": "en",
        "project": "wikipedia",
        "language": "en",
        "access_method": "desktop",
        "is_parseable": True,
        "views": 3882,
        "response_bytes": 0,
        "distinct_pages": 1,
    } | overrides


def replace_hour(frame: Any, planted: dict[str, Any]) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"dt = DATE'{planted['dt']}' AND hour = {planted['hour']}")
        .saveAsTable(TABLE)
    )


def planted_count(spark: Any) -> int:
    return int(spark.sql(f"SELECT count(*) FROM {TABLE} WHERE {PLANTED}").first()[0])


@pytest.fixture(scope="module", autouse=True)
def bronze_tables(spark: Any) -> None:
    for name, model in BRONZE_CONTRACTS.items():
        for statement in delta_statements(model, f"wikiflow_sbx.bronze.{name}"):
            spark.sql(statement)


def test_the_table_carries_every_generated_constraint(spark: Any) -> None:
    properties = spark.sql(f"SHOW TBLPROPERTIES {TABLE}").collect()
    found = {
        r["key"].removeprefix("delta.constraints.")
        for r in properties
        if r["key"].startswith("delta.constraints.")
    }
    assert found == {name.lower() for name in delta_constraints(DomainTotalRecord)}


def test_a_double_views_column_is_refused_by_delta(spark: Any) -> None:
    """wikiflow's real defect, replayed against the table: views written as DOUBLE 3882.0."""
    frame = spark.createDataFrame([row()], schema=spark.table(TABLE).schema).selectExpr(
        *[
            "CAST(views AS DOUBLE) AS views" if name == "views" else name
            for name in columns(DomainTotalRecord)
        ]
    )
    with pytest.raises(Exception, match=r"(?i)views"):
        replace_hour(frame, row())
    assert planted_count(spark) == 0


@pytest.mark.parametrize(
    ("override", "constraint"),
    [
        ({"views": 0}, "views_valid"),
        ({"hour": 1}, "interval_in_partition"),
        ({"access_method": "mobile-app"}, "access_method_valid"),
        ({"project": None}, "parseable_has_attributes"),
    ],
)
def test_a_row_breaking_a_rule_is_refused_by_its_named_constraint(
    spark: Any, override: dict[str, Any], constraint: str
) -> None:
    planted = row(**override)
    frame = spark.createDataFrame([planted], schema=spark.table(TABLE).schema)
    with pytest.raises(Exception, match=rf"(?i){constraint}"):
        replace_hour(frame, planted)
    assert planted_count(spark) == 0


def test_a_well_formed_hour_is_accepted_and_replaced_not_appended(spark: Any) -> None:
    frame = spark.createDataFrame([row()], schema=spark.table(TABLE).schema)
    replace_hour(frame, row())
    replace_hour(frame, row())
    count = planted_count(spark)
    spark.sql(f"DELETE FROM {TABLE} WHERE {PLANTED}")
    assert count == 1
    assert planted_count(spark) == 0
