"""Delta refuses to rewrite audit history: `ops.ingest_audit` is append-only by table property.

Uses the rows real `ingest_hours` runs wrote to wikiflow_sbx. Nothing is planted, because a planted
row in an append-only table could never be removed. The UPDATE tried here sets every column to its
own value, so it would change no data even if Delta accepted it.
"""

from __future__ import annotations

from typing import Any

import pytest

from wikiflow.audit import audit_table, ensure_audit_table

pytestmark = pytest.mark.integration

CATALOG = "wikiflow_sbx"
TABLE = audit_table(CATALOG)


def row_count(spark: Any) -> int:
    return int(spark.sql(f"SELECT count(*) FROM {TABLE}").first()[0])


def latest_version(spark: Any) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 1").first()["version"])


@pytest.fixture(scope="module")
def audited(spark: Any) -> int:
    ensure_audit_table(spark, CATALOG)
    rows = row_count(spark)
    if rows == 0:
        pytest.skip(f"{TABLE} is empty - run ingest_hours in sandbox first")
    return rows


def test_the_table_is_append_only_by_property(spark: Any, audited: int) -> None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {TABLE}").collect()
    assert {row["key"]: row["value"] for row in rows}.get("delta.appendOnly") == "true"


def test_an_audit_update_is_refused_and_history_is_unchanged(spark: Any, audited: int) -> None:
    before = latest_version(spark)

    with pytest.raises(Exception, match=r"(?i)append") as refused:
        spark.sql(f"UPDATE {TABLE} SET error = error").collect()
    print(f"REFUSED: {str(refused.value).splitlines()[0]}")

    assert row_count(spark) == audited
    assert latest_version(spark) == before
