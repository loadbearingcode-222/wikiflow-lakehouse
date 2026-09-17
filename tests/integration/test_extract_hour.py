"""M1-S4 on real serverless Spark: the scan equals its plain-Python specification, a real hour
reconciles with Wikimedia's own projectviews to the view, and a re-run replaces, never appends.

The two uploaded files go to `raw/dumps/_integration/` in the sandbox volume and are removed at
the end. The real hour (2026-09-15T11) must already be landed in wikiflow_sbx, e.g. by running
`ingest_hours` in sandbox; its bronze rows are rewritten here, which a re-run is allowed to do.
"""

from __future__ import annotations

import csv
import gzip
import io
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from wikiflow.extract_spark import (
    ExtractedHour,
    LineGroup,
    bronze_table,
    build_grains,
    extract_hour,
    group_lines,
    raw_path,
    scan_hour,
)
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

pytestmark = pytest.mark.integration

CATALOG = "wikiflow_sbx"
FIXTURES = Path(__file__).parents[1] / "fixtures"
SCRATCH = f"/Volumes/{CATALOG}/raw/dumps/_integration"
HOUR = HourlyDump.for_interval(datetime(2026, 9, 15, 11, tzinfo=UTC))

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
]


@pytest.fixture(scope="module")
def workspace() -> Any:
    sdk = pytest.importorskip("databricks.sdk")
    return sdk.WorkspaceClient(profile="DEFAULT")


@pytest.fixture(scope="module")
def upload(workspace: Any) -> Iterator[Any]:
    uploaded: list[str] = []

    def put(name: str, content: bytes) -> str:
        target = f"{SCRATCH}/{name}"
        workspace.files.upload(target, io.BytesIO(content), overwrite=True)
        uploaded.append(target)
        return target

    yield put
    for target in uploaded:
        workspace.files.delete(target)


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


def test_the_spark_scan_of_the_sample_equals_its_python_specification(
    spark: Any, upload: Any
) -> None:
    pages = fixture_pages()
    sample = FIXTURES / "pageviews-sample-20260914-120000.gz"
    path = upload(sample.name, sample.read_bytes())

    in_spark = scan_hour(spark, path, pages)
    with gzip.open(sample, "rt", encoding="utf-8") as lines:
        in_python = group_lines(lines, pages)
    assert as_set(in_spark) == as_set(in_python)

    result = build_grains(
        in_spark,
        pages,
        HourlyDump.for_interval(datetime(2026, 9, 14, 11, tzinfo=UTC)),
        source_url=path,
        ingested_at=datetime.now(UTC),
        expected_lines=4_379,
    )
    counts = (len(result.company_rows), result.company_views, len(result.domain_rows))
    assert (counts, result.total_views) == ((379, 5_138, 320), 15_255)


def test_awkward_lines_group_the_same_in_spark_and_python(spark: Any, upload: Any) -> None:
    """ANSI mode is on in serverless Spark: one bad field must yield NULL, not fail the scan."""
    content = gzip.compress(("\n".join(AWKWARD_LINES) + "\n").encode("utf-8"))
    path = upload("awkward-lines.gz", content)
    pages = [
        CompanyPage("Q312", "Apple Inc.", "enwiki", "en", "wikipedia", "Apple_Inc.", "en", "desktop"),
        CompanyPage("Q312", "Apple Inc.", "hewiki", "he", "wikipedia", 'אלפבת_בע"מ', "he", "desktop"),
    ]  # fmt: skip

    in_spark = as_set(scan_hour(spark, path, pages))
    assert in_spark == as_set(group_lines(AWKWARD_LINES, pages))
    malformed = [g for g in in_spark if not g.valid]
    assert [g.lines for g in malformed] == [6]


# --- the real hour ----------------------------------------------------------------------------


def hour_predicate(dump: HourlyDump) -> str:
    return f"dt = DATE'{dump.partition_date}' AND hour = {int(dump.partition_hour)}"


def latest_version(spark: Any, table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def hour_rows(spark: Any, table: str) -> list[tuple[Any, ...]]:
    frame = spark.table(table).where(hour_predicate(HOUR)).drop("ingested_at")
    return sorted(tuple(row) for row in frame.collect())


@pytest.fixture(scope="module")
def first_run(spark: Any) -> ExtractedHour:
    try:
        spark.read.text(raw_path(CATALOG, HOUR, "pageviews")).limit(1).collect()
    except Exception as exc:
        pytest.skip(f"{HOUR.partition_path} is not landed in {CATALOG}: {type(exc).__name__}")
    return extract_hour(spark, CATALOG, HOUR)


def test_the_real_hour_reconciles_with_projectviews_to_the_view(
    spark: Any, first_run: ExtractedHour
) -> None:
    from pyspark.sql import functions as F

    theirs = (
        spark.read.text(raw_path(CATALOG, HOUR, "projectviews"))
        .selectExpr("split(value, ' ') AS p")
        .selectExpr("get(p, 0) AS domain_code", "CAST(get(p, 2) AS BIGINT) AS their_views")
        .groupBy("domain_code")
        .agg(F.sum("their_views").alias("their_views"))
    )
    ours = (
        spark.table(bronze_table(CATALOG, "domain_totals"))
        .where(hour_predicate(HOUR))
        .selectExpr("domain_code", "views AS our_views")
    )
    compared = ours.join(theirs, "domain_code", "full_outer").agg(
        F.count(F.lit(1)).alias("domains"),
        F.sum(F.when(~F.col("our_views").eqNullSafe(F.col("their_views")), 1).otherwise(0)).alias(
            "mismatched"
        ),
        F.sum("our_views").alias("our_total"),
        F.sum("their_views").alias("their_total"),
    )
    result = compared.first().asDict()
    print(f"RECONCILED {HOUR.partition_path}: {result}")

    assert result["mismatched"] == 0
    assert result["our_total"] == result["their_total"] == first_run.grains.total_views
    assert result["domains"] == len(first_run.grains.domain_rows)


def test_a_rerun_gives_identical_rows_and_one_replace_commit_per_table(
    spark: Any, first_run: ExtractedHour
) -> None:
    tables = [bronze_table(CATALOG, name) for name in ("company_pageviews", "domain_totals")]
    before = {table: (latest_version(spark, table), hour_rows(spark, table)) for table in tables}

    second = extract_hour(spark, CATALOG, HOUR)

    assert second.grains.total_views == first_run.grains.total_views
    for table in tables:
        version, rows = before[table]
        commits = (
            spark.sql(f"DESCRIBE HISTORY {table}")
            .where(f"version > {version}")
            .select("version", "operation", "operationParameters")
            .collect()
        )
        print(f"RERUN {table}: {len(rows)} rows, commits {[tuple(c) for c in commits]}")
        assert [(c["operation"], c["operationParameters"]["mode"]) for c in commits] == [
            ("WRITE", "Overwrite")
        ]
        assert commits[0]["operationParameters"]["predicate"] == hour_predicate(HOUR)
        assert hour_rows(spark, table) == rows
