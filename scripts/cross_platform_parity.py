"""Same numbers on two platforms: wikiflow's DuckDB marts against this project's, EXCEPT ALL both ways.

    uv run --group connect --group parity python scripts/cross_platform_parity.py \
        --duckdb "C:/Users/longt/data engineer/wikiflow/dbt/warehouse/wikiflow.duckdb"

wikiflow's warehouse was built before its filter gained redirects (its ADR 0007), so its marts
carry sitelink views only and have no `match_type` column. The comparison therefore holds this
project to wikiflow's own scope:

- **Hours:** exactly the hours wikiflow's fact holds. All of them must be in bronze here, or the
  script stops.
- **Rows:** `match_type = 'sitelink'`, for the (company, domain, title) triples in wikiflow's filter
  (tests/fixtures/company_pages.csv).
- **SQL:** each aggregate mart is rebuilt from that fact by the pipeline's OWN committed SQL
  (src/pipelines/pageviews/gold/*.sql), with its source tables re-pointed. What is compared is the
  ported logic, not a hand-written copy of it.
- **Columns:** those both marts have. Write times differ by platform and are left out; timestamps
  are compared as `yyyy-MM-dd HH:mm:ss` text.

`dim_wiki` and the traffic totals do not depend on the filter, so they compare on everything.
Exits 1 if any mart differs.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parents[1]
GOLD = ROOT / "src" / "pipelines" / "pageviews" / "gold"
FILTER = ROOT / "tests" / "fixtures" / "company_pages.csv"

MARTS = [
    "fct_company_pageviews_hourly",
    "agg_company_share_of_traffic",
    "agg_company_daily",
    "dim_company",
    "dim_wiki",
]

#: Pipeline table name -> the parity view that stands in for it.
REPOINT = {
    "gold.fct_company_pageviews_hourly": "parity_fct",
    "silver.stg_company_pageviews": "parity_fct",
    "silver.stg_domain_totals": "parity_domains",
}


def mart_query(name: str) -> str:
    """The SELECT of a gold mart's first statement, re-pointed at the parity views."""
    text = (GOLD / f"{name}.sql").read_text(encoding="utf-8")
    body = re.split(r"(?m)^AS$", text, maxsplit=1)[1].split(";")[0]
    for table, view in REPOINT.items():
        body = body.replace(table, view)
    return body


def wikiflow_marts(duckdb_path: str) -> dict[str, tuple[list[str], list[tuple[Any, ...]]]]:
    """(columns, rows) per mart, with HUGEINT as BIGINT and timestamps as text."""
    import duckdb

    con = duckdb.connect(duckdb_path, read_only=True)
    marts = {}
    for name in MARTS:
        described = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'main_marts' AND table_name = ? ORDER BY ordinal_position",
            [name],
        ).fetchall()
        select = []
        for column, kind in described:
            if kind == "HUGEINT":
                select.append(f"CAST({column} AS BIGINT) AS {column}")
            elif kind.startswith("TIMESTAMP"):
                select.append(f"strftime({column}, '%Y-%m-%d %H:%M:%S') AS {column}")
            elif kind == "DATE":
                select.append(f"CAST({column} AS VARCHAR) AS {column}")
            else:
                select.append(column)
        rows = con.execute(f"SELECT {', '.join(select)} FROM main_marts.{name}").fetchall()
        marts[name] = ([c for c, _ in described], rows)
    return marts


def spark_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    return "STRING"


def to_frame(spark: Any, columns: list[str], rows: list[tuple[Any, ...]]) -> Any:
    kinds = [
        next((spark_type(row[i]) for row in rows if row[i] is not None), "STRING")
        for i in range(len(columns))
    ]
    schema = ", ".join(f"{c} {k}" for c, k in zip(columns, kinds, strict=True))
    return spark.createDataFrame(rows, schema=schema)


def normalised(frame: Any, columns: list[str]) -> Any:
    """Dates and timestamps as text, so both platforms compare on the same representation."""
    from pyspark.sql import functions as F

    kinds = dict(frame.dtypes)
    out = []
    for column in columns:
        if kinds[column] in ("timestamp", "timestamp_ntz"):
            out.append(F.date_format(column, "yyyy-MM-dd HH:mm:ss").alias(column))
        elif kinds[column] == "date":
            out.append(F.col(column).cast("string").alias(column))
        else:
            out.append(F.col(column))
    return frame.select(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cross_platform_parity")
    parser.add_argument("--duckdb", required=True, help="wikiflow's dbt/warehouse/wikiflow.duckdb")
    parser.add_argument("--catalog", default="wikiflow_sbx")
    args = parser.parse_args(argv)

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()
    wikiflow = wikiflow_marts(args.duckdb)

    _, fct_rows = wikiflow["fct_company_pageviews_hourly"]
    hours = sorted({(row[0], row[1]) for row in fct_rows})
    spark.createDataFrame(
        hours, schema="view_date STRING, view_hour BIGINT"
    ).createOrReplaceTempView("parity_hours")
    held = spark.sql(
        f"SELECT count(*) FROM parity_hours h JOIN (SELECT DISTINCT CAST(dt AS STRING) AS view_date, "
        f"hour AS view_hour FROM {args.catalog}.bronze.domain_totals) b USING (view_date, view_hour)"
    ).collect()[0][0]
    print(f"HOURS wikiflow {len(hours)} ({hours[0]} .. {hours[-1]}), held here {held}")
    if held != len(hours):
        print("STOP: ingest every one of wikiflow's hours first")
        return 1

    with FILTER.open(newline="", encoding="utf-8") as handle:
        tracked = {(r["qid"], r["domain_code"], r["page_title"]) for r in csv.DictReader(handle)}
    spark.createDataFrame(
        sorted(tracked), schema="company_qid STRING, domain_code STRING, page_title STRING"
    ).createOrReplaceTempView("parity_filter")

    fct = spark.sql(
        f"""
        SELECT f.* FROM {args.catalog}.gold.fct_company_pageviews_hourly f
        JOIN parity_hours h ON CAST(f.view_date AS STRING) = h.view_date AND f.view_hour = h.view_hour
        JOIN parity_filter t USING (company_qid, domain_code, page_title)
        WHERE f.match_type = 'sitelink'
        """
    )
    fct.createOrReplaceTempView("parity_fct")
    spark.sql(
        f"""
        SELECT d.* FROM {args.catalog}.silver.stg_domain_totals d
        JOIN parity_hours h ON CAST(d.view_date AS STRING) = h.view_date AND d.view_hour = h.view_hour
        """
    ).createOrReplaceTempView("parity_domains")

    out_of_scope = spark.sql(
        f"""
        SELECT match_type, count(*) AS rows, sum(views) AS views
        FROM {args.catalog}.gold.fct_company_pageviews_hourly f
        JOIN parity_hours h ON CAST(f.view_date AS STRING) = h.view_date AND f.view_hour = h.view_hour
        LEFT ANTI JOIN parity_fct p USING (view_date, view_hour, company_qid, domain_code, page_title)
        GROUP BY match_type
        """
    ).collect()
    print(f"OUT OF WIKIFLOW'S SCOPE (same hours): {[r.asDict() for r in out_of_scope]}")

    failures = 0
    for name in MARTS:
        columns, rows = wikiflow[name]
        theirs = to_frame(spark, columns, rows)
        ours = fct if name == "fct_company_pageviews_hourly" else spark.sql(mart_query(name))
        shared = [c for c in columns if c in ours.columns]
        ours_n, theirs_n = normalised(ours, shared), normalised(theirs, shared)
        only_here = ours_n.exceptAll(theirs_n)
        only_there = theirs_n.exceptAll(ours_n)
        counts = (ours_n.count(), theirs_n.count(), only_here.count(), only_there.count())
        dropped = [c for c in columns if c not in shared]
        print(
            f"PARITY {name}: here {counts[0]}, wikiflow {counts[1]}, only here {counts[2]}, "
            f"only wikiflow {counts[3]}; compared {len(shared)} columns"
            + (f" (not here: {dropped})" if dropped else "")
        )
        if counts[2] or counts[3]:
            failures += 1
            for label, frame in (("only here", only_here), ("only wikiflow", only_there)):
                for row in frame.limit(5).collect():
                    print(f"    {label}: {row.asDict()}")
    print("RESULT " + ("identical" if not failures else f"{failures} mart(s) differ"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
