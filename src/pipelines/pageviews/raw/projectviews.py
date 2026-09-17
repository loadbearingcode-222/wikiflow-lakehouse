"""raw.projectviews: Wikimedia's own per-project hourly totals, the reconciliation oracle.

Auto Loader streams each landed `projectviews-*` file (M1-S2 lands one beside every pageviews
file) into a Delta table, one row per line. The raw volume keeps files for 7 days (wikiflow
ADR 0003); this table keeps the oracle for as long as bronze keeps the hours it checks.

It is used ONLY to check bronze (M2-S2's `ops.reconciliation`), never as a modelling input:
reconciling against an input would make the check circular.

Written in Python, not SQL, for one reason. The volume path has to name the catalog, and a
`${catalog}` reference reads the pipeline's `configuration`, which is replaced whenever a job
starts the update (CLAUDE.md trap 5). Python can read `pipelines.catalog` instead, which the
runtime sets from the pipeline's own `catalog:` field.

Details that matter:
- `pathGlobFilter` admits only complete file names. `land_hour` copies to `<name>.partial` and
  then renames, so a half-copied file never matches.
- A re-landed hour rewrites a file under the same name. Auto Loader does not re-ingest a path it
  has seen (`cloudFiles.allowOverwrites` stays false), and the source file is immutable, so
  re-landing never duplicates rows.
- `dt` and `hour` come from the hive path, which names the hour the data describes (ADR 0001).
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()
assert spark is not None, "pipeline source files run inside a pipeline's Spark session"
# The pipeline's own `catalog:` field, set by the runtime (not by `configuration`). Measured
# 2026-09-17 in sandbox, where the obvious alternatives are both wrong:
#   spark.catalog.currentCatalog()          refused: PY4J_BLOCKED_API
#   SELECT current_catalog() at load time   'workspace', the workspace default, not wikiflow_sbx
CATALOG = spark.conf.get("pipelines.catalog")
LANDED = f"/Volumes/{CATALOG}/raw/dumps/projectviews/"


@dp.table(
    name="raw.projectviews",
    comment=(
        "Wikimedia projectviews, one row per line of each landed file: the oracle bronze "
        "domain totals reconcile against (M2-S1). Loaded by Auto Loader from raw.dumps."
    ),
    cluster_by=["dt", "hour"],
)
@dp.expect_all_or_fail(
    {
        "four_fields": "size(split(line, ' ')) = 4",
        "views_is_a_count": "views IS NOT NULL AND views >= 0",
        "hour_from_path": "dt IS NOT NULL AND hour BETWEEN 0 AND 23",
    }
)
def projectviews() -> DataFrame:
    assert spark is not None
    lines = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("pathGlobFilter", "projectviews-????????-??0000")
        .load(LANDED)
    )
    path = F.col("_metadata.file_path")
    return lines.select(
        F.to_date(F.regexp_extract(path, r"/dt=(\d{4}-\d{2}-\d{2})/", 1)).alias("dt"),
        F.regexp_extract(path, r"/hour=(\d{2})/", 1).cast("int").alias("hour"),
        F.expr("get(split(value, ' '), 0)").alias("domain_code"),
        F.expr("try_cast(get(split(value, ' '), 2) AS BIGINT)").alias("views"),
        F.col("value").alias("line"),
        path.alias("source_file"),
        F.current_timestamp().alias("loaded_at"),
    )
