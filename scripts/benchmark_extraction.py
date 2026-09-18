"""Spark vs DuckDB: the same hour, the same serverless compute, the same answer (M5-S2).

Extraction needs one thing from an hourly dump: every line grouped by (valid, domain code,
tracked title), which `build_grains` turns into both bronze grains (`wikiflow.extract_spark`).
This script asks that question of two engines:

- **Spark:** `scan_hour`, the code that ingests every hour in this project.
- **DuckDB:** `scan_hour_duckdb` below, the same grouping in wikiflow's engine, reading the same
  file on the volume; and `duckdb-local`, DuckDB on a copy of it on the machine's own disk.

Both answers go through the same `build_grains`, and each run prints a sha256 of its line groups
and of its grains, so "identical output" is a string comparison across every run of both engines.

    # From the laptop: upload this script and a fresh wheel, run each engine `--runs` times as
    # one-time serverless runs (order rotating, one at a time), then print the table.
    uv run --group connect --group duckdb python scripts/benchmark_extraction.py submit \
        --hour 2026-09-15T11 --runs 3

    # Again later, when system.billing.usage has caught up with those runs:
    uv run --group connect --group duckdb python scripts/benchmark_extraction.py report \
        --run-ids <the ids submit printed>

    # What each serverless run executes:
    benchmark_extraction.py measure --engine spark|duckdb|duckdb-local --catalog ... --hour ...

## Measured 2026-09-18

Hour 2026-09-15T11 (7,118,105 lines, 62 MB gzipped), sandbox, 3 runs each, medians:

| Engine | Scan | Peak RSS of the run's process | Run, end to end |
|---|---|---|---|
| Spark, reading the volume | **6.05 s** | 235 MB: the client only. Executors: 5.0 s of task time, 0 bytes spilled | 32 s |
| DuckDB, reading the volume | 9.70 s | 1,080 MB | 36 s |
| DuckDB, on a local copy | **3.07 s**, after a 6.24 s copy | 1,029 MB | 36 s |

Output was **identical across all 9 runs**: one digest pair, the same 2,432 line groups, 1,946
domain rows, 486 company rows and 18,917,973 views that ingestion wrote to bronze for this hour.
DBUs per run: see the story's PR (M5-S2); billing had not caught up when this was written.

What the numbers say:

- **Spark's executors added nothing.** A gzip stream cannot be split, so Spark scanned the hour as
  one task: 5.0 s of task time in 6.05 s of wall clock. Parallelism would need many files, not
  one big one.
- **DuckDB's engine is the faster one; the volume mount is what slows it.** On local disk, DuckDB
  scanned in half Spark's time on the same 4-CPU machine. Reading through `/Volumes`, a
  network-backed mount, cost it 6.24 s for 62 MB, while Spark's executors read cloud storage
  directly.
- **Neither decides anything here.** Every run took 32-36 s end to end, so the scan is at most a
  third of a run, and a whole `ingest_hour` (download, verify, scan, Delta writes, audit) takes
  30-90 s. Extraction stays on Spark: its grains go straight into Delta and Unity Catalog, and a
  second engine in the wheel would buy at most 3 s an hour.
- **Memory is not comparable with wikiflow's 201 MB.** DuckDB held ~800 MB above the process's
  ~250 MB baseline on a 15.6 GB machine. wikiflow measured a different read (four typed columns,
  errors skipped) on a laptop.

How each number is measured:

- **Time:** wall clock around the engine call only (scan the file, collect the groups). Task
  start-up, the wheel install and loading the filter happen before the clock starts, and are
  reported separately as the run's own duration from the Jobs API.
- **Memory:** DuckDB runs inside this Python process, so its cost is the process's peak RSS
  during the scan (VmHWM after resetting it, and 10 ms samples of VmRSS). Spark runs the scan on
  serverless executors that this process cannot see, and `system.query.history` records their
  task time and spill but not memory. For Spark this reports the client's RSS and the spill.
- **DBUs:** `system.billing.usage` by `job_run_id`. Billing is per run, not per task, which is
  why every engine gets runs of its own.
- **Same compute:** every run is a one-time serverless job run in environment 5 with the same
  wheel. DuckDB uses the one machine the Python process is given. Spark sends the scan to
  serverless executors, which is the difference being measured.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from wikiflow.extract_spark import HourGrains, LineGroup, build_grains, raw_path, scan_hour
from wikiflow.plan_hours import parse_hour
from wikiflow.source import HourlyDump
from wikiflow.wikidata import CompanyPage

#: `duckdb-local` is DuckDB on a copy of the file on the machine's own disk.
ENGINES = ("spark", "duckdb", "duckdb-local")
MEDIANS = ("seconds", "copy_seconds", "run_seconds", "rss_peak_mb", "dbus", "spark_task_seconds")
MARKER = "BENCHMARK "
UPLOAD_DIR = "wikiflow-benchmark"
WAREHOUSE = "Serverless Starter Warehouse"

# --- the DuckDB twin of scan_hour ---------------------------------------------------------------

#: A byte a pageviews line should never hold: the column delimiter for reading lines whole.
DELIMITER = "\x01"

#: Lines are read whole, as one column, and split on single spaces like Spark's `split`: CSV
#: quoting is off because titles legally contain `"`. DuckDB's strict mode makes a line that holds
#: DELIMITER (or a bare `\r`, which Spark would treat as a line break) FAIL the scan, rather than
#: be split differently from Spark without a word.
#:
#: Counts must be plain integers (`[+-]?[0-9]+`), as in `group_lines`: DuckDB's own cast would
#: accept `3882.0` and `1e3`, which Spark's ANSI `try_cast` and the contract refuse.
DUCKDB_SCAN = """
WITH lines AS (
    SELECT coalesce(line, '') AS line
    FROM read_csv($path, columns = {'line': 'VARCHAR'}, header = false, delim = $delim,
                  quote = '', escape = '', auto_detect = false)
),
fields AS (SELECT string_split(line, ' ') AS p FROM lines),
typed AS (
    SELECT
        p[1] AS domain_code,
        p[2] AS page_title,
        CASE WHEN regexp_full_match(p[3], '[+-]?[0-9]+') THEN TRY_CAST(p[3] AS BIGINT) END AS views,
        CASE WHEN regexp_full_match(p[4], '[+-]?[0-9]+') THEN TRY_CAST(p[4] AS BIGINT) END
            AS response_bytes,
        len(p) = 4 AS four_fields
    FROM fields
),
flagged AS (
    SELECT *, four_fields AND views IS NOT NULL AND response_bytes IS NOT NULL AS valid FROM typed
)
SELECT
    f.valid,
    CASE WHEN f.valid THEN f.domain_code END AS domain_code,
    CASE WHEN f.valid AND t.tracked THEN f.page_title END AS tracked_title,
    count(*) AS lines,
    sum(CASE WHEN f.valid THEN f.views END) AS views,
    sum(CASE WHEN f.valid THEN f.response_bytes END) AS response_bytes
FROM flagged f
LEFT JOIN tracked t USING (domain_code, page_title)
GROUP BY ALL
"""


def scan_hour_duckdb(connection: Any, path: str, pages: Sequence[CompanyPage]) -> list[LineGroup]:
    """`scan_hour`'s grouping in DuckDB, on a local path (a volume is one, on serverless)."""
    titles = sorted({(page.domain_code, page.page_title) for page in pages})
    # Two list parameters unnested side by side: one statement, not one INSERT per title
    # (wikiflow measured row-at-a-time inserts at ~8 ms a row).
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE tracked AS "
        "SELECT unnest($codes) AS domain_code, unnest($titles) AS page_title, true AS tracked",
        {"codes": [code for code, _ in titles], "titles": [title for _, title in titles]},
    )
    return [
        LineGroup(
            valid=bool(valid),
            domain_code=domain_code,
            tracked_title=tracked_title,
            lines=int(lines),
            views=None if views is None else int(views),
            response_bytes=None if response_bytes is None else int(response_bytes),
        )
        for valid, domain_code, tracked_title, lines, views, response_bytes in connection.execute(
            DUCKDB_SCAN, {"path": path, "delim": DELIMITER}
        ).fetchall()
    ]


# --- what "identical output" compares -----------------------------------------------------------


def digest(rows: Iterable[Any]) -> str:
    """sha256 of rows in a canonical order, so equal digests mean equal rows."""
    canonical = sorted(json.dumps(row, default=str, ensure_ascii=False) for row in rows)
    return hashlib.sha256("\n".join(canonical).encode("utf-8")).hexdigest()


def groups_digest(groups: Sequence[LineGroup]) -> str:
    return digest(dataclasses.astuple(group) for group in groups)


def grains_digest(grains: HourGrains) -> str:
    return digest(
        [["company", *row] for row in grains.company_rows]
        + [["domain", *row] for row in grains.domain_rows]
    )


# --- memory, as far as this process can see it ---------------------------------------------------


def _status_kb(field: str) -> int | None:
    """A `/proc/self/status` field in kB; None off Linux."""
    try:
        text = Path("/proc/self/status").read_text()
    except OSError:
        return None
    found = re.search(rf"^{field}:\s+(\d+) kB", text, re.MULTILINE)
    return int(found.group(1)) if found else None


@dataclasses.dataclass
class Memory:
    before_mb: float | None = None
    peak_mb: float | None = None


@contextmanager
def peak_rss(interval: float = 0.01) -> Iterator[Memory]:
    """This process's peak resident memory while the block runs.

    Linux resets the high-water mark (VmHWM) when 5 is written to clear_refs; where that is not
    allowed, 10 ms samples of VmRSS still bound the peak from below.
    """
    memory = Memory()
    before = _status_kb("VmRSS")
    memory.before_mb = None if before is None else before / 1024
    try:
        Path("/proc/self/clear_refs").write_text("5")
        reset = True
    except OSError:
        reset = False
    sampled = [before or 0]
    done = threading.Event()

    def sample() -> None:
        while not done.wait(interval):
            sampled.append(_status_kb("VmRSS") or 0)

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()
    try:
        yield memory
    finally:
        done.set()
        thread.join()
        high_water = _status_kb("VmHWM") if reset else None
        peak = max([*sampled, high_water or 0])
        memory.peak_mb = peak / 1024 if peak else None


# --- measure: what each serverless run executes --------------------------------------------------


def measure(engine: str, catalog: str, hour: str) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    from wikiflow.reference import load_current

    spark = SparkSession.builder.getOrCreate()
    dump = HourlyDump.for_interval(parse_hour(hour))
    path = raw_path(catalog, dump, "pageviews")
    published = load_current(spark, catalog)
    if published is None:
        raise SystemExit(f"{catalog} has no published company filter")
    pages = published.pages

    context: dict[str, Any] = {"spark_version": spark.version}
    connection: Any = None
    if engine.startswith("duckdb"):
        import duckdb

        connection = duckdb.connect()
        context["duckdb_version"] = duckdb.__version__
        context["duckdb_threads"] = connection.execute(
            "SELECT current_setting('threads')"
        ).fetchone()[0]
        context["duckdb_memory_limit"] = connection.execute(
            "SELECT current_setting('memory_limit')"
        ).fetchone()[0]
    scanned = path  # the grains keep the volume path either way, like an ingested hour
    if engine == "duckdb-local":
        # Separates the engine from the storage: the volume is a network-backed mount, where
        # Spark's executors read cloud storage directly. The copy is timed on its own.
        copy_started = time.perf_counter()
        local = Path(tempfile.mkdtemp()) / Path(path).name
        shutil.copyfile(path, local)
        context["copy_seconds"] = round(time.perf_counter() - copy_started, 3)
        scanned = str(local)

    with peak_rss() as memory:
        started = time.perf_counter()
        if engine == "spark":
            groups = scan_hour(spark, path, pages)
        else:
            groups = scan_hour_duckdb(connection, scanned, pages)
        seconds = time.perf_counter() - started

    # A fixed ingestion time, so that two runs of the same hour build byte-identical grains.
    grains = build_grains(
        groups, pages, dump, source_url=path, ingested_at=dump.interval_start + timedelta(hours=1)
    )
    mem_total = _meminfo_kb("MemTotal")
    return {
        "engine": engine,
        "hour": hour,
        "seconds": round(seconds, 3),
        "rss_before_mb": _round(memory.before_mb),
        "rss_peak_mb": _round(memory.peak_mb),
        "lines": grains.line_count,
        "malformed": grains.rows_malformed,
        "views": grains.total_views,
        "domain_rows": len(grains.domain_rows),
        "company_rows": len(grains.company_rows),
        "groups": len(groups),
        "groups_sha256": groups_digest(groups),
        "grains_sha256": grains_digest(grains),
        "cpus": _cpus(),
        "mem_total_mb": None if mem_total is None else round(mem_total / 1024),
        "cgroup_memory_max": _read_first("/sys/fs/cgroup/memory.max"),
        **context,
    }


def _cpus() -> int | None:
    """CPUs this process may run on (a container's share), not the host's count."""
    affinity = getattr(os, "sched_getaffinity", None)
    return len(affinity(0)) if affinity else os.cpu_count()


def _meminfo_kb(field: str) -> int | None:
    try:
        text = Path("/proc/meminfo").read_text()
    except OSError:
        return None
    found = re.search(rf"^{field}:\s+(\d+) kB", text, re.MULTILINE)
    return int(found.group(1)) if found else None


def _read_first(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 1)


# --- submit and report: from the laptop ----------------------------------------------------------


def _workspace(profile: str) -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=profile)


def upload(w: Any) -> tuple[str, str]:
    """This script and a freshly built wheel, into a folder of the current user's workspace."""
    from databricks.sdk.service.workspace import ImportFormat

    folder = f"/Workspace/Users/{w.current_user.me().user_name}/{UPLOAD_DIR}"
    w.workspace.mkdirs(folder)
    with tempfile.TemporaryDirectory() as out:
        subprocess.run(["uv", "build", "--wheel", "--out-dir", out], check=True)
        (wheel,) = Path(out).glob("*.whl")
        wheel_path = f"{folder}/{wheel.name}"
        with wheel.open("rb") as handle:
            w.workspace.upload(wheel_path, handle, format=ImportFormat.RAW, overwrite=True)
    script_path = f"{folder}/{Path(__file__).name}"
    with Path(__file__).open("rb") as handle:
        w.workspace.upload(script_path, handle, format=ImportFormat.RAW, overwrite=True)
    return script_path, wheel_path


def submit_one(
    w: Any, engine: str, *, script: str, wheel: str, catalog: str, hour: str, label: str
) -> int:
    """One engine, one one-time serverless run; blocks until it ends and returns its run id."""
    import duckdb
    from databricks.sdk.service import compute, jobs

    parameters = ["measure", "--engine", engine, "--catalog", catalog, "--hour", hour]
    run = w.jobs.submit(
        run_name=f"wikiflow_benchmark_{engine}_{label}",
        tasks=[
            jobs.SubmitTask(
                task_key="measure",
                spark_python_task=jobs.SparkPythonTask(python_file=script, parameters=parameters),
                environment_key="benchmark",
                # A retried run would be a fourth sample hiding inside the third (trap 16).
                max_retries=0,
                disable_auto_optimization=True,
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="benchmark",
                spec=compute.Environment(
                    environment_version="5",
                    dependencies=[wheel, f"duckdb=={duckdb.__version__}"],
                ),
            )
        ],
    ).result(timeout=timedelta(minutes=30))
    return int(run.run_id)


def run_result(w: Any, run_id: int) -> dict[str, Any]:
    """The BENCHMARK line a run printed, plus its duration from the Jobs API."""
    run = w.jobs.get_run(run_id)
    (task,) = run.tasks
    output = w.jobs.get_run_output(task.run_id)
    lines = [line for line in (output.logs or "").splitlines() if line.startswith(MARKER)]
    if not lines:
        state = run.state.result_state if run.state else None
        raise SystemExit(f"run {run_id} ({state}) printed no result: {output.error or ''}")
    result: dict[str, Any] = json.loads(lines[-1][len(MARKER) :])
    result["run_id"] = run_id
    result["task_run_id"] = int(task.run_id)
    result["run_seconds"] = round((run.end_time - run.start_time) / 1000)
    return result


def query(w: Any, sql: str) -> list[dict[str, Any]]:
    from databricks.sdk.service.sql import StatementState

    warehouse = next(wh.id for wh in w.warehouses.list() if wh.name == WAREHOUSE)
    response = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse, wait_timeout="50s"
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise SystemExit(f"{response.status.state}: {response.status.error}")
    names = [column.name for column in response.manifest.schema.columns]
    data = response.result.data_array if response.result else None
    return [dict(zip(names, row, strict=True)) for row in data or []]


def system_tables(w: Any, results: list[dict[str, Any]]) -> None:
    """Add DBUs (billing) and Spark's task time and spill (query history) to each result."""
    ids = ", ".join(f"'{r['run_id']}', '{r['task_run_id']}'" for r in results)
    billed = query(
        w,
        "SELECT usage_metadata.job_run_id AS run_id, sum(usage_quantity) AS dbus, "
        "max(usage_end_time) AS billed_to FROM system.billing.usage "
        f"WHERE usage_metadata.job_run_id IN ({ids}) GROUP BY 1",
    )
    work = query(
        w,
        "SELECT coalesce(query_source.job_info.job_run_id, '') AS run_id, "
        "coalesce(query_source.job_info.job_task_run_id, '') AS task_run_id, "
        "sum(total_task_duration_ms) AS task_ms, sum(spilled_local_bytes) AS spilled, "
        "sum(read_bytes) AS read_bytes FROM system.query.history "
        f"WHERE query_source.job_info.job_run_id IN ({ids}) "
        f"OR query_source.job_info.job_task_run_id IN ({ids}) GROUP BY 1, 2",
    )
    for result in results:
        mine = {str(result["run_id"]), str(result["task_run_id"])}
        dbus = [float(row["dbus"]) for row in billed if row["run_id"] in mine]
        result["dbus"] = round(sum(dbus), 4) if dbus else None
        spark = [row for row in work if {row["run_id"], row["task_run_id"]} & mine]
        result["spark_task_seconds"] = (
            round(sum(int(row["task_ms"] or 0) for row in spark) / 1000, 1) if spark else None
        )
        result["spark_spilled_bytes"] = (
            sum(int(row["spilled"] or 0) for row in spark) if spark else None
        )


def median(values: list[Any]) -> Any:
    present = [v for v in values if v is not None]
    return round(statistics.median(present), 4) if len(present) == len(values) else None


def report(w: Any, run_ids: list[int]) -> int:
    results = [run_result(w, run_id) for run_id in run_ids]
    system_tables(w, results)

    columns = [
        "engine", "run_id", "seconds", "copy_seconds", "run_seconds", "rss_peak_mb", "dbus",
        "spark_task_seconds", "spark_spilled_bytes", "lines", "views", "groups_sha256",
    ]  # fmt: skip
    print("| " + " | ".join(columns) + " |")
    print("|" + "---|" * len(columns))
    for r in results:
        print("| " + " | ".join(str(r.get(c))[:16] for c in columns) + " |")

    print("\nMedians:")
    for engine in ENGINES:
        mine = [r for r in results if r["engine"] == engine]
        if not mine:
            continue
        summary = {key: median([r.get(key) for r in mine]) for key in MEDIANS}
        print(f"  {engine} ({len(mine)} runs): {json.dumps(summary)}")
        context = {k: v for k, v in mine[0].items() if k.startswith(("duckdb_", "spark_v"))}
        print(
            f"    compute: cpus {mine[0]['cpus']}, memory {mine[0]['mem_total_mb']} MB, {context}"
        )

    identical = {(r["groups_sha256"], r["grains_sha256"]) for r in results}
    print(
        f"\nOutput: {'IDENTICAL' if len(identical) == 1 else 'DIFFERENT'} across {len(results)} "
        f"runs ({len(identical)} distinct digest pair(s)); lines {results[0]['lines']:,}, "
        f"views {results[0]['views']:,}, domain rows {results[0]['domain_rows']:,}, "
        f"company rows {results[0]['company_rows']:,}"
    )
    return 0 if len(identical) == 1 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    steps = parser.add_subparsers(dest="step", required=True)

    one = steps.add_parser("measure", help="run inside a serverless task")
    one.add_argument("--engine", choices=ENGINES, required=True)
    one.add_argument("--catalog", required=True)
    one.add_argument("--hour", required=True)

    many = steps.add_parser("submit", help="upload, run both engines, report")
    many.add_argument("--hour", required=True)
    many.add_argument("--runs", type=int, default=3)
    many.add_argument("--engines", default=",".join(ENGINES), help="comma-separated")
    many.add_argument("--catalog", default="wikiflow_sbx")
    many.add_argument("--profile", default="DEFAULT")

    again = steps.add_parser("report", help="rebuild the table from earlier run ids")
    again.add_argument("--run-ids", required=True, help="comma-separated")
    again.add_argument("--profile", default="DEFAULT")

    args = parser.parse_args(argv)
    if args.step == "measure":
        print(MARKER + json.dumps(measure(args.engine, args.catalog, args.hour)), flush=True)
        return 0

    w = _workspace(args.profile)
    if args.step == "report":
        return report(w, [int(i) for i in args.run_ids.split(",")])

    script, wheel = upload(w)
    run_ids = []
    stamp = datetime.now().strftime("%Y%m%dT%H%M")
    engines = args.engines.split(",")
    for index in range(args.runs):
        # Rotate which engine goes first, so none always gets the warmer slot.
        shift = index % len(engines)
        for engine in engines[shift:] + engines[:shift]:
            run_id = submit_one(
                w, engine, script=script, wheel=wheel, catalog=args.catalog, hour=args.hour,
                label=f"{stamp}_{index + 1}",
            )  # fmt: skip
            print(f"{engine} run {index + 1}: {run_id}", flush=True)
            run_ids.append(run_id)
    print(f"--run-ids {','.join(map(str, run_ids))}")
    return report(w, run_ids)


if __name__ == "__main__":
    # Serverless runs this file under IPython, where even SystemExit(0) fails the task: exit only
    # to report a failure.
    if code := main():
        sys.exit(code)
