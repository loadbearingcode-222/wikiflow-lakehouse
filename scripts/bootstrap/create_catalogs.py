"""Create the three catalogs with SQL, once. Safe to re-run: every statement is IF NOT EXISTS.

Why a script and not the bundle: this Free Edition workspace runs on Default Storage, where the
Unity Catalog API that `bundle deploy` uses cannot create a catalog ("Metastore storage root URL
does not exist"), while SQL DDL can. Schemas, volumes and everything else are declared in the
bundle and deploy normally into these catalogs.

Why a one-time serverless job run: SQL DDL needs compute either way, and a job run is the path
the NOAA project proved on this workspace. It also avoids waiting on the single SQL warehouse
Free Edition allows. Note that both paths stop when Free Edition pauses compute for the account:
on 2026-09-17 the warehouse answered "Cannot create the resource, please try again later" and job
submission answered "Triggering new runs for organization ... is currently disabled temporarily".
Creating the three catalogs in Catalog Explorer needs no compute and is an equivalent fallback.

Usage (from the repo root, with the Databricks CLI authenticated):
    uv run python scripts/bootstrap/create_catalogs.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

CATALOGS = {
    "wikiflow_sbx": "wikiflow sandbox - deployed from the laptop; wipe freely.",
    "wikiflow_dev": "wikiflow dev - deployed by CI on every merge to main.",
    "wikiflow": "wikiflow prod - deployed by CI when a release is published.",
}
ENVIRONMENT_VERSION = "5"

RUN_SOURCE = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
for name, comment in {catalogs!r}.items():
    escaped = comment.replace("'", "''")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {{name}} COMMENT '{{escaped}}'")
found = sorted(r[0] for r in spark.sql("SHOW CATALOGS").collect() if r[0].startswith("wikiflow"))
print("catalogs present:", ", ".join(found))
missing = sorted(set({catalogs!r}) - set(found))
if missing:
    raise SystemExit(f"catalogs still missing: {{missing}}")
"""


def cli(*args: str, parse: bool = True) -> Any:
    # encoding="utf-8": on Windows a pipe would decode as cp1252 (CLAUDE.md trap 2).
    command = ["databricks", *args, *(["-o", "json"] if parse else [])]
    result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        raise SystemExit(f"databricks {' '.join(args[:2])} failed:\n{result.stderr.strip()}")
    return json.loads(result.stdout) if parse and result.stdout.strip() else result.stdout


def main() -> int:
    user = cli("current-user", "me")["userName"]
    remote = f"/Workspace/Users/{user}/.wikiflow-bootstrap/create_catalogs_run.py"

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "create_catalogs_run.py"
        local.write_text(RUN_SOURCE.format(catalogs=CATALOGS), encoding="utf-8")
        cli("workspace", "mkdirs", remote.rsplit("/", 1)[0], parse=False)
        cli("workspace", "import", remote, "--file", str(local), "--format", "AUTO",
            "--overwrite", parse=False)  # fmt: skip

    submit = {
        "run_name": "wikiflow bootstrap: create catalogs",
        "tasks": [
            {
                "task_key": "create_catalogs",
                "spark_python_task": {"python_file": remote},
                "environment_key": "default",
            }
        ],
        "environments": [
            {"environment_key": "default", "spec": {"environment_version": ENVIRONMENT_VERSION}}
        ],
    }
    try:
        run = cli("jobs", "submit", "--json", json.dumps(submit), "--timeout", "20m")
    finally:
        cli("workspace", "delete", remote, parse=False)

    state = run.get("status", {}).get("termination_details", {}).get("code")
    for task in run.get("tasks", []):
        output = cli("jobs", "get-run-output", str(task["run_id"]))
        print(output.get("logs", "").strip() or output.get("error", ""))
    print(f"bootstrap run {run.get('run_id')}: {state}  {run.get('run_page_url', '')}")
    return 0 if state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
