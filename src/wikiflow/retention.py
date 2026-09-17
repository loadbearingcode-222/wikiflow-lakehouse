"""Deleting raw files once they age out of the replay window.

Ported from wikiflow's `retention.py`. Raw dumps are ~60 MB an hour, so keeping them forever
costs ~500 GB a year. Keeping none is worse: the first parsing bug would then cost a
re-download. Seven days is the compromise (wikiflow ADR 0003): long enough to fix a bug and
reprocess every affected hour from raw (M3-S3). Bronze, the marts and `raw.projectviews` are
Delta tables, kept indefinitely, and nothing here can reach them.

This is the one module whose bugs destroy data rather than produce wrong answers, so it is
deliberately conservative. wikiflow's rules, unchanged:

* **Only raw is sweepable:** partitions under `raw/dumps/pageviews/` and `raw/dumps/projectviews/`.
* **An unrecognised path is left alone** and reported, never swept up by a pattern.
* **Expiring everything is refused.** A sweep that would remove every partition means the window
  or the clock is wrong, not that all the data expired at once.
* **A window under one day is refused** (added here): 0 days keeps only today's partitions, so the
  rule above never fires while nearly everything goes.
* **Dry run is the default.** Deleting has to be asked for (a bundle variable only prod sets).

What changes on Databricks is how plan and apply agree. wikiflow listed the bucket twice, once
to plan and once to delete. Here the plan is written to `ops.retention_manifest`, and apply
deletes exactly the partitions that plan recorded for the same job run, re-checking each one.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

SWEEPABLE = ("pageviews", "projectviews")
DEFAULT_RETENTION_DAYS = 7

_DT = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})$")
_HOUR = re.compile(r"^hour=(\d{2})$")


class RetentionSafetyError(RuntimeError):
    """A sweep was refused because it looked wrong rather than merely large."""


def raw_root(catalog: str) -> Path:
    return Path(f"/Volumes/{catalog}/raw/dumps")


@dataclass(frozen=True, slots=True)
class Partition:
    dataset: str
    dt: date
    hour: int
    files: int
    bytes: int

    @property
    def relative(self) -> str:
        return f"{self.dataset}/dt={self.dt}/hour={self.hour:02d}"


def find_partitions(root: Path) -> tuple[list[Partition], list[str]]:
    """Every raw partition under `root`, and every path that did not match the layout."""
    partitions: list[Partition] = []
    unparseable: list[str] = []
    for dataset in SWEEPABLE:
        base = root / dataset
        if not base.is_dir():
            continue
        for day in sorted(base.iterdir()):
            day_match = _DT.match(day.name)
            if not day.is_dir() or day_match is None:
                unparseable.append(f"{dataset}/{day.name}")
                continue
            for hour in sorted(day.iterdir()):
                hour_match = _HOUR.match(hour.name)
                if not hour.is_dir() or hour_match is None:
                    unparseable.append(f"{dataset}/{day.name}/{hour.name}")
                    continue
                files = [f for f in hour.iterdir() if f.is_file()]
                partitions.append(
                    Partition(
                        dataset=dataset,
                        dt=date.fromisoformat(day_match.group(1)),
                        hour=int(hour_match.group(1)),
                        files=len(files),
                        bytes=sum(f.stat().st_size for f in files),
                    )
                )
    return partitions, unparseable


@dataclass(frozen=True, slots=True)
class SweepPlan:
    cutoff: date
    expired: list[Partition]
    retained: int
    unparseable: list[str]

    def manifest(self, *, dry_run: bool) -> str:
        verb = "would delete" if dry_run else "will delete"
        size = sum(p.bytes for p in self.expired)
        lines = [
            f"retention plan: cutoff {self.cutoff} (partitions dated before it expire)",
            f"  {verb}: {len(self.expired)} partitions, {sum(p.files for p in self.expired)} files, {size / 1e6:,.1f} MB",
            f"  retained: {self.retained} partitions",
        ]
        if self.unparseable:
            lines.append(f"  UNPARSEABLE, left alone: {len(self.unparseable)}")
            lines.extend(f"    {path}" for path in self.unparseable[:10])
        lines.extend(f"    {p.relative}" for p in self.expired[:50])
        if len(self.expired) > 50:
            lines.append(f"    ... and {len(self.expired) - 50} more")
        return "\n".join(lines)


def plan_sweep(
    partitions: Iterable[Partition], unparseable: list[str], *, now: datetime, retention_days: int
) -> SweepPlan:
    """Which partitions expire. Raises instead of planning to expire every one."""
    partitions = list(partitions)
    cutoff = (now - timedelta(days=retention_days)).date()
    expired = [p for p in partitions if p.dt < cutoff]
    if partitions and len(expired) == len(partitions):
        raise RetentionSafetyError(
            f"refusing to sweep: all {len(partitions)} partitions are dated before {cutoff} "
            f"(retention {retention_days} day(s)). That means the window or the clock is wrong, "
            "not that every hour expired at once. Nothing was deleted."
        )
    if retention_days < 1:
        # Checked second, so a clock or window that expires everything says so first. Measured in
        # the unit tests: 0 days keeps today's partitions, slips past the rule above, and would
        # still delete every earlier hour a reprocess needs.
        raise RetentionSafetyError(
            f"refusing to sweep: a {retention_days}-day window would delete hours still being "
            f"ingested and every hour a reprocess needs ({len(expired)} of {len(partitions)} "
            "partitions). Nothing was deleted."
        )
    return SweepPlan(cutoff, expired, len(partitions) - len(expired), unparseable)


def delete_partition(root: Path, dataset: str, dt: date, hour: int, *, cutoff: date) -> int:
    """Delete one planned partition's files and its now-empty directories. Returns files deleted.

    Re-checks what the plan already decided, because this is the line that destroys data: the
    dataset must be sweepable and the partition must still be older than the cutoff.
    """
    if dataset not in SWEEPABLE:
        raise RetentionSafetyError(f"refusing to delete outside raw datasets: {dataset!r}")
    if dt >= cutoff:
        raise RetentionSafetyError(
            f"refusing to delete {dataset} dt={dt}: not before cutoff {cutoff}"
        )
    hour_dir = root / dataset / f"dt={dt}" / f"hour={hour:02d}"
    if not hour_dir.is_dir():
        return 0
    deleted = 0
    for file in hour_dir.iterdir():
        if file.is_file():
            file.unlink()
            deleted += 1
    hour_dir.rmdir()
    if not any(hour_dir.parent.iterdir()):
        hour_dir.parent.rmdir()
    return deleted


# --- the manifest -------------------------------------------------------------------------

MANIFEST_SCHEMA = (
    "job_run_id STRING NOT NULL, recorded_at TIMESTAMP NOT NULL, step STRING NOT NULL, "
    "cutoff DATE NOT NULL, dataset STRING NOT NULL, dt DATE NOT NULL, hour INT NOT NULL, "
    "files INT NOT NULL, bytes BIGINT NOT NULL"
)


def manifest_table(catalog: str) -> str:
    return f"{catalog}.ops.retention_manifest"


def write_manifest(spark: Any, catalog: str, rows: list[tuple[Any, ...]]) -> None:
    table = manifest_table(catalog)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({MANIFEST_SCHEMA}) "
        "COMMENT 'Raw partitions each retention run planned to delete, and deleted (append-only)' "
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )
    if rows:
        spark.createDataFrame(rows, schema=MANIFEST_SCHEMA).write.mode("append").saveAsTable(table)


def manifest_rows(
    plan: SweepPlan, *, job_run_id: str, recorded_at: datetime, step: str
) -> list[tuple[Any, ...]]:
    return [
        (job_run_id, recorded_at, step, plan.cutoff, p.dataset, p.dt, p.hour, p.files, p.bytes)
        for p in plan.expired
    ]


def load_planned(spark: Any, catalog: str, job_run_id: str) -> list[dict[str, Any]]:
    """The partitions the plan step of `job_run_id` recorded."""
    rows = spark.sql(
        f"SELECT cutoff, dataset, dt, hour, files, bytes FROM {manifest_table(catalog)} "
        f"WHERE job_run_id = '{job_run_id}' AND step = 'planned' ORDER BY dataset, dt, hour"
    ).collect()
    return [row.asDict() for row in rows]
