"""Recording what each ingest attempt actually did, in `<catalog>.ops.ingest_audit`.

Every attempt at an hour appends one row, success or failure: which job run and attempt it was,
what was downloaded (bytes, lines, checksum), which filter snapshot extraction used, how many
lines survived each stage, and how long it took. Without it, "the pipeline is healthy" is an
opinion.

Ported from wikiflow with its two lessons kept:

- **Data is replaced, audit is appended.** An hour is replaced whole on re-run so it cannot
  double-count (wikiflow ADR 0002). Audit is the opposite: an hour that failed twice and then
  succeeded keeps three rows. wikiflow first keyed its audit files on run id alone, and retries
  of one run overwrote each other. Here the table is `delta.appendOnly`, so Delta itself refuses
  an UPDATE or DELETE, and each attempt is told apart by `{{task.execution_count}}`.
- **The schema is declared, never inferred.** wikiflow's inferred schema typed a success row's
  NULL `error` as INTEGER and a failure's as STRING, and the audit broke on the first failure,
  which is exactly when it is needed. Here `AUDIT_SCHEMA` creates the table and types every
  written row.

Measurements a failed attempt never reached are NULL, not 0: zero lines is a claim, NULL is
"not measured".

From M1-S5 every attempt also reconciles its hour against Wikimedia's own projectviews file,
landed beside the pageviews file, and `verify_attempts` turns a run's audit rows into a verdict:
every planned hour succeeded, reconciled to the view, and - where the same file was ingested
before - reproduced the earlier totals.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields, replace
from datetime import UTC, date, datetime
from typing import Any

from wikiflow.download import DownloadResult
from wikiflow.extract_spark import ExtractedHour
from wikiflow.plan_hours import format_hour, parse_hour
from wikiflow.source import HourlyDump

#: Column -> Delta type, in table order. NOT NULL on what every attempt knows before it starts.
AUDIT_SCHEMA: dict[str, str] = {
    "job_run_id": "STRING NOT NULL",
    "task_run_id": "STRING NOT NULL",
    "task_key": "STRING NOT NULL",
    "attempt": "INT NOT NULL",
    "interval_start_utc": "TIMESTAMP_NTZ NOT NULL",
    "dt": "DATE NOT NULL",
    "hour": "INT NOT NULL",
    "pageviews_url": "STRING NOT NULL",
    "status": "STRING NOT NULL",
    "error": "STRING",
    "filter_snapshot": "STRING",
    "compressed_bytes": "BIGINT",
    "decompressed_bytes": "BIGINT",
    "line_count": "BIGINT",
    "sha256": "STRING",
    "rows_scanned": "BIGINT",
    "rows_malformed": "BIGINT",
    "company_rows": "BIGINT",
    "company_views": "BIGINT",
    "domain_rows": "BIGINT",
    "total_views": "BIGINT",
    "started_at": "TIMESTAMP NOT NULL",
    "finished_at": "TIMESTAMP NOT NULL",
    "duration_seconds": "DOUBLE NOT NULL",
    "company_share": "DOUBLE",
    # Added in M1-S5, so last: existing tables gain them by ALTER TABLE ADD COLUMNS.
    "projectviews_views": "BIGINT",
    "domains_mismatched": "BIGINT",
}
AUDIT_DDL = ", ".join(f"{name} {kind}" for name, kind in AUDIT_SCHEMA.items())

#: Long enough for a Spark error's first lines; the full trace stays in the run's own log.
MAX_ERROR_CHARS = 4000


@dataclass(frozen=True, slots=True)
class RunContext:
    """Which attempt this is, from the job's dynamic value references."""

    job_run_id: str
    task_run_id: str
    task_key: str
    attempt: int


@dataclass(frozen=True, slots=True)
class IngestAudit:
    """One attempt at ingesting one hour."""

    job_run_id: str
    task_run_id: str
    task_key: str
    attempt: int
    interval_start_utc: datetime
    dt: date
    hour: int
    pageviews_url: str
    status: str
    """`running` while in flight; only `success` or `failed` is ever written."""
    started_at: datetime
    finished_at: datetime
    error: str | None = None
    filter_snapshot: str | None = None
    compressed_bytes: int | None = None
    decompressed_bytes: int | None = None
    line_count: int | None = None
    sha256: str | None = None
    rows_scanned: int | None = None
    rows_malformed: int | None = None
    company_rows: int | None = None
    company_views: int | None = None
    domain_rows: int | None = None
    total_views: int | None = None
    projectviews_views: int | None = None
    """Wikimedia's own total for the hour, from its projectviews file."""
    domains_mismatched: int | None = None
    """Domain codes whose views differ from projectviews, a code missing on either side included."""

    @classmethod
    def begin(
        cls, run: RunContext, dump: HourlyDump, *, now: datetime | None = None
    ) -> IngestAudit:
        started = now or datetime.now(UTC)
        return cls(
            job_run_id=run.job_run_id,
            task_run_id=run.task_run_id,
            task_key=run.task_key,
            attempt=run.attempt,
            interval_start_utc=dump.interval_start.replace(tzinfo=None),
            dt=dump.interval_start.date(),
            hour=dump.interval_start.hour,
            pageviews_url=dump.pageviews_url,
            status="running",
            started_at=started,
            finished_at=started,
        )

    def landed(self, download: DownloadResult) -> IngestAudit:
        return replace(
            self,
            compressed_bytes=download.compressed_bytes,
            decompressed_bytes=download.decompressed_bytes,
            line_count=download.line_count,
            sha256=download.sha256,
        )

    def extracted(self, hour: ExtractedHour) -> IngestAudit:
        grains = hour.grains
        return replace(
            self,
            filter_snapshot=hour.filter_snapshot,
            line_count=grains.line_count,
            rows_scanned=grains.rows_scanned,
            rows_malformed=grains.rows_malformed,
            company_rows=len(grains.company_rows),
            company_views=grains.company_views,
            domain_rows=len(grains.domain_rows),
            total_views=grains.total_views,
        )

    def reconciled(self, ours: dict[str, int], theirs: dict[str, int]) -> IngestAudit:
        return replace(
            self,
            projectviews_views=sum(theirs.values()),
            domains_mismatched=count_mismatches(ours, theirs),
        )

    def succeeded(self, *, now: datetime | None = None) -> IngestAudit:
        return replace(self, status="success", finished_at=now or datetime.now(UTC))

    def failed(self, error: BaseException, *, now: datetime | None = None) -> IngestAudit:
        message = f"{type(error).__name__}: {error}"
        return replace(
            self,
            status="failed",
            error=message[:MAX_ERROR_CHARS],
            finished_at=now or datetime.now(UTC),
        )

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def company_share(self) -> float | None:
        if self.company_views is None or not self.total_views:
            return None
        return self.company_views / self.total_views

    def as_row(self) -> tuple[Any, ...]:
        """The row in AUDIT_SCHEMA order. Refuses to write a record still in flight."""
        if self.status not in ("success", "failed"):
            raise ValueError(f"audit status must be success or failed, not {self.status!r}")
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        values |= {"duration_seconds": self.duration_seconds, "company_share": self.company_share}
        if set(values) != set(AUDIT_SCHEMA):
            raise ValueError(
                "IngestAudit and AUDIT_SCHEMA have drifted apart: "
                f"not in schema {sorted(set(values) - set(AUDIT_SCHEMA))}, "
                f"not in record {sorted(set(AUDIT_SCHEMA) - set(values))}"
            )
        return tuple(values[name] for name in AUDIT_SCHEMA)


def audit_table(catalog: str) -> str:
    return f"{catalog}.ops.ingest_audit"


def missing_columns(existing: Sequence[str]) -> list[str]:
    """Column definitions to ADD to a table created from an older AUDIT_SCHEMA, in order."""
    missing = [(name, kind) for name, kind in AUDIT_SCHEMA.items() if name not in set(existing)]
    if any("NOT NULL" in kind for _, kind in missing):
        raise ValueError(f"a NOT NULL audit column cannot be added to existing rows: {missing}")
    return [f"{name} {kind}" for name, kind in missing]


def ensure_audit_table(spark: Any, catalog: str) -> None:
    table = audit_table(catalog)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({AUDIT_DDL}) "
        "COMMENT 'One row per ingest attempt per hour, success or failure (append-only)' "
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )
    if added := missing_columns(spark.table(table).columns):
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(added)})")


def write_audit(spark: Any, catalog: str, record: IngestAudit) -> None:
    row = record.as_row()
    ensure_audit_table(spark, catalog)
    spark.createDataFrame([row], schema=AUDIT_DDL).write.mode("append").saveAsTable(
        audit_table(catalog)
    )


# --- reconciliation against projectviews ------------------------------------------------------


def parse_projectviews(text: str) -> dict[str, int]:
    """Views per domain code from a projectviews file, whose lines are `domain_code - views 0`."""
    totals: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split(" ")
        if len(parts) != 4:
            raise ValueError(f"unexpected projectviews line: {line!r}")
        totals[parts[0]] = totals.get(parts[0], 0) + int(parts[2])
    return totals


def count_mismatches(ours: dict[str, int], theirs: dict[str, int]) -> int:
    return sum(1 for code in set(ours) | set(theirs) if ours.get(code) != theirs.get(code))


# --- verifying a run --------------------------------------------------------------------------


class RunVerificationError(RuntimeError):
    """A run's audit rows do not show every planned hour ingested correctly."""


def load_attempts(spark: Any, catalog: str, hours: Sequence[str]) -> list[dict[str, Any]]:
    """Every audit row, from any run, for the given hours."""
    if not hours:
        return []
    ensure_audit_table(spark, catalog)
    wanted = ", ".join(f"TIMESTAMP_NTZ'{parse_hour(h):%Y-%m-%d %H:%M:%S}'" for h in hours)
    rows = spark.sql(
        f"SELECT * FROM {audit_table(catalog)} WHERE interval_start_utc IN ({wanted})"
    ).collect()
    return [row.asDict() for row in rows]


def verify_attempts(
    planned: Sequence[str], attempts: Sequence[dict[str, Any]], job_run_id: str
) -> list[str]:
    """Problems with a run, from its audit rows; an empty list means the run is verified.

    For each planned hour, this run's latest successful attempt must exist and reconcile with
    projectviews to the view. Every earlier successful attempt that read the same file (same
    sha256) must have produced the same domain totals and, with the same filter snapshot, the
    same company rows: re-ingesting an hour is only safe if it is deterministic.
    """
    problems: list[str] = []
    for label in planned:
        of_hour = [a for a in attempts if _label(a) == label]
        ours = sorted(
            (a for a in of_hour if a["job_run_id"] == job_run_id and a["status"] == "success"),
            key=lambda a: a["finished_at"],
        )
        if not ours:
            problems.append(f"{label}: no successful attempt in run {job_run_id}")
            continue
        latest = ours[-1]
        if (
            latest["domains_mismatched"] != 0
            or latest["projectviews_views"] != latest["total_views"]
        ):
            problems.append(
                f"{label}: does not reconcile with projectviews - "
                f"{latest['domains_mismatched']} domain(s) differ; {latest['total_views']} views "
                f"here, {latest['projectviews_views']} in projectviews"
            )
        for earlier in of_hour:
            if earlier["job_run_id"] == job_run_id or earlier["status"] != "success":
                continue
            if earlier["sha256"] != latest["sha256"]:
                continue
            domains = ("total_views", "domain_rows")
            companies = ("company_rows", "company_views")
            differs = [c for c in domains if earlier[c] != latest[c]]
            if earlier["filter_snapshot"] == latest["filter_snapshot"]:
                differs += [c for c in companies if earlier[c] != latest[c]]
            if differs:
                problems.append(
                    f"{label}: re-ingesting the same file (sha256 {latest['sha256'][:12]}) did not "
                    f"reproduce run {earlier['job_run_id']}: {', '.join(differs)} changed"
                )
    return problems


def _label(attempt: dict[str, Any]) -> str:
    return format_hour(attempt["interval_start_utc"].replace(tzinfo=UTC))
