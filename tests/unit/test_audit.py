"""Audit records: one declared schema for success and failure alike, and nothing inferred."""

from __future__ import annotations

from dataclasses import MISSING, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wikiflow.audit import (
    AUDIT_DDL,
    AUDIT_SCHEMA,
    MAX_ERROR_CHARS,
    IngestAudit,
    RunContext,
    count_mismatches,
    missing_columns,
    parse_projectviews,
    verify_attempts,
)
from wikiflow.download import DownloadResult
from wikiflow.extract_spark import ExtractedHour, HourGrains
from wikiflow.source import HourlyDump

DUMP = HourlyDump.for_interval(datetime(2026, 9, 15, 11, tzinfo=UTC))
RUN = RunContext(job_run_id="684212", task_run_id="991307", task_key="ingest_hour", attempt=2)
STARTED = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)

DOWNLOAD = DownloadResult(
    url=DUMP.pageviews_url,
    path=Path("scratch"),
    compressed_bytes=62_266_533,
    decompressed_bytes=250_000_000,
    line_count=7_118_105,
    sha256="ab" * 32,
)
EXTRACTED = ExtractedHour(
    grains=HourGrains(
        company_rows=[("row",)] * 412,
        domain_rows=[("row",)] * 1_946,
        line_count=7_118_105,
        rows_scanned=7_118_105,
        rows_malformed=0,
        company_views=5_021,
        total_views=18_917_973,
    ),
    filter_snapshot="20260917T040517Z",
    source_url="/Volumes/wikiflow_sbx/raw/dumps/pageviews/...",
)


def row_of(record: IngestAudit) -> dict[str, object]:
    return dict(zip(AUDIT_SCHEMA, record.as_row(), strict=True))


def test_a_success_row_carries_the_attempt_the_download_and_the_extraction() -> None:
    record = (
        IngestAudit.begin(RUN, DUMP, now=STARTED)
        .landed(DOWNLOAD)
        .extracted(EXTRACTED)
        .succeeded(now=STARTED + timedelta(seconds=75))
    )
    row = row_of(record)

    assert row["status"] == "success" and row["error"] is None
    assert (row["job_run_id"], row["attempt"]) == ("684212", 2)
    assert (row["dt"], row["hour"], row["interval_start_utc"]) == (
        datetime(2026, 9, 15).date(),
        11,
        datetime(2026, 9, 15, 11),
    )
    assert row["pageviews_url"] == DUMP.pageviews_url
    assert (row["compressed_bytes"], row["sha256"]) == (62_266_533, "ab" * 32)
    assert (row["company_rows"], row["domain_rows"], row["total_views"]) == (412, 1_946, 18_917_973)
    assert row["filter_snapshot"] == "20260917T040517Z"
    assert row["duration_seconds"] == 75.0
    assert row["company_share"] == pytest.approx(5_021 / 18_917_973)


def test_a_failed_download_is_audited_with_what_was_not_measured_left_null() -> None:
    error = RuntimeError("peer closed connection without sending complete message body")
    row = row_of(IngestAudit.begin(RUN, DUMP, now=STARTED).failed(error, now=STARTED))

    assert row["status"] == "failed"
    assert row["error"] == f"RuntimeError: {error}"
    measured = ("compressed_bytes", "line_count", "rows_scanned", "total_views", "company_share")
    assert [row[name] for name in measured] == [None] * len(measured)


def test_a_refused_extraction_keeps_the_landing_numbers() -> None:
    record = IngestAudit.begin(RUN, DUMP, now=STARTED).landed(DOWNLOAD)
    row = row_of(record.failed(ValueError("x" * 10_000), now=STARTED))

    assert row["line_count"] == 7_118_105 and row["rows_scanned"] is None
    assert isinstance(row["error"], str) and len(row["error"]) == MAX_ERROR_CHARS


def test_an_attempt_still_in_flight_is_never_written() -> None:
    with pytest.raises(ValueError, match="success or failed"):
        IngestAudit.begin(RUN, DUMP).as_row()


def test_nullable_columns_are_exactly_the_measurements_an_attempt_may_not_reach() -> None:
    """The DDL and the record agree on what may be NULL; a drift here would fail a real write."""
    optional = {f.name for f in fields(IngestAudit) if f.default is not MISSING}
    nullable = {name for name, kind in AUDIT_SCHEMA.items() if "NOT NULL" not in kind}

    assert nullable == optional | {"company_share"}
    assert AUDIT_DDL.startswith("job_run_id STRING NOT NULL, task_run_id STRING NOT NULL")


# --- reconciliation (M1-S5) -------------------------------------------------------------------

PROJECTVIEWS = Path(__file__).parents[1] / "fixtures" / "projectviews-20260915-120000"


def test_projectviews_parse_to_wikimedias_own_totals() -> None:
    theirs = parse_projectviews(PROJECTVIEWS.read_text(encoding="utf-8"))
    assert (len(theirs), sum(theirs.values()), theirs['""']) == (1_946, 18_917_973, 5_383)


def test_a_domain_missing_on_either_side_or_one_view_off_is_a_mismatch() -> None:
    theirs = {"en": 10, "de": 5, '""': 3}
    assert count_mismatches(dict(theirs), theirs) == 0
    assert count_mismatches(theirs | {"en": 11}, theirs) == 1
    assert count_mismatches({"en": 10, "de": 5}, theirs) == 1
    assert count_mismatches(theirs | {"xx": 1}, theirs) == 1


def test_a_table_from_the_m1_s4_schema_gains_only_the_new_nullable_columns() -> None:
    m1_s4 = [
        name for name in AUDIT_SCHEMA if name not in ("projectviews_views", "domains_mismatched")
    ]
    assert missing_columns(m1_s4) == ["projectviews_views BIGINT", "domains_mismatched BIGINT"]
    assert missing_columns(list(AUDIT_SCHEMA)) == []
    with pytest.raises(ValueError, match="NOT NULL"):
        missing_columns(m1_s4[1:])


# --- verifying a run --------------------------------------------------------------------------

HOUR = "2026-09-15T11"


def attempt(run: str, **overrides: object) -> dict[str, object]:
    return {
        "job_run_id": run,
        "interval_start_utc": datetime(2026, 9, 15, 11),
        "status": "success",
        "finished_at": STARTED,
        "sha256": "ab" * 32,
        "filter_snapshot": "20260917T093226Z",
        "total_views": 18_917_973,
        "projectviews_views": 18_917_973,
        "domains_mismatched": 0,
        "domain_rows": 1_946,
        "company_rows": 486,
        "company_views": 5_614,
    } | overrides


def test_a_reconciled_hour_that_reproduces_earlier_runs_verifies() -> None:
    attempts = [
        attempt("run-2", status="failed", finished_at=STARTED - timedelta(minutes=6)),
        attempt("run-2"),
        attempt("run-1"),
        attempt("run-0", filter_snapshot="20260916T023000Z", company_rows=480, company_views=5_600),
    ]
    assert verify_attempts([HOUR], attempts, "run-2") == []


def test_a_planned_hour_without_a_success_in_this_run_fails_the_run() -> None:
    attempts = [attempt("run-2", status="failed"), attempt("run-1")]
    assert verify_attempts([HOUR, "2026-09-15T12"], attempts, "run-2") == [
        "2026-09-15T11: no successful attempt in run run-2",
        "2026-09-15T12: no successful attempt in run run-2",
    ]


def test_an_hour_that_does_not_reconcile_fails_the_run() -> None:
    attempts = [attempt("run-2", domains_mismatched=1, total_views=18_917_974)]
    (problem,) = verify_attempts([HOUR], attempts, "run-2")
    assert "does not reconcile" in problem and "18917974 views here" in problem


def test_re_ingesting_the_same_file_must_reproduce_the_earlier_totals() -> None:
    attempts = [attempt("run-2", company_views=5_615), attempt("run-1")]
    (problem,) = verify_attempts([HOUR], attempts, "run-2")
    assert "did not reproduce run run-1: company_views changed" in problem

    new_file = [attempt("run-2", sha256="cd" * 32, total_views=1), attempt("run-1")]
    assert "does not reconcile" in verify_attempts([HOUR], new_file, "run-2")[0]
    assert len(verify_attempts([HOUR], new_file, "run-2")) == 1, "a different file is not compared"
