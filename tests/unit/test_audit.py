"""Audit records: one declared schema for success and failure alike, and nothing inferred."""

from __future__ import annotations

from dataclasses import MISSING, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wikiflow.audit import AUDIT_DDL, AUDIT_SCHEMA, MAX_ERROR_CHARS, IngestAudit, RunContext
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
