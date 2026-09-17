"""The ingest task audits every attempt, success or failure, and never swallows a failure."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from wikiflow import download, extract_spark
from wikiflow.audit import IngestAudit, RunContext
from wikiflow.download import DownloadResult, DownloadVerificationError, LandedFile
from wikiflow.extract_spark import ExtractedHour, HourGrains, MalformedRowsError
from wikiflow.source import HourlyDump
from wikiflow.tasks import ingest_hour

DUMP = HourlyDump.for_interval(datetime(2026, 9, 15, 11, tzinfo=UTC))
RUN = RunContext("684212", "991307", "ingest_hour", 1)
LINES = 7_118_105


def landed(dump: HourlyDump, raw_root: Path) -> dict[str, LandedFile]:
    result = DownloadResult(dump.pageviews_url, Path("scratch"), 62, 250, LINES, "ab" * 32)
    return {"pageviews": LandedFile("pageviews", raw_root / dump.raw_key("pageviews"), result)}


def extracted(spark: Any, catalog: str, dump: HourlyDump, *, expected_lines: int) -> ExtractedHour:
    assert expected_lines == LINES, "extraction is checked against the lines landing verified"
    grains = HourGrains([], [("row",)], LINES, LINES, 0, 0, 18_917_973)
    return ExtractedHour(grains, "20260917T040517Z", "/Volumes/...")


@pytest.fixture
def audits(monkeypatch: pytest.MonkeyPatch) -> list[IngestAudit]:
    written: list[IngestAudit] = []
    monkeypatch.setattr(
        ingest_hour, "write_audit", lambda spark, catalog, record: written.append(record)
    )
    monkeypatch.setattr(download, "land_hour", landed)
    monkeypatch.setattr(extract_spark, "extract_hour", extracted)
    return written


def test_job_references_fill_the_run_context_and_a_laptop_run_is_marked_local() -> None:
    args = ingest_hour.parse_args(
        ["--catalog", "wikiflow_dev", "--hour", "2026-09-15T11", "--attempt", "3"]
    )
    assert (args.catalog, args.hour, args.attempt) == ("wikiflow_dev", "2026-09-15T11", 3)
    assert (args.job_run_id, args.task_run_id) == ("local", "local")


def test_a_successful_attempt_writes_one_success_row(audits: list[IngestAudit]) -> None:
    record = ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)

    assert audits == [record]
    assert (record.status, record.line_count, record.domain_rows) == ("success", LINES, 1)


def test_a_failed_download_is_audited_and_re_raised(
    audits: list[IngestAudit], monkeypatch: pytest.MonkeyPatch
) -> None:
    def truncated(dump: HourlyDump, raw_root: Path) -> dict[str, LandedFile]:
        raise DownloadVerificationError("expected 62,266,533 bytes but wrote 18,679,960")

    monkeypatch.setattr(download, "land_hour", truncated)
    with pytest.raises(DownloadVerificationError):
        ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)

    assert [(a.status, a.compressed_bytes) for a in audits] == [("failed", None)]
    assert audits[0].error is not None and audits[0].error.startswith("DownloadVerificationError")


def test_a_refused_extraction_is_audited_with_its_landing_numbers(
    audits: list[IngestAudit], monkeypatch: pytest.MonkeyPatch
) -> None:
    def refused(
        spark: Any, catalog: str, dump: HourlyDump, *, expected_lines: int
    ) -> ExtractedHour:
        raise MalformedRowsError("2,000,000 of 7,118,105 lines did not parse")

    monkeypatch.setattr(extract_spark, "extract_hour", refused)
    with pytest.raises(MalformedRowsError):
        ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)

    assert [(a.status, a.line_count, a.rows_scanned) for a in audits] == [("failed", LINES, None)]
