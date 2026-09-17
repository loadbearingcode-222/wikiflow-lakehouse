"""The ingest task audits every attempt, success or failure, and never lets the audit hide a failure.

Landing and extraction are faked. The projectviews file is the real committed one, and the fake
extraction's domain totals are built from it, so reconciliation runs on 1,946 real domain codes.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from wikiflow import download, extract_spark
from wikiflow.audit import IngestAudit, RunContext, parse_projectviews
from wikiflow.download import DownloadResult, DownloadVerificationError, LandedFile
from wikiflow.extract_spark import ExtractedHour, LineGroup, MalformedRowsError, build_grains
from wikiflow.source import HourlyDump
from wikiflow.tasks import ingest_hour

FIXTURES = Path(__file__).parents[1] / "fixtures"
PROJECTVIEWS = FIXTURES / "projectviews-20260915-120000"
DUMP = HourlyDump.for_interval(datetime(2026, 9, 15, 11, tzinfo=UTC))
RUN = RunContext("684212", "991307", "ingest_hour", 1)
LINES = 7_118_105


def extraction(views_off_by: int = 0) -> Callable[..., ExtractedHour]:
    """A fake extract_hour whose domain totals are projectviews' own, optionally one view off."""
    theirs = parse_projectviews(PROJECTVIEWS.read_text(encoding="utf-8"))
    first = next(iter(theirs))
    groups = [
        LineGroup(True, code, None, 1, views + (views_off_by if code == first else 0), 0)
        for code, views in theirs.items()
    ]
    # Pads the line count to a real hour's without adding a view.
    groups.append(LineGroup(True, "en", None, LINES - len(groups), 0, 0))

    def extract(
        spark: Any, catalog: str, dump: HourlyDump, *, expected_lines: int
    ) -> ExtractedHour:
        assert expected_lines == LINES, "extraction is checked against the lines landing verified"
        grains = build_grains(
            groups, [], dump, source_url="/Volumes/...", ingested_at=datetime.now(UTC)
        )
        return ExtractedHour(grains, "20260917T093226Z", "/Volumes/...")

    return extract


@pytest.fixture
def audits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[IngestAudit]:
    written: list[IngestAudit] = []

    def land(dump: HourlyDump, raw_root: Path) -> dict[str, LandedFile]:
        result = DownloadResult(dump.pageviews_url, Path("scratch"), 62, 250, LINES, "ab" * 32)
        projectviews = tmp_path / dump.raw_key("projectviews")
        projectviews.parent.mkdir(parents=True)
        shutil.copyfile(PROJECTVIEWS, projectviews)
        return {
            "pageviews": LandedFile("pageviews", tmp_path / dump.raw_key("pageviews"), result),
            "projectviews": LandedFile("projectviews", projectviews, result),
        }

    monkeypatch.setattr(
        ingest_hour, "write_audit", lambda spark, catalog, record: written.append(record)
    )
    monkeypatch.setattr(download, "land_hour", land)
    monkeypatch.setattr(extract_spark, "extract_hour", extraction())
    return written


def test_job_references_fill_the_run_context_and_a_laptop_run_is_marked_local() -> None:
    args = ingest_hour.parse_args(
        ["--catalog", "wikiflow_dev", "--hour", "2026-09-15T11", "--attempt", "3"]
    )
    assert (args.catalog, args.hour, args.attempt) == ("wikiflow_dev", "2026-09-15T11", 3)
    assert (args.job_run_id, args.task_run_id) == ("local", "local")


def test_a_successful_attempt_writes_one_reconciled_success_row(audits: list[IngestAudit]) -> None:
    record = ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)

    assert audits == [record]
    assert (record.status, record.line_count, record.domain_rows) == ("success", LINES, 1_946)
    assert (record.domains_mismatched, record.projectviews_views) == (0, 18_917_973)
    assert record.total_views == 18_917_973


def test_an_hour_that_does_not_reconcile_is_recorded_not_raised(
    audits: list[IngestAudit], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The run fails in verify_run, after every hour of it has been audited."""
    monkeypatch.setattr(extract_spark, "extract_hour", extraction(views_off_by=1))
    record = ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)
    assert (record.status, record.domains_mismatched) == ("success", 1)


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


def test_a_success_row_that_cannot_be_written_leaves_a_failed_row_for_the_attempt(
    audits: list[IngestAudit], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Measured: two iterations raced on ALTER TABLE and one success write lost."""

    def success_write_conflicts(spark: Any, catalog: str, record: IngestAudit) -> None:
        if record.status == "success":
            raise RuntimeError("[DELTA_METADATA_CHANGED] MetadataChangedException")
        audits.append(record)

    monkeypatch.setattr(ingest_hour, "write_audit", success_write_conflicts)
    with pytest.raises(RuntimeError, match="DELTA_METADATA_CHANGED"):
        ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)

    assert [(a.status, a.domains_mismatched) for a in audits] == [("failed", 0)]
    assert audits[0].error is not None and "DELTA_METADATA_CHANGED" in audits[0].error


def test_an_audit_that_cannot_be_written_never_hides_the_real_failure(
    audits: list[IngestAudit], monkeypatch: pytest.MonkeyPatch
) -> None:
    def refused(
        spark: Any, catalog: str, dump: HourlyDump, *, expected_lines: int
    ) -> ExtractedHour:
        raise MalformedRowsError("the real failure")

    def audit_down(spark: Any, catalog: str, record: IngestAudit) -> None:
        raise RuntimeError("[NOT_SUPPORTED] compute is paused")

    monkeypatch.setattr(extract_spark, "extract_hour", refused)
    monkeypatch.setattr(ingest_hour, "write_audit", audit_down)
    with pytest.raises(MalformedRowsError, match="the real failure"):
        ingest_hour.ingest(object(), "wikiflow_sbx", DUMP, RUN)
