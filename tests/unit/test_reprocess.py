"""Reprocessing rebuilds an hour from files already landed: re-verified, never downloaded."""

from __future__ import annotations

import gzip
import hashlib
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from wikiflow import download, reprocess
from wikiflow.audit import RunContext
from wikiflow.download import DownloadVerificationError
from wikiflow.reprocess import RawFileMissingError, hours, raw_hours, verify_landed
from wikiflow.source import HourlyDump
from wikiflow.tasks import ingest_hour
from wikiflow.tasks import plan_hours as plan_hours_task

FIXTURES = Path(__file__).parents[1] / "fixtures"
SAMPLE = FIXTURES / "pageviews-sample-20260914-120000.gz"
DUMP = HourlyDump.for_interval(datetime(2026, 9, 14, 11, tzinfo=UTC))


def landed_raw(root: Path, dump: HourlyDump = DUMP) -> Path:
    for dataset, source in (
        ("pageviews", SAMPLE),
        ("projectviews", FIXTURES / "projectviews-20260915-120000"),
    ):
        target = root / dump.raw_key(dataset)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    return root


@pytest.mark.unit
def test_reprocess_hours_are_inclusive_and_cross_midnight() -> None:
    """Parked from wikiflow's tests/unit/test_source.py in M1-S1 until this module existed."""
    got = list(hours(datetime(2026, 9, 14, 22, tzinfo=UTC), datetime(2026, 9, 15, 1, tzinfo=UTC)))
    assert [h.hour for h in got] == [22, 23, 0, 1]
    with pytest.raises(ValueError, match="before start"):
        list(hours(datetime(2026, 9, 15, tzinfo=UTC), datetime(2026, 9, 14, tzinfo=UTC)))


def test_landed_files_are_re_verified_like_a_download(tmp_path: Path) -> None:
    landed = verify_landed(DUMP, landed_raw(tmp_path))

    pageviews = landed["pageviews"].download
    assert pageviews.line_count == 4_379
    assert pageviews.sha256 == hashlib.sha256(SAMPLE.read_bytes()).hexdigest()
    assert pageviews.compressed_bytes == SAMPLE.stat().st_size
    assert landed["projectviews"].download.line_count == 1_946


def test_a_truncated_landed_file_is_refused_not_rebuilt(tmp_path: Path) -> None:
    root = landed_raw(tmp_path)
    path = root / DUMP.raw_key("pageviews")
    path.write_bytes(SAMPLE.read_bytes()[: SAMPLE.stat().st_size * 3 // 10])
    with pytest.raises(DownloadVerificationError, match="failed to decompress"):
        verify_landed(DUMP, root)


def test_an_hour_past_retention_says_it_must_come_from_wikimedia(tmp_path: Path) -> None:
    with pytest.raises(RawFileMissingError, match="past retention"):
        verify_landed(DUMP, tmp_path)


def test_only_hours_with_both_raw_files_can_be_reprocessed(tmp_path: Path) -> None:
    root = landed_raw(tmp_path)
    other = HourlyDump.for_interval(datetime(2026, 9, 14, 12, tzinfo=UTC))
    half = root / other.raw_key("pageviews")
    half.parent.mkdir(parents=True)
    with gzip.open(half, "wb") as handle:
        handle.write(b"en Main_Page 1 0\n")

    assert raw_hours(root) == {DUMP.interval_start}


def test_the_raw_source_never_calls_the_downloader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def no_download(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("reprocess must not download")

    def verified(dump: HourlyDump, raw_root: Path) -> Any:
        calls.append(str(raw_root))
        raise RawFileMissingError("stop here")

    monkeypatch.setattr(download, "land_hour", no_download)
    monkeypatch.setattr(reprocess, "verify_landed", verified)
    monkeypatch.setattr(ingest_hour, "write_audit", lambda *args: None)

    with pytest.raises(RawFileMissingError):
        ingest_hour.ingest(
            object(), "wikiflow_sbx", DUMP, RunContext("1", "2", "reprocess_hour", 1), source="raw"
        )
    assert calls == [str(Path("/Volumes/wikiflow_sbx/raw/dumps"))]


def test_both_tasks_default_to_wikimedia_and_accept_raw() -> None:
    assert (
        ingest_hour.parse_args(["--catalog", "c", "--hour", "2026-09-14T11"]).source == "wikimedia"
    )
    assert plan_hours_task.parse_args(["--catalog", "c", "--source", "raw"]).source == "raw"
