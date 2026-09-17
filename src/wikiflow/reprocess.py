"""Rebuilding bronze hours from the raw volume, without contacting Wikimedia.

This is what the 7-day raw window exists for (wikiflow ADR 0003). When extraction changes, or an
hour is lost from bronze, every hour still in the window can be rebuilt from the files already
landed instead of downloaded again.

wikiflow had a separate `python -m wikiflow.reprocess` script with its own audit call. Here
reprocessing is the ingest path with one step swapped: `ingest_hour --source raw` re-verifies the
landed files (this module) where `--source wikimedia` downloads them. Extraction, the contract, the
audit row, the projectviews reconciliation, `verify_run` and the pipeline refresh are all the
same code. A rebuilt hour is recorded like any other attempt, told apart by its task key.

Landed files are re-verified, not trusted. Scanning the gzip to the end re-checks its CRC and
yields the true line count the malformed-line gate needs, and the sha256 lets `verify_run`
confirm the rebuild reproduced every earlier attempt of the same file.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wikiflow.download import DATASETS, DownloadResult, LandedFile, _scan_content
from wikiflow.retention import find_partitions
from wikiflow.source import HourlyDump

_CHUNK = 1024 * 1024


class RawFileMissingError(FileNotFoundError):
    """An hour's raw file is not in the volume: never landed, or past retention."""


def hours(start: datetime, end: datetime) -> Iterator[datetime]:
    """Every hour from start to end, both inclusive. Ported from wikiflow, error included."""
    if end < start:
        raise ValueError(f"end {end:%Y-%m-%dT%H} is before start {start:%Y-%m-%dT%H}")
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def raw_hours(root: Path) -> set[datetime]:
    """Hours for which BOTH raw datasets hold a partition with at least one file."""
    partitions, _ = find_partitions(root)
    held: dict[str, set[datetime]] = {dataset: set() for dataset in DATASETS}
    for partition in partitions:
        if partition.files:
            day = partition.dt
            held[partition.dataset].add(
                datetime(day.year, day.month, day.day, partition.hour, tzinfo=UTC)
            )
    return held["pageviews"] & held["projectviews"]


def _sha256(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def verify_landed(dump: HourlyDump, raw_root: Path) -> dict[str, LandedFile]:
    """The hour's landed files, re-verified exactly as a download is, without downloading."""
    landed = {}
    for dataset, compressed in DATASETS.items():
        path = raw_root / dump.raw_key(dataset)
        if not path.is_file():
            raise RawFileMissingError(
                f"{dump.raw_key(dataset)} is not in the raw volume (never landed, or past "
                "retention); this hour can only be re-ingested from Wikimedia"
            )
        size, sha256 = _sha256(path)
        decompressed, lines = _scan_content(path, compressed=compressed)
        result = DownloadResult(
            url=str(path),
            path=path,
            compressed_bytes=size,
            decompressed_bytes=decompressed,
            line_count=lines,
            sha256=sha256,
        )
        landed[dataset] = LandedFile(dataset, path, result)
    return landed
