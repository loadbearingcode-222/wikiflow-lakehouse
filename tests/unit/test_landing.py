"""Landing an hour: nothing reaches the raw volume until every file is verified.

Uses the real committed pageviews sample, so "cut at 30%" is a real gzip stream truncated
exactly where a dropped connection would leave it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from wikiflow.download import DownloadVerificationError, land_hour
from wikiflow.source import HourlyDump

FIXTURES = Path(__file__).parents[1] / "fixtures"
PAGEVIEWS = (FIXTURES / "pageviews-sample-20260914-120000.gz").read_bytes()
PROJECTVIEWS = b"en - 3283165 0\nen.m - 4720588 0\n"
DUMP = HourlyDump.for_interval(datetime(2026, 9, 14, 11, tzinfo=UTC))


def serve(pageviews: bytes | int = 200, projectviews: bytes | int = 200) -> httpx.Client:
    """A client answering the two dump URLs; an int is served as that HTTP status."""

    def body(item: bytes | int, full: bytes) -> httpx.Response:
        if isinstance(item, int) and item != 200:
            return httpx.Response(item)
        content = full if isinstance(item, int) else item
        # Content-Length always announces the FULL file, as the server would.
        return httpx.Response(200, content=content, headers={"Content-Length": str(len(full))})

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(".gz"):
            return body(pageviews, PAGEVIEWS)
        return body(projectviews, PROJECTVIEWS)

    return httpx.Client(transport=httpx.MockTransport(handler))


def files_under(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


@pytest.mark.unit
def test_an_hour_lands_both_files_in_the_hive_layout_of_the_hour_it_describes(
    tmp_path: Path,
) -> None:
    landed = land_hour(DUMP, tmp_path, client=serve(), sleep=lambda _: None)

    assert files_under(tmp_path) == [
        "pageviews/dt=2026-09-14/hour=11/pageviews-20260914-120000.gz",
        "projectviews/dt=2026-09-14/hour=11/projectviews-20260914-120000",
    ]
    assert landed["pageviews"].download.line_count == 4_379
    assert landed["pageviews"].path.read_bytes() == PAGEVIEWS


@pytest.mark.unit
def test_a_file_cut_at_30_percent_is_refused_before_anything_is_written(tmp_path: Path) -> None:
    cut = PAGEVIEWS[: int(len(PAGEVIEWS) * 0.30)]
    with pytest.raises(DownloadVerificationError, match="truncated"):
        land_hour(DUMP, tmp_path, client=serve(pageviews=cut), sleep=lambda _: None)
    assert files_under(tmp_path) == []


@pytest.mark.unit
def test_a_good_pageviews_file_is_not_published_when_projectviews_fails(tmp_path: Path) -> None:
    with pytest.raises(httpx.HTTPStatusError, match="404"):
        land_hour(DUMP, tmp_path, client=serve(projectviews=404), sleep=lambda _: None)
    assert files_under(tmp_path) == []


@pytest.mark.unit
def test_relanding_an_hour_leaves_one_file_per_dataset(tmp_path: Path) -> None:
    partition = tmp_path / "pageviews" / "dt=2026-09-14" / "hour=11"
    partition.mkdir(parents=True)
    (partition / "pageviews-20260914-120000.gz.partial").write_bytes(b"left by a crash")

    for _ in range(2):
        land_hour(DUMP, tmp_path, client=serve(), sleep=lambda _: None)

    assert files_under(tmp_path) == [
        "pageviews/dt=2026-09-14/hour=11/pageviews-20260914-120000.gz",
        "projectviews/dt=2026-09-14/hour=11/projectviews-20260914-120000",
    ]
