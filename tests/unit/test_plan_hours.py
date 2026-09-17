"""Planning a run: published minus present, from Wikimedia's directory listings, oldest first."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from wikiflow.plan_hours import (
    fetch_published,
    format_hour,
    hours_between,
    last_ended_hour,
    listing_urls,
    parse_hour,
    plan,
    published_hours,
)
from wikiflow.tasks.plan_hours import parse_args


def hour(text: str) -> datetime:
    return parse_hour(text)


def listing(*names: str) -> str:
    """The shape of the real listing (checked 2026-09-17): one anchor per file."""
    rows = [f'<a href="{name}">{name}</a>   17-Sep-2026 10:07   57908393' for name in names]
    return '<html><body><pre><a href="../">../</a>\n' + "\n".join(rows) + "\n</pre></body></html>"


def test_an_hour_is_published_only_when_both_of_its_files_are_listed() -> None:
    html = listing(
        "pageviews-20260917-070000.gz",
        "projectviews-20260917-070000",
        "pageviews-20260917-080000.gz",  # projectviews for 08:00 not out yet
        "projectviews-20260917-090000.gz",  # wrong suffix: not the file we land
        "pageviews-20260917-090000.gz",
    )
    # Labels are the END of the hour (ADR 0001): the 07:00 label is the 06:00 hour.
    assert published_hours(html) == {hour("2026-09-17T06")}


def test_the_last_hour_of_a_month_is_listed_in_the_next_months_directory() -> None:
    assert listing_urls(hour("2026-09-30T22"), hour("2026-09-30T23")) == [
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-09/",
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-10/",
    ]
    html = listing("pageviews-20261001-000000.gz", "projectviews-20261001-000000")
    assert published_hours(html) == {hour("2026-09-30T23")}


def test_windows_are_inclusive_and_end_at_the_last_hour_that_has_ended() -> None:
    assert last_ended_hour(datetime(2026, 9, 17, 11, 43, tzinfo=UTC)) == hour("2026-09-17T10")
    assert [
        format_hour(h) for h in hours_between(hour("2026-09-15T22"), hour("2026-09-16T01"))
    ] == [
        "2026-09-15T22",
        "2026-09-15T23",
        "2026-09-16T00",
        "2026-09-16T01",
    ]
    assert hours_between(hour("2026-09-16T01"), hour("2026-09-15T22")) == []


WINDOW = hours_between(hour("2026-09-14T00"), hour("2026-09-14T05"))
PUBLISHED = set(WINDOW[:5])  # 05:00 not published yet
PRESENT = {WINDOW[1], WINDOW[3]}


def test_the_plan_is_published_hours_bronze_lacks_oldest_first() -> None:
    result = plan(reversed(WINDOW), PUBLISHED, PRESENT)

    assert [format_hour(h) for h in result.hours] == [
        "2026-09-14T00",
        "2026-09-14T02",
        "2026-09-14T04",
    ]
    assert result.summary() | {"planned": None} == {
        "planned": None,
        "window_hours": 6,
        "published": 5,
        "present": 2,
        "missing": 3,
        "deferred_by_cap": 0,
        "newest_published": "2026-09-14T04",
    }


def test_the_cap_defers_the_newest_hours_to_the_next_run() -> None:
    result = plan(WINDOW, PUBLISHED, PRESENT, max_hours=2)
    assert [format_hour(h) for h in result.hours] == ["2026-09-14T00", "2026-09-14T02"]
    assert result.summary()["deferred_by_cap"] == 1


def test_replacing_present_hours_plans_every_published_hour_and_nothing_unpublished() -> None:
    result = plan(WINDOW, PUBLISHED, PRESENT, replace_present=True)
    assert result.hours == WINDOW[:5]


def test_a_fully_ingested_window_plans_nothing() -> None:
    assert plan(WINDOW, PUBLISHED, PUBLISHED).hours == []
    with pytest.raises(ValueError, match="at least 1"):
        plan(WINDOW, PUBLISHED, PRESENT, max_hours=0)


def test_published_hours_come_from_one_listing_per_month_and_a_future_month_is_skipped() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if "2026-10" in str(request.url):
            return httpx.Response(404)
        return httpx.Response(
            200, text=listing("pageviews-20260930-230000.gz", "projectviews-20260930-230000")
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    published = fetch_published(hour("2026-09-30T22"), hour("2026-09-30T23"), client=client)

    assert published == {hour("2026-09-30T22")}
    assert len(requested) == 2


def test_task_defaults_plan_everything_since_the_projects_first_hour() -> None:
    args = parse_args(["--catalog", "wikiflow"])
    assert (args.start, args.end, args.max_hours, args.replace_present) == (
        "2026-09-14T00",
        "",
        24,
        "false",
    )
