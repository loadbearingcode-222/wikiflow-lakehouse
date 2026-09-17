"""Which hours a run ingests: every published hour since the start that bronze does not hold.

There is no stored watermark (wikiflow ADR 0002). The source is immutable and partitioned by
hour, and so is bronze, so "what is new?" is a set difference computed fresh on every run:

    window     every hour from `start` to the last hour that can have ended
    published  hours whose pageviews AND projectviews files Wikimedia lists
    present    hours bronze.domain_totals already holds
    planned    published - present, oldest first, at most `max_hours`

A run that crashed half-way leaves nothing to reset: the hours it wrote are present, the rest are
planned again. Backfill is the same job with an earlier `start` or a larger `max_hours`.

`domain_totals` is the presence marker because it is written last: `write_hour` replaces the
company grain first, so an hour with domain totals has both grains.

Publication is read from the month's directory listing, one request per month in the window,
rather than a HEAD request per hour as wikiflow's sensor did: the listing is a single ~100 KB page
and names every file. Wikimedia publishes an hour ~1-4 h after it ends (wikiflow ADR 0001), so
the newest hours of a window are normally not planned yet, and the next run picks them up.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from wikiflow.download import REQUEST_TIMEOUT_SECONDS, USER_AGENT
from wikiflow.source import BASE_URL, LABEL_OFFSET

HOUR_FORMAT = "%Y-%m-%dT%H"

#: The first hour this project holds: wikiflow's own data starts here, and the M2-S4 parity
#: check compares the two platforms from it.
DEFAULT_START = "2026-09-14T00"

#: A run's hours are capped so that one run's cost stays bounded (ADR 0010: each hour is about a
#: minute of serverless task time). A longer gap simply takes more runs.
DEFAULT_MAX_HOURS = 24

_LISTED = re.compile(r'href="(pageviews|projectviews)-(\d{8})-(\d{2})0000(\.gz)?"')


def parse_hour(text: str) -> datetime:
    """`2026-09-15T11` -> 2026-09-15 11:00 UTC."""
    return datetime.strptime(text, HOUR_FORMAT).replace(tzinfo=UTC)


def format_hour(hour: datetime) -> str:
    return hour.astimezone(UTC).strftime(HOUR_FORMAT)


def last_ended_hour(now: datetime) -> datetime:
    """The newest hour that has fully ended at `now` - the latest one that could be published."""
    floor = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return floor - timedelta(hours=1)


def hours_between(start: datetime, end: datetime) -> list[datetime]:
    """Every hour from `start` to `end`, both included."""
    if end < start:
        return []
    count = int((end - start) / timedelta(hours=1))
    return [start + timedelta(hours=offset) for offset in range(count + 1)]


def listing_urls(start: datetime, end: datetime) -> list[str]:
    """The month directories whose listings name the files for hours `start`..`end`.

    Directories follow the filename LABEL, the end of the hour (ADR 0001), so the last hour of a
    month is listed in the next month's directory.
    """
    months: list[str] = []
    for hour in hours_between(start, end):
        label = hour + LABEL_OFFSET
        url = f"{BASE_URL}/{label:%Y}/{label:%Y-%m}/"
        if url not in months:
            months.append(url)
    return months


def published_hours(listing_html: str) -> set[datetime]:
    """Hours (by the interval they describe) for which BOTH files are listed."""
    seen: dict[str, set[datetime]] = {"pageviews": set(), "projectviews": set()}
    for dataset, day, hour, gz in _LISTED.findall(listing_html):
        if (dataset == "pageviews") != (gz == ".gz"):
            continue  # a stray pageviews-*.txt or projectviews-*.gz is not the file we land
        label = datetime.strptime(f"{day}{hour}", "%Y%m%d%H").replace(tzinfo=UTC)
        seen[dataset].add(label - LABEL_OFFSET)
    return seen["pageviews"] & seen["projectviews"]


@dataclass(frozen=True, slots=True)
class Plan:
    hours: list[datetime]
    """What this run ingests, oldest first."""
    window: int
    published: int
    present: int
    missing: int
    """Published hours bronze lacks, before the per-run cap."""
    newest_published: datetime | None

    def summary(self) -> dict[str, Any]:
        return {
            "planned": [format_hour(hour) for hour in self.hours],
            "window_hours": self.window,
            "published": self.published,
            "present": self.present,
            "missing": self.missing,
            "deferred_by_cap": self.missing - len(self.hours),
            "newest_published": format_hour(self.newest_published)
            if self.newest_published
            else None,
        }


def plan(
    window: Iterable[datetime],
    published: set[datetime],
    present: set[datetime],
    *,
    max_hours: int = DEFAULT_MAX_HOURS,
    replace_present: bool = False,
) -> Plan:
    """Published hours of the window that bronze lacks (or all of them, when replacing)."""
    if max_hours < 1:
        raise ValueError(f"max_hours must be at least 1, got {max_hours}")
    hours = list(window)
    available = [hour for hour in hours if hour in published]
    wanted = available if replace_present else [h for h in available if h not in present]
    return Plan(
        hours=sorted(wanted)[:max_hours],
        window=len(hours),
        published=len(available),
        present=sum(1 for hour in hours if hour in present),
        missing=len(wanted),
        newest_published=max(available, default=None),
    )


def fetch_published(
    start: datetime, end: datetime, *, client: httpx.Client | None = None
) -> set[datetime]:
    """Published hours in `start`..`end`, from one directory listing per month."""
    owns_client = client is None
    client = client or httpx.Client(
        headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=True
    )
    try:
        published: set[datetime] = set()
        for url in listing_urls(start, end):
            response = client.get(url)
            if response.status_code == 404:
                continue  # a month Wikimedia has not started yet
            response.raise_for_status()
            published |= published_hours(response.text)
        return published
    finally:
        if owns_client:
            client.close()


def present_hours(spark: Any, catalog: str, start: datetime, end: datetime) -> set[datetime]:
    """Hours in `start`..`end` that bronze.domain_totals holds."""
    table = f"{catalog}.bronze.domain_totals"
    if not spark.catalog.tableExists(table):
        return set()
    rows = spark.sql(
        f"SELECT DISTINCT dt, hour FROM {table} "
        f"WHERE dt BETWEEN DATE'{start:%Y-%m-%d}' AND DATE'{end:%Y-%m-%d}'"
    ).collect()
    held = {
        datetime(row["dt"].year, row["dt"].month, row["dt"].day, row["hour"], tzinfo=UTC)
        for row in rows
    }
    return {hour for hour in held if start <= hour <= end}
