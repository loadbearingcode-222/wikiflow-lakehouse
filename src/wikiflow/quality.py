"""Is the warehouse healthy right now: is the data there, recent, plausible, and in the marts?

Ported from wikiflow's `quality.py`. The reconciliation (M2-S2) proves that what arrived is right.
These checks catch the failures that raise no error at all:

* **gaps**: a missing hour. A run that never happened fails nothing, so only a check that looks
  across the range can see the hole.
* **freshness**: ingestion silently stopped. Everything present is correct, and nothing is new.
* **volume_anomaly**: an hour arrived at the wrong size. Each hour is compared with the SAME hour
  of day on other days, because Wikipedia traffic swings about 1.6x across a day, and an
  hour-over-hour check would fire every dawn and dusk until people ignored it.
* **mart_lag**: the marts stopped keeping up with bronze. wikiflow compared its Postgres marts
  with bronze. Here every hour's bronze write time is compared with the write time the gold fact
  carries, so an hour rewritten in bronze but never refreshed into the marts counts too (ADR 0011).

Every check takes an `as_of` time and looks only at hours that had ended by then. Prod passes
now. Sandbox and dev, which hold fixed windows, are checked as of a moment inside them, which is
also how a condition is planted and reset for real.

The pure functions here decide; `load_warehouse` is the only part that reads Spark.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from wikiflow.plan_hours import format_hour, hours_between

PASS, FAIL, SKIPPED = "pass", "fail", "skipped"


class DataQualityError(RuntimeError):
    """At least one check failed; the message names every one."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    status: str
    detail: str
    metric: float | None
    """What was measured: missing hours, age in hours, worst sigma, or lagging hours."""
    threshold: float | None


def hour_floor(moment: datetime) -> datetime:
    return moment.replace(minute=0, second=0, microsecond=0)


def check_gaps(
    present: set[datetime],
    *,
    as_of: datetime,
    window: timedelta,
    freshness: timedelta,
    epoch: datetime,
) -> CheckResult:
    """Hours missing between `window` ago and the freshness horizon.

    Hours newer than the freshness threshold are left to the freshness check. They may simply
    not be published yet (Wikimedia lags up to ~4 h), and a gap alarm on healthy operation is
    how alarms get ignored.
    """
    end = hour_floor(as_of) - freshness
    start = max(epoch, end - window)
    expected = hours_between(start, end)
    missing = [hour for hour in expected if hour not in present]
    if not missing:
        detail = f"no gaps: {len(expected)}/{len(expected)} hours present ({format_hour(start)}..{format_hour(end)})"
    else:
        shown = ", ".join(format_hour(hour) for hour in missing[:12])
        more = "" if len(missing) <= 12 else f" (+{len(missing) - 12} more)"
        detail = f"{len(missing)}/{len(expected)} hours MISSING: {shown}{more}"
    return CheckResult("gaps", FAIL if missing else PASS, detail, float(len(missing)), 0.0)


def check_freshness(latest: datetime | None, *, as_of: datetime, max_age: timedelta) -> CheckResult:
    """How long ago the newest hour held began. The threshold must allow for publication lag."""
    limit = max_age.total_seconds() / 3600
    if latest is None:
        return CheckResult("freshness", FAIL, "no hours held at all", None, limit)
    age = (as_of - latest).total_seconds() / 3600
    status = PASS if age <= limit else FAIL
    detail = f"newest hour {format_hour(latest)}, {age:.1f} h old at {as_of:%Y-%m-%d %H:%M} UTC, limit {limit:.0f} h"
    return CheckResult("freshness", status, detail, round(age, 2), limit)


@dataclass(frozen=True, slots=True)
class Anomaly:
    hour: datetime
    views: int
    expected: float
    deviation: float


def check_volume(
    totals: dict[datetime, int], *, sigma_threshold: float, min_history: int
) -> CheckResult:
    """Hours whose total views deviate from the same hour of day on every other day held.

    An hour is judged only against more than `min_history` samples of its hour of day. With
    fewer, the check says it skipped rather than inventing a spread from two points: a standard
    deviation over two samples is not a signal.
    """
    by_hour_of_day: dict[int, list[tuple[datetime, int]]] = {}
    for hour, views in sorted(totals.items()):
        by_hour_of_day.setdefault(hour.hour, []).append((hour, views))

    anomalies: list[Anomaly] = []
    worst = 0.0
    checked = 0
    for samples in by_hour_of_day.values():
        if len(samples) <= min_history:
            continue
        for hour, views in samples:
            others = [v for other, v in samples if other != hour]
            spread = statistics.stdev(others)
            checked += 1
            if spread == 0:
                continue
            expected = statistics.fmean(others)
            deviation = (views - expected) / spread
            worst = max(worst, abs(deviation))
            if abs(deviation) > sigma_threshold:
                anomalies.append(Anomaly(hour, views, expected, deviation))

    if checked == 0:
        return CheckResult(
            "volume_anomaly",
            SKIPPED,
            f"insufficient history: no hour of day has {min_history} other days to compare with, "
            "so there is no spread to judge against; skipped rather than guessed",
            None,
            sigma_threshold,
        )
    if not anomalies:
        detail = f"no volume anomalies across {checked} hours (worst {worst:.1f} sigma)"
        return CheckResult("volume_anomaly", PASS, detail, round(worst, 2), sigma_threshold)
    top = max(anomalies, key=lambda a: abs(a.deviation))
    detail = (
        f"{len(anomalies)}/{checked} hours anomalous; worst {format_hour(top.hour)} at "
        f"{top.deviation:+.1f} sigma ({top.views:,} views vs {top.expected:,.0f} expected)"
    )
    return CheckResult("volume_anomaly", FAIL, detail, round(worst, 2), sigma_threshold)


def check_mart_lag(
    bronze_written: dict[datetime, int],
    mart_written: dict[datetime, int],
    *,
    now_micros: int,
    max_lag: timedelta,
) -> CheckResult:
    """Hours whose bronze write the gold fact does not carry, written longer than `max_lag` ago.

    Write times are microseconds since the epoch. The grace period covers a run that has
    written bronze and not yet refreshed the pipeline.
    """
    grace = int(max_lag.total_seconds() * 1_000_000)
    lagging = sorted(
        hour
        for hour, written in bronze_written.items()
        if mart_written.get(hour) != written and written <= now_micros - grace
    )
    minutes = max_lag.total_seconds() / 60
    if not lagging:
        detail = f"the gold fact carries every bronze write older than {minutes:.0f} min ({len(bronze_written)} hours)"
        return CheckResult("mart_lag", PASS, detail, 0.0, minutes)
    shown = ", ".join(format_hour(hour) for hour in lagging[:12])
    detail = f"{len(lagging)} hour(s) written to bronze but not in the gold fact after {minutes:.0f} min: {shown}"
    return CheckResult("mart_lag", FAIL, detail, float(len(lagging)), minutes)


# --- Spark side ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Warehouse:
    totals: dict[datetime, int]
    """Total views per hour held (bronze.domain_totals)."""
    bronze_written: dict[datetime, int]
    mart_written: dict[datetime, int]
    now_micros: int


def _hour(row: Any) -> datetime:
    from datetime import UTC

    day = row["dt"]
    return datetime(day.year, day.month, day.day, int(row["hour"]), tzinfo=UTC)


def load_warehouse(spark: Any, catalog: str, *, as_of: datetime) -> Warehouse:
    """What the checks need, restricted to hours that had ended by `as_of`.

    Write times are compared as `unix_micros`, computed in Spark, so no session time zone gets
    between two timestamps that must be equal.
    """
    ended = f"interval_start_utc < TIMESTAMP_NTZ'{hour_floor(as_of):%Y-%m-%d %H:00:00}'"
    totals = {
        _hour(r): int(r["views"])
        for r in spark.sql(
            f"SELECT dt, hour, sum(views) AS views FROM {catalog}.bronze.domain_totals "
            f"WHERE {ended} GROUP BY dt, hour"
        ).collect()
    }
    bronze_written = {
        _hour(r): int(r["written"])
        for r in spark.sql(
            f"SELECT dt, hour, max(unix_micros(ingested_at)) AS written "
            f"FROM {catalog}.bronze.company_pageviews WHERE {ended} GROUP BY dt, hour"
        ).collect()
    }
    fact = f"{catalog}.gold.fct_company_pageviews_hourly"
    mart_written = (
        {
            _hour(r): int(r["written"])
            for r in spark.sql(
                f"SELECT view_date AS dt, view_hour AS hour, max(unix_micros(ingested_at)) AS written "
                f"FROM {fact} WHERE {ended} GROUP BY view_date, view_hour"
            ).collect()
        }
        if spark.catalog.tableExists(fact)
        else {}
    )
    now_micros = int(
        spark.sql("SELECT unix_micros(current_timestamp()) AS now").collect()[0]["now"]
    )
    return Warehouse(totals, bronze_written, mart_written, now_micros)


RESULTS_SCHEMA = (
    "job_run_id STRING NOT NULL, checked_at TIMESTAMP NOT NULL, as_of TIMESTAMP NOT NULL, "
    "check_name STRING NOT NULL, status STRING NOT NULL, detail STRING NOT NULL, "
    "metric DOUBLE, threshold DOUBLE"
)


def results_table(catalog: str) -> str:
    return f"{catalog}.ops.quality_results"


def ensure_results_table(spark: Any, catalog: str) -> None:
    """Created by `plan_hours` too, not only by the first quality run: the dashboard and the
    alerts read this table, and a catalog where the checks had not yet run left the dashboard's
    quality panel failing TABLE_OR_VIEW_NOT_FOUND (dev, M4-S1)."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {results_table(catalog)} ({RESULTS_SCHEMA}) "
        "COMMENT 'One row per quality check per run (append-only); the alerts read the newest' "
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )


def write_results(
    spark: Any,
    catalog: str,
    results: list[CheckResult],
    *,
    job_run_id: str,
    checked_at: datetime,
    as_of: datetime,
) -> None:
    table = results_table(catalog)
    ensure_results_table(spark, catalog)
    rows = [
        (job_run_id, checked_at, as_of, r.check, r.status, r.detail, r.metric, r.threshold)
        for r in results
    ]
    spark.createDataFrame(rows, schema=RESULTS_SCHEMA).write.mode("append").saveAsTable(table)
