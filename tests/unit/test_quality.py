"""The four quality checks: each fires on its condition, stays quiet otherwise, and says why."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from wikiflow.plan_hours import hours_between, parse_hour
from wikiflow.quality import (
    FAIL,
    PASS,
    SKIPPED,
    Warehouse,
    check_freshness,
    check_gaps,
    check_mart_lag,
    check_volume,
)
from wikiflow.tasks.quality_checks import evaluate, parse_args

EPOCH = parse_hour("2026-09-14T00")
HELD = set(hours_between(EPOCH, parse_hour("2026-09-16T07")))


def at(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M").replace(tzinfo=UTC)


# --- gaps ---------------------------------------------------------------------------------------


def test_a_complete_window_has_no_gaps_and_recent_hours_are_left_to_freshness() -> None:
    result = check_gaps(
        HELD,
        as_of=at("2026-09-16T15:40"),
        window=timedelta(hours=72),
        freshness=timedelta(hours=8),
        epoch=EPOCH,
    )
    assert (result.status, result.metric) == (PASS, 0.0)
    assert "2026-09-14T00..2026-09-16T07" in result.detail


def test_a_missing_hour_inside_the_window_is_named() -> None:
    result = check_gaps(
        HELD - {parse_hour("2026-09-15T02")},
        as_of=at("2026-09-16T15:40"),
        window=timedelta(hours=72),
        freshness=timedelta(hours=8),
        epoch=EPOCH,
    )
    assert (result.status, result.metric) == (FAIL, 1.0)
    assert "MISSING: 2026-09-15T02" in result.detail


def test_hours_before_the_projects_first_hour_are_never_expected() -> None:
    result = check_gaps(
        HELD,
        as_of=at("2026-09-14T20:00"),
        window=timedelta(hours=72),
        freshness=timedelta(hours=8),
        epoch=EPOCH,
    )
    assert result.status == PASS and "2026-09-14T00..2026-09-14T12" in result.detail


# --- freshness ----------------------------------------------------------------------------------


def test_freshness_passes_at_the_limit_and_fails_past_it() -> None:
    newest = parse_hour("2026-09-16T07")
    assert (
        check_freshness(newest, as_of=at("2026-09-16T15:00"), max_age=timedelta(hours=8)).status
        == PASS
    )
    stale = check_freshness(newest, as_of=at("2026-09-16T15:30"), max_age=timedelta(hours=8))
    assert (stale.status, stale.metric) == (FAIL, 8.5)


def test_an_empty_warehouse_is_not_fresh() -> None:
    assert (
        check_freshness(None, as_of=at("2026-09-16T15:00"), max_age=timedelta(hours=8)).status
        == FAIL
    )


# --- volume anomaly -----------------------------------------------------------------------------


def days_of(hour_of_day: int, views: list[int]) -> dict[datetime, int]:
    return {
        datetime(2026, 9, 14, hour_of_day, tzinfo=UTC) + timedelta(days=day): v
        for day, v in enumerate(views)
    }


def test_fewer_than_three_other_days_is_skipped_not_guessed() -> None:
    totals = days_of(2, [18_000_000, 18_400_000, 17_900_000]) | days_of(
        14, [21_000_000, 20_800_000]
    )
    result = check_volume(totals, sigma_threshold=4.0, min_history=3)
    assert result.status == SKIPPED and "insufficient history" in result.detail


def test_a_half_sized_hour_is_flagged_against_the_same_hour_on_other_days() -> None:
    totals = days_of(2, [18_000_000, 18_400_000, 9_100_000, 17_900_000, 18_200_000])
    result = check_volume(totals, sigma_threshold=4.0, min_history=3)
    assert result.status == FAIL
    assert "2026-09-16T02" in result.detail


def test_the_daily_cycle_is_not_an_anomaly() -> None:
    """Night and afternoon differ by ~1.6x; only the same hour of day is compared."""
    night = days_of(3, [13_000_000, 13_200_000, 12_900_000, 13_100_000])
    afternoon = days_of(15, [21_000_000, 21_300_000, 20_800_000, 21_100_000])
    result = check_volume(night | afternoon, sigma_threshold=4.0, min_history=3)
    assert result.status == PASS
    assert "across 8 hours" in result.detail


# --- mart lag -----------------------------------------------------------------------------------

MINUTE = 60 * 1_000_000
NOW = 1_789_700_000 * 1_000_000


def test_a_bronze_write_the_fact_never_carried_lags_after_the_grace_period() -> None:
    hour = parse_hour("2026-09-15T11")
    bronze = {hour: NOW - 180 * MINUTE}
    fresh = check_mart_lag(
        bronze, {hour: NOW - 180 * MINUTE}, now_micros=NOW, max_lag=timedelta(minutes=120)
    )
    lagging = check_mart_lag(
        bronze, {hour: NOW - 400 * MINUTE}, now_micros=NOW, max_lag=timedelta(minutes=120)
    )
    missing = check_mart_lag(bronze, {}, now_micros=NOW, max_lag=timedelta(minutes=120))

    assert fresh.status == PASS
    assert (lagging.status, lagging.metric) == (FAIL, 1.0)
    assert "2026-09-15T11" in lagging.detail
    assert missing.status == FAIL


def test_a_write_inside_the_grace_period_is_a_run_still_in_progress() -> None:
    hour = parse_hour("2026-09-15T11")
    result = check_mart_lag(
        {hour: NOW - 30 * MINUTE}, {}, now_micros=NOW, max_lag=timedelta(minutes=120)
    )
    assert result.status == PASS


# --- the task -----------------------------------------------------------------------------------


def test_all_four_checks_run_with_the_task_defaults() -> None:
    args = parse_args(["--catalog", "wikiflow_sbx"])
    warehouse = Warehouse(
        totals={hour: 20_000_000 for hour in HELD},
        bronze_written={hour: NOW for hour in HELD},
        mart_written={hour: NOW for hour in HELD},
        now_micros=NOW,
    )
    results = evaluate(args, at("2026-09-16T15:00"), warehouse)

    assert [(r.check, r.status) for r in results] == [
        ("gaps", PASS),
        ("freshness", PASS),
        ("volume_anomaly", SKIPPED),
        ("mart_lag", PASS),
    ]


@pytest.mark.parametrize("flag", ["--freshness-hours", "--gap-window-hours", "--mart-lag-minutes"])
def test_thresholds_are_integers(flag: str) -> None:
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "x", flag, "two"])
