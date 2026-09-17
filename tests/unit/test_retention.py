"""Raw retention refuses what looks wrong and deletes only what a plan named, older than the cutoff."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from wikiflow.retention import (
    RetentionSafetyError,
    delete_partition,
    find_partitions,
    manifest_rows,
    plan_sweep,
)
from wikiflow.tasks.raw_retention import parse_args

NOW = datetime(2026, 9, 17, 16, 0, tzinfo=UTC)


def land(root: Path, dataset: str, day: str, hour: int, size: int = 10) -> Path:
    directory = root / dataset / f"dt={day}" / f"hour={hour:02d}"
    directory.mkdir(parents=True, exist_ok=True)
    name = "pageviews-x.gz" if dataset == "pageviews" else "projectviews-x"
    (directory / name).write_bytes(b"x" * size)
    return directory


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    for day in ("2026-09-09", "2026-09-10", "2026-09-14", "2026-09-17"):
        for dataset in ("pageviews", "projectviews"):
            land(tmp_path, dataset, day, 0)
    land(tmp_path, "pageviews", "2026-09-09", 1)
    (tmp_path / "pageviews" / "not-a-partition").mkdir()
    (tmp_path / "spike").mkdir()  # outside the sweepable datasets: never even listed
    (tmp_path / "spike" / "pageviews-20260915-120000.gz").write_bytes(b"x")
    return tmp_path


def test_partitions_are_found_per_dataset_and_odd_paths_reported_not_swept(raw: Path) -> None:
    partitions, unparseable = find_partitions(raw)
    assert len(partitions) == 9
    assert unparseable == ["pageviews/not-a-partition"]
    assert {p.dataset for p in partitions} == {"pageviews", "projectviews"}


def test_seven_days_expires_only_partitions_dated_before_the_cutoff(raw: Path) -> None:
    plan = plan_sweep(*find_partitions(raw), now=NOW, retention_days=7)
    assert plan.cutoff == date(2026, 9, 10)
    assert sorted(p.relative for p in plan.expired) == [
        "pageviews/dt=2026-09-09/hour=00",
        "pageviews/dt=2026-09-09/hour=01",
        "projectviews/dt=2026-09-09/hour=00",
    ]
    assert plan.retained == 6


def test_a_config_that_expires_everything_is_refused(raw: Path) -> None:
    with pytest.raises(RetentionSafetyError, match="all 9 partitions"):
        plan_sweep(*find_partitions(raw), now=NOW, retention_days=-1)


def test_a_zero_day_window_is_refused_although_todays_partitions_would_survive(raw: Path) -> None:
    with pytest.raises(RetentionSafetyError, match=r"0-day window .*\(7 of 9 partitions\)"):
        plan_sweep(*find_partitions(raw), now=NOW, retention_days=0)


def test_an_empty_raw_volume_plans_nothing_rather_than_refusing(tmp_path: Path) -> None:
    plan = plan_sweep(*find_partitions(tmp_path), now=NOW, retention_days=7)
    assert (plan.expired, plan.retained) == ([], 0)


def test_deleting_a_planned_partition_removes_its_files_and_empty_directories(raw: Path) -> None:
    assert delete_partition(raw, "pageviews", date(2026, 9, 9), 0, cutoff=date(2026, 9, 10)) == 1
    assert not (raw / "pageviews" / "dt=2026-09-09" / "hour=00").exists()
    assert (raw / "pageviews" / "dt=2026-09-09" / "hour=01").exists(), "the day still has an hour"
    assert delete_partition(raw, "pageviews", date(2026, 9, 9), 1, cutoff=date(2026, 9, 10)) == 1
    assert not (raw / "pageviews" / "dt=2026-09-09").exists()


def test_apply_re_checks_the_plan_before_it_destroys_anything(raw: Path) -> None:
    with pytest.raises(RetentionSafetyError, match="not before cutoff"):
        delete_partition(raw, "pageviews", date(2026, 9, 14), 0, cutoff=date(2026, 9, 10))
    with pytest.raises(RetentionSafetyError, match="outside raw"):
        delete_partition(raw, "spike", date(2026, 9, 9), 0, cutoff=date(2026, 9, 10))
    assert (raw / "pageviews" / "dt=2026-09-14" / "hour=00").exists()


def test_the_manifest_records_each_planned_partition(raw: Path) -> None:
    plan = plan_sweep(*find_partitions(raw), now=NOW, retention_days=7)
    rows = manifest_rows(plan, job_run_id="42", recorded_at=NOW, step="planned")
    assert [(r[2], r[4], str(r[5]), r[6], r[7], r[8]) for r in rows] == [
        ("planned", "pageviews", "2026-09-09", 0, 1, 10),
        ("planned", "pageviews", "2026-09-09", 1, 1, 10),
        ("planned", "projectviews", "2026-09-09", 0, 1, 10),
    ]


def test_dry_run_is_the_default() -> None:
    assert parse_args(["--catalog", "wikiflow_sbx", "--step", "apply"]).mode == "dry_run"
