"""story-lint refuses every way a PR can leave the plan's order, each with its own code."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import story_lint
from story_lint import Violation, branch_prefix, check, plan_ids, story_id_from_branch

PLAN_TEXT = """
### M0-S1: repo scaffold
### M0-S2: the bundle
### M1-S4: two grains
### M1-S4.1: hotfix for two grains
### M1-S5: hours on a schedule
"""
PLAN = plan_ids(PLAN_TEXT)


def merge(branch: str, title: str) -> str:
    return f"Merge pull request #7 from owner/{branch}\n\n{title}\n"


def codes(violations: list[Violation]) -> list[str]:
    return [v.code for v in violations]


def test_plan_ids_are_read_in_order() -> None:
    assert PLAN == ["M0-S1", "M0-S2", "M1-S4", "M1-S4.1", "M1-S5"]


def test_first_story_needs_no_predecessor() -> None:
    assert check("feat/m0-s1-repo-scaffold", "M0-S1: repo scaffold", PLAN, "") == []


def test_next_story_passes_once_its_predecessor_merged() -> None:
    log = merge("feat/m0-s1-repo-scaffold", "M0-S1: repo scaffold")
    assert check("feat/m0-s2-bundle-platform", "M0-S2: the bundle", PLAN, log) == []


@pytest.mark.parametrize(
    "branch",
    ["feature/m0-s1-scaffold", "feat/M0-S1-scaffold", "M6/docs-record-F091-park", "feat/m0-s1"],
)
def test_branch_name_is_refused(branch: str) -> None:
    assert codes(check(branch, "M0-S1: repo scaffold", PLAN, "")) == ["BRANCH-NAME"]


@pytest.mark.parametrize("title", ["repo scaffold", "M0-S1 repo scaffold", "m0-s1: scaffold"])
def test_title_format_is_refused(title: str) -> None:
    assert codes(check("feat/m0-s1-repo-scaffold", title, PLAN, "")) == ["TITLE-FORMAT"]


def test_branch_and_title_naming_different_stories_is_refused() -> None:
    log = merge("feat/m0-s1-repo-scaffold", "M0-S1: repo scaffold")
    result = check("feat/m0-s2-bundle-platform", "M0-S1: repo scaffold", PLAN, log)
    assert "ID-MISMATCH" in codes(result)


def test_story_missing_from_the_plan_is_refused() -> None:
    assert codes(check("feat/m0-s9-extra", "M0-S9: extra", PLAN, "")) == ["ID-NOT-IN-PLAN"]


def test_skipping_ahead_is_refused() -> None:
    log = merge("feat/m0-s1-repo-scaffold", "M0-S1: repo scaffold")
    result = check("feat/m1-s5-orchestration", "M1-S5: hours on a schedule", PLAN, log)
    assert codes(result) == ["PREDECESSOR-OPEN"]
    assert "M1-S4.1" in str(result[0])


def test_a_story_cannot_merge_twice() -> None:
    log = merge("feat/m0-s1-repo-scaffold", "M0-S1: repo scaffold")
    result = check("feat/m0-s1-follow-up", "M0-S1: follow up", PLAN, log)
    assert codes(result) == ["ALREADY-MERGED"]


def test_point_story_is_parsed_from_its_branch() -> None:
    assert story_id_from_branch("feat/m1-s4-1-null-views") == "M1-S4.1"
    assert story_id_from_branch("feat/m1-s4-spark-extract") == "M1-S4"


def test_point_story_follows_its_base_story() -> None:
    log = merge("feat/m1-s4-spark-extract", "M1-S4: two grains")
    assert check("feat/m1-s4-1-null-views", "M1-S4.1: hotfix for two grains", PLAN, log) == []


@pytest.mark.parametrize(
    ("story", "branch", "matches"),
    [
        ("M1-S4", "feat/m1-s4-spark-extract", True),
        ("M1-S4", "feat/m1-s4-1-null-views", False),
        ("M1-S1", "feat/m1-s10-later", False),
        ("M1-S4.1", "feat/m1-s4-1-null-views", True),
        ("M1-S4.1", "feat/m1-s4-12-other", False),
    ],
)
def test_branch_prefix_does_not_confuse_neighbouring_stories(
    story: str, branch: str, matches: bool
) -> None:
    assert bool(branch_prefix(story).search(merge(branch, "t"))) is matches


def test_every_refusal_code_is_distinct() -> None:
    documented = re.findall(r"^\s{4}([A-Z-]+)\s{2,}", story_lint.__doc__ or "", re.MULTILINE)
    assert len(documented) == len(set(documented)) == 6


def test_the_committed_plan_is_well_formed() -> None:
    text = (Path(__file__).resolve().parents[2] / "docs" / "MILESTONES.md").read_text("utf-8")
    ids = plan_ids(text)
    assert ids, "docs/MILESTONES.md lists no stories"
    assert len(ids) == len(set(ids)), "a story id appears twice"
    keys = [(int(i[1]), int(i.split("-S")[1].split(".")[0])) for i in ids]
    assert keys == sorted(keys), "stories are not in milestone/story order"
