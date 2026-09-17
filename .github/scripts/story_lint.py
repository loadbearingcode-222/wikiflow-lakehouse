"""Refuse a pull request that is not the next story in docs/MILESTONES.md.

The PR list is meant to read as the plan, in order: one story, one branch, one PR. That only
holds if something refuses the exceptions, so this runs on every pull request. Each refusal
has its own code, so a failed check says which rule broke without anyone reading the source.

    BRANCH-NAME         branch is not feat/m<M>-s<S>[-<P>]-<slug>
    TITLE-FORMAT        title is not "M<M>-S<S>[.<P>]: <words>"
    ID-MISMATCH         branch and title name different stories
    ID-NOT-IN-PLAN      the story is not a heading in docs/MILESTONES.md
    PREDECESSOR-OPEN    the story before it in the plan has not merged into main
    ALREADY-MERGED      this story merged once already; fixes ride in the next story

Usage (CI passes the PR's branch and title through the environment, never the shell):
    python .github/scripts/story_lint.py --branch "$HEAD_REF" --title "$PR_TITLE"
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

BRANCH = re.compile(r"^feat/m(?P<m>[0-5])-s(?P<s>\d+)(?:-(?P<p>\d+))?-[a-z0-9]+(?:-[a-z0-9]+)*$")
TITLE = re.compile(r"^(?P<id>M[0-5]-S\d+(?:\.\d+)?): \S.*$")
PLAN_HEADING = re.compile(r"^### (?P<id>M[0-5]-S\d+(?:\.\d+)?): \S", re.MULTILINE)


@dataclass(frozen=True)
class Violation:
    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def story_id_from_branch(branch: str) -> str | None:
    match = BRANCH.match(branch)
    if match is None:
        return None
    story = f"M{match['m']}-S{match['s']}"
    return f"{story}.{match['p']}" if match["p"] else story


def branch_prefix(story_id: str) -> re.Pattern[str]:
    """How a story's branch appears in a merge commit message.

    `M1-S4` must not match `feat/m1-s4-1-...`, which is the point story `M1-S4.1`, and must not
    match `feat/m1-s40-...` either.
    """
    base, _, point = story_id.partition(".")
    milestone, story = base.lower().split("-")
    prefix = f"feat/{milestone}-{story}-"
    if point:
        return re.compile(re.escape(f"{prefix}{point}-"))
    return re.compile(re.escape(prefix) + r"(?!\d+-)")


def plan_ids(milestones: str) -> list[str]:
    return [match["id"] for match in PLAN_HEADING.finditer(milestones)]


def check(branch: str, title: str, plan: list[str], merged_log: str) -> list[Violation]:
    """All rules, evaluated together so one run reports everything wrong at once."""
    violations: list[Violation] = []

    branch_id = story_id_from_branch(branch)
    if branch_id is None:
        violations.append(
            Violation("BRANCH-NAME", f"{branch!r} is not feat/m<M>-s<S>[-<P>]-<slug>")
        )

    title_match = TITLE.match(title)
    title_id = title_match["id"] if title_match else None
    if title_id is None:
        violations.append(Violation("TITLE-FORMAT", f"{title!r} is not 'M<M>-S<S>: <words>'"))

    if branch_id and title_id and branch_id != title_id:
        violations.append(
            Violation("ID-MISMATCH", f"branch names {branch_id}, title names {title_id}")
        )

    story = title_id or branch_id
    if story is None:
        return violations

    if story not in plan:
        violations.append(
            Violation("ID-NOT-IN-PLAN", f"{story} is not a story heading in docs/MILESTONES.md")
        )
        return violations

    if branch_prefix(story).search(merged_log):
        violations.append(
            Violation(
                "ALREADY-MERGED",
                f"{story} merged already; put the fix in the next story under 'Also fixes'",
            )
        )

    position = plan.index(story)
    if position > 0:
        predecessor = plan[position - 1]
        if not branch_prefix(predecessor).search(merged_log):
            violations.append(
                Violation(
                    "PREDECESSOR-OPEN", f"{predecessor} comes first and has not merged into main"
                )
            )

    return violations


def merged_log(main_ref: str) -> str:
    result = subprocess.run(
        ["git", "log", main_ref, "--merges", "--format=%B"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return result.stdout


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--branch", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--milestones", type=Path, default=Path("docs/MILESTONES.md"))
    parser.add_argument("--main-ref", default="origin/main")
    args = parser.parse_args(argv)

    plan = plan_ids(args.milestones.read_text(encoding="utf-8"))
    violations = check(args.branch, args.title, plan, merged_log(args.main_ref))

    for violation in violations:
        print(f"::error title=story-lint::{violation}")
    if violations:
        return 1
    print(f"story-lint: {args.title!r} is the next story in the plan")
    return 0


if __name__ == "__main__":
    sys.exit(main())
