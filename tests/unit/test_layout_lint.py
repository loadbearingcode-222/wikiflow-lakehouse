"""layout-lint accepts exactly what the declared tree allows, and names what it refuses."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from layout_lint import NoLayoutError, parse_layout, undeclared

REPO_ROOT = Path(__file__).resolve().parents[2]

TREE = """
## Repo layout

```
demo/
├─ .github/
│  ├─ workflows/ci.yml                         M0-S1  checks
│  └─ pull_request_template.md                 M0-S1
├─ src/
│  ├─ pkg/                                            a closed directory
│  │  ├─ __init__.py                           M0-S2
│  │  └─ a.py · b.py                           M1-S1
│  └─ pipelines/sql/                                  one .sql per table
│     └─ raw/ · silver/                        M2-S1
├─ tests/
│  └─ unit/                                    M0-S1  anything inside
├─ README.md                                   init   stub
└─ LICENSE · .gitignore                        init
```
"""


@pytest.fixture
def layout():  # type: ignore[no-untyped-def]
    return parse_layout(TREE)


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/ci.yml",
        "src/pkg/__init__.py",
        "src/pkg/b.py",
        "src/pipelines/sql/silver/stg.sql",
        "tests/unit/deep/test_x.py",
        "README.md",
        ".gitignore",
    ],
)
def test_declared_paths_are_covered(layout, path: str) -> None:  # type: ignore[no-untyped-def]
    assert layout.covers(path)


@pytest.mark.parametrize(
    "path",
    [
        "HANDOFF.md",
        ".github/workflows/other.yml",
        "src/pkg/c.py",
        "src/pipelines/sql/gold/fct.sql",
        "tests/integration/test_y.py",
        "docs/notes.md",
    ],
)
def test_undeclared_paths_are_refused(layout, path: str) -> None:  # type: ignore[no-untyped-def]
    assert undeclared(layout, [path, "README.md"]) == [path]


def test_a_directory_with_listed_contents_is_closed(layout) -> None:  # type: ignore[no-untyped-def]
    assert "src/pkg/" not in layout.open_dirs
    assert "tests/unit/" in layout.open_dirs


def test_missing_layout_block_is_refused() -> None:
    with pytest.raises(NoLayoutError):
        parse_layout("# MILESTONES\n\nno tree here\n")


def test_every_tracked_path_is_declared_in_the_committed_layout() -> None:
    text = (REPO_ROOT / "docs" / "MILESTONES.md").read_text("utf-8")
    tracked = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout.split("\0")
    assert undeclared(parse_layout(text), [p for p in tracked if p]) == []
