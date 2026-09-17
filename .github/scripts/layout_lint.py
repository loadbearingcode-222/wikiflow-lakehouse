"""Refuse any tracked path that the repo layout in docs/MILESTONES.md does not declare.

The layout is the first thing a reader judges a repo by, so it is declared before work starts
and a path arrives only through a story that edits the declaration first. This reads the tree
in the "## Repo layout" code block and compares it with `git ls-files`.

Tree grammar, one entry per line:

    <glyphs><name>[ · <name>...]  [<story>]  [<note>]

- Glyphs are 3 characters per level (`├─ `, `└─ `, `│  `, or three spaces).
- A name ending in `/` is a directory; any other name is a file, and may contain `/`.
- A directory with entries nested under it may hold only those entries. A directory with
  nothing nested under it may hold anything.

    UNDECLARED-PATH     a tracked file the layout does not cover
    NO-LAYOUT           docs/MILESTONES.md has no "## Repo layout" code block
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

GLYPHS = re.compile(r"^(?:[│ ] {2}|[├└]─ )*")
ENTRY = re.compile(
    r"^(?P<names>\S.*?)(?:\s{2,}(?P<story>M\d-S\d+(?:\.\d+)?|init))?(?:\s{2,}(?P<note>.*))?$"
)
LAYOUT_BLOCK = re.compile(r"^## Repo layout\s*$.*?^```\s*$\n(?P<tree>.*?)^```\s*$", re.S | re.M)


@dataclass
class Layout:
    files: set[str] = field(default_factory=set)
    open_dirs: set[str] = field(default_factory=set)
    """Directories declared with nothing nested under them: anything inside is allowed."""

    def covers(self, path: str) -> bool:
        return path in self.files or any(path.startswith(d) for d in self.open_dirs)


class NoLayoutError(ValueError):
    pass


def parse_layout(milestones: str) -> Layout:
    block = LAYOUT_BLOCK.search(milestones)
    if block is None:
        raise NoLayoutError("docs/MILESTONES.md has no '## Repo layout' code block")

    lines = [line.rstrip() for line in block["tree"].splitlines() if line.strip()]
    entries: list[tuple[int, list[str]]] = []
    for line in lines:
        glyphs = GLYPHS.match(line)
        prefix = glyphs.group(0) if glyphs else ""
        depth = len(prefix) // 3
        match = ENTRY.match(line[len(prefix) :])
        if match is None:
            continue
        names = [name.strip() for name in match["names"].split(" · ")]
        entries.append((depth, names))

    layout = Layout()
    # parents[d] is the full path of the directory that entries at depth d+1 live in.
    parents: dict[int, str] = {}
    for index, (depth, names) in enumerate(entries):
        if depth == 0:
            parents[0] = ""  # the repository root line, e.g. `wikiflow-lakehouse/`
            continue
        base = parents.get(depth - 1, "")
        has_children = index + 1 < len(entries) and entries[index + 1][0] > depth
        for name in names:
            full = base + name
            if name.endswith("/"):
                if has_children and len(names) == 1:
                    parents[depth] = full
                else:
                    layout.open_dirs.add(full)
            else:
                layout.files.add(full)
    return layout


def undeclared(layout: Layout, tracked: list[str]) -> list[str]:
    return [path for path in tracked if not layout.covers(path)]


def tracked_paths() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"], capture_output=True, text=True, encoding="utf-8", check=True
    )
    return [path for path in result.stdout.split("\0") if path]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--milestones", type=Path, default=Path("docs/MILESTONES.md"))
    args = parser.parse_args(argv)

    try:
        layout = parse_layout(args.milestones.read_text(encoding="utf-8"))
    except NoLayoutError as exc:
        print(f"::error title=layout-lint::NO-LAYOUT: {exc}")
        return 1

    missing = undeclared(layout, tracked_paths())
    for path in missing:
        print(
            f"::error title=layout-lint::UNDECLARED-PATH: {path} is not in the repo layout "
            "(docs/MILESTONES.md); declare it in the story that adds it"
        )
    if missing:
        return 1
    print("layout-lint: every tracked path is declared in docs/MILESTONES.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
