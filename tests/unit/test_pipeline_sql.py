"""The pipeline's SQL rules that are written more than once stay written the same way.

The url_decode rule appears in two pipeline files, between `-- url_decode:begin` and
`-- url_decode:end`: once where silver applies it, once where the 14 pinned cases check it.
url_decode_cases.sql explains why it is not a shared view joined in. If the copies drift, the
cases would pass while silver decodes differently, so any difference fails CI here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SILVER = Path(__file__).parents[2] / "src" / "pipelines" / "pageviews" / "silver"
BLOCK = re.compile(r"-- url_decode:begin\n(.*?)\n\s*-- url_decode:end", re.DOTALL)

#: wikiflow's dbt/tests/assert_url_decode_behaves_identically.sql, as (input, expected output).
WIKIFLOW_CASES = [
    ("Apple_Inc.", "Apple_Inc."),
    ("Caf%C3%A9", "Café"),
    ("Tom%27s", "Tom's"),
    ("%E3%82%A2", "ア"),
    ("%F0%9F%8D%8E", "🍎"),
    ("%41%42", "AB"),
    ("a+b%20c", "a+b c"),
    ("50%25_off", "50%_off"),
    ("%%41", "%A"),
    ("100%", "100%"),
    ("%zz", "%zz"),
    ("%4", "%4"),
    ("%E3", "%E3"),
    ("%E3x", "%E3x"),
]


def url_decode_rule(filename: str) -> str:
    """The rule's text in a pipeline file, with indentation normalised."""
    blocks = BLOCK.findall((SILVER / filename).read_text(encoding="utf-8"))
    assert len(blocks) == 1, f"{filename} must hold exactly one url_decode block"
    return "\n".join(line.strip() for line in blocks[0].splitlines())


def pinned_cases() -> list[tuple[str, str]]:
    text = (SILVER / "url_decode_cases.sql").read_text(encoding="utf-8")
    values = text[text.index("FROM VALUES") :]
    literal = r"""('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")"""
    return [
        (raw[1:-1], expected[1:-1])
        for raw, expected in re.findall(rf"\(\s*{literal},\s*{literal}\s*\)", values)
    ]


@pytest.mark.unit
def test_silver_and_the_pinned_cases_apply_the_same_url_decode_rule() -> None:
    assert url_decode_rule("stg_company_pageviews.sql") == url_decode_rule("url_decode_cases.sql")


@pytest.mark.unit
def test_the_pipeline_pins_exactly_wikiflows_fourteen_cases() -> None:
    assert pinned_cases() == WIKIFLOW_CASES
