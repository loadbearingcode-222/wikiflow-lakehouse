"""The pipeline's url_decode rule, run from its own SQL file on serverless Spark, against wikiflow's cases.

The pipeline already fails an update if a case breaks (url_decode_cases.sql). This runs the same
rule text outside the pipeline, next to Spark's built-in url_decode, so the reason for writing our
own stays demonstrated rather than asserted.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration

SILVER = Path(__file__).parents[2] / "src" / "pipelines" / "pageviews" / "silver"
BLOCK = re.compile(r"-- url_decode:begin\n(.*?)\n\s*-- url_decode:end", re.DOTALL)
CASES = [
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


def test_every_case_decodes_as_wikiflow_expects(spark: Any) -> None:
    (rule,) = BLOCK.findall((SILVER / "stg_company_pageviews.sql").read_text(encoding="utf-8"))
    frame = spark.createDataFrame(CASES, schema="raw_title STRING, expected STRING")
    rows = frame.selectExpr("raw_title", "expected", f"{rule} AS decoded").collect()

    decoded = {row["raw_title"]: row["decoded"] for row in rows}
    print(f"DECODED {decoded}")
    assert decoded == dict(CASES)


def test_sparks_built_in_url_decode_is_not_that_rule(spark: Any) -> None:
    assert spark.sql("SELECT url_decode('a+b%20c')").first()[0] == "a b c"
    with pytest.raises(Exception, match="CANNOT_DECODE_URL"):
        spark.sql("SELECT url_decode('%%41')").collect()
