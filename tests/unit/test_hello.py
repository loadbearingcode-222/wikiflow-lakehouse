"""The hello task refuses a catalog that is missing any of the six schemas."""

from __future__ import annotations

import pytest

from wikiflow.tasks.hello import EXPECTED_SCHEMAS, missing_schemas, parse_args


def test_catalog_is_required() -> None:
    with pytest.raises(SystemExit):
        parse_args([])
    assert parse_args(["--catalog", "wikiflow_sbx"]).catalog == "wikiflow_sbx"


def test_all_six_schemas_present_is_clean() -> None:
    assert missing_schemas([*EXPECTED_SCHEMAS, "default", "information_schema"]) == []


def test_a_missing_schema_is_named() -> None:
    assert missing_schemas(["raw", "bronze", "silver", "gold", "ops"]) == ["reference"]
