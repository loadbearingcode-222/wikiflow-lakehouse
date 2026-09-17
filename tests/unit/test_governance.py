"""Governance is applied as a diff, skips what the pipeline owns, and documents every bronze column."""

from __future__ import annotations

from wikiflow.governance import (
    BRONZE_COLUMN_COMMENTS,
    CatalogState,
    contract_columns,
    desired_tags,
    plan_statements,
)

OBJECTS = [
    ("bronze", "domain_totals", "MANAGED"),
    ("gold", "dim_company", "MATERIALIZED_VIEW"),
    ("gold", "__materialization_mat_cb32_dim_company_1", "MANAGED"),
    ("silver", "event_log_cb32", "MANAGED"),
    ("raw", "projectviews", "STREAMING_TABLE"),
]


def state(**overrides: object) -> CatalogState:
    base = {
        "objects": OBJECTS,
        "volumes": [("raw", "dumps")],
        "schema_tags": {},
        "table_tags": {},
        "volume_tags": {},
        "column_comments": {},
    }
    return CatalogState(**(base | overrides))  # type: ignore[arg-type]


def test_every_bronze_contract_column_has_a_comment() -> None:
    """A column added to the contract without a comment fails here."""
    for table, cols in contract_columns().items():
        assert set(BRONZE_COLUMN_COMMENTS[table]) == set(cols), table


def test_a_bare_catalog_gets_tags_on_every_kind_of_object_and_comments_on_bronze() -> None:
    statements = plan_statements("wikiflow_sbx", state())

    assert (
        "ALTER SCHEMA wikiflow_sbx.gold SET TAGS ('layer' = 'gold', 'personal_data' = 'none', 'project' = 'wikiflow', 'source' = 'wikimedia-dumps')"
        in statements
    )
    assert any(
        s.startswith("ALTER MATERIALIZED VIEW wikiflow_sbx.gold.dim_company SET TAGS")
        for s in statements
    )
    assert any(
        s.startswith("ALTER STREAMING TABLE wikiflow_sbx.raw.projectviews SET TAGS")
        for s in statements
    )
    assert any(s.startswith("ALTER VOLUME wikiflow_sbx.raw.dumps SET TAGS") for s in statements)
    assert any(
        s.startswith("ALTER TABLE wikiflow_sbx.bronze.domain_totals ALTER COLUMN views COMMENT")
        for s in statements
    )
    assert not any("__materialization" in s or "event_log_" in s for s in statements), (
        "pipeline internals"
    )
    # company_pageviews is not in this catalog, so none of its columns are commented
    assert not any("company_pageviews" in s for s in statements)


def test_a_catalog_already_governed_gets_no_statements() -> None:
    tags = {(schema, name): desired_tags(schema) for schema, name, _ in OBJECTS}
    governed = state(
        schema_tags={
            schema: desired_tags(schema)
            for schema in ("raw", "reference", "bronze", "silver", "gold", "ops")
        },
        table_tags=tags,
        volume_tags={("raw", "dumps"): desired_tags("raw")},
        column_comments={
            ("bronze", "domain_totals"): dict(BRONZE_COLUMN_COMMENTS["domain_totals"])
        },
    )
    assert plan_statements("wikiflow_sbx", governed) == []


def test_only_the_tags_that_differ_are_set_and_quotes_are_escaped() -> None:
    drifted = state(
        objects=[("bronze", "domain_totals", "MANAGED")],
        volumes=[],
        schema_tags={
            s: desired_tags(s) for s in ("raw", "reference", "bronze", "silver", "gold", "ops")
        },
        table_tags={("bronze", "domain_totals"): desired_tags("bronze") | {"layer": "silver"}},
        column_comments={
            ("bronze", "domain_totals"): dict(BRONZE_COLUMN_COMMENTS["domain_totals"])
            | {"domain_code": "old"}
        },
    )
    assert plan_statements("c", drifted) == [
        "ALTER TABLE c.bronze.domain_totals SET TAGS ('layer' = 'bronze')",
        "ALTER TABLE c.bronze.domain_totals ALTER COLUMN domain_code COMMENT "
        "'Wikimedia domain code. Unparseable codes (a literal \"\") are kept: Wikimedia counts them.'",
    ]
