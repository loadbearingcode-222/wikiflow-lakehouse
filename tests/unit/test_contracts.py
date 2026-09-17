"""The bronze contract: strict row validation, and the Delta DDL generated from the same models."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from wikiflow.contracts import (
    CompanyPageviewRecord,
    ContractViolation,
    DomainTotalRecord,
    columns,
    delta_columns,
    delta_constraints,
    delta_statements,
    validate_rows,
)

PARTITION: dict[str, Any] = {
    "dt": date(2026, 9, 14),
    "hour": 11,
    "interval_start_utc": datetime(2026, 9, 14, 11),
    "source_url": "/Volumes/wikiflow_sbx/raw/dumps/pageviews/x.gz",
    "ingested_at": datetime(2026, 9, 17, 4, tzinfo=UTC),
}


def domain_row(**overrides: Any) -> dict[str, Any]:
    return (
        PARTITION
        | {
            "domain_code": "en.m",
            "project": "wikipedia",
            "language": "en",
            "access_method": "mobile-web",
            "is_parseable": True,
            "views": 3882,
            "response_bytes": 0,
            "distinct_pages": 12,
        }
        | overrides
    )


def company_row(**overrides: Any) -> dict[str, Any]:
    return (
        PARTITION
        | {
            "qid": "Q312",
            "company_name": "Apple Inc.",
            "language": "en",
            "project": "wikipedia",
            "access_method": "desktop",
            "match_type": "redirect",
            "canonical_title": "Apple_Inc.",
            "domain_code": "en",
            "page_title": "Apple_Computer",
            "count_views": 40,
            "total_response_size": 0,
        }
        | overrides
    )


def check(model: Any, row: dict[str, Any]) -> int:
    names = columns(model)
    return validate_rows(names, [tuple(row[n] for n in names)], model, label="test")


@pytest.mark.unit
def test_well_formed_rows_pass() -> None:
    assert check(DomainTotalRecord, domain_row()) == 1
    assert check(CompanyPageviewRecord, company_row()) == 1


@pytest.mark.unit
def test_a_double_where_an_integer_belongs_is_refused() -> None:
    """The real defect wikiflow shipped: early partitions stored views as DOUBLE (3882.0)."""
    with pytest.raises(ContractViolation, match=r"views: Input should be a valid integer"):
        check(DomainTotalRecord, domain_row(views=3882.0))


@pytest.mark.unit
def test_a_row_filed_under_the_wrong_hour_is_refused() -> None:
    with pytest.raises(ContractViolation, match="not in partition dt=2026-09-14/hour=12"):
        check(DomainTotalRecord, domain_row(hour=12))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"match_type": "alias"}, "match_type"),
        ({"access_method": "mobile-app"}, "access_method"),
        ({"count_views": 0}, "count_views"),
        ({"qid": "Apple"}, "qid"),
        ({"canonical_title": ""}, "canonical_title"),
    ],
)
def test_company_rows_break_the_contract_in_the_ways_that_matter(
    override: dict[str, Any], message: str
) -> None:
    with pytest.raises(ContractViolation, match=message):
        check(CompanyPageviewRecord, company_row(**override))


@pytest.mark.unit
def test_unparseable_domains_may_lack_attributes_but_parseable_ones_may_not() -> None:
    unparseable = domain_row(
        domain_code='""', project=None, language=None, access_method=None, is_parseable=False
    )
    assert check(DomainTotalRecord, unparseable) == 1
    with pytest.raises(ContractViolation, match="parseable but has no project"):
        check(DomainTotalRecord, domain_row(project=None))


@pytest.mark.unit
def test_a_missing_column_is_reported_as_a_contract_change() -> None:
    names = [n for n in columns(CompanyPageviewRecord) if n != "canonical_title"]
    row = company_row()
    with pytest.raises(ContractViolation, match=r"missing:    \['canonical_title'\]"):
        validate_rows(names, [tuple(row[n] for n in names)], CompanyPageviewRecord, label="t")


@pytest.mark.unit
def test_contract_order_starts_with_the_partition_keys() -> None:
    for model in (CompanyPageviewRecord, DomainTotalRecord):
        assert columns(model)[:5] == (
            "dt",
            "hour",
            "interval_start_utc",
            "source_url",
            "ingested_at",
        )


@pytest.mark.unit
def test_delta_columns_follow_the_contract() -> None:
    derived = {name: (kind, nullable) for name, kind, nullable in delta_columns(DomainTotalRecord)}
    assert list(derived) == list(columns(DomainTotalRecord))
    assert derived["dt"] == ("DATE", False)
    assert derived["hour"] == ("INT", False)
    assert derived["interval_start_utc"] == ("TIMESTAMP_NTZ", False)
    assert derived["ingested_at"] == ("TIMESTAMP", False)
    assert derived["views"] == ("BIGINT", False)
    assert derived["project"] == ("STRING", True)
    assert derived["access_method"] == ("STRING", True)
    assert derived["is_parseable"] == ("BOOLEAN", False)


@pytest.mark.unit
def test_every_rule_becomes_a_check_constraint() -> None:
    company = delta_constraints(CompanyPageviewRecord)
    assert company["count_views_valid"] == "count_views >= 1"
    assert company["qid_valid"] == "qid RLIKE '^Q[1-9][0-9]*$'"
    assert company["match_type_valid"] == "match_type IN ('sitelink', 'redirect')"
    assert company["hour_valid"] == "hour >= 0"
    assert company["hour_1"] == "hour <= 23"
    assert "interval_in_partition" in company

    domain = delta_constraints(DomainTotalRecord)
    assert domain["access_method_valid"] == (
        "access_method IS NULL OR (access_method IN ('desktop', 'mobile-web'))"
    )
    assert "parseable_has_attributes" in domain


@pytest.mark.unit
def test_statements_are_idempotent_create_then_constraints() -> None:
    statements = delta_statements(DomainTotalRecord, "wikiflow_sbx.bronze.domain_totals")
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS wikiflow_sbx.bronze.domain_totals")
    assert "CLUSTER BY (dt, hour)" in statements[0]
    drops = [s for s in statements if "DROP CONSTRAINT IF EXISTS" in s]
    adds = [s for s in statements if "ADD CONSTRAINT" in s]
    assert len(drops) == len(adds) == len(delta_constraints(DomainTotalRecord))
