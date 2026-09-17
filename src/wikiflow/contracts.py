"""The data contract for bronze: what every published row must look like.

Ported from wikiflow (its ADR 0009), where the contract was declared once as strict Pydantic
models and enforced on every row before a partition was published. The declaration is unchanged
here. What changes is where it is enforced: the same models now generate the Delta tables
themselves - column types, NOT NULL, and a CHECK constraint for every rule a row must satisfy -
so Delta refuses a bad write even if a row never passed through Python.

Two layers, on purpose:

1. `validate_rows` runs in the ingest task before the write. It is strict: `3882.0` in an
   integer column fails instead of being coerced to `3882`, which is exactly the defect that
   once slipped through wikiflow.
2. The Delta table enforces types, nullability and CHECK constraints on every commit, and rejects
   the whole write atomically.

`delta_statements(model, table)` is the only place table DDL is written.
"""

from __future__ import annotations

import re
import types
from collections.abc import Sequence
from datetime import date, datetime
from typing import Annotated, Any, ClassVar, Literal, Union, get_args, get_origin

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NaiveDatetime,
    TypeAdapter,
    ValidationError,
    model_validator,
)
from pydantic.fields import FieldInfo

AccessMethod = Literal["desktop", "mobile-web"]
MatchType = Literal["sitelink", "redirect"]

NonNegative = Annotated[int, Field(ge=0)]
Positive = Annotated[int, Field(ge=1)]
NonEmpty = Annotated[str, Field(min_length=1)]


class _BronzeRecord(BaseModel):
    """Partition keys and provenance, carried by every bronze row."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    #: Row rules that span columns, as Delta CHECK expressions. Each has a Python twin below.
    ROW_CHECKS: ClassVar[dict[str, str]] = {
        "interval_in_partition": (
            "to_date(interval_start_utc) = dt AND hour(interval_start_utc) = hour"
        ),
    }
    #: Columns stored with a narrower or more specific Delta type than the Python type implies.
    DELTA_TYPES: ClassVar[dict[str, str]] = {
        "hour": "INT",
        "interval_start_utc": "TIMESTAMP_NTZ",
        "ingested_at": "TIMESTAMP",
    }

    dt: date
    hour: Annotated[int, Field(ge=0, le=23)]
    interval_start_utc: NaiveDatetime
    """UTC, stored without a zone - the hour the data DESCRIBES (wikiflow ADR 0001)."""
    source_url: NonEmpty
    ingested_at: datetime
    """When this partition was written. Rewritten hours are found by it."""

    @model_validator(mode="after")
    def _interval_matches_partition(self) -> _BronzeRecord:
        # A row filed under one hour but describing another would be counted in the wrong place
        # by every consumer, and reconcile nowhere.
        if (self.interval_start_utc.date(), self.interval_start_utc.hour) != (self.dt, self.hour):
            raise ValueError(
                f"interval_start_utc {self.interval_start_utc} is not in partition "
                f"dt={self.dt}/hour={self.hour}"
            )
        return self


class CompanyPageviewRecord(_BronzeRecord):
    """Narrow grain: one row per tracked article title, per hour, per access method."""

    qid: Annotated[str, Field(pattern=r"^Q[1-9]\d*$")]
    company_name: NonEmpty
    language: str | None
    project: NonEmpty
    access_method: AccessMethod
    match_type: MatchType
    canonical_title: NonEmpty
    domain_code: NonEmpty
    page_title: NonEmpty
    count_views: Positive
    """A dump row exists only because something was viewed, so zero is impossible."""
    total_response_size: NonNegative


class DomainTotalRecord(_BronzeRecord):
    """Wide grain: total views per domain code per hour."""

    ROW_CHECKS: ClassVar[dict[str, str]] = {
        **_BronzeRecord.ROW_CHECKS,
        "parseable_has_attributes": (
            "NOT is_parseable OR (project IS NOT NULL AND access_method IS NOT NULL)"
        ),
    }

    domain_code: str
    """Can be `""`-like garbage from upstream; kept, because Wikimedia counts it."""
    project: str | None
    language: str | None
    access_method: AccessMethod | None
    is_parseable: bool
    views: Positive
    response_bytes: NonNegative
    distinct_pages: Positive

    @model_validator(mode="after")
    def _parsed_iff_parseable(self) -> DomainTotalRecord:
        if self.is_parseable and (self.project is None or self.access_method is None):
            raise ValueError(f"{self.domain_code!r} is parseable but has no project/access")
        return self


BRONZE_CONTRACTS: dict[str, type[_BronzeRecord]] = {
    "company_pageviews": CompanyPageviewRecord,
    "domain_totals": DomainTotalRecord,
}


class ContractViolation(ValueError):
    """Rows do not satisfy their contract and must not be written."""


def columns(model: type[BaseModel]) -> tuple[str, ...]:
    """Column names in contract order - the order bronze tables are created and written in."""
    return tuple(model.model_fields)


def validate_rows(
    names: Sequence[str],
    rows: Sequence[Sequence[Any]],
    model: type[_BronzeRecord],
    *,
    label: str,
    max_errors: int = 5,
) -> int:
    """Validate rows (e.g. `df.columns` and `df.collect()`) against a contract. Returns the count.

    Checks the column set and order first: a missing or extra column is a contract change, and is
    reported as one instead of as thousands of row errors.
    """
    expected = columns(model)
    found = tuple(names)
    if found != expected:
        detail = (
            f"  missing:    {sorted(set(expected) - set(found))}\n"
            f"  unexpected: {sorted(set(found) - set(expected))}"
            if set(found) != set(expected)
            else f"  same columns, different order: {list(found)}"
        )
        raise ContractViolation(f"{label}: columns do not match {model.__name__}.\n{detail}")

    adapter: TypeAdapter[list[Any]] = TypeAdapter(list[model])  # type: ignore[valid-type]
    try:
        adapter.validate_python([dict(zip(expected, row, strict=True)) for row in rows])
    except ValidationError as exc:
        shown = "\n  ".join(
            f"row {error['loc'][0]}, {'.'.join(map(str, error['loc'][1:])) or 'row'}: "
            f"{error['msg']} (got {error.get('input')!r})"
            for error in exc.errors()[:max_errors]
        )
        raise ContractViolation(
            f"{label}: {exc.error_count()} contract violation(s) in {len(rows):,} rows "
            f"against {model.__name__}; refusing to write.\n  {shown}"
        ) from None
    return len(rows)


# --- the Delta side, derived from the same declaration ---------------------------------------

_DELTA_TYPES: dict[Any, str] = {date: "DATE", int: "BIGINT", str: "STRING", bool: "BOOLEAN"}


def _unwrap(annotation: Any) -> tuple[Any, bool, list[Any]]:
    """(base type, nullable, metadata) for a field annotation."""
    nullable = False
    if get_origin(annotation) in (Union, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        nullable = len(args) < len(get_args(annotation))
        annotation = args[0]
    metadata: list[Any] = []
    if get_origin(annotation) is Annotated:
        annotation, *metadata = get_args(annotation)
    return annotation, nullable, metadata


def delta_columns(model: type[_BronzeRecord]) -> tuple[tuple[str, str, bool], ...]:
    """(name, Delta type, nullable) per field, in contract order."""
    out = []
    for name, field in model.model_fields.items():
        base, nullable, _ = _unwrap(field.annotation)
        if name in model.DELTA_TYPES:
            kind = model.DELTA_TYPES[name]
        elif get_origin(base) is Literal:
            kind = _DELTA_TYPES[type(get_args(base)[0])]
        else:
            kind = _DELTA_TYPES[base]
        out.append((name, kind, nullable))
    return tuple(out)


def _field_checks(name: str, field: FieldInfo) -> list[str]:
    base, nullable, metadata = _unwrap(field.annotation)
    constraints = [*metadata, *field.metadata]
    checks: list[str] = []
    for item in constraints:
        for attr, op in (("ge", ">="), ("gt", ">"), ("le", "<="), ("lt", "<")):
            if getattr(item, attr, None) is not None:
                checks.append(f"{name} {op} {getattr(item, attr)}")
        if getattr(item, "min_length", None):
            checks.append(f"length({name}) >= {item.min_length}")
        if getattr(item, "pattern", None):
            # `\d` becomes `[0-9]` so the SQL literal needs no backslash escaping.
            pattern = re.sub(r"\\d", "[0-9]", item.pattern)
            checks.append(f"{name} RLIKE '{pattern}'")
    if get_origin(base) is Literal:
        allowed = ", ".join(f"'{value}'" for value in get_args(base))
        checks.append(f"{name} IN ({allowed})")
    if nullable and checks:
        return [f"{name} IS NULL OR ({' AND '.join(checks)})"]
    return checks


def delta_constraints(model: type[_BronzeRecord]) -> dict[str, str]:
    """Constraint name -> CHECK expression: every field rule, then every cross-column rule."""
    out: dict[str, str] = {}
    for name, field in model.model_fields.items():
        for index, check in enumerate(_field_checks(name, field)):
            out[f"{name}_{index}" if index else f"{name}_valid"] = check
    out.update(model.ROW_CHECKS)
    return out


def delta_statements(model: type[_BronzeRecord], table: str) -> list[str]:
    """Idempotent SQL that creates the table and (re)applies its constraints.

    Liquid clustering on (dt, hour) matches how every read and every replace is keyed.
    """
    column_sql = ",\n  ".join(
        f"{name} {kind}{'' if nullable else ' NOT NULL'}"
        for name, kind, nullable in delta_columns(model)
    )
    statements = [
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {column_sql}\n) CLUSTER BY (dt, hour)"
        f" COMMENT 'Bronze grain under the {model.__name__} contract (src/wikiflow/contracts.py)'"
    ]
    for name, check in delta_constraints(model).items():
        statements.append(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
        statements.append(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({check})")
    return statements
