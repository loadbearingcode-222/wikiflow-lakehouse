"""Unity Catalog tags and column comments, applied so that a catalog explains itself.

Every schema, table, view and volume in the project's six schemas carries the same small tag set,
so a search in Catalog Explorer or a query on `information_schema.table_tags` finds all of it and
says what each object is:

    project        = wikiflow
    layer          = raw | reference | bronze | silver | gold | ops
    source         = where the data comes from
    personal_data  = none   (page titles and aggregate counts; no user-level data exists upstream)

Table comments are written where each table is defined (bundle schemas, pipeline SQL, the Python
that creates bronze and ops). Column comments are added here for the two bronze grains, the
contract-enforced tables every other layer is derived from.

Applied as a diff against `information_schema`: a tag or comment that already matches is not
touched, so the daily run changes nothing on an unchanged catalog. Objects the pipeline manages
internally (`__materialization_*`, `event_log_*`) are skipped.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from wikiflow.contracts import BRONZE_CONTRACTS, columns

SCHEMAS = ("raw", "reference", "bronze", "silver", "gold", "ops")

SOURCE = {
    "raw": "wikimedia-dumps",
    "reference": "wikidata",
    "bronze": "wikimedia-dumps",
    "silver": "wikimedia-dumps",
    "gold": "wikimedia-dumps",
    "ops": "pipeline-metadata",
}

KIND = {
    "MANAGED": "TABLE",
    "EXTERNAL": "TABLE",
    "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
    "STREAMING_TABLE": "STREAMING TABLE",
    "VIEW": "VIEW",
}

_PARTITION_KEYS = {
    "dt": "UTC date of the hour the data describes (not the dump filename's date).",
    "hour": "UTC hour, 0-23, the data describes. The dump filename is labelled with the hour's END.",
    "interval_start_utc": "Start of the hour the views happened in, UTC.",
    "source_url": "The raw volume file this hour was extracted from.",
    "ingested_at": "When this hour was written to bronze; a rewrite updates it (ADR 0011).",
}

BRONZE_COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    "company_pageviews": _PARTITION_KEYS
    | {
        "qid": "Wikidata id of the tracked entity, e.g. Q95 for Google.",
        "company_name": "The entity's English label on Wikidata.",
        "language": "Language edition, e.g. en or de; NULL for projects without one (commons).",
        "project": "Wikimedia project: wikipedia, wikiquote, wikimedia, and so on.",
        "access_method": "desktop or mobile-web.",
        "match_type": "sitelink: the article itself. redirect: another title that redirects to it.",
        "canonical_title": "The article the views belong to; equals page_title for a sitelink.",
        "domain_code": "Wikimedia domain code as in the dump, e.g. en.m for mobile English Wikipedia.",
        "page_title": "Title exactly as requested, underscored and possibly percent-encoded.",
        "count_views": "Views of this title in this hour; at least 1, since a dump row means a view.",
        "total_response_size": "Bytes served, as published by Wikimedia (currently always 0).",
    },
    "domain_totals": _PARTITION_KEYS
    | {
        "domain_code": 'Wikimedia domain code. Unparseable codes (a literal "") are kept: Wikimedia counts them.',
        "project": "Project decoded from domain_code; NULL when unparseable.",
        "language": "Language decoded from domain_code; NULL when unparseable or language-less.",
        "access_method": "desktop or mobile-web, decoded from domain_code; NULL when unparseable.",
        "is_parseable": "Whether domain_code fits the documented scheme.",
        "views": "All views of every page on this domain in this hour; reconciles with projectviews.",
        "response_bytes": "Bytes served across the domain, as published (currently always 0).",
        "distinct_pages": "Number of distinct titles viewed on this domain in this hour.",
    },
}


def desired_tags(schema: str) -> dict[str, str]:
    return {
        "project": "wikiflow",
        "layer": schema,
        "source": SOURCE[schema],
        "personal_data": "none",
    }


def is_pipeline_internal(name: str) -> bool:
    return name.startswith("__") or name.startswith("event_log_")


def _quote(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def set_tags_sql(
    kind: str, full_name: str, current: dict[str, str], desired: dict[str, str]
) -> str | None:
    """One SET TAGS for the tags that are missing or differ; None when all already match."""
    changed = {k: v for k, v in desired.items() if current.get(k) != v}
    if not changed:
        return None
    pairs = ", ".join(f"{_quote(k)} = {_quote(v)}" for k, v in sorted(changed.items()))
    return f"ALTER {kind} {full_name} SET TAGS ({pairs})"


def column_comment_sql(table: str, column: str, current: str | None, desired: str) -> str | None:
    if current == desired:
        return None
    return f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT {_quote(desired)}"


@dataclass(frozen=True, slots=True)
class CatalogState:
    objects: list[tuple[str, str, str]]
    """(schema, name, information_schema table_type)."""
    volumes: list[tuple[str, str]]
    schema_tags: dict[str, dict[str, str]]
    table_tags: dict[tuple[str, str], dict[str, str]]
    volume_tags: dict[tuple[str, str], dict[str, str]]
    column_comments: dict[tuple[str, str], dict[str, str | None]]


def plan_statements(catalog: str, state: CatalogState) -> list[str]:
    """Every statement needed to bring the catalog to the declared tags and comments."""
    statements: list[str | None] = []
    for schema in SCHEMAS:
        statements.append(
            set_tags_sql(
                "SCHEMA",
                f"{catalog}.{schema}",
                state.schema_tags.get(schema, {}),
                desired_tags(schema),
            )
        )
    for schema, name, table_type in state.objects:
        if schema not in SCHEMAS or is_pipeline_internal(name) or table_type not in KIND:
            continue
        statements.append(
            set_tags_sql(
                KIND[table_type],
                f"{catalog}.{schema}.{name}",
                state.table_tags.get((schema, name), {}),
                desired_tags(schema),
            )
        )
    for schema, name in state.volumes:
        if schema in SCHEMAS:
            statements.append(
                set_tags_sql(
                    "VOLUME",
                    f"{catalog}.{schema}.{name}",
                    state.volume_tags.get((schema, name), {}),
                    desired_tags(schema),
                )
            )
    held = {(schema, name) for schema, name, _ in state.objects}
    for table, comments in BRONZE_COLUMN_COMMENTS.items():
        if ("bronze", table) not in held:
            continue
        current = state.column_comments.get(("bronze", table), {})
        for column, comment in comments.items():
            statements.append(
                column_comment_sql(
                    f"{catalog}.bronze.{table}", column, current.get(column), comment
                )
            )
    return [s for s in statements if s is not None]


# --- Spark side ---------------------------------------------------------------------------


def read_state(spark: Any, catalog: str) -> CatalogState:
    info = f"{catalog}.information_schema"
    in_scope = "IN (" + ", ".join(f"'{s}'" for s in SCHEMAS) + ")"

    def grouped(sql: str, key_width: int) -> dict[Any, dict[str, Any]]:
        out: dict[Any, dict[str, Any]] = {}
        for row in spark.sql(sql).collect():
            key = row[0] if key_width == 1 else tuple(row[:key_width])
            out.setdefault(key, {})[row[key_width]] = row[key_width + 1]
        return out

    return CatalogState(
        objects=[
            (r["table_schema"], r["table_name"], r["table_type"])
            for r in spark.sql(
                f"SELECT table_schema, table_name, table_type FROM {info}.tables WHERE table_schema {in_scope}"
            ).collect()
        ],
        volumes=[
            (r["volume_schema"], r["volume_name"])
            for r in spark.sql(
                f"SELECT volume_schema, volume_name FROM {info}.volumes WHERE volume_schema {in_scope}"
            ).collect()
        ],
        schema_tags=grouped(f"SELECT schema_name, tag_name, tag_value FROM {info}.schema_tags", 1),
        table_tags=grouped(
            f"SELECT schema_name, table_name, tag_name, tag_value FROM {info}.table_tags", 2
        ),
        volume_tags=grouped(
            f"SELECT schema_name, volume_name, tag_name, tag_value FROM {info}.volume_tags", 2
        ),
        column_comments=grouped(
            f"SELECT table_schema, table_name, column_name, comment FROM {info}.columns WHERE table_schema = 'bronze'",
            2,
        ),
    )


def apply(spark: Any, catalog: str) -> list[str]:
    statements = plan_statements(catalog, read_state(spark, catalog))
    for statement in statements:
        spark.sql(statement)
    return statements


def contract_columns() -> dict[str, tuple[str, ...]]:
    """Bronze columns per table, from the contract - what BRONZE_COLUMN_COMMENTS must cover."""
    return {name: columns(model) for name, model in BRONZE_CONTRACTS.items()}
