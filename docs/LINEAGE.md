# Lineage walkthrough: from a dump file to a share of traffic

Unity Catalog records which tables every job and pipeline read and wrote, down to columns, in
`system.access.table_lineage` and `system.access.column_lineage`. This walks one number,
`gold.agg_company_share_of_traffic.share_of_traffic`, back to where it came from, using only those
tables. Every edge below was read from sandbox (`wikiflow_sbx`) on 2026-09-17; the queries are
included so the walk can be repeated on any catalog.

## The table graph

```sql
SELECT source_table_full_name AS source, target_table_full_name AS target, entity_type, count(*) AS events
  FROM system.access.table_lineage
 WHERE target_table_catalog = 'wikiflow_sbx'
   AND source_table_full_name IS NOT NULL AND source_table_full_name <> target_table_full_name
   AND target_table_name NOT LIKE '\_\_%'            -- the pipeline's internal materializations
 GROUP BY ALL ORDER BY target, source;
```

What it returned, drawn as the graph it describes (events = refreshes that read the edge):

```
 ingest_hours job (837081261688398)  ─┐                            ┌─> silver.stg_company_pageviews ─┬─> gold.fct_company_pageviews_hourly ─┬─> gold.agg_company_daily        (25)
 reprocess job (91642880047726)      ─┼─> bronze.company_pageviews ─┘            (36)                 │              (25)                     └─> gold.agg_company_share_of_traffic (26)
                                      │                                                              └─> gold.dim_company                 (25)            ▲
                                      └─> bronze.domain_totals ─────┬─> silver.stg_domain_totals ──────┬─> gold.dim_wiki                  (19)            │
                                                                    │            (31)                  └──────────────────────────────────────────────────┘ (26)
                                                                    └─> ops.reconciliation (14) <── raw.projectviews (26)
```

- **Bronze has writers, not sources.** Both grains are written by the `ingest_hours` job (89 writes
  each) and once by `reprocess` (the M3-S3 drill), and the rows record the job id and run id. No
  source table appears, because bronze is built from collected aggregates with `createDataFrame`,
  not from a query over another table.
- **Silver, gold and ops are pipeline edges.** Every one of them names the `PIPELINE` that refreshed
  it, and the counts are refreshes.
- **The reconciliation reads bronze directly**, not silver, alongside `raw.projectviews`. That is a
  design choice made in M2-S2 (CLAUDE.md trap 13), and lineage confirms the pipeline did what the
  SQL says.

## One column, end to end

```sql
SELECT source_table_full_name, source_column_name, target_column_name
  FROM system.access.column_lineage
 WHERE target_table_full_name = 'wikiflow_sbx.gold.agg_company_share_of_traffic'
   AND target_column_name NOT LIKE '\_\_%'
 GROUP BY ALL;
```

`share_of_traffic` has exactly two inputs, which is the definition of the number:

| target column | ← source | ← its source | ← bronze |
|---|---|---|---|
| `share_of_traffic` | `gold.fct_company_pageviews_hourly.views` | `silver.stg_company_pageviews.views` | `bronze.company_pageviews.count_views` |
| `share_of_traffic` | `silver.stg_domain_totals.views` | | `bronze.domain_totals.views` |
| `company_views` | `gold.fct_company_pageviews_hourly.views` | `silver.stg_company_pageviews.views` | `bronze.company_pageviews.count_views` |
| `total_views` | `silver.stg_domain_totals.views` | | `bronze.domain_totals.views` |

Each further hop is its own query with a new `target_table_full_name`:

- **`silver.stg_company_pageviews.views`** ← `bronze.company_pageviews.count_views` (36 refreshes).
- **`page_title_display`** ← `bronze.company_pageviews.page_title`. Earlier refreshes also show
  `silver.page_titles.decoded`: the joined lookup view M2-S1 measured serving stale rows and removed.
  Lineage keeps the history of that design change.
- **`gold.fct_company_pageviews_hourly.views`** ← `silver.stg_company_pageviews.views` (25).

## What lineage does not show, and where to look instead

| Gap | Why | Where the fact lives |
|---|---|---|
| Dump file → bronze | Bronze rows come from Python-collected aggregates, not a table read | `ops.ingest_audit.pageviews_url`, `line_count`, `sha256`; bronze's own `source_url` column |
| Volume file → `raw.projectviews` | The Auto Loader flow's volume path is not recorded as a lineage source here | the streaming table's `source_file` column |
| `__enzyme__row__id__`, `__mv__expectations` | Internal columns of incremental refresh and expectations | ignore them (filtered above) |
| Dashboard and Genie reads | Recorded as reads with no target table (`target_table_full_name IS NULL`) | the same table, filtered on `source_table_full_name` |

## Tags

Every schema, table, view and the raw volume carry `project`, `layer`, `source` and `personal_data`
(`wikiflow.governance`, applied daily by the `governance` job). Everything in one layer, across
catalogs:

```sql
SELECT catalog_name, schema_name, table_name
  FROM system.information_schema.table_tags
 WHERE tag_name = 'layer' AND tag_value = 'gold' AND catalog_name LIKE 'wikiflow%';
```
