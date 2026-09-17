# ADR 0011: Materialized views replace the `ingested_at` watermark

- **Status:** Accepted
- **Date:** 2026-09-17
- **Decision:** The marts are Lakeflow materialized views with no watermark. The pipeline decides,
  per refresh, what changed upstream. A rewritten earlier hour reaching the marts is measured
  (below), not assumed, and is kept checked by `tests/integration/test_marts_match_bronze.py`.

## Context

wikiflow built its fact table incrementally in dbt (`delete+insert` on `(view_date, view_hour)`).
The first version selected hours newer than the newest `view_date` already built. That broke the
day an old hour was reprocessed: the redirect backfill (wikiflow ADR 0007) rewrote earlier hours,
the watermark never looked at them again, and both warehouses needed a manual `--full-refresh`.

The fix was a better watermark: select hours whose bronze was *written* after the newest
`ingested_at` already built, overlapped by an hour because two ingest runs can finish out of order.
That works, but it is state the model maintains by hand. The overlap is a guess about timing, and
the rule has to be right in every incremental model.

Lakeflow materialized views remove the question. There is no incremental rule to write: on each
update the pipeline reads what changed in its sources (Delta row tracking) and chooses a technique
per flow. It can process only the changed rows (ROW_BASED, APPEND_ONLY), re-aggregate the affected
groups (GENERIC_AGGREGATE), or recompute from scratch (COMPLETE_RECOMPUTE). The risk moves from "is
our watermark right?" to "does the platform really see a rewrite of an old hour?", which can be
measured.

## Measurement (2026-09-17, sandbox)

Setup: 14 hours in bronze, the newest `2026-09-16T00`, so `2026-09-15T11` is an earlier-date hour,
exactly the shape that defeated the first wikiflow watermark.

1. Every silver, gold and ops table was snapshotted, row by row.
2. `ingest_hours` re-ingested `2026-09-15T11` (`replace_present=true`,
   [run 282836423932273](https://dbc-5cb7afce-0a32.cloud.databricks.com/jobs/837081261688398/runs/282836423932273?o=991861943955874)).
   Bronze took one replace commit per table (`company_pageviews` v40: 486 rows;
   `domain_totals` v45: 1,946 rows; predicate `dt = DATE'2026-09-15' AND hour = 11`). The job's
   `refresh_pageviews` task then started pipeline update `2f63f714` (cause `JOB_TASK`).
3. Everything was snapshotted again and diffed per hour.

**What changed, from the diff:**

| Table | Rows before = after | Rows changed | Hours touched | Columns changed |
|---|---|---|---|---|
| `silver.stg_company_pageviews` | 6,370 | 486 | `2026-09-15T11` only | `ingested_at` only |
| `silver.stg_domain_totals` | 27,268 | 1,946 | `2026-09-15T11` only | `ingested_at` only |
| `gold.fct_company_pageviews_hourly` | 6,370 | 486 | `2026-09-15T11` only | `ingested_at` only |
| `gold.agg_company_share_of_traffic`, `agg_company_daily`, `dim_company`, `dim_wiki`, `ops.reconciliation`, `raw.projectviews` | unchanged | 0 | none | none |

The re-ingested file was the same file, so every count reproduced. The marts that carry no
`ingested_at` are correctly identical, and the tables that carry it show exactly the rewritten
hour's new write time, and nothing else.

**How the pipeline did it, from the event log of update `2f63f714`:**

```
Update 2f63f7 started by JOB_TASK.
Flow 'wikiflow_sbx.silver.stg_domain_totals' has been planned to be executed as ROW_BASED.
Flow 'wikiflow_sbx.gold.fct_company_pageviews_hourly' has been planned to be executed as ROW_BASED.
Flow 'wikiflow_sbx.gold.dim_company' has been planned to be executed as GENERIC_AGGREGATE.
Flow 'wikiflow_sbx.gold.agg_company_daily' has been planned to be executed as GENERIC_AGGREGATE.
Flow 'wikiflow_sbx.gold.agg_company_share_of_traffic' has been planned to be executed as COMPLETE_RECOMPUTE. Another option is available:ROW_BASED.
```

The fact table was refreshed row-based: the platform found the rewritten rows of an old hour on its
own, with no watermark, no overlap and no full refresh.

## Consequences

- **Nothing to reset after a reprocess or backfill.** M3-S3's `reprocess` job only rewrites bronze.
  The next refresh carries it through, as measured above.
- **The technique is the platform's choice, so correctness is checked.** A materialized view that
  joins another view of the same pipeline was measured serving stale rows in M2-S1 (CLAUDE.md
  trap 13). The one gold join is pinned to `REFRESH POLICY FULL`, `ops.reconciliation` reads bronze
  directly, and `test_marts_match_bronze.py` asserts per hour that silver and the fact equal bronze,
  `ingested_at` included, so a rewrite that failed to arrive would fail it.
- **No row-level change history.** A materialized view does not support `DESCRIBE HISTORY`. The
  evidence of what a refresh did is the event log (technique per flow, row counts) plus comparing
  snapshots, as done here.
- **This does not generalise to every source.** It works because bronze is Delta with row tracking,
  and every change arrives as a replace of whole hours. A source the pipeline cannot see changes in
  would fall back to full recomputes, correct but costlier.

## Revisit when

- The pipeline plans the fact or a silver view as COMPLETE_RECOMPUTE on routine hourly updates.
  That would be a cost issue at M4-S3, not a correctness issue.
- `test_marts_match_bronze.py` fails after a reprocess.
