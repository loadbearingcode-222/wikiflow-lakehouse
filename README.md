# wikiflow-lakehouse

[wikiflow](https://github.com/loadbearingcode-222/wikiflow), an hourly pipeline over Wikimedia
pageviews, rebuilt on Databricks. It answers the same question:

> How does public attention to the largest tech companies move hour by hour, across every
> language edition of Wikipedia, and what share of all Wikimedia traffic is it?

Databricks Free Edition · Lakeflow Jobs · Lakeflow Declarative Pipelines · Delta Lake · Unity
Catalog · Asset Bundles · AI/BI dashboard · Genie · GitHub Actions

**Status:** all 22 planned stories are merged, one pull request each and in plan order
([`docs/MILESTONES.md`](docs/MILESTONES.md)). Every hour loaded reconciles exactly with
Wikimedia's own published totals, and the marts match wikiflow's row for row. Releases reach
prod only when published (see [How a change reaches prod](#how-a-change-reaches-prod)).

---

## The evidence

Every figure here was measured on the running workspace, not asserted. The linked pull request
or document shows how.

| | |
|---|---|
| **Correctness** | Each hour's per-domain totals, computed from ~7M lines, equal Wikimedia's `projectviews` **exactly**: 82 hours, 159,717 domain-hours, 1,453,938,744 views, 0 off (sandbox, 2026-09-18). The check is a pipeline expectation that **fails the update**: one planted extra view stopped it, naming hour 22 and domain `en` ([#11](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/11)) |
| **Parity with wikiflow** | The same five marts, built by Spark and Lakeflow here and by DuckDB and dbt there, are **identical row for row, doubles included**, over wikiflow's 49 hours: 17,342 fact rows, 343 share rows, 1,993 wikis ([#13](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/13)) |
| **Idempotency** | Re-running an hour leaves the same totals and exactly one replace commit per table ([#8](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/8)). Every merge re-proves it: the dev smoke re-ingests two fixed hours and fails unless they reproduce every earlier run of the same files |
| **Rewritten hours** | Re-ingesting an earlier-dated hour changed that hour's rows, and no other hour's, in silver and gold at the next refresh. There is no watermark to reset ([ADR 0011](docs/adr/0011-mvs-replace-the-watermark.md)) |
| **Contract** | Bronze's Delta CHECK constraints are generated from the same contract the rows are validated against. Delta itself refuses `3882.0` as a view count ([#5](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/5)) |
| **Integrity** | A download cut at 30% is refused at landing, and Spark refuses to read it. A *valid* but short file is refused by ingest's line count, and by reprocess's checksum ([drills](docs/FAILURE_DRILLS.md)) |
| **Recovery** | A lost hour came back byte-identical by Delta `RESTORE` in **27.8 s**, or was rebuilt from raw by `reprocess` in 350 s and re-proved against Wikimedia's totals ([drills](docs/FAILURE_DRILLS.md)) |
| **Operation** | The gaps, freshness, same-hour-of-day volume and mart-lag checks each **fired** on a planted state and reset, through SQL alerts ([#14](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/14)). Retention refused an expire-everything config and kept all 160 files ([#15](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/15)) |
| **Coverage** | The company filter resolves 7 companies into 2,240 articles and 6,502 redirects (8,742 rows), exactly wikiflow's. A refresh that lost the redirects is refused ([#7](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/7)) |
| **Throughput** | One hour (7,118,105 lines) scans in **6.05 s** on Spark, and in 3.07 s in DuckDB on a local copy of the file. Output was identical across 9 runs. A gzip file is one Spark task, so serverless executors add nothing to it ([benchmark](scripts/benchmark_extraction.py)) |
| **Cost** | **0.141 DBUs per ingested hour** for the ingest job, plus **0.078** for the pipeline. That is sandbox's upper bound, from `system.billing.usage` on the dashboard's Cost page ([ADR 0010](docs/adr/0010-where-the-download-runs.md)) |
| **Asked in words** | Genie's answers to all 5 certified questions equal hand-written SQL that reads only bronze and raw ([#18](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/18)) |

---

## The 20-minute tour

This runs top to bottom against the sandbox catalog `wikiflow_sbx`. The timings were measured on
2026-09-18. You need:
- [uv](https://docs.astral.sh/uv/);
- the Databricks CLI (1.16+), with a `DEFAULT` profile for the workspace;
- the three catalogs, created once by `scripts/bootstrap/create_catalogs.py`.

**1. The code, with no cloud involved** (16 s)

```bash
uv sync --group dev --group duckdb
uv run pytest tests/unit -q          # 233 passed
```

**2. The bundle, deployed to your sandbox** (33 s): jobs, the pipeline, the dashboard, the Genie
space and the alerts, all from `databricks.yml` and `resources/`.

```bash
databricks bundle validate -t sandbox
databricks bundle deploy -t sandbox
```

**3. One hour, end to end** (4 min 39 s). Download → verify → one Spark scan → two Delta writes →
reconcile against Wikimedia → audit → verify the run → refresh the pipeline, whose reconciliation
expectation would fail the update on a single view:

```bash
databricks bundle run ingest_hours -t sandbox \
  --params start=2026-09-15T11,end=2026-09-15T11,replace_present=true
# VERIFIED {"hours": 1, "earlier_attempts_compared": 5, "problems": []}
```

`earlier_attempts_compared: 5`: the same file, ingested 5 times before, gave the same totals
every time.

**4. Check it yourself, from the laptop** (2 min 48 s): every hour against Wikimedia, and every
silver and gold hour against bronze, through Databricks Connect:

```bash
uv run --group connect pytest -s -q \
  tests/integration/test_reconciliation.py tests/integration/test_marts_match_bronze.py
# RECONCILIATION {'hours': 82, 'domains': 159717, 'mismatched': 0, 'ours': 1453938744, 'theirs': 1453938744}
# company grain: 82 hours, 0 differ · domain grain: 82 hours, 0 differ
```

**5. Rebuild the hour from raw, without downloading it** (~6 min; 16 min 26 s on this run, see
below). The file's sha256 must match the one recorded at ingest:

```bash
databricks bundle run reprocess -t sandbox --params start=2026-09-15T11,end=2026-09-15T11
# VERIFIED {"hours": 1, "earlier_attempts_compared": 6, "problems": []}
```

**6. Spark vs DuckDB on the same hour and the same compute** (2 min 36 s):

```bash
uv run --group connect --group duckdb python scripts/benchmark_extraction.py submit \
  --hour 2026-09-15T11 --runs 1 --engines spark,duckdb
# Output: IDENTICAL across 2 runs (1 distinct digest pair(s)); lines 7,118,105, views 18,917,973, ...
```

One run of each is a demonstration. The measured comparison (medians of 3, and a local-disk
variant) is in the script's docstring.

**7. Look at it** (6 s): links to the dashboard, the Genie space, the four alerts and every job.

```bash
databricks bundle summary -t sandbox
```

**The run these timings come from took 27 min 24 s.** About 10 of those minutes were Free
Edition itself. In step 5, the pipeline update that reprocess starts failed five times with
`RESOURCE_EXHAUSTED: You've hit the limit for serverless compute for free usage`. At that moment
step 4's Databricks Connect session and the SQL warehouse were also up. The update retried itself
and completed once capacity freed, with nothing done by hand. Without that wait the tour takes
about 17 minutes.

---

## What changed moving to Databricks

The design survived the move: an hour is still one immutable file, and one scan still yields two
grains. The partition is still the only state, and reconciling against Wikimedia's totals is still
the test that matters. What changed is who enforces each rule.

| | wikiflow | Here | Why |
|---|---|---|---|
| **Scheduling** | Airflow; the data interval is the watermark | A Lakeflow job every 3 hours. `plan_hours` takes the hours Wikimedia has published, minus the hours bronze holds | A cron job has no data interval, so the plan comes from the partitions. There is still no stored watermark, and a missed run catches up on its own |
| **Download** | An Airflow task, with a pool of 2 against Wikimedia's 3-connection limit | The job's own serverless task, `for_each` at concurrency 2 | Free Edition limits egress to an unpublished set of trusted domains. All three hosts were measured reachable before any pipeline code was written ([ADR 0010](docs/adr/0010-where-the-download-runs.md)) |
| **Raw files** | MinIO, `s3://` | The Unity Catalog volume `raw.dumps`, in the same hive layout | It is governed like the tables. Spark, Auto Loader and plain Python all read it |
| **Extraction** | One DuckDB scan, two Parquet files | One Spark scan, two Delta tables, each written by `REPLACE WHERE (dt, hour)` | Serverless cannot cache. The scan groups every line into ~2,400 rows, which are collected once, and Python builds both grains from them |
| **Contract** | Validated before publishing | The same validation, plus Delta CHECK constraints generated from it | The table refuses a bad row even from a writer that skipped the validation |
| **Audit** | One object per attempt | `ops.ingest_audit` with `delta.appendOnly`: an UPDATE is refused | The table enforces the immutability, not a convention |
| **Transform** | dbt, incremental on `ingested_at` | A Lakeflow Declarative Pipeline of materialized views, with expectations | wikiflow needed a watermark on write time to see rewritten hours. A materialized view recomputes what changed ([ADR 0011](docs/adr/0011-mvs-replace-the-watermark.md)) |
| **Reconciliation** | A dbt test, reading the raw files | `ops.reconciliation`: an expectation that **fails the update**, fed by Auto Loader | A mismatch stops the marts from refreshing, instead of being reported next to them |
| **Second warehouse** | Postgres, with row-by-row parity | Dropped. Parity is with wikiflow's own DuckDB marts instead ([#13](https://github.com/loadbearingcode-222/wikiflow-lakehouse/pull/13)) | That proves the port itself, which a second engine here would not |
| **Operations** | DAGs and a webhook | Jobs, 4 SQL alerts, email on every failure, and [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | |
| **Presentation** | Streamlit | An AI/BI dashboard with a Cost page, and a Genie space with certified questions | Anyone with workspace access can read it, and ask in words |
| **Governance** | — | UC tags and column comments, applied as a diff, plus lineage from system tables ([`docs/LINEAGE.md`](docs/LINEAGE.md)) | |
| **Environments** | Docker Compose on a laptop | One Asset Bundle with three targets and three catalogs | The same files deploy to sandbox, dev and prod |

Databricks also brought new traps. Serverless Spark runs in ANSI mode and cannot cache, and a
materialized view that joins another one can read its old version. Several more were learned the
hard way; each is written down with its measurement in [`CLAUDE.md`](CLAUDE.md) ("Known traps").

---

## How a change reaches prod

```
docs/MILESTONES.md          the plan: 22 numbered stories, each with "Accept when" boxes
      │
      ▼  branch feat/m<M>-s<S>-<slug>  ·  PR "M<M>-S<S>: <title>"  ·  one story open at a time
   CI (every push)          story-lint: branch, title and plan order · layout-lint: every path
                            declared in the plan · ruff · mypy --strict · unit tests ·
                            bundle validate for sandbox, dev and prod
      │
      ▼  deployed to sandbox from the laptop; the PR records what was measured there
   merge commit to main     (a pre-push hook refuses direct pushes to main)
      │
      ▼  deploy-dev.yml
   dev                      bundle deploy -t dev → hello → ingest_hours re-ingests two fixed
                            hours and verifies them → pipeline refresh (reconciliation can fail
                            it) → governance
      │
      ▼  a milestone's last story: its release is drafted; a person publishes it
   deploy-prod.yml          the tag is a published release, and it matches the package version →
                            bundle deploy -t prod → hello → company filter refresh → governance.
                            Only prod runs schedules
```

Rolling back means running `deploy-prod` by hand with an earlier published tag. The repo is
private on GitHub's free plan, where branch rulesets and environment approvals are not enforced.
So the gates are the pre-push hook, CI, and publishing a release, which is always a person's step.

---

## Architecture

```
dumps.wikimedia.org   one immutable .gz per hour, ~7M lines, 45-70 MB; 3 connections per IP
      │
      ▼  job ingest_hours (prod: every 3 h)   plan_hours → for_each ×2 ingest_hour → verify_run
      │    ingest_hour: download → verify (length, gzip, lines) → land → one Spark scan → two
      │                 Delta writes → reconcile against projectviews → audit row
      ▼
   raw.dumps (volume)   pageviews + projectviews, dt=/hour= layout             7-day retention
   bronze               company_pageviews  ~500 rows/h   the fact              kept
                        domain_totals    ~1,950 rows/h   denominator and reconciliation key
   ops.ingest_audit     one row per attempt, append-only
      │
      ▼  pipeline wikiflow_pageviews (the job's last task)
   raw.projectviews     Auto Loader: Wikimedia's own totals, the oracle
   silver               stg_company_pageviews, stg_domain_totals     materialized views + expectations
   gold                 dim_company, dim_wiki, fct_company_pageviews_hourly,
                        agg_company_share_of_traffic, agg_company_daily
   ops.reconciliation   EXPECT mismatched_domains = 0 ON VIOLATION FAIL UPDATE
      │
      ▼
   AI/BI dashboard (Attention, Cost) · Genie space (5 certified questions)

   other jobs:  company_pages_refresh   daily filter snapshot; refuses a shrink over 10%
                quality_checks          gaps, freshness, volume anomaly, mart lag → 4 SQL alerts
                raw_retention           manifest first; deletes only in prod
                reprocess               rebuild hours from raw, no download, checksum-guarded
                governance              UC tags and comments, as a diff
```

---

## When things broke

[`docs/FAILURE_DRILLS.md`](docs/FAILURE_DRILLS.md) records four real incidents and eight
deliberate drills, all with measured outcomes:
- **Two parallel iterations raced on `ALTER TABLE`.** One hour's attempt was committed with no
  audit row. Tables are now created before the fan-out.
- **Serverless retried a failure that was a finding.** A quality failure ran twice and wrote its
  results twice, because serverless retries by default.
- **A materialized view read another view's old rows.** Every new title came through undecoded.
  An expectation caught it, not a person.
- **Found by a drill: a valid but short raw file.** Reprocess accepted it, and only the reconciliation caught it
  downstream. A rebuild now checks the file's sha256 against the one recorded at ingest.
- **Also drilled:** a truncated download, a lost partition (`RESTORE` vs reprocess, both timed),
  a filter that lost its redirects, a run killed mid-flight, a kill between the two grain
  commits, retention set to expire everything, and tests proven able to fail.

---

## What this is not

- **Not a streaming system.** Wikimedia publishes hourly. Prod ingests every 3 hours, which the
  measured DBUs favour ([ADR 0010](docs/adr/0010-where-the-download-runs.md)).
- **Not horizontally scaled, because it doesn't need to be.** A gzip file is one Spark task.
  More parallelism would need more files, not bigger compute.
- **Not paged.** Failures and alerts email the owner, with no on-call and no SLA.
- **Short on history.** The volume check compares an hour with the same hour on other days, and
  skips until at least 3 other days are held rather than guessing.
- **On a free workspace shared with another project.** Free Edition allows one active pipeline
  per type and can pause all compute without warning (both in [`CLAUDE.md`](CLAUDE.md)).

---

## Data source and attribution

Pageviews data is published by the Wikimedia Foundation under
[CC0](https://creativecommons.org/publicdomain/zero/1.0/) at
[dumps.wikimedia.org](https://dumps.wikimedia.org/other/pageviews/). Requests identify
themselves with a descriptive `User-Agent`, as Wikimedia asks of automated clients.
