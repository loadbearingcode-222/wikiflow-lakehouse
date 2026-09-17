# Runbook

What runs, what can go wrong, and what to do about it. Reprocess, backfill, both retention
refusals, the reconciliation failure and all four alerts were exercised for real in sandbox (the
PRs of M1-S5, M2-S2 and M3-S1..S3 hold the evidence). The other failure rows describe what the code
does when it raises that error.

Replace `<t>` with the target (`sandbox`, `dev`, `prod`) and `<catalog>` with its catalog
(`wikiflow_sbx`, `wikiflow_dev`, `wikiflow`). Run CLI commands from the repo root.

## What runs

| Job / pipeline | Prod schedule (UTC) | Does | Writes |
|---|---|---|---|
| `wikiflow_ingest_hours_<t>` | :20 every 3 h | plan published hours bronze lacks → ingest each (2 at a time) → verify → refresh pipeline | `raw.dumps`, `bronze.*`, `ops.ingest_audit` |
| `wikiflow_pageviews_<t>` (pipeline) | started by the two jobs above and below | `raw.projectviews`, `silver.*`, `gold.*`, `ops.reconciliation` | |
| `wikiflow_quality_checks_<t>` | :50 every 3 h | gaps, freshness, volume anomaly, mart lag | `ops.quality_results` |
| 4 SQL alerts `wikiflow quality - *` | :55 every 3 h | newest result of each check | email on TRIGGERED and OK |
| `wikiflow_company_pages_refresh_<t>` | 02:30 daily | resolve the 7 companies into titles and redirects | `reference.company_pages_*` |
| `wikiflow_raw_retention_<t>` | 04:00 daily | delete raw partitions older than 7 days (prod only; dry run elsewhere) | `ops.retention_manifest` |
| `wikiflow_reprocess_<t>` | on demand | rebuild bronze hours from raw, no download | as ingest |

Every job in this table emails its owner on failure; the pipeline emails on a failed update or flow.
Sandbox and dev schedules and alerts are paused: they run only when started by hand or by
`deploy-dev` (which re-ingests 2026-09-15T10..11 on every merge).

## Alerts

Each alert reads the newest row of its check in `ops.quality_results`, and triggers when that row
failed, is more than 4 hours old, or does not exist. Start with the row itself:

```sql
SELECT checked_at, as_of, check_name, status, detail FROM <catalog>.ops.quality_results
ORDER BY checked_at DESC LIMIT 8;
```

If every alert fired at once with rows over 4 hours old, the checks stopped running: look at
`quality_checks` itself (a paused schedule, or compute paused - see below).

### gaps - an hour is missing from bronze

The `detail` names the hours. An hour older than the freshness horizon (8 h) is missing.

1. Is it published? `plan_hours` only plans hours Wikimedia lists. Open the month's listing
   (`https://dumps.wikimedia.org/other/pageviews/<yyyy>/<yyyy-mm>/`) and look for both
   `pageviews-<label>.gz` and `projectviews-<label>`, where the label is the hour + 1.
2. Published, and its raw files are still in the volume (under 7 days old): **reprocess** it (below).
3. Published, raw gone: run `ingest_hours` for it. It plans every published hour bronze lacks:
   ```bash
   databricks bundle run ingest_hours -t <t> --params start=2026-09-16T12,end=2026-09-16T12
   ```
4. Not published after a day: Wikimedia skipped it. Record it; there is nothing to ingest.

### freshness - ingestion stopped

The newest hour held is more than 8 hours old.

1. Read the latest `ingest_hours` runs. A failed run emailed; a run that never started did not.
2. Schedule paused, or compute paused (below): resume, then run `ingest_hours` with no parameters.
   It catches up 24 hours per run; pass `max_hours=100` to catch up faster.
3. All runs succeeded but planned nothing: Wikimedia is publishing late. `PLANNED` shows
   `newest_published`. Wait; the check clears when the hours arrive.

### volume_anomaly - an hour is the wrong size

An hour's total views are more than 4 sigma from the same hour of day on other days.

1. Compare its audit row with neighbours:
   ```sql
   SELECT interval_start_utc, task_key, status, line_count, total_views, projectviews_views, domains_mismatched
   FROM <catalog>.ops.ingest_audit ORDER BY interval_start_utc DESC, started_at DESC LIMIT 20;
   ```
2. `total_views = projectviews_views` and the lines look normal: the traffic is real (news
   spikes do happen). Nothing to fix; it clears as the hour ages out of comparison or more days
   accrue.
3. Bronze differs from what the audit recorded, or from projectviews: **reprocess** the hour.

### mart_lag - the marts stopped following bronze

Bronze holds writes the gold fact has not carried for over 2 hours.

1. Read `refresh_pageviews` of the latest `ingest_hours` run, then the pipeline's newest update.
2. `Only one active pipeline per type`: NOAA's `medallion` pipeline held the slot. The task retries
   twice, 10 minutes apart; if it still failed, start the pipeline by hand:
   `databricks bundle run pageviews -t <t>`.
3. An expectation failed: see "The pipeline update failed" below.

## Failures

### `ingest_hours`

| Failed task | Message | Do |
|---|---|---|
| `plan_hours` | `no company filter is published` | `databricks bundle run company_pages_refresh -t <t>` (dev needed this once) |
| `ingest_hour` | `HTTPStatusError ... 404` | the hour was delisted between plan and download; re-run the job |
| `ingest_hour` | `429 Too Many Requests` / `deferring to the task-level retry` | the task retries after 5 min; if it keeps failing, something else is downloading from this IP |
| `ingest_hour` | `DownloadVerificationError` | a truncated transfer; the retry downloads again. Raw was not touched |
| `ingest_hour` | `MalformedRowsError` | the file parsed short or changed after landing; bronze was not written. Re-run; if it persists, inspect the raw file |
| `ingest_hour` | `ContractViolation` | a row broke the bronze contract; bronze was not written. This is a code or source change: investigate before re-running |
| `verify_run` | `does not reconcile with projectviews` | bronze is written but disagrees with Wikimedia; reprocess the hour, and if it reproduces, investigate extraction |
| `verify_run` | `did not reproduce run ...` | re-ingesting the same file changed the totals: extraction is not deterministic. Investigate before anything else |
| `refresh_pageviews` | slot held / expectation | see mart_lag, and "The pipeline update failed" |

Every attempt, failed or not, has a row in `ops.ingest_audit` with the error.

### The pipeline update failed

Read the update's error: `databricks pipelines list-pipeline-events <pipeline-id>`.

- `domain_totals_equal_projectviews`: `ops.reconciliation` found an hour whose bronze totals differ
  from Wikimedia's. The message names the hour and the domain codes. **Reprocess** that hour.
- `url_decode_edge_cases`: someone changed the url_decode rule and a pinned case broke.
  `tests/unit/test_pipeline_sql.py` should have failed first; fix the rule.
- Any other `EXPECTATION_VIOLATION`: the named expectation and the violating row are in the
  message. A contract-enforced bronze makes these rare; treat one as a code change to investigate.

### `company_pages_refresh`

`refusing to publish: rows fell from X to Y` is the shrink guard: a Wikidata or MediaWiki outage
made the filter lose over 10% of its rows. The current filter stays in use, so nothing is wrong
downstream. Wait for the next daily run. If the loss is real (a company was removed from
`seeds/companies.csv`), the guard needs a deliberate code change, in a story.

### `raw_retention`

`refusing to sweep` means the window or the clock is wrong. Nothing was deleted and nothing was
recorded. Check the `retention_days` parameter. Every run's plan and deletions are in:

```sql
SELECT job_run_id, step, count(*) AS partitions, sum(bytes) AS bytes
FROM <catalog>.ops.retention_manifest GROUP BY ALL ORDER BY min(recorded_at) DESC;
```

## Procedures

### Reprocess hours from raw

For hours still in the raw volume (7 days). Rebuilds bronze from the landed files without
downloading, verifies each hour like an ingest (reconciliation, and reproducing any earlier attempt
of the same file), then refreshes the marts:

```bash
databricks bundle run reprocess -t <t> --params start=2026-09-16T12,end=2026-09-16T14
```

`PLANNED` lists the hours that had raw files; hours without them are not planned (use
`ingest_hours` for those). The audit rows carry `task_key = reprocess_hour`.

### Backfill

`ingest_hours` with an earlier `start` and a larger cap. Nothing needs resetting (no watermark):

```bash
databricks bundle run ingest_hours -t <t> --params start=2026-09-14T00,max_hours=100
```

40 hours took 29 minutes in sandbox (2 downloads at a time), plus one pipeline refresh.

### Release to prod

A merged milestone has a draft release. Publishing it runs `deploy-prod`: deploy, `hello`, and a
company filter refresh. **Publish in order**: publishing an older release after a newer one deploys
the older code. To roll back, run the `deploy-prod` workflow by hand with an earlier published tag.

### Compute is paused

Free Edition can pause all compute for the account (CLAUDE.md trap 8): jobs fail with
`Triggering new runs ... is currently disabled temporarily` and the warehouse with
`Cannot create the resource`. It is not a code error and cleared within hours when it happened.
Wait, then run `ingest_hours` once; it catches up every missed hour on its own.

### Rehearse an alert

Alerts v2 have no run-now. Set the alert's schedule to `0 * * * * ?` UNPAUSED
(`databricks alerts-v2 update-alert <id> schedule --json '{"schedule": {...}}'`), plant the
condition as a real state, and watch `databricks alerts-v2 get-alert <id>`. `bundle deploy` puts
the committed schedule back. M3-S1's PR shows the full rehearsal of all four.
