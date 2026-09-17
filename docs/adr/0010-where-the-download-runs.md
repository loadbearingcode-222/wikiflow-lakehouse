# ADR 0010 — Where the download runs, and how often prod ingests

- **Status:** Accepted
- **Date:** 2026-09-17
- **Numbering:** continues wikiflow's ADRs 0001–0009, which this project inherits.
- **Decision:** The hourly download runs **inside Databricks**, in the ingest job's own
  serverless tasks. No external fetcher is needed. Prod ingests **every 3 hours**, catching up
  every published hour that bronze does not yet hold.

## Context

wikiflow downloaded each hour from `dumps.wikimedia.org` inside an Airflow task. Moving that
task to Databricks Free Edition carries one risk nothing else in the plan has: Free Edition
states that "outbound internet access is restricted to a limited set of trusted domains", and
publishes no list. If the dumps server, Wikidata or the Wikipedia APIs were outside that set,
ingestion would need a fetcher running elsewhere (GitHub Actions uploading to a volume, with a
file-arrival trigger starting the job). That is a different architecture, so it was measured
before any pipeline code was written.

## Measurements (2026-09-17, sandbox, serverless environment 5)

**1. Egress.** The spike job ran on serverless job compute
([run 485863836557709](https://dbc-5cb7afce-0a32.cloud.databricks.com/jobs/900610693993671/runs/485863836557709?o=991861943955874)).

| Host | Request | Result |
|---|---|---|
| `dumps.wikimedia.org` | `projectviews-20260915-120000` | HTTP 200 in 170 ms |
| `www.wikidata.org` | `w/api.php?action=query&meta=siteinfo` | HTTP 200 in 109 ms |
| `en.wikipedia.org` | `w/api.php?action=query&meta=siteinfo` | HTTP 200 in 89 ms |

**2. One real hour** (`pageviews-20260915-120000.gz`, the interval 11:00–11:59 UTC).

| Step | Measured |
|---|---|
| Download to `/Volumes/wikiflow_sbx/raw/dumps/` | 62,266,533 bytes in **17.5 s**; bytes written = `Content-Length` |
| gzip decompressed end to end (CRC checked) | 7,118,105 lines in **4.2 s** |
| Spark scan: `read.text` → `split` → sum by domain | 7,118,105 rows in **17.4 s** (Spark 4.2.0) |
| Reconciliation against Wikimedia's `projectviews` for the same hour | **1,946 domains, 0 mismatched; 18,917,973 = 18,917,973 views** |
| Whole task, submit to terminate | **75 s** |

The reconciliation matches wikiflow's own measurement for this hour: the same 1,946 domain
codes, reconciled exactly. It also confirms that Spark's `split` on single spaces parses the
dump correctly with no CSV quoting, the same property wikiflow's DuckDB reader needed
`quote=''` for.

**3. Triggers.** Probed with two throwaway jobs, created and deleted through the CLI so that
nothing entered the repo.

| Trigger | Measured |
|---|---|
| File arrival on `/Volumes/wikiflow_sbx/raw/dumps/trigger_probe/` | Accepted, and **fired**: file copied 08:58:45Z, run started 08:59:11Z (26 s), run `SUCCESS` |
| Table update | Accepted by the Jobs API. Not fired: that needs a table this project writes, so the fire test belongs to M2-S2, where it would replace the pipeline task |

**4. Databricks Connect.** Laptop → serverless works. `databricks-connect` 19.1 on
Python 3.12.13 opened a serverless session in 7.4 s. The first query (Spark 4.2.0, reading
`wikiflow_sbx.information_schema`) returned 11.9 s after start. Integration tests can run
against sandbox from a laptop or CI, with no cluster to manage.

**5. DBUs: not measurable today.** `system.billing.usage` held no rows for 2026-09-17, and its
newest `usage_end_time` was **2026-09-14 15:00**, three days behind. Serverless task wall time
stands in as the cost measure until M4-S3 builds the cost page from the real table.

## Decision

1. **The download runs in the ingest job's own serverless tasks.** Egress to all three hosts
   works, a whole hour is well inside a task's limits, and one scan reconciles exactly. The
   GitHub Actions fallback is not built. Its one prerequisite, a file-arrival trigger, is proven
   to exist and to fire, so this decision is cheap to reverse if Free Edition ever narrows
   egress.
2. **Prod ingests every 3 hours**, not hourly. Each run plans the published hours that bronze
   lacks and ingests all of them, so cadence changes nothing about correctness: the partition is
   the state (wikiflow ADR 0002).
   - **Why not hourly:** most of a 75 s task is fixed cost (task start, wheel install, session).
     Batching 3 hours into one run pays that once instead of three times, and this account's
     quota is shared with the NOAA project and was exhausted once today.
   - **Why not 6 hours:** wikiflow's freshness check fails past 8 h (its ADR 0006), and the
     measured publication lag reaches ~3 h 50 min after the covered hour ends (its ADR 0001).
     A 6-hour cadence would breach 8 h by design; 3 hours stays under it (~3 h + ~3 h 50 min).
   - Sandbox and dev never run the schedule (paused presets); they ingest fixed windows on demand.

## Consequences

- M1-S2 ports `download.py` to write straight into the `raw.dumps` volume from the task. At
  ~62 MB per hour, a 7-day raw window is ~10 GB, the same as wikiflow's.
- The source's 3-connection limit still binds. M1-S5 caps `for_each` concurrency at 2, as
  wikiflow's Airflow pool did.
- Integration tests (M1-S4 onward) use Databricks Connect against `wikiflow_sbx`.
- The cadence is revisited at M4-S3, when real DBUs per ingested hour are visible, or at once if
  the account's compute is paused again.

## Revisit when

- An egress probe starts failing (build the file-arrival fallback).
- DBUs per hour show that hourly would fit, or that 3-hourly does not (M4-S3).
- The freshness threshold or the publication lag changes.
