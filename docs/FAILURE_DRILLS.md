# Failures, real and rehearsed

A green run shows the happy path works. What matters in operation is what happens when it doesn't:
whether a failure is **detected**, whether it is **contained** before bad data reaches anyone, and
whether **recovery** is routine.

Part one is four incidents that actually happened while building this project. Part two is a set of
deliberate drills against the running sandbox. Every number here was measured on 2026-09-17 and
2026-09-18; nothing is illustrative. wikiflow's own drills are in its `docs/FAILURE_DRILLS.md`, and
the ones repeated here are marked with what changed on Databricks.

---

## Incidents

### 1. Two parallel iterations raced on `ALTER TABLE` (M1-S5)

A run ingesting four hours two at a time left hour 12 with **no audit row for its first attempt**
and a second attempt five minutes later. The audit table had just gained two columns, and both
iterations ran `ALTER TABLE ... ADD COLUMNS` at once:

```
MetadataChangedException: [DELTA_METADATA_CHANGED] The metadata of the Delta table
wikiflow_sbx.ops.ingest_audit has been changed by a concurrent update.
```

The write that lost was the **success-path audit write**, so the attempt's data was committed while
its record was not. A first run on empty catalogs would have raced the same way on CREATE TABLE and
ADD CONSTRAINT.

**Fixed:** `plan_hours` creates and migrates every table before the fan-out, so iterations only
write data; and a success row that cannot be written now leaves a `failed` row instead of nothing.

### 2. Serverless retried a failure that was a finding (M3-S1)

The first planted quality failure ran **twice** (attempts 0 and 1) and wrote every result row
twice, although the task set no retries: serverless retries a failed task by default.
**Fixed:** `max_retries: 0` and `disable_auto_optimization: true` on tasks whose failure is a
finding. The rerun on the same planted state had one attempt and four rows.

### 3. A materialized view read another view's old rows (M2-S1)

Silver looked up decoded titles in a small view of the same pipeline. When bronze and that view
both changed in one update, the incremental refresh joined against the view's **previous** version,
so every newly seen title came through undecoded. It was caught by an expectation
(`title_decoded`), not by a person, and only because the join was written as a LEFT JOIN with a
NOT NULL check rather than an inner join, which would have dropped those rows silently.
**Fixed:** per-row rules are applied inline (one copy of the rule per file, kept identical by a unit
test), and the one gold view-to-view join is pinned to `REFRESH POLICY FULL`.

### 4. A panel that had never run in a fresh catalog (M4-S1)

The dashboard's quality panel failed in dev with `TABLE_OR_VIEW_NOT_FOUND ops.quality_results`:
the table is created by the first quality run, which dev had never had. Sandbox, where the checks
had run, showed nothing wrong. **Fixed:** `plan_hours` creates that table too, so the first ingest
in any catalog makes the panel renderable.

---

## Drill 1 — a download truncated mid-transfer

**Question:** if a transfer dies partway through, what reaches the warehouse?

**Method:** take the real hour 2026-09-15T11 (62,266,533 bytes gzipped, 7,118,105 lines), keep the
first 30% (18,679,959 bytes), and offer it to each layer.

| Layer | Result |
|---|---|
| Landing verification (`_scan_content`, gzip read to the end) | **Refused:** `content failed to decompress (Compressed file ended before the end-of-stream marker was reached)` |
| Spark reading the intact file | 7,118,105 lines, 18,917,973 views, 11.5 s |
| Spark reading the truncated file | **Raised** `SparkException: [FAILED_READ_FILE.NO_HINT]`, 8.6 s |

**What changed from wikiflow:** DuckDB with `ignore_errors=true` returned 2,111,414 rows and **no
error** for the same 30% file, which is why wikiflow added a malformed-line gate. Spark on
Databricks refuses the file outright, so here the gate is the second line of defence, not the first.

## Drill 1b — a landed file replaced by a *valid* short file

**Question:** a truncated file is refused; what about a file that is perfectly valid and simply
holds less? (A re-compressed prefix, or an interrupted copy that was re-gzipped.)

**Method:** replace the landed file for 2026-09-15T11 with a valid gzip of its first 2,135,431
lines (30%), and run each path that reads it.

| Path | Result |
|---|---|
| Spark | reads it happily: 2,135,431 lines, no error |
| **Ingest** (`--source wikimedia`) | **Refused:** `Spark read 2,135,431 lines … but landing verified 7,118,105. The file changed or was read short` |
| **Reprocess** (`--source raw`), before this drill | **Accepted:** 2,135,431 lines, 5,190,209 views written to bronze. It recounts lines from the file itself, so it had no independent number to compare with |
| …and then the reconciliation | caught it: **1,514 of 1,946 domains differ**, 5,190,209 vs 18,917,973 views, so `verify_run` fails the run before the marts refresh |

**Change made:** a rebuild now also checks the file's **sha256** against the one the audit recorded
when the hour was ingested. Raw files are immutable at the source, so a different checksum means
the local copy changed. Drilled through the real job afterwards:

```
RawFileChangedError: the raw file for 2026-09-15T11 has changed since it was landed:
sha256 2faf6808f630 now, ffc1a697d63c when it was ingested.
```

Bronze was **untouched** (486 rows / 5,614 views and 1,946 / 18,917,973, same `ingested_at`), and
re-ingesting the hour from Wikimedia restored it, with `verify_run` comparing it against 4 earlier
attempts of the same file.

## Drill 2 — a published hour is lost, two ways back

**Question:** if an hour disappears from bronze, is it noticed, and what does each recovery cost?

**Method:** snapshot 2026-09-16T13 (535 company rows, 1,950 domain rows), `DELETE` both grains,
recover, compare.

| Recovery | Time | Result |
|---|---|---|
| `RESTORE TABLE ... TO VERSION AS OF` (both grains) | **27.8 s** | **Byte-identical, `ingested_at` included**: 535 and 1,950 rows, `EXCEPT ALL` empty both ways. `numRestoredFiles: 1` per grain |
| `reprocess` job from raw (plan → rebuild → verify → refresh marts) | **350 s** | Identical apart from a new `ingested_at`; `VERIFIED … problems: []` |

**Trade-off, and the reason both are documented:** `RESTORE` is fast and exact, but it rewinds the
**whole table** to that version, so any other hour written since would be undone with it. Reprocess
touches only the hours asked for and re-proves them against projectviews. The runbook's default is
reprocess; `RESTORE` is for when nothing else has been written since.

## Drill 3 — the filter refresh loses the redirects

**Question:** if a refresh comes back with the articles but no redirects (a MediaWiki outage while
Wikidata answers), does the smaller filter get published?

**Method:** the sandbox integration test replays it against the real published filter.

**Refused:** `rows fell from 8,742 to 2,240 (74.4%, limit 10%). A resolution that lost this much is
more likely a failed lookup than a real change; the current filter stays in use.` Both reference
tables are unchanged at the same versions, and `company_pages_current` still returns the full
filter. Every article survives this failure, which is why the guard counts rows, not articles.

## Drill 4 — a run killed in flight

**Question:** what does a hard kill leave behind?

**Method:** start `ingest_hours` for two new hours and cancel the run ~20 s into its two running
ingest iterations.

| Checked | Result |
|---|---|
| Run | `CANCELED`; `verify_run`, `any_hours_planned` and `refresh_pageviews` all `SKIPPED (UPSTREAM_CANCELED)` |
| Bronze | 0 rows for either hour, **and no commit at all** since the previous drill: Delta leaves no partial commit |
| Raw volume | both hours **fully landed** (57,640,214 and 57,908,393 bytes): the kill landed between landing and extraction |
| Audit | **no rows** — see the limit below |
| Recovery | one ordinary `ingest_hours` run: planned both hours, `VERIFIED … problems: []`, **304 s** |

**Limit found:** a killed attempt writes no audit row, because the process is stopped before its
error handler runs. "One row per attempt" holds for attempts that fail, not for attempts that are
killed. What notices instead is the hour's absence: the planner re-plans it and the gap check
reports it. The job run list is the record that the attempt happened at all.

## Drill 4b — a kill between the two grain commits

**Question:** the two bronze grains are two commits. What if only the first lands?

**Method:** write `company_pageviews` for 2026-09-17T06 (483 rows) and not `domain_totals`, exactly
what a kill between the commits leaves.

| Checked | Result |
|---|---|
| Gap check | **`3/73 hours MISSING: 2026-09-17T06, …`** — the half-written hour counts as missing |
| Planner | planned it (`present: 0`): presence is the **domain** grain, which is written second |
| Recovery | `ingest_hours`, 304 s: hour 06 ends with 483 company rows from **one** write (replaced, not doubled), 1,936 domain rows, 0 domains mismatched |

The order of the two writes is what makes this safe: the grain written last is the one everything
else treats as proof the hour exists.

## Drill 5 — retention misconfigured to expire everything

**Question:** if the window or the clock is wrong, does the sweep destroy the raw layer?

**Method:** run retention with `retention_days=-1` **and `mode=apply`**, so nothing but the guard
stands between the config and the files.

| Step | Result |
|---|---|
| Raw files before | 160 |
| The sweep | **Refused:** `all 160 partitions are dated before 2026-09-18 (retention -1 day(s)). That means the window or the clock is wrong, not that every hour expired at once. Nothing was deleted.` |
| Raw files after | 160 — unchanged; the manifest recorded nothing |

A 0-day window is refused by a second rule (M3-S2): it keeps today's partitions, so the
all-expired rule would not fire while nearly everything went.

## Drill 6 — the tests are proven able to fail

**Question:** would these checks notice if the rule they protect changed?

| Mutation | Result |
|---|---|
| `count_views: Positive` → `NonNegative` (a zero-view row becomes legal) | **2 unit tests fail**: the contract's row test and the generated CHECK-constraint test |
| The url_decode rule changed in one of its two copies | **1 unit test fails**: `test_silver_and_the_pinned_cases_apply_the_same_url_decode_rule` |
| Restored | 226 pass |

The pipeline's own gates were proven able to fail the same way in earlier stories: a wrong pinned
url_decode case failed the update (M2-S1), a planted one-view difference failed
`ops.reconciliation` naming hour 22 and domain `en` (M2-S2), and all four quality alerts were seen
firing on planted conditions and resetting (M3-S1).
