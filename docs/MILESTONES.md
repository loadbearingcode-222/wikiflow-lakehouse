# MILESTONES — the plan, in the order it merges

Two sections, in this order of priority: **the repo layout**, then **the stories**. Every pull
request is one story below, titled `M#-S#: <title>`, from branch `feat/m#-s#-<slug>`, merged
with a merge commit in the order listed. Nothing else becomes a pull request.

- A problem found after a story merged is fixed in the **next** story's PR, under "Also fixes".
- Only broken production earns a point story (`M1-S4.1`, branch `feat/m1-s4-1-<slug>`), added
  here in the PR that does the work.
- A path not declared in the layout arrives only through a story that edits the layout first.
- A milestone closes inside its **last** story. Its merge is released (`v0.M.0`); publishing that
  GitHub Release is the approval that deploys prod.

Both rules are enforced in CI: `.github/scripts/story_lint.py` (branch, title, order) and
`.github/scripts/layout_lint.py` (every tracked path is declared below).

---

## Repo layout

Each path is tagged with the story that adds it. A directory with nothing listed under it may
hold any files; a directory whose contents are listed may hold only those.

```
wikiflow-lakehouse/
├─ .github/
│  ├─ workflows/ci.yml                                   M0-S1  story-lint · layout-lint · lint · types · tests
│  ├─ workflows/deploy-dev.yml                           M0-S3  merge to main → dev + smoke
│  ├─ workflows/deploy-prod.yml                          M0-S3  release published → prod
│  ├─ scripts/story_lint.py                              M0-S1  branch · title · order
│  ├─ scripts/layout_lint.py                             M0-S1  every tracked path is declared here
│  ├─ rulesets/main.json                                 M0-S1  applied if the repo goes public
│  └─ pull_request_template.md                           M0-S1
├─ .githooks/pre-push                                    M0-S1  refuses direct pushes to main
├─ databricks.yml                                        M0-S2  sandbox / dev / prod targets
├─ resources/                                                   one YAML per workload
│  ├─ platform.yml                                       M0-S2  schemas, volume raw.dumps
│  ├─ job_hello.yml                                      M0-S2
│  ├─ job_feasibility_spike.yml                          M0-S4
│  ├─ job_company_pages_refresh.yml                      M1-S3
│  ├─ job_ingest_hours.yml                               M1-S2  grows: land (S2) → one ingest task per hour (S4) → plan + fan-out (S5)
│  ├─ pipeline_pageviews.yml                             M2-S1
│  ├─ job_quality_checks.yml · alerts_quality.yml        M3-S1
│  ├─ job_raw_retention.yml                              M3-S2
│  ├─ job_reprocess.yml                                  M3-S3
│  ├─ dashboard_attention.yml                            M4-S1
│  └─ genie_attention.yml                                M4-S2
├─ src/
│  ├─ wikiflow/                                                 Python package, built as a wheel
│  │  ├─ __init__.py                                     M0-S2
│  │  ├─ tasks/                                          M0-S2  thin job entry points, no logic
│  │  ├─ source.py · parsing.py · contracts.py           M1-S1
│  │  ├─ download.py                                     M1-S2
│  │  ├─ wikidata.py · redirects.py · reference.py       M1-S3
│  │  ├─ extract_spark.py · audit.py                     M1-S4
│  │  ├─ plan_hours.py                                   M1-S5
│  │  ├─ quality.py                                      M3-S1
│  │  ├─ retention.py                                    M3-S2
│  │  └─ reprocess.py                                    M3-S3
│  ├─ pipelines/pageviews/                                      one file per table
│  │  ├─ raw/ · silver/                                  M2-S1
│  │  └─ gold/ · ops/                                    M2-S2
│  ├─ dashboards/attention.lvdash.json                   M4-S1
│  └─ genie/attention.geniespace.json                    M4-S2
├─ seeds/companies.csv                                   M1-S3
├─ tests/
│  ├─ unit/                                              M0-S1  grows every story
│  ├─ fixtures/                                          M1-S1
│  └─ integration/                                       M1-S1  Databricks Connect → sandbox
├─ scripts/
│  ├─ bootstrap/                                         M0-S2  one-time: catalogs + binds
│  ├─ cross_platform_parity.py                           M2-S4  wikiflow's DuckDB marts vs these, EXCEPT ALL
│  └─ benchmark_extraction.py                            M5-S2
├─ docs/
│  ├─ MILESTONES.md                                      M0-S1  this file
│  ├─ adr/                                               M0-S4  numbered from 0010, after wikiflow's 0001–0009
│  ├─ RUNBOOK.md                                         M3-S3
│  └─ FAILURE_DRILLS.md                                  M5-S1
├─ CLAUDE.md                                             M0-S1  facts and traps, under 150 lines
├─ pyproject.toml · uv.lock · .python-version            M0-S1
├─ .pre-commit-config.yaml                               M0-S1
├─ README.md                                             init   stub; full tour in M5-S3
└─ LICENSE · .gitignore · .gitattributes                 init   the only direct push to main
```

**Never in the repo:** `HANDOFF.md`, ledgers, `automation/` logs, red-team script folders,
`.idea/`, `.claude/`, session transcripts, `.databricks/` bundle state, downloaded dumps.

---

## Stories

Environments: `sandbox` → catalog `wikiflow_sbx` (laptop), `dev` → `wikiflow_dev` (every merge
to `main`), `prod` → `wikiflow` (a published release). Only prod runs schedules.

## M0 — Foundation → v0.1.0

### M0-S1: repo scaffold, CI and the story rules
Branch `feat/m0-s1-repo-scaffold`

**Scope:**
- uv project (Python 3.12), ruff, mypy, pytest and pre-commit.
- `CLAUDE.md`, this file, the PR template and the pre-push hook.
- `ci.yml` running story-lint, layout-lint, lint, types and tests.
- The ruleset kept ready for a public repo.

**Accept when:**
- [ ] CI is green on this PR, including story-lint and layout-lint.
- [ ] story-lint refuses, each with its own message: a wrong branch name, a branch/title mismatch, an id not in the plan, a skipped predecessor, and a story merged twice.
- [ ] layout-lint refuses an undeclared tracked path, and names that path.
- [ ] The pre-push hook refuses a push to `main` (shown in a disposable clone).

### M0-S2: the bundle, three catalogs and a hello job
Branch `feat/m0-s2-bundle-platform`

**Scope:**
- `databricks.yml` with `sandbox`/`dev`/`prod` targets and a warehouse lookup.
- `resources/platform.yml`: schemas `raw, reference, bronze, silver, gold, ops` and volume `raw.dumps`.
- Catalog bootstrap by SQL plus `bundle deployment bind`, because a bundle cannot create a catalog on Default Storage.
- A `hello` job; `bundle validate` in CI.

**Accept when:**
- [ ] `bundle validate` passes for all three targets, and schema names are unprefixed.
- [ ] `hello` ran in sandbox.
- [ ] `SHOW SCHEMAS IN wikiflow_sbx` lists all six.

### M0-S3: deploy on merge, promote on release
Branch `feat/m0-s3-deploy-workflows`

**Scope:**
- `deploy-dev.yml`: a push to `main` runs `bundle deploy -t dev`.
- `deploy-prod.yml`: a published release deploys that tag with `bundle deploy -t prod`.

**Accept when:**
- [ ] Merging this PR deploys dev, and the run is linked.
- [ ] `hello` exists in the dev workspace from CI's deploy, not the laptop's.

### M0-S4: can Free Edition reach Wikimedia? + M0 close
Branch `feat/m0-s4-feasibility-spike` · releases **v0.1.0**

**Scope:**
- Spike job measuring:
  - reachability of `dumps.wikimedia.org`, `www.wikidata.org` and `*.wikipedia.org`;
  - one hour downloaded → verified → Spark-extracted, with wall time and DBUs;
  - whether file-arrival and table-update triggers exist on Free Edition;
  - Databricks Connect serverless from the laptop.
- ADR 0010 on where the download runs.
- The prod cadence, chosen from the measured DBUs.

**Accept when:**
- [ ] ADR 0010 cites the measured numbers.
- [ ] Release v0.1.0 is published, and `hello` is live in prod.

## M1 — Ingestion → v0.2.0

### M1-S1: the hour, the domain code and the contract
Branch `feat/m1-s1-core-contract`

**Scope:**
- Port `source.py`, `parsing.py` and `contracts.py` with their unit tests.
- `delta_ddl()` / `delta_constraints()` create the bronze tables.

**Accept when:**
- [ ] ADR 0001's tests pass, month rollover included.
- [ ] All 1,946 real domain codes parse as before.
- [ ] Delta refuses `3882.0` in `views`.

### M1-S2: landing, verified
Branch `feat/m1-s2-landing`

**Scope:** Port `download.py` (length, gzip CRC, lines, `Retry-After`), writing to `raw.dumps` in the wikiflow hive layout.

**Accept when:**
- [ ] One hour lands verified.
- [ ] A file cut at 30% is refused before anything is written.
- [ ] Re-landing an hour leaves one file.

### M1-S3: the company filter, with redirects
Branch `feat/m1-s3-company-filter`

**Scope:**
- Port `wikidata.py`, `redirects.py` and `reference.py`.
- `company_pages_refresh` job → `reference.company_pages_snapshots` + `_current`, with the shrink guard.

**Accept when:**
- [ ] Filter size matches wikiflow's (~8,742 rows), or the difference is explained.
- [ ] A simulated redirect outage is refused, and `_current` is unchanged.

### M1-S4: two grains from one Spark scan
Branch `feat/m1-s4-spark-extract`

**Scope:**
- `extract_spark.py`: read-text + split, malformed gate, domain totals + parse, company join.
- `REPLACE WHERE (dt, hour)`.
- `ops.ingest_audit` with `delta.appendOnly`.

**Accept when:**
- [ ] One hour's domain totals equal its `projectviews` exactly.
- [ ] A re-run gives identical totals and one replace commit.
- [ ] An audit UPDATE is refused.

### M1-S5: hours on a schedule, backfill + M1 close
Branch `feat/m1-s5-orchestration-backfill` · releases **v0.2.0**

**Scope:**
- `ingest_hours` job: `plan_hours` → `for_each` with concurrency 2.
- Wheel artifact and a pinned `environment_version`.
- The dev smoke test switched on.

**Accept when:**
- [ ] Dev smoke is green.
- [ ] v0.2.0 is published.
- [ ] Prod backfill from 2026-09-14 to now leaves 0 missing hours and one audit row per attempt.

## M2 — Modelling → v0.3.0

### M2-S1: the oracle and silver
Branch `feat/m2-s1-oracle-silver`

**Scope:**
- Lakeflow pipeline: `raw.projectviews` via Auto Loader.
- Silver `stg_company_pageviews` and `stg_domain_totals` materialized views with expectations.
- The 14 url_decode edge cases pinned.

**Accept when:**
- [ ] The pipeline update is green in sandbox.
- [ ] Every url_decode edge case gives the output wikiflow expects.

### M2-S2: five marts and a reconciliation that can fail
Branch `feat/m2-s2-gold-reconciliation`

**Scope:**
- `dim_company`, `dim_wiki`, `fct_company_pageviews_hourly`, `agg_company_share_of_traffic` and `agg_company_daily`.
- `ops.reconciliation` with `EXPECT … ON VIOLATION FAIL UPDATE`.
- A pipeline task at the end of `ingest_hours`.

**Accept when:**
- [ ] 0 mismatches across all loaded hours.
- [ ] A planted one-view difference fails the update.

### M2-S3: rewritten hours reach the marts
Branch `feat/m2-s3-rewritten-hours`

**Scope:** ADR 0011: materialized views replace wikiflow's `ingested_at` watermark.

**Accept when:**
- [ ] Rewriting an earlier-date hour makes the next refresh change exactly that hour, with the event log quoted.

### M2-S4: same numbers on two platforms + M2 close
Branch `feat/m2-s4-cross-platform-parity` · releases **v0.3.0**

**Scope:** Export wikiflow's DuckDB marts, then run `EXCEPT ALL` both ways for each mart.

**Accept when:**
- [ ] Every mart is identical for the shared hours, or matches on `match_type = 'sitelink'` with the reason recorded.
- [ ] v0.3.0 is published.

## M3 — Operations → v0.4.0

### M3-S1: gaps, freshness, anomalies, lag
Branch `feat/m3-s1-quality-alerts`

**Scope:** Port `quality.py`; `quality_checks` job → `ops.quality_results`; bundle SQL alerts.

**Accept when:**
- [ ] Each check fires once on a planted condition, then resets.
- [ ] The anomaly check skips with fewer than 3 days of history.

### M3-S2: retention that refuses
Branch `feat/m3-s2-retention`

**Scope:** Port `retention.py`; `raw_retention` plan task → `ops.retention_manifest` → apply task, dry-run by default.

**Accept when:**
- [ ] An expire-everything config is refused, and every file is kept.
- [ ] A normal run deletes only raw files older than 7 days.

### M3-S3: reprocess, notify, runbook + M3 close
Branch `feat/m3-s3-reprocess-runbook` · releases **v0.4.0**

**Scope:** `reprocess` job (from raw, no download); failure notifications; `docs/RUNBOOK.md`.

**Accept when:**
- [ ] A deleted bronze hour is found by the gap check and restored identically.
- [ ] v0.4.0 is published.

## M4 — Presentation → v0.5.0

### M4-S1: the attention dashboard
Branch `feat/m4-s1-dashboard`

**Scope:** An AI/BI dashboard porting the Streamlit panels.

**Accept when:**
- [ ] It renders in dev from the bundle.
- [ ] Three tiles are cross-checked by hand-written SQL.

### M4-S2: ask it in words: Genie
Branch `feat/m4-s2-genie`

**Scope:** A Genie space with certified questions.

**Accept when:**
- [ ] Each certified answer equals its hand-written SQL.

### M4-S3: lineage, tags and cost + M4 close
Branch `feat/m4-s3-governance-cost` · releases **v0.5.0**

**Scope:**
- Unity Catalog comments and tags.
- A lineage walkthrough.
- A cost page from `system.billing.usage`.

**Accept when:**
- [ ] DBUs per ingested hour are shown.
- [ ] Lineage is traced from bronze to gold.
- [ ] v0.5.0 is published.

## M5 — Evidence → v1.0.0

### M5-S1: failure drills on Databricks
Branch `feat/m5-s1-failure-drills`

**Scope:** Drills for:
- a truncated download;
- a lost partition (`RESTORE` vs reprocess);
- a filter shrink;
- a write killed mid-partition;
- a retention misconfiguration;
- a test proven able to fail.

**Accept when:**
- [ ] Every drill has a measured outcome.

### M5-S2: Spark vs DuckDB on serverless
Branch `feat/m5-s2-benchmark`

**Scope:** The same hour on both engines and the same compute, comparing time, memory and DBUs.

**Accept when:**
- [ ] Median of 3 runs.
- [ ] Identical output from both engines.

### M5-S3: the 20-minute tour + v1.0.0
Branch `feat/m5-s3-tour-close` · releases **v1.0.0**

**Scope:** A README in wikiflow's evidence style, covering what changed moving to Databricks and how a change reaches prod.

**Accept when:**
- [ ] The tour runs top to bottom.
- [ ] v1.0.0 is published.
