PROTOCOL_MODE: learning
Protocol version last confirmed by the user: 2.18 (2026-09-17)

**Scope.** Chartered work is a numbered story from `docs/MILESTONES.md`: it gets the
protocol's ceremony, in the story's own PR. Ad-hoc questions in this repo get the knowledge and
none of the ceremony.

@~/.claude/templates/UNIVERSAL_PROTOCOL.md
@~/.claude/templates/ENGINEERING_DOCTRINE.md

## Project rules that override the protocol's defaults (user decisions, 2026-09-17)

- **No `HANDOFF.md`, ledgers or session logs in the repo** (declared "never in the repo").
  The handoff for a story in flight is its **draft PR body**: the Accept-when boxes, plus a
  `Next:` line kept current at every checkpoint. After merge, the record is the PR, the release
  notes and the ADRs.
- **The plan is `docs/MILESTONES.md`.** Its layout comes first and its story list second. A new
  path, or a re-split story, is edited there inside the story that needs it.
- **One story = one branch `feat/m<M>-s<S>-<slug>` = one PR `M<M>-S<S>: <title>` = one merge
  commit**, trunk-based into `main`, in plan order, one story open at a time. A milestone closes
  inside its last story. CI enforces it: `story-lint`, `layout-lint`.
- **The repo is private** (the user's choice). GitHub's free plan does not enforce rulesets or
  environments on private repos, so:
  - `.githooks/pre-push` refuses direct pushes to `main`. Enable it once per clone with
    `git config core.hooksPath .githooks`.
  - Prod deploys only when a GitHub Release is **published**. Claude creates the release as a
    draft at milestone close; the user publishes it.
  - `.github/rulesets/main.json` is ready to apply if the repo goes public.
- **Secrets:** Claude never reads or writes tokens. `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
  GitHub secrets are set by the user.

## Environment facts

- Workspace `https://dbc-5cb7afce-0a32.cloud.databricks.com`: **Databricks Free Edition**,
  shared with the NOAA project (`ghcn-lakehouse`). CLI profile `DEFAULT`.
- Free Edition limits:
  - serverless only;
  - ≤5 concurrent job tasks per account;
  - 1 active pipeline per type;
  - one SQL warehouse, 2X-Small, `Serverless Starter Warehouse` (look it up by name, never by id);
  - a quota overrun silently stops compute for the day;
  - outbound internet restricted to "trusted domains". MEASURED 2026-09-17 (ADR 0010):
    `dumps.wikimedia.org`, `www.wikidata.org` and `en.wikipedia.org` all answer HTTP 200
    from serverless job compute.
- Catalogs: `wikiflow_sbx` (target `sandbox`, the laptop), `wikiflow_dev` (`dev`, every merge
  to `main`), `wikiflow` (`prod`, a published release). Only prod runs schedules.
- Prod ingests every 3 hours (ADR 0010): `ingest_hours` = `plan_hours` → `for_each` (2 at a
  time) `ingest_hour` → `verify_run`. One hour is ~30-90 s of task time; a 4-hour run took
  7 min 18 s end to end, and a run with nothing to plan 42 s (sandbox, 2026-09-17).
- Each merge's dev smoke re-ingests 2026-09-15T10..11 and fails unless they reconcile and
  reproduce earlier runs, then refreshes the pageviews pipeline (whose `ops.reconciliation` fails
  the update on a one-view difference). Dev's filter was published once by hand (`company_pages_refresh -t dev`);
  prod's is refreshed by deploy-prod at every release.
- `system.billing.usage` lags ~3 days here (2026-09-17: newest row ended 2026-09-14 15:00).
- Databricks Connect 19.1 from the laptop to serverless works (session in ~7 s).
- Local: Windows 11, Git Bash. The repo path contains a space (`cloud platform`).
- GitHub: `loadbearingcode-222/wikiflow-lakehouse`. Author `loadbearingcode-222` (noreply).

| Tool (OBSERVED values, re-verify live) | Version | Checked |
|---|---|---|
| Databricks CLI | 1.16.1 | 2026-09-17 |
| uv | 0.11.25 | 2026-09-17 |
| Python (project) | 3.12 | 2026-09-17 |
| Java (local PySpark) | 17.0.12 | 2026-09-17 |
| Serverless environment 5 → Python / Spark on the task | 3.12.3 / 4.2.0 | 2026-09-17 (hello run) |

## Commands

```bash
uv sync --group dev                      # dev environment
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest tests/unit
uv run --group connect pytest tests/integration -s   # real Spark on serverless, writes wikiflow_sbx only
databricks bundle deploy -t sandbox && databricks bundle run ingest_hours -t sandbox --params hour=2026-09-15T11
python .github/scripts/layout_lint.py    # is every tracked path declared?
python .github/scripts/story_lint.py --branch "$(git branch --show-current)" --title "M0-S1: ..."
```

## Known traps (silent ones first)

1. **Git Bash rewrites arguments that start with `/`** (`/Volumes/...`, `/Workspace/...`) into
   Windows paths before the CLI sees them. Prefix with `MSYS_NO_PATHCONV=1` - but then a LOCAL
   `/tmp/x` reaches the Windows CLI unconverted and "does not exist" (and `--json @/tmp/x`
   fails the same way). Pass local files as `$(cygpath -w /tmp/x)`.
2. **Piping CLI JSON into Python on Windows decodes it as cp1252**, which turns non-ASCII into
   mojibake. Use `subprocess.run(..., encoding="utf-8")`, never a pipe into stdin.
3. **A bundle cannot create a catalog on Default Storage.** The catalogs are created once with
   SQL by `scripts/bootstrap/create_catalogs.py` and are not declared in the bundle, so nothing
   needs binding. Schemas, volumes, jobs and pipelines deploy normally.
4. **`mode: development` prefixes Unity Catalog names too** (`bronze` → `dev_<user>_bronze`), and
   `presets: name_prefix: ""` does not switch it off. Non-prod targets spell out their presets.
   Ten-second check: `databricks bundle validate -t dev -o json` → `.resources.schemas`.
5. **A job task that starts a pipeline overwrites the pipeline's `configuration`.** Pipeline SQL
   uses two-part names, with the catalog taken from the pipeline's `catalog:` field; Python reads
   `spark.conf.get("pipelines.catalog")`, which survived a job-started update (M2-S2, `JOB_TASK`).
6. **A serverless task that names no environment gets its client from how it was launched.**
   Pin `environment_version` on every Python task.
7. **One active pipeline per type is shared with NOAA.** Never let this project's pipeline update
   overlap NOAA's.
8. **Free Edition can pause ALL compute for the account without warning.** The warehouse answers
   "Cannot create the resource, please try again later" and `jobs submit` answers "Triggering
   new runs for organization ... is currently disabled temporarily" (2026-09-17, cleared within
   hours). It is not a code error: stop, and retry later.
9. **Serverless Spark runs with ANSI mode on, and cannot cache.** One bad value makes `CAST` or
   `p[2]` fail the whole scan: use `try_cast` and `get(p, 2)`. `df.cache()`, `persist()` and
   `CACHE TABLE` answer `NOT_SUPPORTED_WITH_SERVERLESS`, so each action re-reads its source;
   collect a small aggregate once instead (`extract_spark.py`). Measured 2026-09-17.
10. **`SHOW TBLPROPERTIES` redacts some values**: the `source_url_valid` constraint reads back as
   `*********(redacted)`. Compare such constraints by name (`constraints_drifted`), or they
   look changed on every run and get re-applied, one commit each.
11. **Parallel `for_each` iterations must not run DDL.** Two iterations that both found the
   audit table missing a column both ran ALTER TABLE; one failed `DELTA_METADATA_CHANGED`
   (sandbox run 744903363204838). Tables are created and migrated in `plan_hours`, before the
   fan-out. Concurrent `REPLACE WHERE` writes of DIFFERENT hours to the same clustered table
   did not conflict (4 hours, 2 at a time, WriteSerializable).
12. **Pipeline Python cannot find its catalog the obvious ways.** `spark.catalog.currentCatalog()`
   is refused (`PY4J_BLOCKED_API`), and `SELECT current_catalog()` at load time answers
   `workspace`. `spark.conf.get("pipelines.catalog")` is the pipeline's own `catalog:` field.
13. **A materialized view that JOINS another view of the same pipeline can read its OLD version.**
   A lookup view (page_titles) joined into silver missed every new title when both changed in one
   update (M2-S1, four updates). Aggregates over pipeline views refreshed exactly (M2-S2, every mart
   = bronze after a new hour). Apply per-row rules inline (`url_decode:begin` markers, kept identical
   by `test_pipeline_sql.py`); pin view-to-view joins to `REFRESH POLICY FULL`.
14. **Pipeline SQL details:** a materialized view with a `TIMESTAMP_NTZ` column needs
   `TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')`. Spark SQL reads `'Tom''s'` as
   `'Toms'`: write `"Tom's"`.
15. **A condition task compares only numbers, strings and booleans.** A list task value fails the
   run (`INTERNAL_ERROR ... not primitive`), so `plan_hours` also publishes `count`.
16. **Serverless retries a failed task that sets no `max_retries`** (auto-optimization). A quality
   failure ran twice and wrote every result twice. Tasks whose failure is a finding set
   `max_retries: 0` and `disable_auto_optimization: true`.
17. **Alerts v2 have no run-now.** To watch one fire, set its schedule to `0 * * * * ?` UNPAUSED
   (`databricks alerts-v2 update-alert <id> schedule --json ...`) and let it tick;
   `bundle deploy` puts the committed schedule back.
