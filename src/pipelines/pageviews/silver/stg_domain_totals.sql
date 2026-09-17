-- silver.stg_domain_totals: the wide grain, typed and renamed. The denominator for share of
-- traffic, and the side of the hour that reconciles against raw.projectviews (M2-S2).
--
-- Ported from wikiflow's dbt/models/staging/stg_domain_totals.sql. The domain code was already
-- decomposed into (project, language, access_method) in Python at extraction, where the parser is
-- tested against all 1,946 real codes. It is deliberately not re-implemented in SQL: `.m` means
-- mobile in `en.m`, wikimedia.org in `commons.m`, and both in `commons.m.m`.

CREATE OR REFRESH MATERIALIZED VIEW silver.stg_domain_totals (
  CONSTRAINT domain_code_present EXPECT (domain_code IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT views_present EXPECT (views IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- Unparseable codes (a real `""` carries ~5,000 views an hour) legitimately have none.
  CONSTRAINT access_method_known_when_parseable
    EXPECT (NOT is_parseable OR access_method IN ('desktop', 'mobile-web')) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (view_date, view_hour)
COMMENT "Views per domain code per hour: bronze.domain_totals renamed."
-- interval_start_utc is TIMESTAMP_NTZ, which a materialized view refuses without this feature
-- (DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT, measured).
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
SELECT
  dt              AS view_date,
  hour            AS view_hour,
  interval_start_utc,

  domain_code,
  project         AS wiki_project,
  language        AS wiki_language,
  access_method,
  -- Rows that do not fit the documented scheme are KEPT: Wikimedia counts them in projectviews.
  is_parseable,

  views,
  response_bytes,
  distinct_pages,

  source_url,
  ingested_at

FROM bronze.domain_totals;


-- dbt's unique_combination_of_columns test, as a check that fails the update. A duplicate domain
-- row would double the denominator and silently halve every share-of-traffic figure. The view is
-- empty unless a key repeats.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW stg_domain_totals_repeated_keys (
  CONSTRAINT one_row_per_domain_per_hour EXPECT (row_count = 1) ON VIOLATION FAIL UPDATE
)
COMMENT "Keys of silver.stg_domain_totals that occur more than once (normally none)."
AS
SELECT view_date, view_hour, domain_code, count(*) AS row_count
FROM silver.stg_domain_totals
GROUP BY view_date, view_hour, domain_code
HAVING count(*) > 1
