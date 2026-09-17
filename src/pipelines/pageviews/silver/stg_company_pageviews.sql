-- silver.stg_company_pageviews: the narrow grain, typed, renamed and decoded for display.
--
-- Ported from wikiflow's dbt/models/staging/stg_company_pageviews.sql. Bronze already carries the
-- company attributes (they came from the filter join at extraction), so staging only renames and
-- decodes. dbt's tests on this model become expectations that fail the update.
--
-- Dropped in the port: wikiflow's coalesce(match_type, 'sitelink') and
-- coalesce(canonical_title, page_title). They covered parquet partitions written before redirect
-- resolution existed. Every bronze row here has both, by contract (M1-S1).
--
-- The display title applies the url_decode rule inline, with the same text as url_decode_cases.sql
-- (kept identical by tests/unit/test_pipeline_sql.py; that file says why it is not a joined view).

CREATE OR REFRESH MATERIALIZED VIEW silver.stg_company_pageviews (
  CONSTRAINT company_qid_present EXPECT (company_qid IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT view_date_present EXPECT (view_date IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT views_at_least_one EXPECT (views >= 1) ON VIOLATION FAIL UPDATE,
  CONSTRAINT access_method_known EXPECT (access_method IN ('desktop', 'mobile-web')) ON VIOLATION FAIL UPDATE,
  CONSTRAINT match_type_known EXPECT (match_type IN ('sitelink', 'redirect')) ON VIOLATION FAIL UPDATE,
  CONSTRAINT title_decoded EXPECT (page_title_display IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (view_date, view_hour)
COMMENT "Tracked company articles, hourly: bronze.company_pageviews renamed, with a display title."
-- interval_start_utc is TIMESTAMP_NTZ, which a materialized view refuses without this feature
-- (DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT, measured).
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
SELECT
  -- partition keys: the hour the data DESCRIBES, not the filename's hour (ADR 0001)
  dt                                AS view_date,
  hour                              AS view_hour,
  interval_start_utc,

  qid                               AS company_qid,
  company_name,

  language                          AS wiki_language,
  project                           AS wiki_project,
  access_method,
  domain_code,

  -- The raw title is kept: it is the exact string that joins back to the source file.
  page_title,
  match_type,
  canonical_title,
  replace(
    -- url_decode:begin
    CASE
      WHEN instr(raw_title, '%') = 0 THEN raw_title
      ELSE coalesce(
        try_validate_utf8(CAST(aggregate(
          -- tokens: a valid escape, a run without `%`, or a lone `%`
          regexp_extract_all(raw_title, '(%[0-9A-Fa-f]{2}|[^%]+|%)', 1),
          CAST(X'' AS BINARY),
          (bytes, token) -> concat(bytes, CASE
            WHEN token RLIKE '^%[0-9A-Fa-f]{2}$' THEN unhex(substr(token, 2))
            ELSE encode(token, 'UTF-8')
          END)
        ) AS STRING)),
        raw_title
      )
    END
    -- url_decode:end
    , '_', ' '
  )                                 AS page_title_display,

  count_views                       AS views,
  total_response_size               AS response_bytes,

  source_url,
  ingested_at

FROM (SELECT *, page_title AS raw_title FROM bronze.company_pageviews)
