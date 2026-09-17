-- gold.fct_company_pageviews_hourly: the fact. Grain: one row per (hour, company, domain, article).
-- Ported from wikiflow's dbt/models/marts/fct_company_pageviews_hourly.sql.
--
-- wikiflow built this incrementally, with delete+insert and an `ingested_at` watermark overlapped by
-- an hour, because a reprocessed OLD hour was otherwise never looked at again. A materialized view
-- has no watermark to get wrong: the platform tracks what changed upstream. Whether a rewritten
-- earlier hour really reaches this table is M2-S3's question (ADR 0011). `ingested_at` is kept,
-- because it still says when an hour's bronze was written.

CREATE OR REFRESH MATERIALIZED VIEW gold.fct_company_pageviews_hourly (
  CONSTRAINT company_qid_present EXPECT (company_qid IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT views_at_least_one EXPECT (views >= 1) ON VIOLATION FAIL UPDATE,
  CONSTRAINT match_type_known EXPECT (match_type IN ('sitelink', 'redirect')) ON VIOLATION FAIL UPDATE,
  CONSTRAINT ingested_at_present EXPECT (ingested_at IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (view_date, view_hour)
COMMENT "Tracked company article views per hour; `redirect` rows reached the article via another title."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
SELECT
  view_date,
  view_hour,
  interval_start_utc,

  company_qid,
  company_name,
  domain_code,
  wiki_project,
  wiki_language,
  access_method,
  page_title,
  page_title_display,
  -- `redirect`: the reader requested another title and landed on this article (wikiflow ADR 0007).
  match_type,
  canonical_title,

  views,
  response_bytes,
  ingested_at
FROM silver.stg_company_pageviews;


-- dbt's unique_combination_of_columns test on the grain. Empty unless a key repeats.
CREATE OR REFRESH PRIVATE MATERIALIZED VIEW fct_company_pageviews_hourly_repeated_keys (
  CONSTRAINT one_row_per_article_per_hour EXPECT (row_count = 1) ON VIOLATION FAIL UPDATE
)
COMMENT "Grain keys of gold.fct_company_pageviews_hourly that occur more than once (normally none)."
AS
SELECT view_date, view_hour, company_qid, domain_code, page_title, count(*) AS row_count
FROM gold.fct_company_pageviews_hourly
GROUP BY view_date, view_hour, company_qid, domain_code, page_title
HAVING count(*) > 1
