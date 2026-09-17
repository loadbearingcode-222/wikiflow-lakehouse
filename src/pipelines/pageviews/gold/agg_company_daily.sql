-- gold.agg_company_daily: daily rollup per company, with the desktop/mobile split.
-- Ported from wikiflow's dbt/models/marts/agg_company_daily.sql.
--
-- dbt's uniqueness test on (view_date, company_qid) is not ported: the GROUP BY makes it true.

CREATE OR REFRESH MATERIALIZED VIEW gold.agg_company_daily (
  CONSTRAINT mobile_share_between_zero_and_one EXPECT (mobile_share BETWEEN 0 AND 1) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (view_date)
COMMENT "Daily views per tracked company: desktop/mobile split, redirect views, language reach."
AS
SELECT
  view_date,
  company_qid,
  max(company_name)                                                AS company_name,
  sum(views)                                                       AS views,
  sum(CASE WHEN access_method = 'mobile-web' THEN views ELSE 0 END) AS mobile_views,
  sum(CASE WHEN access_method = 'desktop'    THEN views ELSE 0 END) AS desktop_views,
  sum(CASE WHEN match_type = 'redirect'      THEN views ELSE 0 END) AS redirect_views,
  CAST(sum(CASE WHEN access_method = 'mobile-web' THEN views ELSE 0 END) AS DOUBLE)
    / nullif(sum(views), 0)                                        AS mobile_share,
  count(DISTINCT wiki_language)                                    AS language_editions,
  count(DISTINCT view_hour)                                        AS hours_present
FROM gold.fct_company_pageviews_hourly
GROUP BY view_date, company_qid
