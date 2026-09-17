-- gold.agg_company_share_of_traffic: company attention per hour as a share of ALL Wikimedia traffic.
-- Ported from wikiflow's dbt/models/marts/agg_company_share_of_traffic.sql.
--
-- This is the table the two-grain design exists for. Without the wide grain you could report
-- "Apple got 12,000 views", but not whether that was a spike or a quiet Tuesday.

CREATE OR REFRESH MATERIALIZED VIEW gold.agg_company_share_of_traffic (
  CONSTRAINT total_views_present EXPECT (total_views IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- Above 1 means the denominator is wrong.
  CONSTRAINT share_between_zero_and_one EXPECT (share_of_traffic BETWEEN 0 AND 1) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (view_date, view_hour)
COMMENT "Hourly views per tracked company, and their share of all Wikimedia pageviews that hour."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
-- A join of two views of this pipeline. The planner offered an incremental ROW_BASED refresh, the
-- path that read a joined view's OLD version in M2-S1 (CLAUDE.md trap 13). ~7 rows an hour
-- recompute in seconds, so the incremental path is not worth the risk.
REFRESH POLICY FULL
AS
WITH company_hourly AS (
  SELECT
    view_date,
    view_hour,
    company_qid,
    max(company_name)       AS company_name,
    max(interval_start_utc) AS interval_start_utc,
    sum(views)              AS company_views
  FROM gold.fct_company_pageviews_hourly
  GROUP BY view_date, view_hour, company_qid
),

traffic_hourly AS (
  -- Every domain, including codes that failed to parse: they are real traffic, and leaving them out
  -- would inflate every share.
  SELECT view_date, view_hour, sum(views) AS total_views
  FROM silver.stg_domain_totals
  GROUP BY view_date, view_hour
)

SELECT
  c.view_date,
  c.view_hour,
  c.interval_start_utc,
  c.company_qid,
  c.company_name,
  c.company_views,
  -- LEFT JOIN plus total_views_present: an hour without totals fails loudly instead of vanishing.
  t.total_views,
  CAST(c.company_views AS DOUBLE) / nullif(t.total_views, 0) AS share_of_traffic
FROM company_hourly AS c
LEFT JOIN traffic_hourly AS t
  ON  c.view_date = t.view_date
  AND c.view_hour = t.view_hour
