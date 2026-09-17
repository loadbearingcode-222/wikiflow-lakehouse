-- gold.dim_company: one row per tracked entity. Ported from wikiflow's dbt/models/marts/dim_company.sql.
--
-- Meta Platforms (Q380) and Facebook (Q355) are separate entities on purpose: one is the company,
-- the other the service, and they draw very different traffic.
--
-- dbt's `unique` test on company_qid is not ported: GROUP BY company_qid makes it true by construction.

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_company (
  CONSTRAINT company_qid_present EXPECT (company_qid IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT seen_in_at_least_one_language EXPECT (language_editions >= 1) ON VIOLATION FAIL UPDATE
)
COMMENT "One row per tracked Wikidata entity, with its reach and lifetime views."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
SELECT
  company_qid,
  min(company_name)              AS company_name,
  count(DISTINCT wiki_language)  AS language_editions,
  count(DISTINCT page_title)     AS distinct_titles,
  count(DISTINCT domain_code)    AS distinct_domains,
  min(interval_start_utc)        AS first_seen_utc,
  max(interval_start_utc)        AS last_seen_utc,
  sum(views)                     AS lifetime_views
FROM silver.stg_company_pageviews
GROUP BY company_qid
