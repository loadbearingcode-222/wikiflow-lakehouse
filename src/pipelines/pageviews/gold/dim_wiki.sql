-- gold.dim_wiki: one row per domain code seen, with the decomposition carried from extraction.
-- Ported from wikiflow's dbt/models/marts/dim_wiki.sql. Built from the WIDE grain, so it covers every
-- wiki, not only those with a tracked company article.
--
-- dbt's `unique` test on domain_code is not ported: GROUP BY domain_code makes it true by construction.

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_wiki (
  CONSTRAINT domain_code_present EXPECT (domain_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "One row per Wikimedia domain code, decomposed into project, language and access method."
AS
SELECT
  domain_code,
  max(wiki_project)       AS wiki_project,
  max(wiki_language)      AS wiki_language,
  max(access_method)      AS access_method,
  bool_and(is_parseable)  AS is_parseable,
  sum(views)              AS lifetime_views,
  count(*)                AS hours_observed
FROM silver.stg_domain_totals
GROUP BY domain_code
