-- ops.reconciliation: every hour bronze holds, checked against Wikimedia's own totals, to the view.
--
-- Ported from wikiflow's dbt/tests/assert_domain_totals_reconcile_with_projectviews.sql. Bronze's
-- domain totals are computed independently, by summing ~7M per-page lines an hour. If they equal
-- projectviews for every domain, the whole extraction path is correct in one assertion: the file
-- arrived intact, every line parsed, nothing was dropped, domain codes grouped correctly, and the
-- hour was filed under the right partition.
--
-- Exact equality, no tolerance. That is measured, not optimistic: wikiflow matched all 1,955
-- domains of its first hour to the view before writing its test, and every hour here has too.
-- A tolerance would hide exactly the bugs worth catching.
--
-- Compared: hours present on BOTH sides. An hour landed but not extracted is a gap, which the gap
-- check reports (M3-S1). Failing here would freeze every mart over one missing hour.
--
-- One row per hour. A mismatch fails the pipeline update and names the hour and the domain codes.
-- `ours` reads bronze directly, not silver: a view joined to another view of this pipeline can
-- read its old version within an update (CLAUDE.md trap 13).

CREATE OR REFRESH MATERIALIZED VIEW ops.reconciliation (
  CONSTRAINT domain_totals_equal_projectviews EXPECT (mismatched_domains = 0) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (view_date, view_hour)
COMMENT "Per hour: bronze domain totals vs Wikimedia projectviews, domain by domain. Any mismatch fails the update."
AS
WITH theirs AS (
  SELECT dt AS view_date, hour AS view_hour, domain_code, sum(views) AS views
  FROM raw.projectviews
  GROUP BY dt, hour, domain_code
),

ours AS (
  SELECT dt AS view_date, hour AS view_hour, domain_code, views
  FROM bronze.domain_totals
),

comparable_hours AS (
  SELECT DISTINCT view_date, view_hour FROM theirs
  INTERSECT
  SELECT DISTINCT view_date, view_hour FROM ours
),

compared AS (
  SELECT
    coalesce(o.view_date, t.view_date)     AS view_date,
    coalesce(o.view_hour, t.view_hour)     AS view_hour,
    coalesce(o.domain_code, t.domain_code) AS domain_code,
    o.views                                AS our_views,
    t.views                                AS their_views
  FROM (SELECT o.* FROM ours o JOIN comparable_hours USING (view_date, view_hour)) AS o
  FULL OUTER JOIN (SELECT t.* FROM theirs t JOIN comparable_hours USING (view_date, view_hour)) AS t
    ON  o.view_date   = t.view_date
    AND o.view_hour   = t.view_hour
    AND o.domain_code = t.domain_code
)

SELECT
  view_date,
  view_hour,
  count(*)                                                     AS domains,
  -- `<=>`, not `=`: a domain on only one side is a NULL on the other, and must count as a mismatch.
  count_if(NOT our_views <=> their_views)                      AS mismatched_domains,
  sum(our_views)                                               AS our_total_views,
  sum(their_views)                                             AS their_total_views,
  array_sort(collect_list(CASE WHEN NOT our_views <=> their_views THEN domain_code END))
                                                               AS mismatched_domain_codes
FROM compared
GROUP BY view_date, view_hour
