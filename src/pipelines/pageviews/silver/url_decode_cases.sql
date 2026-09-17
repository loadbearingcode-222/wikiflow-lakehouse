-- url_decode_cases (private): the 14 cases that pin the url_decode rule. A wrong output fails the update.
--
-- Dump titles are underscored and sometimes percent-encoded. wikiflow decoded them for display with its
-- own rule, pinned by these 14 cases (dbt/tests/assert_url_decode_behaves_identically.sql, ported
-- verbatim). Each one is a case a plausible implementation gets wrong.
--
-- Spark's built-in url_decode is NOT that rule (measured 2026-09-17 on serverless):
--   url_decode('a+b%20c') = 'a b c'    (turns `+` into a space)
--   url_decode('%%41')    raises CANNOT_DECODE_URL, so one odd title would fail the whole update
-- The rule below decodes only valid %XX sequences, keeps everything else literally, and returns the
-- title unchanged when the decoded bytes are not valid UTF-8.
--
-- The rule is written twice, here and in stg_company_pageviews.sql, between `url_decode:begin/end`
-- markers. tests/unit/test_pipeline_sql.py fails CI if the two copies differ. It is not a shared
-- view joined in, because that was measured to be wrong: when bronze and the joined view both change
-- in one update, the incremental refresh of the join read the joined view's OLD version, and every
-- newly seen title came through undecoded (sandbox, 2026-09-17).
--
-- String literals: Spark SQL reads 'Tom''s' as two adjacent literals, 'Toms'. Hence "Tom's".

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW url_decode_cases (
  CONSTRAINT url_decode_edge_cases EXPECT (decoded <=> expected) ON VIOLATION FAIL UPDATE
)
COMMENT "wikiflow's 14 url_decode cases, each decoded by the same rule as silver.stg_company_pageviews."
AS
SELECT
  raw_title,
  expected,
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
  AS decoded
FROM VALUES
  ('Apple_Inc.',   'Apple_Inc.'),
  ('Caf%C3%A9',    'Café'),
  ('Tom%27s',      "Tom's"),
  ('%E3%82%A2',    'ア'),
  ('%F0%9F%8D%8E', '🍎'),
  ('%41%42',       'AB'),
  ('a+b%20c',      'a+b c'),
  ('50%25_off',    '50%_off'),
  ('%%41',         '%A'),
  ('100%',         '100%'),
  ('%zz',          '%zz'),
  ('%4',           '%4'),
  ('%E3',          '%E3'),
  ('%E3x',         '%E3x')
AS cases (raw_title, expected)
