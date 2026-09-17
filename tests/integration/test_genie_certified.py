"""Each certified answer in the Genie space equals a hand-written query that reads bronze instead.

The certified SQL is read from src/genie/attention.geniespace.json itself, so what is tested is
what Genie was taught. The hand-written side is independent on purpose: it never reads a gold
table, so a mart bug and a certified-SQL bug cannot agree with each other by accident.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration

CATALOG = "wikiflow_sbx"
SPACE = Path(__file__).parents[2] / "src" / "genie" / "attention.geniespace.json"
PLACEHOLDER = "${resources.schemas.gold.catalog_name}"

HAND_WRITTEN = {
    "Which tracked company got the most Wikipedia views?": """
        SELECT company_name, sum(count_views) AS views
          FROM {c}.bronze.company_pageviews
         GROUP BY company_name
         ORDER BY views DESC
    """,
    "What share of all Wikimedia traffic did Google get each day?": """
        WITH google AS (
          SELECT dt, sum(count_views) AS views FROM {c}.bronze.company_pageviews
           WHERE qid = 'Q95' GROUP BY dt
        ), everyone AS (
          SELECT dt, sum(views) AS views FROM {c}.bronze.domain_totals GROUP BY dt
        )
        SELECT dt, round(100 * google.views / everyone.views, 4)
          FROM google JOIN everyone USING (dt)
         ORDER BY dt
    """,
    "Which language editions of Wikipedia gave Apple Inc. the most views?": """
        SELECT language, sum(count_views) AS views
          FROM {c}.bronze.company_pageviews
         WHERE qid = 'Q312'
         GROUP BY language
         ORDER BY views DESC, language
         LIMIT 10
    """,
    "What share of each company's views came from the mobile web?": """
        SELECT company_name,
               round(100 * sum(CASE WHEN access_method = 'mobile-web' THEN count_views ELSE 0 END)
                     / sum(count_views), 1) AS share
          FROM {c}.bronze.company_pageviews
         GROUP BY company_name
         ORDER BY share DESC, company_name
    """,
    "Did the loaded hours match Wikimedia's own published totals?": """
        WITH theirs AS (SELECT dt, hour, domain_code, sum(views) AS views FROM {c}.raw.projectviews GROUP BY ALL),
             ours   AS (SELECT dt, hour, domain_code, views FROM {c}.bronze.domain_totals),
             hours  AS (SELECT DISTINCT dt, hour FROM theirs INTERSECT SELECT DISTINCT dt, hour FROM ours)
        SELECT (SELECT count(*) FROM hours),
               count(*),
               count_if(NOT o.views <=> t.views),
               sum(o.views),
               sum(t.views)
          FROM (SELECT o.* FROM ours o JOIN hours USING (dt, hour)) o
          FULL OUTER JOIN (SELECT t.* FROM theirs t JOIN hours USING (dt, hour)) t
            ON o.dt = t.dt AND o.hour = t.hour AND o.domain_code = t.domain_code
    """,
}


def certified() -> dict[str, str]:
    space = json.loads(SPACE.read_text(encoding="utf-8").replace(PLACEHOLDER, CATALOG))
    return {
        pair["question"][0]: "".join(pair["sql"])
        for pair in space["instructions"]["example_question_sqls"]
    }


def rows(spark: Any, sql: str) -> list[tuple[str, ...]]:
    return [tuple(str(value) for value in row) for row in spark.sql(sql).collect()]


def test_every_certified_question_has_a_hand_written_check() -> None:
    assert set(certified()) == set(HAND_WRITTEN)


@pytest.mark.parametrize("question", list(HAND_WRITTEN))
def test_the_certified_answer_equals_the_hand_written_one(spark: Any, question: str) -> None:
    answer = rows(spark, certified()[question])
    by_hand = rows(spark, HAND_WRITTEN[question].format(c=CATALOG))
    print(
        f"\nQUESTION {question}\n  certified   {answer[:3]}{' ...' if len(answer) > 3 else ''}\n  hand-written {by_hand[:3]}"
    )
    assert answer, "an empty answer proves nothing"
    assert answer == by_hand
