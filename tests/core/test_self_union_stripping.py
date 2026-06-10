"""Tests for self-referencing UNION-branch stripping in the CLL registry preprocessing.

dbt incremental SCD2 / attribute-history models accumulate state via
``SELECT ... FROM {{ this }} UNION ALL SELECT ... FROM <new source>``. In compiled SQL the
``{{ this }}`` branch is the model's own relation; left in place it makes the column a
multi-source UNION with no origin. ``_strip_self_referencing_union_branches`` removes the
self-branch (when a real branch remains) so the column resolves to its true upstream.
"""
from __future__ import annotations

from dbt_osmosis_cll.cll_generator.artifacts.registry import (
    _strip_self_referencing_union_branches as strip,
)
from dbt_osmosis_cll.cll_generator.parser.sql_parser import SQLColumnParser


def _norm(sql: str) -> str:
    return " ".join(sql.split()).lower()


# Mirrors the real incremental SCD2 shape: a self-CTE (FROM the model's own relation) is
# one UNION branch; the real source is the other. A self-reference also sits harmlessly in
# the new-data branch's WHERE high-watermark — it must NOT make that branch "self".
_SCD2 = """
WITH latest AS (SELECT v FROM m),
     new_data AS (SELECT v FROM stg WHERE ts > (SELECT MAX(ts) FROM m)),
     combined AS (
         SELECT t.v AS v FROM latest AS t
         UNION ALL
         SELECT s.v AS v FROM new_data AS s
     )
SELECT v FROM combined
"""


def test_scd2_self_cte_union_branch_is_stripped_and_column_resolves_to_real_source():
    out = strip(_SCD2, "m")
    assert out != _SCD2
    # After stripping, the column is a passthrough of the real source (stg), not a UNION.
    cl = SQLColumnParser().parse_column_lineage(out).column_lineage["v"][0]
    assert cl.transformation_type != "union"
    assert any("stg" in src for src in cl.source_columns)


def test_direct_self_union_branch_stripped():
    sql = "SELECT v FROM stg_src UNION ALL SELECT v FROM m"
    assert _norm(strip(sql, "m")) == _norm("SELECT v FROM stg_src")


def test_no_self_reference_union_unchanged():
    sql = "WITH c AS (SELECT v FROM a UNION ALL SELECT v FROM b) SELECT v FROM c"
    assert strip(sql, "m") == sql


def test_self_reference_only_in_where_subquery_untouched():
    sql = "WITH n AS (SELECT v FROM stg WHERE ts > (SELECT MAX(ts) FROM m)) SELECT v FROM n"
    assert strip(sql, "m") == sql


def test_union_of_only_self_branches_unchanged():
    # No real branch remains — keep the query rather than produce an empty one.
    sql = "SELECT v FROM m UNION ALL SELECT v FROM m"
    assert strip(sql, "m") == sql


def test_three_branch_single_self_keeps_both_real_branches():
    sql = "SELECT v FROM a UNION ALL SELECT v FROM m UNION ALL SELECT v FROM b"
    assert _norm(strip(sql, "m")) == _norm("SELECT v FROM a UNION ALL SELECT v FROM b")


def test_self_match_is_case_insensitive():
    sql = "SELECT v FROM stg UNION ALL SELECT v FROM M"
    assert _norm(strip(sql, "m")) == _norm("SELECT v FROM stg")


def test_unparseable_sql_returned_unchanged():
    sql = "not valid sql ;;;"
    assert strip(sql, "m") == sql
