"""Roadmap item 4: SQL parser core hardening.

Covers the three structural weaknesses called out in the Phase 2 roadmap:

1. **Per-scope alias resolution** — aliases declared inside CTEs or subqueries
   must not leak into (or shadow) the outer SELECT's alias scope.
2. **Multi-source preservation through CTE hops** — ``COALESCE(a.x, b.y)``
   defined in a CTE must surface ALL contributing source columns at the final
   SELECT, not an empty sentinel set.
3. **Schema-aware unqualified-column resolution** — with catalog column lists
   available, an unqualified column in a join resolves to the table that
   actually HAS the column, not blindly to the first FROM table.

All tests drive ``SQLColumnParser.parse_column_lineage`` directly — the stable
``SQLParseResult`` contract.
"""

from __future__ import annotations

import typing as t

from dbt_osmosis_cll.cll_generator.parser import SQLColumnParser


def _lineage(result, col):
    assert col in result.column_lineage, (
        f"column {col!r} missing; got {sorted(result.column_lineage)}"
    )
    return result.column_lineage[col][0]


# ---------------------------------------------------------------------------
# 1. Per-scope alias resolution
# ---------------------------------------------------------------------------


class TestScopedAliases:
    def test_subquery_alias_does_not_shadow_outer_alias(self):
        """A WHERE-subquery reusing alias `o` must not hijack the outer `o`."""
        sql = (
            "SELECT o.id AS the_id FROM orders o "
            "WHERE EXISTS (SELECT 1 FROM customers o WHERE o.region = 'x')"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "the_id").source_columns == {"orders.id"}

    def test_same_alias_in_two_ctes_resolves_per_cte(self):
        sql = (
            "WITH a AS (SELECT o.id AS id_a FROM orders o), "
            "b AS (SELECT o.cust_id AS id_b FROM customers o) "
            "SELECT a.id_a, b.id_b FROM a JOIN b ON a.id_a = b.id_b"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "id_a").source_columns == {"orders.id"}
        assert _lineage(result, "id_b").source_columns == {"customers.cust_id"}

    def test_cte_alias_does_not_leak_into_final_select(self):
        """CTE-internal alias `x` for customers must not capture the outer x.col."""
        sql = (
            "WITH helper AS (SELECT x.k AS k FROM customers x) "
            "SELECT x.amount AS amt FROM payments x"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "amt").source_columns == {"payments.amount"}


# ---------------------------------------------------------------------------
# 2. Multi-source sets preserved through CTE hops
# ---------------------------------------------------------------------------


class TestMultiSourcePreservation:
    def test_coalesce_in_cte_preserves_both_sources(self):
        sql = (
            "WITH c AS ("
            "  SELECT COALESCE(a.x, b.y) AS merged FROM tbl_a a JOIN tbl_b b ON a.k = b.k"
            ") "
            "SELECT merged FROM c"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        lin = _lineage(result, "merged")
        assert lin.source_columns == {"tbl_a.x", "tbl_b.y"}
        assert lin.transformation_type == "derived"

    def test_multi_source_survives_two_cte_hops(self):
        sql = (
            "WITH c1 AS ("
            "  SELECT COALESCE(a.x, b.y) AS merged FROM tbl_a a JOIN tbl_b b ON a.k = b.k"
            "), c2 AS ("
            "  SELECT merged FROM c1"
            ") "
            "SELECT merged FROM c2"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "merged").source_columns == {"tbl_a.x", "tbl_b.y"}

    def test_multi_source_in_final_select_unchanged(self):
        """Direct (non-CTE) multi-source expressions already worked — lock it in."""
        sql = "SELECT COALESCE(a.x, b.y) AS merged FROM tbl_a a JOIN tbl_b b ON a.k = b.k"
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "merged").source_columns == {"tbl_a.x", "tbl_b.y"}

    def test_single_source_through_cte_still_single(self):
        sql = (
            "WITH c AS (SELECT a.x AS x2 FROM tbl_a a) "
            "SELECT x2 FROM c"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "x2").source_columns == {"tbl_a.x"}


# ---------------------------------------------------------------------------
# 3. Schema-aware unqualified-column resolution
# ---------------------------------------------------------------------------


class TestSchemaAwareResolution:
    TABLE_COLUMNS: t.ClassVar[dict[str, set[str]]] = {
        "orders": {"id", "cust_id", "order_date"},
        "customers": {"id", "amount", "region"},
    }

    def test_unqualified_column_resolves_to_owning_table(self):
        sql = "SELECT amount FROM orders o JOIN customers c ON o.cust_id = c.id"
        parser = SQLColumnParser(table_columns=self.TABLE_COLUMNS)
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "amount").source_columns == {"customers.amount"}

    def test_unqualified_column_in_expression_resolves(self):
        sql = (
            "SELECT UPPER(region) AS region_uc "
            "FROM orders o JOIN customers c ON o.cust_id = c.id"
        )
        parser = SQLColumnParser(table_columns=self.TABLE_COLUMNS)
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "region_uc").source_columns == {"customers.region"}

    def test_ambiguous_column_falls_back_to_first_table(self):
        """`id` exists in both tables → keep the historical first-FROM-table answer."""
        sql = "SELECT id FROM orders o JOIN customers c ON o.cust_id = c.id"
        parser = SQLColumnParser(table_columns=self.TABLE_COLUMNS)
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "id").source_columns == {"orders.id"}

    def test_without_table_columns_behaviour_unchanged(self):
        sql = "SELECT amount FROM orders o JOIN customers c ON o.cust_id = c.id"
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "amount").source_columns == {"orders.amount"}

    def test_schema_aware_inside_cte(self):
        sql = (
            "WITH j AS ("
            "  SELECT amount FROM orders o JOIN customers c ON o.cust_id = c.id"
            ") "
            "SELECT amount FROM j"
        )
        parser = SQLColumnParser(table_columns=self.TABLE_COLUMNS)
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "amount").source_columns == {"customers.amount"}
