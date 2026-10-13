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
        sql = "WITH c AS (SELECT a.x AS x2 FROM tbl_a a) SELECT x2 FROM c"
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert _lineage(result, "x2").source_columns == {"tbl_a.x"}


# ---------------------------------------------------------------------------
# 3. Schema-aware unqualified-column resolution
# ---------------------------------------------------------------------------


class TestTopLevelUnionStarBranches:
    """Top-level UNION ALL with ``SELECT * FROM <cte>`` branches.

    Sanitized from a real Snowflake repo corpus finding: such models produced
    ZERO lineage columns because the top-level union handler only knew explicit
    expression names. Branches must expand against the CTEs' recorded columns,
    with per-branch sources surfaced via union_branches.
    """

    SQL = (
        "WITH base AS ("
        "  SELECT contract_id, tariff_key, created_dt"
        "  FROM db.schema_a.stg_contracts"
        "  WHERE full_dt = (SELECT MAX(full_dt) FROM db.schema_a.stg_contracts)"
        "), late_rows AS ("
        "  SELECT contract_id, tariff_key, created_dt"
        "  FROM db.schema_b.stg_contracts_late AS rdv"
        "  WHERE NOT EXISTS (SELECT 1 FROM base WHERE rdv.contract_id = base.contract_id)"
        "  QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY created_dt) = 1"
        ") "
        "SELECT * FROM base UNION ALL SELECT * FROM late_rows"
    )

    def test_star_union_branches_yield_columns(self):
        parser = SQLColumnParser(dialect="snowflake")
        result = parser.parse_column_lineage(self.SQL)
        assert set(result.column_lineage) == {"contract_id", "tariff_key", "created_dt"}

    def test_star_union_columns_are_union_type_with_branches(self):
        parser = SQLColumnParser(dialect="snowflake")
        result = parser.parse_column_lineage(self.SQL)
        lin = result.column_lineage["contract_id"][0]
        assert lin.transformation_type == "union"
        assert lin.union_branches == [
            "stg_contracts.contract_id",
            "stg_contracts_late.contract_id",
        ]

    def test_explicit_top_level_union_now_carries_branches(self):
        sql = "SELECT a.x AS val FROM tbl_a a UNION ALL SELECT b.y AS val FROM tbl_b b"
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        lin = result.column_lineage["val"][0]
        assert lin.transformation_type == "union"
        assert lin.union_branches == ["tbl_a.x", "tbl_b.y"]

    def test_three_branch_nested_union(self):
        sql = (
            "SELECT a.x AS val FROM tbl_a a"
            " UNION ALL SELECT b.y AS val FROM tbl_b b"
            " UNION ALL SELECT c.z AS val FROM tbl_c c"
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        lin = result.column_lineage["val"][0]
        assert lin.union_branches == ["tbl_a.x", "tbl_b.y", "tbl_c.z"]


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
        sql = "SELECT UPPER(region) AS region_uc FROM orders o JOIN customers c ON o.cust_id = c.id"
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


# ---------------------------------------------------------------------------
# 4. Union branches survive CTE hops
# ---------------------------------------------------------------------------


class TestUnionBranchesThroughCteHops:
    """A set-op CTE consumed by a LATER CTE must keep its per-branch sources.

    Real-repo pattern (dbt models with `u AS (... UNION ALL ...)` followed by
    ranking / dedup CTEs): the branches were only reachable when the final
    SELECT read the set-op CTE directly. One CTE hop later every column came out
    as `union` / `aggregate` with no sources and no branches.
    """

    UNION_CTE = "u AS (SELECT a.id, a.v FROM tbl_a a UNION ALL SELECT b.id, b.v FROM tbl_b b)"

    def test_passthrough_hop_keeps_branches(self):
        sql = f"WITH {self.UNION_CTE}, r AS (SELECT u.id, u.v FROM u) SELECT r.id, r.v FROM r"
        result = SQLColumnParser().parse_column_lineage(sql)
        lin = _lineage(result, "id")
        assert lin.transformation_type == "union"
        assert lin.union_branches == ["tbl_a.id", "tbl_b.id"]

    def test_aliased_passthrough_hop_keeps_branches(self):
        sql = f"WITH {self.UNION_CTE}, r AS (SELECT u.id AS the_id FROM u) SELECT the_id FROM r"
        result = SQLColumnParser().parse_column_lineage(sql)
        assert _lineage(result, "the_id").union_branches == ["tbl_a.id", "tbl_b.id"]

    def test_star_hop_keeps_branches(self):
        sql = f"WITH {self.UNION_CTE}, r AS (SELECT * FROM u) SELECT r.id FROM r"
        result = SQLColumnParser().parse_column_lineage(sql)
        assert _lineage(result, "id").union_branches == ["tbl_a.id", "tbl_b.id"]

    def test_star_exclude_hop_keeps_branches(self):
        sql = f"WITH {self.UNION_CTE}, r AS (SELECT * EXCLUDE (v) FROM u) SELECT * FROM r"
        result = SQLColumnParser(dialect="snowflake").parse_column_lineage(sql)
        assert _lineage(result, "id").union_branches == ["tbl_a.id", "tbl_b.id"]
        assert "v" not in result.column_lineage

    def test_aggregate_over_union_column_gets_all_branch_sources(self):
        sql = (
            f"WITH {self.UNION_CTE}, r AS (SELECT u.id, MAX(u.v) AS v FROM u GROUP BY u.id) "
            "SELECT r.id, r.v FROM r"
        )
        result = SQLColumnParser().parse_column_lineage(sql)
        lin = _lineage(result, "v")
        assert lin.transformation_type == "aggregate"
        assert lin.source_columns == {"tbl_a.v", "tbl_b.v"}

    def test_two_hops_keep_branches(self):
        sql = (
            f"WITH {self.UNION_CTE}, r AS (SELECT u.id FROM u), s AS (SELECT r.id FROM r) "
            "SELECT s.id FROM s"
        )
        result = SQLColumnParser().parse_column_lineage(sql)
        assert _lineage(result, "id").union_branches == ["tbl_a.id", "tbl_b.id"]


class TestUnionBranchInputs:
    """A union branch that is computed, or reads a multi-source CTE column, has no
    single qualifier. It must not be pinned to the upstream CTE's first FROM table
    (old behaviour: `tbl_a.m` for `COALESCE(a.x, b.y) AS m`); its real inputs go to
    source_columns instead, and survive further CTE hops.
    """

    BASE = "base AS (SELECT a.k, COALESCE(a.x, b.y) AS m FROM tbl_a a JOIN tbl_b b ON a.k = b.k)"

    def test_branch_over_multi_source_cte_column_is_not_misattributed(self):
        sql = (
            f"WITH {self.BASE}, "
            "u AS (SELECT k, m FROM base UNION ALL SELECT k, m FROM base WHERE k > 0) "
            "SELECT u.m FROM u"
        )
        lin = _lineage(SQLColumnParser().parse_column_lineage(sql), "m")
        assert lin.transformation_type == "union"
        assert "tbl_a.m" not in lin.union_branches
        assert lin.source_columns == {"tbl_a.x", "tbl_b.y"}

    def test_computed_branch_contributes_its_inputs(self):
        sql = (
            "WITH u AS ("
            "  SELECT CASE WHEN a.f = '1' THEN a.x END AS v FROM tbl_a a "
            "  UNION ALL SELECT b.v FROM tbl_b b"
            ") SELECT v FROM u"
        )
        lin = _lineage(SQLColumnParser().parse_column_lineage(sql), "v")
        assert lin.union_branches == ["tbl_b.v"]
        assert lin.source_columns == {"tbl_a.f", "tbl_a.x", "tbl_b.v"}

    def test_top_level_union_computed_branch_contributes_its_inputs(self):
        sql = "SELECT a.x + a.z AS v FROM tbl_a a UNION ALL SELECT b.v FROM tbl_b b"
        lin = _lineage(SQLColumnParser().parse_column_lineage(sql), "v")
        assert lin.union_branches == ["tbl_b.v"]
        assert lin.source_columns == {"tbl_a.x", "tbl_a.z", "tbl_b.v"}

    def test_inputs_survive_window_and_case_hops(self):
        sql = (
            f"WITH {self.BASE}, "
            "u AS (SELECT k, m FROM base UNION ALL SELECT k, m FROM base WHERE k > 0), "
            "ranked AS (SELECT *, MAX(CASE WHEN m = 'VIP' THEN 3 END) "
            "OVER (PARTITION BY k) AS rnk FROM u) "
            "SELECT CASE WHEN rnk = 3 THEN 'VIP' ELSE m END AS d FROM ranked"
        )
        lin = _lineage(SQLColumnParser().parse_column_lineage(sql), "d")
        assert {"tbl_a.x", "tbl_b.y"} <= lin.source_columns
