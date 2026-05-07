"""Tests for ephemeral model lineage (stop_at_ephemeral / include_ephemeral).

Covers:
- Default mode: ephemeral is transparent, child traces through to real upstream
- stop_at_ephemeral=True: explicit columns stop at ephemeral boundary
- stop_at_ephemeral=True: star passthrough columns stop at ephemeral boundary
- stop_at_ephemeral=True: derived/renamed columns inside ephemeral are captured
  in ephemeral_cte_lineage
- Qualified star from JOIN (enp.*, other.*) with ephemeral boundary
- _resolve_progenitor strips __dbt__cte__ prefix
"""
from __future__ import annotations

import pytest

from dbt_column_lineage.parser import SQLColumnParser
from dbt_column_lineage.api import _resolve_progenitor
from dbt_column_lineage.models.schema import ColumnLineage

# dbt injects ephemeral models as __dbt__cte__<UPPER_MODEL_NAME>
EPH = "__dbt__cte__MY_EPH_MODEL"
EPH_L = EPH.lower()


def _sql_with_ephemeral(eph_body: str, final_select: str) -> str:
    """Build compiled SQL the way dbt does: ephemeral CTE first, then main query."""
    return f"""
    WITH {EPH} AS (
        {eph_body}
    )
    {final_select}
    """


# ---------------------------------------------------------------------------
# Default mode: ephemeral is transparent
# ---------------------------------------------------------------------------

class TestEphemeralTransparent:
    """With stop_at_ephemeral=False (default) the parser traces straight through."""

    def test_explicit_column_traces_through(self):
        sql = _sql_with_ephemeral(
            "SELECT id, name FROM upstream",
            f"SELECT {EPH_L}.id, {EPH_L}.name FROM {EPH_L}",
        )
        parser = SQLColumnParser()
        result = parser.parse_column_lineage(sql)
        assert result.column_lineage["id"][0].source_columns == {"upstream.id"}
        assert result.column_lineage["name"][0].source_columns == {"upstream.name"}

    def test_ephemeral_cte_lineage_not_populated(self):
        sql = _sql_with_ephemeral(
            "SELECT id FROM upstream",
            f"SELECT id FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql)
        assert result.ephemeral_cte_lineage == {}

    def test_star_traces_through_to_base_table(self):
        sql = _sql_with_ephemeral(
            "SELECT * FROM upstream",
            f"SELECT * FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql)
        # star_sources should contain the real upstream, not the ephemeral
        assert "upstream" in result.star_sources
        assert EPH_L not in result.star_sources


# ---------------------------------------------------------------------------
# stop_at_ephemeral=True: explicit column boundary
# ---------------------------------------------------------------------------

class TestEphemeralBoundaryExplicit:
    """Explicit columns stop at the ephemeral CTE name."""

    def test_direct_column_stops_at_ephemeral(self):
        sql = _sql_with_ephemeral(
            "SELECT id, name FROM upstream",
            f"SELECT {EPH_L}.id, {EPH_L}.name FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        assert result.column_lineage["id"][0].source_columns == {f"{EPH_L}.id"}
        assert result.column_lineage["name"][0].source_columns == {f"{EPH_L}.name"}

    def test_transformation_type_is_direct_at_boundary(self):
        sql = _sql_with_ephemeral(
            "SELECT id FROM upstream",
            f"SELECT {EPH_L}.id FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        assert result.column_lineage["id"][0].transformation_type == "direct"

    def test_renamed_column_stops_at_ephemeral(self):
        """Even if the ephemeral renames, the child just sees ephemeral.new_name."""
        sql = _sql_with_ephemeral(
            "SELECT id AS customer_id FROM upstream",
            f"SELECT {EPH_L}.customer_id FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        assert result.column_lineage["customer_id"][0].source_columns == {
            f"{EPH_L}.customer_id"
        }


# ---------------------------------------------------------------------------
# stop_at_ephemeral=True: ephemeral_cte_lineage contents
# ---------------------------------------------------------------------------

class TestEphemeralCteLineage:
    """ephemeral_cte_lineage captures the internal lineage of the ephemeral."""

    def test_direct_column_recorded(self):
        sql = _sql_with_ephemeral(
            "SELECT id FROM upstream",
            f"SELECT id FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        assert EPH_L in result.ephemeral_cte_lineage
        assert "id" in result.ephemeral_cte_lineage[EPH_L]
        assert result.ephemeral_cte_lineage[EPH_L]["id"].source_columns == {"upstream.id"}

    def test_rename_recorded_with_correct_type(self):
        sql = _sql_with_ephemeral(
            "SELECT id AS customer_id FROM upstream",
            f"SELECT customer_id FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        lin = result.ephemeral_cte_lineage[EPH_L]["customer_id"]
        assert lin.source_columns == {"upstream.id"}
        assert lin.transformation_type == "renamed"

    def test_derived_column_recorded(self):
        sql = _sql_with_ephemeral(
            "SELECT SUM(amount) AS total FROM upstream",
            f"SELECT total FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        lin = result.ephemeral_cte_lineage[EPH_L]["total"]
        assert lin.transformation_type == "derived"
        assert "sum" in lin.sql_expression.lower()

    def test_not_populated_when_stop_at_ephemeral_false(self):
        sql = _sql_with_ephemeral(
            "SELECT id FROM upstream",
            f"SELECT id FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=False)
        assert result.ephemeral_cte_lineage == {}


# ---------------------------------------------------------------------------
# stop_at_ephemeral=True: star passthrough
# ---------------------------------------------------------------------------

class TestEphemeralBoundaryStar:
    """SELECT * through an ephemeral CTE."""

    def test_star_stops_at_ephemeral_in_star_sources(self):
        """When ephemeral does SELECT * FROM upstream, child's star_sources
        should contain the ephemeral, not upstream."""
        sql = _sql_with_ephemeral(
            "SELECT * FROM upstream",
            f"SELECT * FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        assert EPH_L in result.star_sources
        assert "upstream" not in result.star_sources

    def test_mixed_star_and_derived_stops_at_ephemeral(self):
        """Ephemeral: SELECT *, SUM(x) AS y FROM upstream.
        Explicit column y stops at ephemeral; star also stops at ephemeral."""
        sql = _sql_with_ephemeral(
            "SELECT *, SUM(amount) AS total FROM upstream",
            f"SELECT * FROM {EPH_L}",
        )
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        # explicit derived column captured in ephemeral_cte_lineage
        assert "total" in result.ephemeral_cte_lineage.get(EPH_L, {})
        # star_sources points to ephemeral, not upstream
        assert EPH_L in result.star_sources
        assert "upstream" not in result.star_sources


# ---------------------------------------------------------------------------
# Qualified star with JOIN (sqlfluff-style: enp.*, other.*)
# ---------------------------------------------------------------------------

class TestEphemeralQualifiedStarJoin:
    """Qualified stars from a JOIN — each resolved individually via expand_from_cte."""

    def test_qualified_star_from_ephemeral_stops_at_boundary(self):
        EPH2 = "__dbt__cte__EPH2"
        EPH2_L = EPH2.lower()
        sql = f"""
        WITH {EPH2} AS (
            SELECT id, name FROM upstream
        )
        SELECT {EPH2_L}.*, other.amount
        FROM {EPH2_L}
        JOIN other ON other.id = {EPH2_L}.id
        """
        result = SQLColumnParser().parse_column_lineage(sql, stop_at_ephemeral=True)
        # id and name come from the ephemeral CTE
        assert result.column_lineage["id"][0].source_columns == {f"{EPH2_L}.id"}
        assert result.column_lineage["name"][0].source_columns == {f"{EPH2_L}.name"}
        # amount comes directly from other (not an ephemeral)
        assert result.column_lineage["amount"][0].source_columns == {"other.amount"}


# ---------------------------------------------------------------------------
# _resolve_progenitor strips __dbt__cte__ prefix
# ---------------------------------------------------------------------------

class TestResolveProgenitorEphemeral:

    def test_strips_dbt_cte_prefix(self):
        lin = ColumnLineage(
            source_columns={"__dbt__cte__my_eph_model.col1"},
            transformation_type="direct",
        )
        model, col = _resolve_progenitor(lin)
        assert model == "my_eph_model"
        assert col == "col1"

    def test_normal_model_unaffected(self):
        lin = ColumnLineage(
            source_columns={"stg_orders.order_id"},
            transformation_type="direct",
        )
        model, col = _resolve_progenitor(lin)
        assert model == "stg_orders"
        assert col == "order_id"
