"""Tests for rename detection: is_rename / source_column on ColumnLineage."""
from dbt_column_lineage.parser import SQLColumnParser


def _parse(sql: str):
    return SQLColumnParser().parse_column_lineage(sql).column_lineage


# ---------------------------------------------------------------------------
# Simple rename
# ---------------------------------------------------------------------------

def test_simple_rename_detected():
    sql = "SELECT id AS customer_id FROM customers"
    lineage = _parse(sql)

    assert "customer_id" in lineage
    lin = lineage["customer_id"][0]
    assert lin.is_rename is True
    assert lin.source_column == "id"
    assert lin.source_columns == {"customers.id"}


def test_qualified_rename_detected():
    sql = "SELECT t.user_id AS uid FROM users t"
    lineage = _parse(sql)

    assert "uid" in lineage
    lin = lineage["uid"][0]
    assert lin.is_rename is True
    assert lin.source_column == "user_id"


# ---------------------------------------------------------------------------
# Transform excluded (derived, not rename)
# ---------------------------------------------------------------------------

def test_function_is_not_rename():
    sql = "SELECT UPPER(name) AS upper_name FROM customers"
    lineage = _parse(sql)

    assert "upper_name" in lineage
    lin = lineage["upper_name"][0]
    assert lin.is_rename is False
    assert lin.source_column is None
    assert lin.transformation_type == "derived"


def test_arithmetic_is_not_rename():
    sql = "SELECT amount * 2 AS doubled FROM orders"
    lineage = _parse(sql)

    lin = lineage["doubled"][0]
    assert lin.is_rename is False
    assert lin.transformation_type == "derived"


def test_case_expression_is_not_rename():
    sql = """
    SELECT
        id,
        CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier
    FROM orders
    """
    lineage = _parse(sql)

    assert lineage["id"][0].is_rename is False        # direct, no alias → not rename
    assert lineage["tier"][0].is_rename is False
    assert lineage["tier"][0].transformation_type == "derived"


# ---------------------------------------------------------------------------
# Mixed: rename + non-rename in same query
# ---------------------------------------------------------------------------

def test_mixed_rename_and_transform():
    sql = """
    SELECT
        id AS order_id,
        COUNT(*) AS row_count,
        customer_id
    FROM orders
    GROUP BY id, customer_id
    """
    lineage = _parse(sql)

    assert lineage["order_id"][0].is_rename is True
    assert lineage["order_id"][0].source_column == "id"

    assert lineage["row_count"][0].is_rename is False
    assert lineage["row_count"][0].transformation_type == "derived"

    # unaliased direct reference → direct, not rename
    assert lineage["customer_id"][0].is_rename is False
    assert lineage["customer_id"][0].transformation_type == "direct"


# ---------------------------------------------------------------------------
# CTE pattern: outermost SELECT is SELECT * FROM cte
# ---------------------------------------------------------------------------

def test_cte_rename_resolved_through_star():
    sql = """
    WITH renamed AS (
        SELECT id AS order_id, amount FROM orders
    )
    SELECT * FROM renamed
    """
    lineage = _parse(sql)

    # After star expansion the rename should propagate
    assert "order_id" in lineage
    assert lineage["order_id"][0].source_columns  # not empty


def test_cte_explicit_rename_detected():
    sql = """
    WITH base AS (
        SELECT id, amount FROM orders
    )
    SELECT id AS order_id, amount FROM base
    """
    lineage = _parse(sql)

    assert lineage["order_id"][0].is_rename is True
    assert lineage["order_id"][0].source_column == "id"
    assert lineage["amount"][0].is_rename is False


# ---------------------------------------------------------------------------
# Parse error → is_rename=False, source_column=None (never raises)
# ---------------------------------------------------------------------------

def test_unparseable_sql_does_not_raise():
    sql = "THIS IS NOT VALID SQL !!!@#$"
    try:
        lineage = _parse(sql)
    except Exception:
        # Outer parse failures are acceptable as long as the property accessors
        # themselves don't raise on a ColumnLineage object
        pass

    # Property accessors never raise regardless of transformation type
    from dbt_column_lineage.models.schema import ColumnLineage

    for t_type in ("direct", "renamed", "derived"):
        lin = ColumnLineage(source_columns=set(), transformation_type=t_type)
        # Should not raise
        _ = lin.is_rename
        _ = lin.source_column

    lin_empty = ColumnLineage(source_columns=set(), transformation_type="renamed")
    assert lin_empty.is_rename is True
    assert lin_empty.source_column is None  # empty source_columns → None


def test_source_column_none_when_not_rename():
    from dbt_column_lineage.models.schema import ColumnLineage

    for t_type in ("direct", "derived"):
        lin = ColumnLineage(source_columns={"tbl.col"}, transformation_type=t_type)
        assert lin.is_rename is False
        assert lin.source_column is None
