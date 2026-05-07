"""Tests for get_column_lineage() public API.

No warehouse connection required — catalog resolver and registry are mocked.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dbt_column_lineage.api import ColumnLineageResult, get_column_lineage, _resolve_progenitor
from dbt_column_lineage.artifacts.exceptions import CompiledSqlMissingError
from dbt_column_lineage.models.schema import Column, ColumnLineage, Model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_model(name: str, cols: dict[str, list[ColumnLineage]]) -> Model:
    columns = {
        col_name: Column(name=col_name, model_name=name, lineage=lineage_list)
        for col_name, lineage_list in cols.items()
    }
    return Model(
        name=name,
        schema="main",
        database="dev",
        columns=columns,
        resource_type="model",
    )


def _make_manifest(tmp_path: Path) -> Path:
    manifest = {
        "metadata": {"adapter_type": "snowflake"},
        "nodes": {
            # Minimal node with compiled_code so the pre-flight check passes.
            # Tests that mock the full registry don't depend on this content.
            "model.pkg.dummy": {
                "resource_type": "model",
                "name": "dummy",
                "language": "sql",
                "compiled_code": "select 1 as id",
            }
        },
        "sources": {},
    }
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return p


def _make_catalog(tmp_path: Path) -> Path:
    catalog = {"nodes": {}, "sources": {}}
    p = tmp_path / "catalog.json"
    p.write_text(json.dumps(catalog))
    return p


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------

def test_both_catalog_and_live_db_raises(tmp_path):
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)
    with pytest.raises(ValueError, match="not both"):
        get_column_lineage(str(m), catalog_path=str(c), live_db=True)


def test_neither_catalog_nor_live_db_raises(tmp_path):
    m = _make_manifest(tmp_path)
    with pytest.raises(ValueError, match="required"):
        get_column_lineage(str(m))


# ---------------------------------------------------------------------------
# Catalog resolver path
# ---------------------------------------------------------------------------

def test_catalog_path_returns_results(tmp_path):
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    direct_lin = ColumnLineage(
        source_columns={"stg_orders.id"},
        transformation_type="direct",
    )
    rename_lin = ColumnLineage(
        source_columns={"stg_orders.user_id"},
        transformation_type="renamed",
    )

    models = {
        "orders": _make_model("orders", {
            "order_id": [direct_lin],
            "customer_id": [rename_lin],
        }),
    }

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c))

    assert len(results) == 2
    by_col = {r.column: r for r in results}

    direct = by_col["order_id"]
    assert direct.is_rename is False
    assert direct.source_column is None
    assert direct.progenitor_model == "stg_orders"
    assert direct.progenitor_column == "id"

    renamed = by_col["customer_id"]
    assert renamed.is_rename is True
    assert renamed.source_column == "user_id"
    assert renamed.progenitor_model == "stg_orders"
    assert renamed.progenitor_column == "user_id"


# ---------------------------------------------------------------------------
# Live DB resolver path (adapter mocked — no warehouse)
# ---------------------------------------------------------------------------

def test_live_db_path_calls_live_db_reader(tmp_path):
    m = _make_manifest(tmp_path)

    mock_reader = MagicMock()
    mock_reader.get_models_nodes.return_value = {
        "payments": _make_model("payments", {
            "payment_id": [
                ColumnLineage(
                    source_columns={"raw_payments.id"},
                    transformation_type="renamed",
                )
            ]
        })
    }

    with (
        patch("dbt_column_lineage.api._build_catalog_reader", return_value=mock_reader),
        patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry,
    ):
        instance = MockRegistry.return_value
        instance.get_models.return_value = {
            "payments": _make_model("payments", {
                "payment_id": [
                    ColumnLineage(
                        source_columns={"raw_payments.id"},
                        transformation_type="renamed",
                    )
                ]
            })
        }
        instance.load.return_value = None

        results = get_column_lineage(
            str(m),
            live_db=True,
            project_dir=str(tmp_path),
            profiles_dir=str(tmp_path),
        )

    assert len(results) == 1
    r = results[0]
    assert r.model == "payments"
    assert r.column == "payment_id"
    assert r.is_rename is True
    assert r.source_column == "id"


# ---------------------------------------------------------------------------
# Model filter
# ---------------------------------------------------------------------------

def test_model_filter_restricts_output(tmp_path):
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    lin = ColumnLineage(source_columns={"src.col"}, transformation_type="direct")
    all_models = {
        "model_a": _make_model("model_a", {"col1": [lin]}),
        "model_b": _make_model("model_b", {"col2": [lin]}),
    }

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = all_models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c), models=["model_a"])

    assert all(r.model == "model_a" for r in results)


# ---------------------------------------------------------------------------
# _resolve_progenitor helper
# ---------------------------------------------------------------------------

def test_resolve_progenitor_qualified():
    lin = ColumnLineage(source_columns={"my_model.my_col"}, transformation_type="direct")
    model, col = _resolve_progenitor(lin)
    assert model == "my_model"
    assert col == "my_col"


def test_resolve_progenitor_unqualified():
    lin = ColumnLineage(source_columns={"bare_col"}, transformation_type="direct")
    model, col = _resolve_progenitor(lin)
    assert model is None
    assert col == "bare_col"


def test_resolve_progenitor_empty():
    lin = ColumnLineage(source_columns=set(), transformation_type="direct")
    model, col = _resolve_progenitor(lin)
    assert model is None
    assert col is None


# ---------------------------------------------------------------------------
# compiled_sql_source validation
# ---------------------------------------------------------------------------

def test_auto_compile_requires_project_dir(tmp_path):
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)
    with pytest.raises(ValueError, match="project_dir is required"):
        get_column_lineage(str(m), catalog_path=str(c), compiled_sql_source="auto_compile")


def test_manifest_source_raises_when_no_compiled_code(tmp_path):
    """Manifest with no compiled_code should raise CompiledSqlMissingError."""
    manifest = {
        "metadata": {"adapter_type": "snowflake"},
        "nodes": {
            "model.pkg.my_model": {
                "resource_type": "model",
                "name": "my_model",
                "language": "sql",
                # no compiled_code or compiled_sql
            }
        },
        "sources": {},
    }
    m = tmp_path / "manifest.json"
    import json
    m.write_text(json.dumps(manifest))
    c = _make_catalog(tmp_path)

    with pytest.raises(CompiledSqlMissingError, match="dbt parse"):
        get_column_lineage(str(m), catalog_path=str(c), compiled_sql_source="manifest")


def test_target_dir_source_logs_warning_and_proceeds(tmp_path):
    """compiled_sql_source='target_dir' should not raise even with no inline SQL."""
    import json
    manifest = {
        "metadata": {"adapter_type": "snowflake"},
        "nodes": {},
        "sources": {},
    }
    m = tmp_path / "manifest.json"
    m.write_text(json.dumps(manifest))
    c = _make_catalog(tmp_path)

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = {}
        instance.load.return_value = None

        # Should not raise — empty manifest just produces empty results
        results = get_column_lineage(str(m), catalog_path=str(c), compiled_sql_source="target_dir")

    assert results == []


def test_auto_compile_calls_dbt_compile_subprocess(tmp_path):
    """compiled_sql_source='auto_compile' invokes dbt compile before resolving."""
    import json
    manifest = {
        "metadata": {"adapter_type": "snowflake"},
        "nodes": {},
        "sources": {},
    }
    m = tmp_path / "manifest.json"
    m.write_text(json.dumps(manifest))
    c = _make_catalog(tmp_path)

    mock_completed = MagicMock()
    mock_completed.returncode = 0

    with (
        patch("subprocess.run", return_value=mock_completed) as mock_run,
        patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry,
    ):
        instance = MockRegistry.return_value
        instance.get_models.return_value = {}
        instance.load.return_value = None

        results = get_column_lineage(
            str(m),
            catalog_path=str(c),
            compiled_sql_source="auto_compile",
            project_dir=str(tmp_path),
        )

    mock_run.assert_called_once()
    call_args = mock_run.call_args[0][0]
    assert "compile" in call_args
    assert str(tmp_path) in call_args
    assert results == []


def test_auto_compile_raises_on_dbt_failure(tmp_path):
    """auto_compile raises RuntimeError if dbt compile exits non-zero."""
    import json
    manifest = {
        "metadata": {"adapter_type": "snowflake"},
        "nodes": {},
        "sources": {},
    }
    m = tmp_path / "manifest.json"
    m.write_text(json.dumps(manifest))
    c = _make_catalog(tmp_path)

    mock_failed = MagicMock()
    mock_failed.returncode = 1
    mock_failed.stdout = "some output"
    mock_failed.stderr = "dbt compile error"

    with patch("subprocess.run", return_value=mock_failed):
        with pytest.raises(RuntimeError, match="dbt compile.*failed"):
            get_column_lineage(
                str(m),
                catalog_path=str(c),
                compiled_sql_source="auto_compile",
                project_dir=str(tmp_path),
            )


# ---------------------------------------------------------------------------
# is_computed flag
# ---------------------------------------------------------------------------

def test_is_computed_true_for_derived_columns(tmp_path):
    """Columns with transformation_type='derived' should have is_computed=True."""
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    derived_lin = ColumnLineage(
        source_columns={"stg_orders.amount", "stg_orders.tax"},
        transformation_type="derived",
    )
    models = {
        "orders": _make_model("orders", {"total_amount": [derived_lin]}),
    }

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c))

    assert len(results) == 1
    r = results[0]
    assert r.column == "total_amount"
    assert r.is_computed is True
    assert r.is_rename is False


def test_is_computed_false_for_direct_and_renamed_columns(tmp_path):
    """Direct and renamed columns must have is_computed=False."""
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    models = {
        "orders": _make_model("orders", {
            "order_id": [ColumnLineage(source_columns={"stg.id"}, transformation_type="direct")],
            "customer_key": [ColumnLineage(source_columns={"stg.user_id"}, transformation_type="renamed")],
        }),
    }

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c))

    by_col = {r.column: r for r in results}
    assert by_col["order_id"].is_computed is False
    assert by_col["customer_key"].is_computed is False


def test_is_computed_false_when_no_lineage(tmp_path):
    """Columns with no lineage (parser couldn't trace them) must have is_computed=False
    and is_first_in_chain=True (treated as source origin)."""
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    models = {
        "orders": _make_model("orders", {"mystery_col": []}),
    }

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c))

    assert len(results) == 1
    r = results[0]
    assert r.is_computed is False
    assert r.is_first_in_chain is True


def test_is_first_in_chain_false_when_progenitor_is_dbt_model(tmp_path):
    """Columns whose progenitor is a dbt model must have is_first_in_chain=False."""
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    models = {
        "stg": _make_model("stg", {"col": []}),   # source type, no lineage
        "orders": _make_model("orders", {
            "order_id": [ColumnLineage(source_columns={"stg.id"}, transformation_type="direct")],
            "total":    [ColumnLineage(source_columns={"stg.amount"}, transformation_type="derived")],
        }),
    }
    # Make "stg" a dbt model (resource_type="model") so it is NOT in source_names
    # — the default from _make_model already sets resource_type="model"

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c))

    by_col = {r.column: r for r in results if r.model == "orders"}
    assert by_col["order_id"].is_first_in_chain is False   # progenitor is a dbt model
    assert by_col["total"].is_first_in_chain is False      # computed with dbt model progenitor


def test_is_first_in_chain_true_when_progenitor_is_source_or_seed(tmp_path):
    """Columns whose progenitor is a dbt source or seed node are first-in-chain."""
    m = _make_manifest(tmp_path)
    c = _make_catalog(tmp_path)

    raw_source = _make_model("raw_table", {"id": []})
    raw_source.resource_type = "source"

    seed_node = _make_model("my_seed", {"code": []})
    seed_node.resource_type = "seed"

    models = {
        "raw_table": raw_source,
        "my_seed": seed_node,
        "stg_orders": _make_model("stg_orders", {
            "order_id": [ColumnLineage(source_columns={"raw_table.id"}, transformation_type="direct")],
            "status_code": [ColumnLineage(source_columns={"my_seed.code"}, transformation_type="direct")],
        }),
    }

    with patch("dbt_column_lineage.artifacts.registry.ModelRegistry") as MockRegistry:
        instance = MockRegistry.return_value
        instance.get_models.return_value = models
        instance.load.return_value = None

        results = get_column_lineage(str(m), catalog_path=str(c))

    stg_cols = {r.column: r for r in results if r.model == "stg_orders"}
    assert stg_cols["order_id"].is_first_in_chain is True    # progenitor is a source
    assert stg_cols["status_code"].is_first_in_chain is True  # progenitor is a seed

