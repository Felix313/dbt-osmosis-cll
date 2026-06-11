"""Roadmap item 3: registry identity by manifest unique_id.

Two manifest entries sharing a lowercase short name (cross-package models, or
a model vs a source identifier) must no longer silently overwrite each other.
The registry keys its state by unique_id, resolves SQL relation names through
an alias map (last-wins in nodes-then-sources order, matching the historical
dict-overwrite behaviour), and ``ColumnLineageResult`` carries the additive
``unique_id`` field so consumers can disambiguate.
"""

from __future__ import annotations

import json

import pytest

from dbt_osmosis_cll.cll_generator.api import clear_lineage_caches, get_column_lineage
from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader


@pytest.fixture(autouse=True)
def _fresh_registry_cache():
    clear_lineage_caches()
    yield
    clear_lineage_caches()


def _write_manifest(tmp_path) -> str:
    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg_a.orders": {
                "name": "orders",
                "resource_type": "model",
                "language": "sql",
                "schema": "main",
                "database": "db",
                "columns": {},
                "compiled_code": "select id as order_id from base_table",
                "depends_on": {"nodes": []},
            },
            "model.pkg_b.orders": {
                "name": "orders",
                "resource_type": "model",
                "language": "sql",
                "schema": "other",
                "database": "db",
                "columns": {},
                "compiled_code": "select amount from other_table",
                "depends_on": {"nodes": []},
            },
            "model.pkg_a.customers": {
                "name": "customers",
                "resource_type": "model",
                "language": "sql",
                "schema": "main",
                "database": "db",
                "columns": {},
                "compiled_code": "select customer_id from raw_customers",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {
            "source.pkg_a.raw.customers": {
                "name": "customers",
                "identifier": "customers",
                "source_name": "raw",
                "schema": "raw_schema",
                "database": "db",
                "columns": {"customer_id": {"description": "src pk"}},
            },
        },
        "exposures": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return str(path)


@pytest.fixture()
def lineage_results(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=manifest_path)
    reader.load()
    return get_column_lineage(
        manifest_path=manifest_path,
        _catalog_reader_override=reader,
    )


def test_colliding_models_both_survive(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=manifest_path)
    reader.load()
    models = reader.get_models_nodes()
    assert "model.pkg_a.orders" in models
    assert "model.pkg_b.orders" in models
    assert models["model.pkg_a.orders"].unique_id == "model.pkg_a.orders"


def test_lineage_rows_emitted_for_both_colliding_models(lineage_results):
    orders_rows = [r for r in lineage_results if r.model == "orders"]
    uids = {r.unique_id for r in orders_rows}
    assert uids == {"model.pkg_a.orders", "model.pkg_b.orders"}

    by_uid = {(r.unique_id, r.column): r for r in orders_rows}
    pkg_a = by_uid[("model.pkg_a.orders", "order_id")]
    assert pkg_a.progenitor_model == "base_table"
    assert pkg_a.progenitor_column == "id"
    pkg_b = by_uid[("model.pkg_b.orders", "amount")]
    assert pkg_b.progenitor_model == "other_table"


def test_every_result_carries_unique_id(lineage_results):
    assert lineage_results
    assert all(r.unique_id for r in lineage_results)


def test_model_vs_source_identifier_collision_resolves_like_before(tmp_path):
    """Alias map is last-wins in nodes-then-sources order → source shadows model,
    exactly as the old name-keyed dict overwrite did — but the model node is no
    longer lost from the registry."""
    from dbt_osmosis_cll.cll_generator.artifacts.registry import ModelRegistry

    manifest_path = _write_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=manifest_path)
    registry = ModelRegistry(
        catalog_path=None,
        manifest_path=manifest_path,
        _catalog_reader_override=reader,
    )
    registry.load()

    # Name lookup yields the source (historical overwrite winner)...
    assert registry.get_model("customers").resource_type == "source"
    # ...while both entries remain reachable by unique_id.
    assert registry.get_model("model.pkg_a.customers").resource_type == "model"
    by_id = registry.get_models_by_id()
    assert "model.pkg_a.customers" in by_id
    assert "source.pkg_a.raw.customers" in by_id
    # Compat view stays name-keyed with one winner per name.
    compat = registry.get_models()
    assert compat["customers"].resource_type == "source"
    assert compat["orders"].resource_type == "model"
