"""Unit tests for the CLL engine's ManifestReader node index.

Roadmap item 2: ``_find_node`` must be an O(1) indexed lookup instead of a
linear scan over every manifest node, while preserving the historical
behaviour (case-insensitive match, first occurrence wins on collisions,
defensive copy returned).
"""

from __future__ import annotations

import json

import pytest

from dbt_osmosis_cll.cll_generator.artifacts.manifest import ManifestReader


def _make_manifest(nodes: dict) -> dict:
    return {"metadata": {"adapter_type": "duckdb"}, "nodes": nodes, "sources": {}}


@pytest.fixture()
def reader(tmp_path):
    nodes = {
        "model.pkg.orders": {
            "name": "Orders",
            "resource_type": "model",
            "language": "sql",
            "original_file_path": "models/orders.sql",
            "description": "orders model",
            "tags": ["t1"],
        },
        "model.other_pkg.orders": {
            "name": "orders",
            "resource_type": "model",
            "language": "sql",
            "original_file_path": "models/dupe/orders.sql",
            "description": "duplicate name in another package",
            "tags": [],
        },
        "model.pkg.customers": {
            "name": "customers",
            "resource_type": "model",
            "language": "sql",
            "original_file_path": "models/customers.sql",
            "description": "",
            "tags": [],
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(_make_manifest(nodes)), encoding="utf-8")
    r = ManifestReader(str(path))
    r.load()
    return r


def test_find_node_is_case_insensitive(reader):
    assert reader._find_node("CUSTOMERS")["original_file_path"] == "models/customers.sql"


def test_find_node_first_occurrence_wins_on_collision(reader):
    # Two nodes share the name "orders"; iteration order of the manifest dict
    # decides, exactly as the old linear scan did.
    node = reader._find_node("orders")
    assert node["original_file_path"] == "models/orders.sql"


def test_find_node_returns_copy(reader):
    node = reader._find_node("customers")
    node["description"] = "mutated"
    assert reader._find_node("customers")["description"] == ""


def test_find_node_missing_returns_none(reader):
    assert reader._find_node("does_not_exist") is None


def test_find_node_without_load_builds_index_lazily(tmp_path):
    r = ManifestReader(str(tmp_path / "missing.json"))
    # Manifest assigned directly (no load()) — index must build lazily.
    r.manifest = _make_manifest({
        "model.pkg.a": {"name": "a", "resource_type": "model"},
    })
    assert r._find_node("a")["name"] == "a"


def test_find_node_empty_manifest_returns_none():
    r = ManifestReader()
    assert r._find_node("anything") is None


# ---------------------------------------------------------------------------
# DAG wiring: which depends_on node types become graph edges
# ---------------------------------------------------------------------------


def _dag_reader(tmp_path, deps: list[str], sources: dict | None = None) -> ManifestReader:
    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.stg_customers": {
                "name": "stg_customers",
                "resource_type": "model",
                "language": "sql",
                "depends_on": {"nodes": deps},
            },
            "seed.pkg.raw_customers": {"name": "raw_customers", "resource_type": "seed"},
            "snapshot.pkg.customers_snap": {
                "name": "customers_snap",
                "resource_type": "snapshot",
                "depends_on": {"nodes": []},
            },
        },
        "sources": sources or {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    r = ManifestReader(str(path))
    r.load()
    return r


def test_seed_dependency_becomes_an_upstream_edge(tmp_path):
    """A seed is a terminal origin like a source: models selecting from one have
    real column lineage into it, so it must appear in the DAG rather than leaving
    the chain to stop one hop short of where the data comes from."""
    r = _dag_reader(tmp_path, ["seed.pkg.raw_customers"])
    assert r.get_model_upstream()["stg_customers"] == {"raw_customers"}


def test_seed_dependency_becomes_a_downstream_edge(tmp_path):
    r = _dag_reader(tmp_path, ["seed.pkg.raw_customers"])
    assert r.get_model_downstream()["raw_customers"] == {"stg_customers"}


def test_model_source_and_snapshot_edges_still_wire(tmp_path):
    r = _dag_reader(
        tmp_path,
        ["model.pkg.other", "source.pkg.raw.orders", "snapshot.pkg.customers_snap"],
        sources={"source.pkg.raw.orders": {"name": "orders", "identifier": "raw_orders"}},
    )
    # Sources resolve through their identifier — that is the name the SQL uses.
    assert r.get_model_upstream()["stg_customers"] == {"other", "raw_orders", "customers_snap"}


def test_test_and_operation_dependencies_are_not_wired(tmp_path):
    """Only node types that can carry column lineage become edges."""
    r = _dag_reader(tmp_path, ["test.pkg.not_null_x", "operation.pkg.hook"])
    assert r.get_model_upstream()["stg_customers"] == set()
