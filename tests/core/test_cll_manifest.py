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
