"""Roadmap item 5: multi-source origins end-to-end (``progenitors``).

``ColumnLineageResult.progenitors`` generalizes ``union_branches``: an ordered
list of ``(model, column)`` pairs naming every direct input of the column.

- single-source columns: ``[(progenitor_model, progenitor_column)]``
- multi-source computed columns (COALESCE through CTEs, arithmetic over two
  tables, …): one pair per contributing source column
- union columns: identical to ``union_branches``

The annotation layer renders multi-source computed columns as
``Computed here from A.X, B.Y`` instead of the bare ``Computed here``.
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
    def model(name, sql):
        return {
            "name": name,
            "resource_type": "model",
            "language": "sql",
            "schema": "main",
            "database": "db",
            "columns": {},
            "compiled_code": sql,
            "depends_on": {"nodes": []},
        }

    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.coalesced": model(
                "coalesced",
                "with c as (select coalesce(a.x, b.y) as merged"
                " from tbl_a a join tbl_b b on a.k = b.k) select merged from c",
            ),
            "model.pkg.renamed": model(
                "renamed", "select id as order_id from base_table"
            ),
            "model.pkg.unioned": model(
                "unioned",
                "with u as (select a.x as val from tbl_a a"
                " union all select b.y as val from tbl_b b) select val from u",
            ),
        },
        "sources": {},
        "exposures": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return str(path)


@pytest.fixture()
def results_by_model_col(tmp_path):
    manifest_path = _write_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=manifest_path)
    reader.load()
    results = get_column_lineage(
        manifest_path=manifest_path, _catalog_reader_override=reader
    )
    return {(r.model, r.column): r for r in results}


def test_multi_source_column_lists_all_progenitors(results_by_model_col):
    r = results_by_model_col[("coalesced", "merged")]
    assert r.is_computed
    assert r.progenitor_model is None
    assert sorted(r.progenitors) == [("tbl_a", "x"), ("tbl_b", "y")]


def test_single_source_column_has_one_progenitor(results_by_model_col):
    r = results_by_model_col[("renamed", "order_id")]
    assert r.progenitors == [("base_table", "id")]
    assert (r.progenitor_model, r.progenitor_column) == ("base_table", "id")


def test_union_column_progenitors_match_union_branches(results_by_model_col):
    r = results_by_model_col[("unioned", "val")]
    assert r.is_union
    assert r.union_branches
    assert r.progenitors == r.union_branches


def test_computed_here_annotation_includes_inputs():
    from dbt_osmosis_cll.osmosis_propagation.annotations import format_computed_here_tag

    tag = format_computed_here_tag(inputs=["TBL_A.X", "TBL_B.Y"])
    assert "here from TBL_A.X, TBL_B.Y" in tag

    bare = format_computed_here_tag()
    assert bare.endswith("here")
