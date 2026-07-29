"""Lazy lineage parsing (Variante B): the registry defers the whole-project SQL
parse; ``get_column_lineage`` ensures lineage only for the requested models, so
cold-start cost tracks the request size instead of the repo size.

Guarantees under test:
- scoping: unselected models are NOT parsed (their parse warnings never fire);
- equivalence: scoped results are identical to full-parse results;
- ``select *`` recursion: star sources are parsed on demand so parser-discovered
  stub columns on undocumented models still resolve, exactly as in eager mode;
- eager mode (default) is unchanged for direct ModelRegistry users.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from dbt_osmosis_cll.cll_generator.api import clear_lineage_caches, get_column_lineage
from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
from dbt_osmosis_cll.cll_generator.artifacts.registry import ModelRegistry


@pytest.fixture(autouse=True)
def _fresh_caches():
    clear_lineage_caches()
    yield
    clear_lineage_caches()


@pytest.fixture()
def cll_caplog(caplog):
    """caplog wired to the dbt_osmosis_cll package logger.

    The osmosis logger module sets ``propagate=False`` on that hierarchy (to avoid
    double-printing next to dbt's root handler), so records never reach the
    root-attached pytest handler — attach it directly instead.
    """
    pkg = logging.getLogger("dbt_osmosis_cll")
    pkg.addHandler(caplog.handler)
    yield caplog
    pkg.removeHandler(caplog.handler)


def _node(name: str, sql: str, deps: list[str], columns: dict | None = None) -> dict:
    return {
        "name": name,
        "resource_type": "model",
        "language": "sql",
        "schema": "main",
        "database": "db",
        "columns": columns or {},
        "compiled_code": sql,
        "depends_on": {"nodes": deps},
    }


def _write_manifest(tmp_path: Path, nodes: dict) -> str:
    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": nodes,
        "sources": {},
        "exposures": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return str(path)


def _reader(manifest_path: str) -> ManifestCatalogReader:
    reader = ManifestCatalogReader(manifest_path=manifest_path)
    reader.load()
    return reader


def test_lazy_scoped_call_does_not_parse_unselected_models(tmp_path, cll_caplog):
    caplog = cll_caplog
    manifest_path = _write_manifest(
        tmp_path,
        {
            "model.pkg.good_model": _node(
                "good_model", "select id as order_id from raw_orders", []
            ),
            # Unparseable SQL — would log a 'Failed to process lineage' warning if parsed.
            "model.pkg.broken_model": _node("broken_model", "select ??? !! from", []),
        },
    )

    with caplog.at_level(logging.WARNING):
        results = get_column_lineage(
            manifest_path=manifest_path,
            models=["good_model"],
            _catalog_reader_override=_reader(manifest_path),
        )

    assert {r.model for r in results} == {"good_model"}
    assert "broken_model" not in caplog.text

    # Selecting the broken model DOES trigger its parse (and the warning).
    with caplog.at_level(logging.WARNING):
        get_column_lineage(
            manifest_path=manifest_path,
            models=["broken_model"],
            _catalog_reader_override=_reader(manifest_path),
        )
    assert "broken_model" in caplog.text


def test_lazy_scoped_results_match_full_parse(tmp_path):
    nodes = {
        "model.pkg.stg_orders": _node("stg_orders", "select id as order_id from raw_orders", []),
        "model.pkg.mart_orders": _node(
            "mart_orders", "select order_id from stg_orders", ["model.pkg.stg_orders"]
        ),
    }
    manifest_path = _write_manifest(tmp_path, nodes)

    scoped = get_column_lineage(
        manifest_path=manifest_path,
        models=["mart_orders"],
        _catalog_reader_override=_reader(manifest_path),
    )

    clear_lineage_caches()
    full = get_column_lineage(
        manifest_path=manifest_path,
        _catalog_reader_override=_reader(manifest_path),
    )
    full_mart = [r for r in full if r.model == "mart_orders"]

    assert [vars(r) for r in scoped] == [vars(r) for r in full_mart]


def test_lazy_star_reference_parses_source_on_demand(tmp_path):
    """X does `select * from y`; Y is undocumented, so X's star lineage depends on
    Y's parser-discovered stub columns — the lazy path must parse Y on demand."""
    nodes = {
        "model.pkg.star_child": _node(
            "star_child",
            "select * from star_base",
            ["model.pkg.star_base"],
            columns={"order_id": {"name": "order_id"}},
        ),
        # No YAML columns — order_id only exists as a parse stub.
        "model.pkg.star_base": _node("star_base", "select id as order_id from raw_orders", []),
    }
    manifest_path = _write_manifest(tmp_path, nodes)

    results = get_column_lineage(
        manifest_path=manifest_path,
        models=["star_child"],
        _catalog_reader_override=_reader(manifest_path),
    )

    child_row = next(r for r in results if r.model == "star_child" and r.column == "order_id")
    assert child_row.progenitor_model == "star_base"
    assert child_row.progenitor_column == "order_id"


def test_eager_registry_unchanged_by_default(tmp_path):
    manifest_path = _write_manifest(
        tmp_path,
        {"model.pkg.stg_orders": _node("stg_orders", "select id as order_id from raw_orders", [])},
    )
    registry = ModelRegistry(
        catalog_path=None,
        manifest_path=manifest_path,
        _catalog_reader_override=_reader(manifest_path),
    )
    registry.load()
    model = registry.get_model("stg_orders")
    assert model.columns["order_id"].lineage  # parsed at load, no ensure_lineage needed


def test_lazy_registry_parses_on_ensure(tmp_path):
    manifest_path = _write_manifest(
        tmp_path,
        {"model.pkg.stg_orders": _node("stg_orders", "select id as order_id from raw_orders", [])},
    )
    registry = ModelRegistry(
        catalog_path=None,
        manifest_path=manifest_path,
        _catalog_reader_override=_reader(manifest_path),
        lazy_lineage=True,
    )
    registry.load()
    model = registry.get_model("stg_orders")
    # Deferred at load: the parse-discovered stub column does not exist yet.
    assert "order_id" not in model.columns

    registry.ensure_lineage(model)
    assert model.columns["order_id"].lineage
    assert model.columns["order_id"].lineage[0].source_columns == {"raw_orders.id"}
