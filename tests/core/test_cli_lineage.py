"""Roadmap item 6: the lineage explorer wired into the main CLI.

``dbt-osmosis-cll lineage explore`` serves the (previously orphaned) HTML
explorer manifest-only: ``ManifestCatalogReader`` for column lists, inline
``compiled_code`` / ``target/compiled/`` for SQL — no catalog.json, no
warehouse connection. The FastAPI/uvicorn dependencies stay optional behind
the ``lineage-ui`` extra.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_osmosis_cll.cli.main import cli

_HAS_LINEAGE_UI = importlib.util.find_spec("fastapi") is not None


def test_lineage_group_help_exits_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["lineage", "--help"])
    assert result.exit_code == 0
    assert "explore" in result.output


def test_lineage_explore_help_exits_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["lineage", "explore", "--help"])
    assert result.exit_code == 0
    assert "--port" in result.output


@pytest.mark.skipif(_HAS_LINEAGE_UI, reason="lineage-ui extra installed; error path untestable")
def test_lineage_explore_without_extra_gives_install_hint(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["lineage", "explore", "--project-dir", str(tmp_path)])
    assert result.exit_code == 1


def _write_manifest(tmp_path) -> Path:
    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.stg_orders": {
                "name": "stg_orders",
                "resource_type": "model",
                "language": "sql",
                "schema": "main",
                "database": "db",
                "columns": {"order_id": {"description": "pk"}},
                "compiled_code": "select id as order_id from raw_orders",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
        "exposures": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_lineage_service_runs_manifest_only(tmp_path):
    """LineageService accepts an injected ManifestCatalogReader — no catalog.json."""
    from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
    from dbt_osmosis_cll.cll_generator.lineage.service import LineageService

    manifest_path = _write_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=str(manifest_path))
    reader.load()
    service = LineageService(
        catalog_path=None,
        manifest_path=manifest_path,
        catalog_reader=reader,
        use_target_dir_fallback=True,
    )
    model = service.registry.get_model("stg_orders")
    assert model.unique_id == "model.pkg.stg_orders"
    lineage = model.columns["order_id"].lineage
    assert lineage and lineage[0].source_columns == {"raw_orders.id"}


# ---------------------------------------------------------------------------
# --from-cll: serving lineage from a precomputed artifact
# ---------------------------------------------------------------------------


def _write_cll_artifact(target: Path) -> Path:
    """A minimal parse-cll payload covering the fixture's single model."""
    path = target / "cll-result.json"
    path.write_text(
        json.dumps({
            "schema_version": 2,
            "selectors": ["+stg_orders"],
            "models": ["stg_orders"],
            "fingerprints": {},
            "results": [
                {
                    "model": "stg_orders",
                    "column": "order_id",
                    "transformation_type": "renamed",
                    "sql_expression": None,
                    "progenitor_model": "raw_orders",
                    "progenitor_column": "id",
                    "is_rename": True,
                    "source_column": "id",
                    "progenitors": [["raw_orders", "id"]],
                }
            ],
        }),
        encoding="utf-8",
    )
    return path


def _explore_project(tmp_path) -> Path:
    target = tmp_path / "target"
    target.mkdir()
    manifest = json.loads(_write_manifest(tmp_path).read_text(encoding="utf-8"))
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return target


def test_from_cll_appears_in_help():
    result = CliRunner().invoke(cli, ["lineage", "explore", "--help"])
    assert result.exit_code == 0
    assert "--from-cll" in result.output


@pytest.mark.skipif(not _HAS_LINEAGE_UI, reason="needs the lineage-ui extra")
def test_from_cll_serves_without_parsing_sql(tmp_path, monkeypatch):
    """The explorer starts from the artifact alone — no whole-project SQL parse."""
    from dbt_osmosis_cll.cll_generator.artifacts import registry as registry_module
    from dbt_osmosis_cll.cll_generator.lineage.display.html import explore as explore_module

    target = _explore_project(tmp_path)
    _write_cll_artifact(target)

    served = {}
    monkeypatch.setattr(
        explore_module.LineageExplorer, "start", lambda self: served.update(self.__dict__)
    )
    parsed: list[str] = []
    original = registry_module.ModelRegistry._parse_model_lineage
    monkeypatch.setattr(
        registry_module.ModelRegistry,
        "_parse_model_lineage",
        lambda self, model: (parsed.append(model.name), original(self, model))[1],
    )

    result = CliRunner().invoke(
        cli, ["lineage", "explore", "--project-dir", str(tmp_path), "--from-cll"]
    )
    assert result.exit_code == 0, result.output
    assert parsed == []
    assert served["context"]["mode"] == "selector"
    assert served["context"]["selectors"] == ["+stg_orders"]

    service = served["lineage_service"]
    lineage = service.registry.get_model("stg_orders").columns["order_id"].lineage
    assert lineage and lineage[0].source_columns == {"raw_orders.id"}


@pytest.mark.skipif(not _HAS_LINEAGE_UI, reason="needs the lineage-ui extra")
def test_from_cll_without_artifact_exits_one(tmp_path):
    _explore_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["lineage", "explore", "--project-dir", str(tmp_path), "--from-cll"]
    )
    assert result.exit_code == 1


@pytest.mark.skipif(not _HAS_LINEAGE_UI, reason="needs the lineage-ui extra")
def test_from_cll_with_unreadable_artifact_exits_one(tmp_path):
    target = _explore_project(tmp_path)
    (target / "cll-result.json").write_text("{not json", encoding="utf-8")
    result = CliRunner().invoke(
        cli, ["lineage", "explore", "--project-dir", str(tmp_path), "--from-cll"]
    )
    assert result.exit_code == 1


def test_lineage_reaches_upstream_seeds(tmp_path):
    """Column lineage terminates at the seed a model selects from, not one hop short.

    Seeds are registered as models by the catalog reader but were absent from the
    dependency graph, so the service dropped their column references as unresolvable
    (dbt-osmosis-cll-17z).
    """
    from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
    from dbt_osmosis_cll.cll_generator.lineage.service import LineageSelector, LineageService

    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.stg_customers": {
                "name": "stg_customers",
                "resource_type": "model",
                "language": "sql",
                "schema": "main",
                "database": "db",
                "columns": {},
                "compiled_code": "select id as customer_id from raw_customers",
                "depends_on": {"nodes": ["seed.pkg.raw_customers"]},
            },
            "seed.pkg.raw_customers": {
                "name": "raw_customers",
                "resource_type": "seed",
                "schema": "main",
                "database": "db",
                "columns": {"id": {"description": "Seed PK"}},
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
        "exposures": {},
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    reader = ManifestCatalogReader(manifest_path=str(manifest_path))
    reader.load()
    service = LineageService(
        catalog_path=None,
        manifest_path=manifest_path,
        catalog_reader=reader,
        use_target_dir_fallback=True,
    )

    assert "raw_customers" in service.registry.get_model("stg_customers").upstream

    info = service.get_column_info(LineageSelector.from_string("+stg_customers.customer_id"))
    assert "id" in info["upstream"]["raw_customers"]

    # ...and the seed knows what it feeds, so impact analysis works from that end too.
    impact = service.get_column_impact("raw_customers", "id")
    assert impact["summary"]["affected_models"] == 1
    assert impact["affected_columns"][0]["column"] == "customer_id"


# ---------------------------------------------------------------------------
# Graph contract: every edge connects nodes that exist
# ---------------------------------------------------------------------------


def _fanin_manifest(tmp_path) -> Path:
    """A downstream column fed by two columns of the same upstream model.

    Exploring one of them used to emit an edge from the sibling without ever
    creating its node, leaving the renderer an edge into nothing.
    """
    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.stg_payments": {
                "name": "stg_payments",
                "alias": "stg_payments",
                "resource_type": "model",
                "language": "sql",
                "schema": "main",
                "database": "db",
                "columns": {"amount": {}, "payment_method": {}},
                "compiled_code": "select amount, payment_method from raw_payments",
                "depends_on": {"nodes": []},
            },
            "model.pkg.orders": {
                "name": "orders",
                "alias": "orders",
                "resource_type": "model",
                "language": "sql",
                "schema": "main",
                "database": "db",
                "columns": {},
                "compiled_code": (
                    "select sum(case when payment_method = 'card' then amount end) "
                    "as card_amount from stg_payments"
                ),
                "depends_on": {"nodes": ["model.pkg.stg_payments"]},
            },
        },
        "sources": {},
        "exposures": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


@pytest.mark.skipif(not _HAS_LINEAGE_UI, reason="needs the lineage-ui extra")
def test_graph_edges_never_reference_missing_nodes(tmp_path):
    from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
    from dbt_osmosis_cll.cll_generator.lineage.display.html.explore import LineageExplorer
    from dbt_osmosis_cll.cll_generator.lineage.service import LineageService

    manifest_path = _fanin_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=str(manifest_path))
    reader.load()
    service = LineageService(
        catalog_path=None,
        manifest_path=manifest_path,
        catalog_reader=reader,
        use_target_dir_fallback=True,
    )
    explorer = LineageExplorer()
    explorer.set_lineage_service(service)

    explorer.data.main_node = "col_stg_payments_payment_method"
    explorer._process_lineage_tree("stg_payments", "payment_method")
    explorer._drop_dangling_edges()

    node_ids = {n["id"] for n in explorer.data.nodes}
    dangling = [
        (e["source"], e["target"])
        for e in explorer.data.edges
        if e["source"] not in node_ids or e["target"] not in node_ids
    ]
    assert dangling == []
    # The sibling that also feeds the downstream column is shown, not dropped.
    assert "col_stg_payments_amount" in node_ids
    assert explorer.data.main_node in node_ids


@pytest.mark.skipif(not _HAS_LINEAGE_UI, reason="needs the lineage-ui extra")
def test_terminal_column_still_renders_its_own_node(tmp_path):
    """A column with no downstream must not come back as an empty graph."""
    from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
    from dbt_osmosis_cll.cll_generator.lineage.display.html.explore import LineageExplorer
    from dbt_osmosis_cll.cll_generator.lineage.service import LineageService

    manifest_path = _fanin_manifest(tmp_path)
    reader = ManifestCatalogReader(manifest_path=str(manifest_path))
    reader.load()
    service = LineageService(
        catalog_path=None,
        manifest_path=manifest_path,
        catalog_reader=reader,
        use_target_dir_fallback=True,
    )
    explorer = LineageExplorer()
    explorer.set_lineage_service(service)

    model = service.get_model("orders")
    explorer._set_column_info(model.columns["card_amount"])
    explorer.data.main_node = "col_orders_card_amount"
    explorer._process_lineage_tree("orders", "card_amount")
    explorer._drop_dangling_edges()

    assert explorer.data.main_node in {n["id"] for n in explorer.data.nodes}
