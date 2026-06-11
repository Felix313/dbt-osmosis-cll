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
