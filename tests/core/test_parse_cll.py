"""Standalone CLL build for a dbt selector: ``dbt-osmosis-cll parse-cll -s +model``.

Covers the graph selector parsing (``model``, ``+model``, ``N+model``, ``model+``,
``model+N``, ``+model+``), the manifest-graph resolution (models only, sources
terminate the upstream walk, depth limits in both directions), and the CLI
end-to-end path writing a dedicated ``target/cll-result.json`` that never touches
osmosis' ``cll_cache.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_osmosis_cll.cli.main import cli
from dbt_osmosis_cll.cll_generator.selector import (
    GraphSelector,
    parse_selector,
    parse_selectors,
    resolve_selected_models,
)

# ---------------------------------------------------------------------------
# Selector parsing
# ---------------------------------------------------------------------------


def test_parse_selector_bare_model():
    assert parse_selector("stg_orders") == GraphSelector("stg_orders", up_depth=0, down_depth=0)


def test_parse_selector_full_upstream():
    assert parse_selector("+stg_orders") == GraphSelector("stg_orders", up_depth=None, down_depth=0)


def test_parse_selector_depth_limited_upstream():
    assert parse_selector("2+stg_orders") == GraphSelector("stg_orders", up_depth=2, down_depth=0)


def test_parse_selector_full_downstream():
    assert parse_selector("stg_orders+") == GraphSelector("stg_orders", up_depth=0, down_depth=None)


def test_parse_selector_depth_limited_downstream():
    assert parse_selector("stg_orders+2") == GraphSelector("stg_orders", up_depth=0, down_depth=2)


def test_parse_selector_both_directions():
    assert parse_selector("+stg_orders+") == GraphSelector(
        "stg_orders", up_depth=None, down_depth=None
    )
    assert parse_selector("1+stg_orders+2") == GraphSelector("stg_orders", up_depth=1, down_depth=2)


def test_parse_selector_rejects_method_selectors():
    with pytest.raises(ValueError, match="method selectors"):
        parse_selector("tag:nightly")
    with pytest.raises(ValueError, match="method selectors"):
        parse_selector("@stg_orders")


def test_parse_selector_rejects_bare_operator():
    with pytest.raises(ValueError, match="not valid"):
        parse_selector("+")


def test_parse_selectors_splits_on_whitespace_and_commas():
    parsed = parse_selectors(["+mart_a stg_b", "1+int_c,mart_d+"])
    assert parsed == [
        GraphSelector("mart_a", up_depth=None, down_depth=0),
        GraphSelector("stg_b", up_depth=0, down_depth=0),
        GraphSelector("int_c", up_depth=1, down_depth=0),
        GraphSelector("mart_d", up_depth=0, down_depth=None),
    ]


def test_parse_selectors_rejects_empty():
    with pytest.raises(ValueError, match="No selector"):
        parse_selectors(["  ,  "])


# ---------------------------------------------------------------------------
# Graph resolution over the manifest
# ---------------------------------------------------------------------------


def _graph_manifest() -> dict:
    """source raw_orders → stg_orders → int_orders → mart_orders (+ unrelated other_model)."""

    def node(name: str, deps: list[str]) -> dict:
        return {
            "name": name,
            "resource_type": "model",
            "language": "sql",
            "schema": "main",
            "database": "db",
            "columns": {},
            "depends_on": {"nodes": deps},
        }

    return {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.stg_orders": node("stg_orders", ["source.pkg.raw.raw_orders"]),
            "model.pkg.int_orders": node("int_orders", ["model.pkg.stg_orders"]),
            "model.pkg.mart_orders": node("mart_orders", ["model.pkg.int_orders"]),
            "model.pkg.other_model": node("other_model", []),
        },
        "sources": {
            "source.pkg.raw.raw_orders": {
                "name": "raw_orders",
                "identifier": "raw_orders",
                "resource_type": "source",
                "schema": "raw",
                "database": "db",
                "columns": {},
            },
        },
        "exposures": {},
    }


def test_resolve_bare_model_selects_only_anchor():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("mart_orders")])
    assert models == ["mart_orders"]


def test_resolve_full_upstream_reaches_sources_but_excludes_them():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("+mart_orders")])
    assert models == ["int_orders", "mart_orders", "stg_orders"]


def test_resolve_upstream_depth_one_stops_after_direct_parents():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("1+mart_orders")])
    assert models == ["int_orders", "mart_orders"]


def test_resolve_full_downstream_reaches_endpoints():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("stg_orders+")])
    assert models == ["int_orders", "mart_orders", "stg_orders"]


def test_resolve_downstream_depth_one_stops_after_direct_children():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("stg_orders+1")])
    assert models == ["int_orders", "stg_orders"]


def test_resolve_both_directions():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("+int_orders+")])
    assert models == ["int_orders", "mart_orders", "stg_orders"]


def test_resolve_is_case_insensitive():
    models = resolve_selected_models(_graph_manifest(), [parse_selector("1+MART_ORDERS")])
    assert models == ["int_orders", "mart_orders"]


def test_resolve_union_of_multiple_selectors():
    models = resolve_selected_models(
        _graph_manifest(),
        parse_selectors(["other_model", "1+mart_orders"]),
    )
    assert models == ["int_orders", "mart_orders", "other_model"]


def test_resolve_unknown_model_raises():
    with pytest.raises(KeyError, match="nope"):
        resolve_selected_models(_graph_manifest(), [parse_selector("+nope")])


# ---------------------------------------------------------------------------
# CLI end-to-end
# ---------------------------------------------------------------------------


def _write_project(tmp_path: Path) -> Path:
    """Write a compiled manifest (inline compiled_code) under <tmp>/target/manifest.json."""
    manifest = _graph_manifest()
    compiled = {
        "model.pkg.stg_orders": "select id as order_id, amount from raw_orders",
        "model.pkg.int_orders": "select order_id, amount from stg_orders",
        "model.pkg.mart_orders": "select order_id from int_orders",
        "model.pkg.other_model": "select 1 as one",
    }
    for uid, sql in compiled.items():
        manifest["nodes"][uid]["compiled_code"] = sql
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return target


def test_parse_cll_help_exits_zero():
    result = CliRunner().invoke(cli, ["parse-cll", "--help"])
    assert result.exit_code == 0
    assert "--select" in result.output


def test_parse_cll_writes_dedicated_result_file(tmp_path):
    target = _write_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["parse-cll", "-s", "+mart_orders", "--project-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output

    out_path = target / "cll-result.json"
    assert out_path.exists()
    # The osmosis CLL cache must not be created or touched.
    assert not (target / "cll_cache.json").exists()

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert payload["selectors"] == ["+mart_orders"]
    assert payload["models"] == ["int_orders", "mart_orders", "stg_orders"]

    rows = payload["results"]
    assert {r["model"] for r in rows} == {"int_orders", "mart_orders", "stg_orders"}
    stg_order_id = next(r for r in rows if r["model"] == "stg_orders" and r["column"] == "order_id")
    assert stg_order_id["is_rename"] is True
    assert stg_order_id["source_column"] == "id"
    # schema_version 2: the row states its transformation kind and carries the
    # expression field, so the lineage explorer can rebuild a ColumnLineage without
    # re-parsing the SQL. (A pure rename has no expression of its own — the parser
    # only records one for derived/literal/window columns.)
    assert stg_order_id["transformation_type"] == "renamed"
    assert "sql_expression" in stg_order_id

    # Per-model source-SQL hashes drive staleness detection in the explorer. This
    # fixture's manifest has no original_file_path, so the map is present but empty.
    assert payload["fingerprints"] == {}


def test_parse_cll_depth_limit_restricts_models(tmp_path):
    target = _write_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["parse-cll", "-s", "1+mart_orders", "--project-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads((target / "cll-result.json").read_text(encoding="utf-8"))
    assert payload["models"] == ["int_orders", "mart_orders"]
    assert {r["model"] for r in payload["results"]} == {"int_orders", "mart_orders"}


def test_parse_cll_downstream_selector(tmp_path):
    target = _write_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["parse-cll", "-s", "stg_orders+", "--project-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads((target / "cll-result.json").read_text(encoding="utf-8"))
    assert payload["models"] == ["int_orders", "mart_orders", "stg_orders"]


def test_parse_cll_custom_output_path(tmp_path):
    _write_project(tmp_path)
    out = tmp_path / "elsewhere" / "my-cll.json"
    result = CliRunner().invoke(
        cli,
        ["parse-cll", "-s", "mart_orders", "--project-dir", str(tmp_path), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["models"] == ["mart_orders"]


def test_parse_cll_unknown_model_exits_one(tmp_path):
    _write_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["parse-cll", "-s", "+does_not_exist", "--project-dir", str(tmp_path)]
    )
    assert result.exit_code == 1


def test_parse_cll_method_selector_exits_one(tmp_path):
    _write_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["parse-cll", "-s", "tag:nightly", "--project-dir", str(tmp_path)]
    )
    assert result.exit_code == 1


def test_parse_cll_missing_manifest_exits_one(tmp_path):
    result = CliRunner().invoke(
        cli, ["parse-cll", "-s", "+mart_orders", "--project-dir", str(tmp_path)]
    )
    assert result.exit_code == 1
