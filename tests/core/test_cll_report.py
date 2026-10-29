"""Data-product report (``lineage report``): anchors, requirements, waves, outputs.

A small synthetic lineage graph covers the rules: a data product needs every
column, interim models only what a data product reaches (as value or as
join / filter input), tracing stops at other data products, and the build waves
follow the column-level dependencies. The browser engine (``static/engine.js``)
must compute the same as the Python reference — checked through Node when available.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_osmosis_cll.cli.main import cli
from dbt_osmosis_cll.cll_generator.report.analysis import analyse, build_graph, column_status
from dbt_osmosis_cll.cll_generator.report.data_products import (
    load_data_products,
    parse_model_list,
)
from dbt_osmosis_cll.cll_generator.report.render import compact_graph, render_html
from dbt_osmosis_cll.config import reset_config

STATIC = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "dbt_osmosis_cll"
    / "cll_generator"
    / "report"
    / "static"
)


def _row(model, column, ttype="direct", progenitors=(), in_sql=True):
    return {
        "model": model,
        "column": column,
        "transformation_type": ttype,
        "progenitors": [list(p) for p in progenitors],
        "in_sql": in_sql,
    }


def _usage(model, usage, source_model, source_column, self_ref=False):
    return {
        "model": model,
        "usage": usage,
        "source_model": source_model,
        "source_column": source_column,
        "scope": "main",
        "predicate_sql": f"{source_column} = 1",
        "is_self_reference": self_ref,
    }


# raw (source) -> stg_a, stg_b -> dp_one -> int_c (+ stg_b) -> dp_two
CLL = {
    "schema_version": 3,
    "selectors": ["+dp_two"],
    "results": [
        _row("stg_a", "id", progenitors=[("raw", "id")]),
        _row("stg_a", "amount", progenitors=[("raw", "amount")]),
        _row("stg_a", "unused_a", progenitors=[("raw", "unused_a")]),
        _row("stg_b", "id", progenitors=[("raw", "id")]),
        _row("stg_b", "flag", progenitors=[("raw", "flag")]),
        _row("stg_b", "label", progenitors=[("raw", "label")]),
        _row("stg_b", "unused_b", progenitors=[("raw", "unused_b")]),
        _row("stg_b", "stale_doc", in_sql=False),
        _row("dp_one", "id", progenitors=[("stg_a", "id")]),
        _row("dp_one", "total", "aggregate", [("stg_a", "amount")]),
        _row("dp_one", "loaded_at", "generated"),
        _row("int_c", "id", progenitors=[("dp_one", "id")]),
        _row("int_c", "total", progenitors=[("dp_one", "total")]),
        _row("int_c", "label", progenitors=[("stg_b", "label")]),
        _row("int_c", "rank_no", "window"),
        _row("int_c", "dropped_c", progenitors=[("stg_b", "id")]),
        _row("dp_two", "id", progenitors=[("int_c", "id")]),
        _row("dp_two", "total", progenitors=[("int_c", "total")]),
        _row("dp_two", "label", progenitors=[("int_c", "label")]),
    ],
    "usages": [
        _usage("dp_one", "group_by", "stg_a", "id"),
        _usage("int_c", "join_key", "stg_b", "id"),
        _usage("int_c", "filter", "stg_b", "flag"),
        _usage("int_c", "filter", "int_c", "id", self_ref=True),
        _usage("dp_two", "filter", "int_c", "missing_col"),
    ],
    "coverage": {"relations_without_columns": [], "star_over_unknown": []},
}


def _node(name, deps, path="models/LAYER/x.sql"):
    return {
        "resource_type": "model",
        "name": name,
        "original_file_path": path,
        "config": {"materialized": "table"},
        "columns": {},
        "depends_on": {"nodes": [f"model.p.{d}" if d != "raw" else "source.p.s.raw" for d in deps]},
    }


MANIFEST = {
    "nodes": {
        "model.p.stg_a": _node("stg_a", ["raw"], "models/STAGING/stg_a.sql"),
        "model.p.stg_b": _node("stg_b", ["raw"], "models/STAGING/stg_b.sql"),
        "model.p.dp_one": _node("dp_one", ["stg_a"], "models/MARTS/dp_one.sql"),
        "model.p.int_c": _node("int_c", ["dp_one", "stg_b"], "models/MARTS/int_c.sql"),
        "model.p.dp_two": _node("dp_two", ["int_c", "dp_one"], "models/MARTS/dp_two.sql"),
    },
    "sources": {
        "source.p.s.raw": {
            "name": "raw",
            "identifier": "raw",
            "source_name": "s",
            "original_file_path": "models/sources.yml",
            "columns": {"id": {}, "amount": {}, "never_read": {}},
        }
    },
}

DPS = ["dp_one", "dp_two"]


def _status(graph, result):
    return {(r["model"], r["column"]): r for r in column_status(graph, result)}


def test_data_products_are_anchors_with_build_waves():
    graph = build_graph(CLL, MANIFEST, DPS)
    result = analyse(graph, DPS)
    assert result.data_products == DPS
    assert result.waves == {"dp_one": 1, "dp_two": 2}
    assert set(result.anchors["dp_two"]) == {"dp_one"}
    assert result.anchors["dp_two"]["dp_one"] == {
        ("dp_one", "id"): {"data"},
        ("dp_one", "total"): {"data"},
    }
    # dp_two depends on dp_one in dbt AND reads its columns: no dbt-only edge.
    assert result.dbt_only_deps == {}
    # Origins stop at the anchor instead of running through to the source.
    assert result.origins["dp_two"]["total"] == [["dp_one", "total", "data_product"]]
    assert result.origins["dp_two"]["label"] == [["raw", "label", "source"]]
    assert result.origins["dp_one"]["loaded_at"] == []


def test_interim_columns_are_required_only_when_a_data_product_reaches_them():
    graph = build_graph(CLL, MANIFEST, DPS)
    result = analyse(graph, DPS)
    st = _status(graph, result)
    assert st[("int_c", "label")]["status"] == "required"
    assert st[("int_c", "label")]["category"] == "data"
    assert st[("int_c", "dropped_c")]["status"] == "droppable"
    assert st[("int_c", "rank_no")]["status"] == "droppable"
    # Join key / filter inputs of a needed model are required as logic.
    assert st[("stg_b", "id")]["category"] == "logic"
    assert st[("stg_b", "id")]["usages"] == ["join_key"]
    assert st[("stg_b", "flag")]["status"] == "required"
    assert st[("stg_b", "unused_b")]["status"] == "droppable"
    assert st[("stg_a", "unused_a")]["status"] == "droppable"
    # Data-product columns are always required, even without a downstream reader.
    assert st[("dp_one", "loaded_at")]["status"] == "required"
    # dp_two reads dp_one as an anchor: dp_one's columns are needed by dp_one itself.
    assert st[("dp_one", "id")]["needed_by"] == ["dp_one"]
    # Sources: read columns required, documented-but-unread ones unused.
    assert st[("raw", "amount")]["status"] == "required"
    assert st[("raw", "never_read")]["status"] == "unused"
    # Undocumented and unread: the column does not appear at all.
    assert ("raw", "unused_a") not in st


def test_without_anchor_the_lineage_runs_through_to_the_sources():
    graph = build_graph(CLL, MANIFEST, ["dp_two"])
    result = analyse(graph, ["dp_two"])
    st = _status(graph, result)
    assert result.waves == {"dp_two": 1}
    assert st[("dp_one", "id")]["role"] == "interim"
    assert st[("dp_one", "loaded_at")]["status"] == "droppable"
    assert result.origins["dp_two"]["total"] == [["raw", "amount", "source"]]


def test_quality_signals():
    graph = build_graph(CLL, MANIFEST, DPS)
    result = analyse(graph, DPS)
    assert graph.documented_only == [["stg_b", "stale_doc"]]
    assert ["int_c", "rank_no", "no_input"] in graph.gaps
    assert result.dangling == [["int_c", "missing_col"]]
    assert analyse(graph, ["dp_one", "nope"]).missing == ["nope"]


def test_old_results_without_in_sql_flag_are_classified_by_signature():
    legacy = json.loads(json.dumps(CLL))
    for row in legacy["results"]:
        flag = row.pop("in_sql")
        if not flag:
            row.update({"progenitor_model": None, "sql_expression": None, "union_branches": []})
    graph = build_graph(legacy, MANIFEST, DPS)
    assert graph.documented_only == [["stg_b", "stale_doc"]]


def test_data_products_config_section(tmp_path):
    (tmp_path / ".osmosis-cll").write_text(
        textwrap.dedent(
            """
            [osmosis]
            [data-products]
            models =
                DP_ONE   # comment
                dp_two, dp_one
            """
        ),
        encoding="utf-8",
    )
    reset_config()
    assert load_data_products(tmp_path) == ["dp_one", "dp_two"]
    assert parse_model_list("a b,c\nd") == ["a", "b", "c", "d"]
    assert load_data_products(tmp_path / "missing_dir_is_fine") == ["dp_one", "dp_two"]


def test_lineage_report_cli_writes_all_outputs(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    (target / "cll-result.json").write_text(json.dumps(CLL), encoding="utf-8")
    (target / "manifest.json").write_text(json.dumps(MANIFEST), encoding="utf-8")
    (tmp_path / ".osmosis-cll").write_text("[data-products]\nmodels = dp_one dp_two\n")
    reset_config()
    result = CliRunner().invoke(
        cli, ["lineage", "report", "--project-dir", str(tmp_path), "--lang", "de"]
    )
    assert result.exit_code == 0, result.output
    html = (target / "cll-report.html").read_text(encoding="utf-8")
    assert '<html lang="de">' in html and "CllEngine" in html and "/*__DATA__*/" not in html
    columns = (target / "cll-report_columns.csv").read_text(encoding="utf-8-sig")
    assert "INT_C,DROPPED_C,interim,MARTS,droppable" in columns
    waves = (target / "cll-report_data_products.csv").read_text(encoding="utf-8-sig")
    assert "2,DP_TWO,DP_ONE,2,0" in waves
    assert "| 2 | DP_TWO | DP_ONE (2 columns) |" in (target / "cll-report.md").read_text(
        encoding="utf-8"
    )


def test_lineage_report_cli_needs_data_products(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    (target / "cll-result.json").write_text(json.dumps(CLL), encoding="utf-8")
    (target / "manifest.json").write_text(json.dumps(MANIFEST), encoding="utf-8")
    reset_config()
    result = CliRunner().invoke(cli, ["lineage", "report", "--project-dir", str(tmp_path)])
    assert result.exit_code == 1


def _js_analysis(graph, dps):
    node = shutil.which("node")
    if node is None:
        pytest.skip("node not available")
    script = textwrap.dedent(
        """
        const E = require(process.argv[process.argv.length - 1]);
        const input = JSON.parse(require("fs").readFileSync(0, "utf8"));
        const ix = E.index(E.expand(input.blob));
        const r = E.analyse(ix, input.dps);
        const status = E.columnStatus(ix, r);
        const anchors = {};
        for (const dp of Object.keys(r.anchors)) {
          anchors[dp] = {};
          for (const up of Object.keys(r.anchors[dp])) {
            anchors[dp][up] = Object.values(r.anchors[dp][up]).map(
              (e) => [e.column, Object.keys(e.categories).sort()]).sort();
          }
        }
        process.stdout.write(JSON.stringify({
          waves: r.waves, missing: r.missing, dangling: r.dangling, dbtOnly: r.dbtOnly,
          origins: r.origins, anchors,
          status: status.map((s) => [s.model, s.column, s.role, s.status, s.category,
                                     s.usages, s.neededBy]),
        }));
        """
    )
    proc = subprocess.run(
        [node, "-e", script, str(STATIC / "engine.js")],
        input=json.dumps({"blob": compact_graph(graph), "dps": dps}),
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return json.loads(proc.stdout)


@pytest.mark.parametrize("dps", [DPS, ["dp_two"], ["dp_one", "nope"]])
def test_browser_engine_matches_python_reference(dps):
    graph = build_graph(CLL, MANIFEST, dps)
    py = analyse(graph, dps)
    js = _js_analysis(graph, dps)
    assert js["waves"] == py.waves
    assert js["missing"] == py.missing
    assert js["dangling"] == py.dangling
    assert js["dbtOnly"] == py.dbt_only_deps
    assert js["origins"] == py.origins
    assert js["anchors"] == {
        dp: {
            up: sorted([c, sorted(cats)] for (_m, c), cats in cols.items())
            for up, cols in ups.items()
        }
        for dp, ups in py.anchors.items()
    }
    py_status = [
        [
            r["model"],
            r["column"],
            r["role"],
            r["status"],
            r["category"],
            r["usages"],
            r["needed_by"],
        ]
        for r in column_status(graph, py)
    ]
    assert js["status"] == py_status


def test_html_report_scripts_are_valid_javascript(tmp_path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("node not available")
    for name in ("engine.js", "app.js"):
        subprocess.run([node, "--check", str(STATIC / name)], check=True)
    graph = build_graph(CLL, MANIFEST, DPS)
    assert "dp_one" in render_html(graph)
