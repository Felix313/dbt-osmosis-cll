"""Offline compile for CLL (``compile-cll``): introspective macros replaced by config.

A tiny DuckDB project reproduces the patterns that make a plain offline ``dbt
compile`` emit the wrong SQL: a body gated by a compile-time query on a config
table, a custom introspective macro, ``is_incremental()``, and a model that is an
empty shell. The project's own macros call ``run_query`` — any of them reaching the
warehouse would fail the compile, so a green run proves the stubs took over.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_osmosis_cll.cli.main import cli
from dbt_osmosis_cll.cll_generator.api import clear_lineage_caches
from dbt_osmosis_cll.cll_generator.offline_compile import (
    detect_empty_shell,
    load_compile_config,
)
from dbt_osmosis_cll.config import reset_config

pytest.importorskip("dbt.adapters.duckdb")

FILES = {
    "dbt_project.yml": """
        name: cll_offline_fixture
        version: '1.0.0'
        config-version: 2
        profile: cll_offline_fixture
        model-paths: ["models"]
        macro-paths: ["macros"]
    """,
    "macros/introspect.sql": """
        {% macro get_column_values(table, column, order_by='count(*) desc', max_records=none, default=none, where=none) %}
            {% if not execute %}{{ return([]) }}{% endif %}
            {% set res = run_query('select ' ~ column ~ ' from ' ~ table) %}
            {{ return(res.columns[0].values()) }}
        {% endmacro %}
        {% macro latest_loaded_id() %}
            {% if not execute %}{{ return(0) }}{% endif %}
            {% set res = run_query('select max(id) from loaded') %}
            {{ return(res.columns[0].values()[0]) }}
        {% endmacro %}
    """,
    "models/load_config.sql": "select 'Y' as load_flg, current_timestamp as created_ts\n",
    "models/facts.sql": "select 1 as id, 'A' as status_cd\n",
    "models/gated.sql": """
        -- depends_on: {{ ref('facts') }}
        {%- set flg = get_column_values(ref('load_config'), column='load_flg', order_by='max(created_ts) desc') -%}
        {% if flg and flg[0] == 'Y' %}
        select f.id from {{ ref('facts') }} as f
        where f.status_cd = 'A' and f.id > {{ latest_loaded_id() }}
        {% else %}
        select * from {{ this }} limit 0
        {% endif %}
    """,
    "models/inc.sql": """
        {{ config(materialized='incremental') }}
        select f.id, f.status_cd from {{ ref('facts') }} as f
        {% if is_incremental() %} where f.id > (select max(id) from {{ this }}) {% endif %}
    """,
    "models/shell.sql": "select * from {{ this }} limit 0\n",
    "models/probe.sql": """
        -- depends_on: {{ ref('facts') }}
        {%- set present = adapter.get_relation(database=target.database, schema=target.schema, identifier='facts') -%}
        {%- set absent = adapter.get_relation(database=target.database, schema=target.schema, identifier='not_there') -%}
        {% if present is not none and absent is none %}
        select f.id from {{ ref('facts') }} as f
        {% else %}
        select 1 as broken
        {% endif %}
    """,
}

CONFIG = """
    [compile]
    adapter = duckdb
    introspection-values =
        load_config.load_flg = Y
    macro-stubs =
        latest_loaded_id = 0
"""


def _project(tmp_path: Path, config: str = CONFIG) -> Path:
    for rel, body in FILES.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    (tmp_path / ".osmosis-cll").write_text(textwrap.dedent(config).lstrip(), encoding="utf-8")
    reset_config()
    return tmp_path


def _compiled(project: Path, model: str) -> str:
    path = next((project / "target" / "compiled").rglob(f"{model}.sql"))
    return path.read_text(encoding="utf-8")


def test_compile_config_is_read_from_osmosis_file(tmp_path):
    cfg = load_compile_config(_project(tmp_path))
    assert cfg.adapter == "duckdb"
    assert cfg.introspection_values == {"load_config.load_flg": ["Y"]}
    assert cfg.macro_stubs == {"latest_loaded_id": "0"}
    assert cfg.is_incremental is False


def test_compile_cll_compiles_the_pinned_branch_without_a_connection(tmp_path):
    project = _project(tmp_path)
    result = CliRunner().invoke(
        cli, ["compile-cll", "-s", "gated inc shell", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, result.output

    gated = _compiled(project, "gated")
    assert "f.status_cd = 'A'" in gated and "f.id > 0" in gated
    assert "limit 0" not in gated.lower()
    assert "where f.id >" not in _compiled(project, "inc").lower()

    sidecar = json.loads((project / "target" / "cll-compile.json").read_text(encoding="utf-8"))
    models = sidecar["models"]
    assert models["gated"]["pinned_values"] == ["load_config.load_flg"]
    assert models["gated"]["stubbed_calls"] == ["latest_loaded_id"]
    assert models["inc"]["is_incremental"] is False
    assert set(models["shell"]["empty_shell"]) == {"limit_0", "self_only"}
    assert "empty_shell" not in models["gated"]
    assert any("is_incremental() = False" in w for w in sidecar["warnings"])
    # nothing in the project was touched
    assert (project / "macros" / "introspect.sql").read_text(encoding="utf-8").count(
        "run_query"
    ) == 2


def test_unpinned_value_is_reported_and_the_dummy_branch_flagged(tmp_path):
    config = CONFIG.replace("        load_config.load_flg = Y\n", "")
    project = _project(tmp_path, config)
    result = CliRunner().invoke(cli, ["compile-cll", "-s", "gated", "--project-dir", str(project)])
    assert result.exit_code == 0, result.output
    sidecar = json.loads((project / "target" / "cll-compile.json").read_text(encoding="utf-8"))
    gated = sidecar["models"]["gated"]
    assert gated["unpinned_values"] == ["load_config.load_flg"]
    assert set(gated["empty_shell"]) == {"limit_0", "self_only"}


def test_parse_cll_reports_compile_assumptions_and_usages(tmp_path):
    project = _project(tmp_path)
    runner = CliRunner()
    assert (
        runner.invoke(
            cli, ["compile-cll", "-s", "gated inc", "--project-dir", str(project)]
        ).exit_code
        == 0
    )
    clear_lineage_caches()
    result = runner.invoke(cli, ["parse-cll", "-s", "gated", "--project-dir", str(project)])
    assert result.exit_code == 0, result.output
    payload = json.loads((project / "target" / "cll-result.json").read_text(encoding="utf-8"))
    assert payload["compile"]["gated"]["pinned_values"] == ["load_config.load_flg"]
    usages = {(u["usage"], u["source_model"], u["source_column"]) for u in payload["usages"]}
    assert ("filter", "facts", "status_cd") in usages
    assert ("compile_time", "load_config", "load_flg") in usages
    assert ("compile_time", "load_config", "created_ts") in usages
    assert "coverage" in payload
    clear_lineage_caches()


def test_empty_shell_detection():
    assert detect_empty_shell("select * from m limit 0", "m", "duckdb") == ["limit_0", "self_only"]
    assert detect_empty_shell("select a from src where 1 = 0", "m", "duckdb") == ["false_predicate"]
    assert detect_empty_shell("select a from src where false", "m", "duckdb") == ["false_predicate"]
    assert detect_empty_shell("select a from src where a = 1", "m", "duckdb") == []
    # a literal-only config model is no shell
    assert detect_empty_shell("select 'Y' as flg", "m", "duckdb") == []


def test_layered_catalog_fills_relations_without_yaml_columns(tmp_path):
    from dbt_osmosis_cll.cll_generator.artifacts.layered_catalog import LayeredCatalogReader

    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {},
        "sources": {
            "source.pkg.raw.documented": {
                "name": "documented",
                "identifier": "documented",
                "schema": "raw",
                "database": "db",
                "columns": {"id": {"name": "id"}},
            },
            "source.pkg.raw.bare": {
                "name": "bare",
                "identifier": "bare",
                "schema": "raw",
                "database": "db",
                "columns": {},
            },
        },
    }
    catalog = {
        "nodes": {},
        "sources": {
            "source.pkg.raw.bare": {
                "metadata": {"name": "bare"},
                "columns": {"ID": {"type": "int"}, "FLAG": {"type": "text"}},
            }
        },
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / "catalog.json").write_text(json.dumps(catalog), encoding="utf-8")

    reader = LayeredCatalogReader(
        str(tmp_path / "manifest.json"),
        sources=["manifest", "catalog"],
        catalog_path=str(tmp_path / "catalog.json"),
    )
    reader.load()
    models = reader.get_models_nodes()
    assert set(models["source.pkg.raw.bare"].columns) == {"id", "flag"}
    assert reader.column_origin == {
        "source.pkg.raw.documented": "manifest",
        "source.pkg.raw.bare": "catalog",
    }

    manifest_only = LayeredCatalogReader(str(tmp_path / "manifest.json"))
    manifest_only.load()
    manifest_only.get_models_nodes()
    assert manifest_only.column_origin["source.pkg.raw.bare"] is None

    with pytest.raises(ValueError, match="Unknown column source"):
        LayeredCatalogReader(str(tmp_path / "manifest.json"), sources=["nope"])


def test_direct_adapter_introspection_is_answered_from_the_manifest(tmp_path):
    project = _project(tmp_path)
    result = CliRunner().invoke(cli, ["compile-cll", "-s", "probe", "--project-dir", str(project)])
    assert result.exit_code == 0, result.output
    assert "broken" not in _compiled(project, "probe")
    sidecar = json.loads((project / "target" / "cll-compile.json").read_text(encoding="utf-8"))
    probe = sidecar["models"]["probe"]
    assert probe["relations_assumed_existing"] == ["facts"]
    assert probe["relations_assumed_missing"] == ["not_there"]
    assert any("not_there" in w for w in sidecar["warnings"])


COLUMN_QUERY_FILES = {
    "macros/list_columns.sql": """
        {% macro list_columns(rel, verb='show') %}
            {% if verb == 'show' %}
                {% set res = run_query('SHOW COLUMNS IN TABLE ' ~ rel) %}
                {% set names = res.columns[2].values() if execute else [] %}
            {% else %}
                {% set res = run_query('DESCRIBE TABLE ' ~ rel) %}
                {% set names = res.columns[0].values() if execute else [] %}
            {% endif %}
            {{ return(names) }}
        {% endmacro %}
    """,
    "models/models.yml": """
        version: 2
        models:
          - name: facts
            columns:
              - name: id
              - name: status_cd
    """,
    "models/raw_bare.sql": "select 1 as bare_id, 'x' as bare_txt\n",
    "models/copy_show.sql": """
        -- depends_on: {{ ref('facts') }}
        select {{ list_columns(ref('facts')) | join(', ') | lower }} from {{ ref('facts') }}
    """,
    "models/copy_describe.sql": """
        -- depends_on: {{ ref('raw_bare') }}
        select {{ list_columns(ref('raw_bare'), 'describe') | join(', ') | lower }}
        from {{ ref('raw_bare') }}
    """,
    "models/other_query.sql": """
        {% set res = run_query('select max(id) from loaded') %}
        select 1 as id
    """,
}


def _column_query_project(tmp_path: Path) -> Path:
    project = _project(tmp_path)
    for rel, body in COLUMN_QUERY_FILES.items():
        (project / rel).write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return project


def test_column_listing_queries_are_answered_offline(tmp_path):
    project = _column_query_project(tmp_path)
    (project / "target").mkdir(exist_ok=True)
    catalog = {
        "nodes": {
            "model.cll_offline_fixture.raw_bare": {
                "metadata": {"name": "raw_bare"},
                "columns": {
                    "BARE_TXT": {"name": "BARE_TXT", "type": "TEXT", "index": 2},
                    "BARE_ID": {"name": "BARE_ID", "type": "INTEGER", "index": 1},
                },
            }
        },
        "sources": {},
    }
    (project / "target" / "catalog.json").write_text(json.dumps(catalog), encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "compile-cll",
            "-s",
            "copy_show copy_describe",
            "--project-dir",
            str(project),
            "--column-sources",
            "catalog",
        ],
    )
    assert result.exit_code == 0, result.output
    # YAML columns answer SHOW COLUMNS; catalog.json fills the undocumented relation.
    assert "select id, status_cd from" in " ".join(_compiled(project, "copy_show").split())
    assert "select bare_id, bare_txt" in " ".join(_compiled(project, "copy_describe").split())


def test_other_warehouse_queries_fail_with_a_hint(tmp_path):
    project = _column_query_project(tmp_path)
    result = CliRunner().invoke(
        cli, ["compile-cll", "-s", "other_query", "--project-dir", str(project)]
    )
    assert result.exit_code == 1
    sidecar = json.loads((project / "target" / "cll-compile.json").read_text(encoding="utf-8"))
    assert any("macro-stubs" in e and "select max(id)" in e for e in sidecar["errors"])


STAR_OVER_MODEL = {
    "models/inner_model.sql": "select 1 as z_id, 'x' as a_txt\n",
    "models/star_user.sql": "select {{ dbt_utils_star(ref('inner_model')) }} from {{ ref('inner_model') }}\n",
    "macros/dbt_utils_star.sql": """
        {% macro dbt_utils_star(rel) %}
            {% set cols = adapter.get_columns_in_relation(rel) %}
            {{ return(cols | map(attribute='name') | join(', ') | lower) }}
        {% endmacro %}
    """,
}


def test_model_columns_come_from_its_sql_compiled_earlier_in_the_run(tmp_path):
    project = _project(tmp_path)
    for rel, body in STAR_OVER_MODEL.items():
        (project / rel).write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    result = CliRunner().invoke(
        cli, ["compile-cll", "-s", "+star_user", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, result.output
    # No YAML, no warehouse: order and content come from the upstream SQL, not a sorted guess.
    assert "select z_id, a_txt from" in " ".join(_compiled(project, "star_user").split())


def test_live_needs_a_real_profile(tmp_path):
    project = _project(tmp_path)
    result = CliRunner().invoke(
        cli,
        ["compile-cll", "-s", "facts", "--project-dir", str(project), "--column-sources", "live"],
    )
    assert result.exit_code == 1
    assert "needs a real profile" in result.output or "needs a real profile" in str(result)


def test_live_reads_physical_columns_once_and_parse_cll_reuses_them(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    project = _column_query_project(tmp_path)
    db_file = project / "warehouse.duckdb"
    con = duckdb.connect(str(db_file))
    # Physical order and content differ from the YAML of `facts` (id, status_cd).
    con.execute("create table main.facts (status_cd varchar, extra_col int, id int)")
    con.close()
    profiles = project / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        textwrap.dedent(
            f"""
            cll_offline_fixture:
              target: cll_offline
              outputs:
                cll_offline:
                  type: duckdb
                  path: {db_file.as_posix()}
                  schema: main
            """
        ),
        encoding="utf-8",
    )
    args = ["--project-dir", str(project), "--profiles-dir", str(profiles)]
    result = CliRunner().invoke(
        cli, ["compile-cll", "-s", "copy_show", *args, "--column-sources", "live"]
    )
    assert result.exit_code == 0, result.output
    assert "select status_cd, extra_col, id from" in " ".join(
        _compiled(project, "copy_show").split()
    )

    snapshot = json.loads((project / "target" / "cll-columns.json").read_text(encoding="utf-8"))
    facts = snapshot["nodes"]["model.cll_offline_fixture.facts"]
    assert list(facts["columns"]) == ["status_cd", "extra_col", "id"]

    # parse-cll answers from the file: no profile, no connection.
    clear_lineage_caches()
    out = project / "result.json"
    parsed = CliRunner().invoke(
        cli,
        [
            "parse-cll",
            "-s",
            "copy_show",
            "--project-dir",
            str(project),
            "--column-sources",
            "live",
            "-o",
            str(out),
        ],
    )
    assert parsed.exit_code == 0, parsed.output
    coverage = json.loads(out.read_text(encoding="utf-8"))["coverage"]
    assert coverage["column_sources"].get("live", 0) >= 1


def test_column_sources_order_is_the_priority(tmp_path):
    from dbt_osmosis_cll.cll_generator.artifacts.layered_catalog import LayeredCatalogReader

    manifest = {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {},
        "sources": {
            "source.pkg.raw.partly": {
                "name": "partly",
                "identifier": "partly",
                "schema": "raw",
                "database": "db",
                "columns": {"id": {"name": "id", "description": "kept"}},
            }
        },
    }
    catalog = {
        "nodes": {},
        "sources": {
            "source.pkg.raw.partly": {
                "metadata": {"name": "partly"},
                "columns": {"Z_NEW": {"type": "int"}, "ID": {"type": "int"}},
            }
        },
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / "catalog.json").write_text(json.dumps(catalog), encoding="utf-8")

    first = LayeredCatalogReader(
        str(tmp_path / "manifest.json"),
        sources=["catalog", "manifest"],
        catalog_path=str(tmp_path / "catalog.json"),
    )
    first.load()
    columns = first.get_models_nodes()["source.pkg.raw.partly"].columns
    assert list(columns) == ["z_new", "id"]
    assert columns["id"].description == "kept"
    assert first.column_origin["source.pkg.raw.partly"] == "catalog"

    yaml_first = LayeredCatalogReader(
        str(tmp_path / "manifest.json"),
        sources=["manifest", "catalog"],
        catalog_path=str(tmp_path / "catalog.json"),
    )
    yaml_first.load()
    assert list(yaml_first.get_models_nodes()["source.pkg.raw.partly"].columns) == ["id"]


def test_documented_but_not_produced_column_does_not_outrank_a_select_alias(tmp_path):
    project = _project(tmp_path)
    (project / "models" / "stale.yml").write_text(
        textwrap.dedent(
            """
            version: 2
            models:
              - name: facts
                columns:
                  - name: id
                  - name: stale_ts
            """
        ),
        encoding="utf-8",
    )
    (project / "models" / "alias_user.sql").write_text(
        "select f.id, (select max(c.created_ts) from {{ ref('load_config') }} c) as stale_ts\n"
        "from {{ ref('facts') }} as f\nwhere stale_ts is not null\n",
        encoding="utf-8",
    )
    compiled = CliRunner().invoke(
        cli, ["compile-cll", "-s", "+alias_user", "--project-dir", str(project)]
    )
    assert compiled.exit_code == 0, compiled.output
    clear_lineage_caches()
    out = project / "result.json"
    parsed = CliRunner().invoke(
        cli, ["parse-cll", "-s", "+alias_user", "--project-dir", str(project), "-o", str(out)]
    )
    assert parsed.exit_code == 0, parsed.output
    result = json.loads(out.read_text(encoding="utf-8"))
    filters = [u for u in result["usages"] if u["model"] == "alias_user" and u["usage"] == "filter"]
    assert {(u["source_model"], u["source_column"]) for u in filters} == {
        ("load_config", "created_ts")
    }


def test_macro_over_an_unknown_column_list_still_yields_lineage(tmp_path):
    project = _project(tmp_path)
    (project / "macros" / "hash_list.sql").write_text(
        textwrap.dedent(
            """
            {% macro hash_list(cols) %}
                {%- set parts = [] -%}
                {%- for c in cols %}{% do parts.append(c.name) %}{% endfor -%}
                IFF({{ parts | join(" || ") }} = '', NULL, SHA1(CONCAT({{ parts | join(", ") }})))
            {%- endmacro %}
            """
        ),
        encoding="utf-8",
    )
    (project / "models" / "sources.yml").write_text(
        textwrap.dedent(
            """
            version: 2
            sources:
              - name: ext
                schema: main
                tables:
                  - name: bare_src
            """
        ),
        encoding="utf-8",
    )
    (project / "models" / "over_bare.sql").write_text(
        "{% set cols = adapter.get_columns_in_relation(source('ext', 'bare_src')) %}\n"
        "select new.a_ts as loaded_ts, {{ hash_list(cols) }} as hashdiff,\n"
        "{{ dbt_utils_like_star() }}\nfrom {{ source('ext', 'bare_src') }} as new\n",
        encoding="utf-8",
    )
    (project / "macros" / "star_like.sql").write_text(
        "{% macro dbt_utils_like_star() %}new.a_ts{% endmacro %}\n", encoding="utf-8"
    )
    compiled = CliRunner().invoke(
        cli, ["compile-cll", "-s", "over_bare", "--project-dir", str(project)]
    )
    assert compiled.exit_code == 0, compiled.output
    clear_lineage_caches()
    out = project / "result.json"
    parsed = CliRunner().invoke(
        cli, ["parse-cll", "-s", "over_bare", "--project-dir", str(project), "-o", str(out)]
    )
    assert parsed.exit_code == 0, parsed.output
    rows = {r["column"]: r for r in json.loads(out.read_text(encoding="utf-8"))["results"]}
    assert rows["loaded_ts"]["progenitors"] == [["bare_src", "a_ts"]]
    assert "cll_unknown_columns" not in rows
    assert rows["hashdiff"]["progenitors"] == []
