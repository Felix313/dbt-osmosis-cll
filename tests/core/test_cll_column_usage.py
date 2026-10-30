"""Join / filter / qualify / window / group-by / compile-time column usage.

The lineage pass traces projected columns only; these tests cover the usage pass that
records every upstream column a model *reads* without outputting it, attributed to the
base relation through CTE hops, and kept apart from the projected lineage.
"""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from dbt_osmosis_cll.cli.main import cli
from dbt_osmosis_cll.cll_generator.api import clear_lineage_caches, get_column_usage
from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
from dbt_osmosis_cll.cll_generator.parser import SQLColumnParser
from dbt_osmosis_cll.cll_generator.parser.jinja_usage import (
    extract_compile_time_usages,
    extract_var_names,
    tag_var_predicates,
)


def _usages(sql: str, table_columns=None, dialect: str = "snowflake"):
    parser = SQLColumnParser(dialect=dialect, table_columns=table_columns)
    return parser.parse_column_lineage(sql).column_usages


def _rows(usages):
    return {(u.usage, u.source, u.scope, u.detail) for u in usages}


# ---------------------------------------------------------------------------
# QUALIFY / windows
# ---------------------------------------------------------------------------

LATEST_CONTRACT_SQL = """
with contracts_latest as (
    select sat.contract_id, sat.status_cd
    from db.vault.contract_sat as sat
    qualify row_number() over (partition by sat.contract_id order by sat.valid_from_ts desc) = 1
),
active as (
    select c.contract_id from contracts_latest as c where c.status_cd = 'A'
)
select a.contract_id from active as a
"""


def test_qualify_column_not_projected_by_its_cte_is_attributed_to_base_relation():
    rows = _rows(_usages(LATEST_CONTRACT_SQL))
    assert ("qualify", "contract_sat.valid_from_ts", "contracts_latest", "order_by") in rows
    assert ("qualify", "contract_sat.contract_id", "contracts_latest", "partition_by") in rows


def test_filter_in_later_cte_resolves_through_cte_hop():
    rows = _rows(_usages(LATEST_CONTRACT_SQL))
    assert ("filter", "contract_sat.status_cd", "active", "predicate") in rows


def test_projected_lineage_is_unchanged_by_usage_pass():
    parser = SQLColumnParser(dialect="snowflake")
    result = parser.parse_column_lineage(LATEST_CONTRACT_SQL)
    assert set(result.column_lineage) == {"contract_id"}
    assert result.column_lineage["contract_id"][0].source_columns == {"contract_sat.contract_id"}


def test_window_spec_of_projection_is_window_usage():
    sql = """
    select
        t.account_id,
        max(t.billing_dt) over (partition by t.run_dt, t.account_id order by t.seq_no) as last_bill
    from billing as t
    """
    rows = _rows(_usages(sql))
    assert ("window", "billing.run_dt", "<final>", "partition_by") in rows
    assert ("window", "billing.account_id", "<final>", "partition_by") in rows
    assert ("window", "billing.seq_no", "<final>", "order_by") in rows
    # the windowed value itself is projected lineage, not a usage
    assert not any(src == "billing.billing_dt" for _, src, _, _ in rows)


def test_qualify_on_select_alias_resolves_to_alias_sources():
    sql = """
    select p.contract_id, coalesce(d.product_cd, p.product_cd) as product_total
    from base as p left join details as d on d.contract_id = p.contract_id
    qualify row_number() over (partition by p.contract_id, product_total order by p.contract_id) = 1
    """
    usages = _usages(
        sql,
        table_columns={
            "base": {"contract_id", "product_cd"},
            "details": {"contract_id", "product_cd"},
        },
    )
    alias_rows = {(u.source, u.resolution) for u in usages if u.column_ref == "product_total"}
    assert alias_rows == {
        ("base.product_cd", "select_alias"),
        ("details.product_cd", "select_alias"),
    }


# ---------------------------------------------------------------------------
# JOIN ... ON / WHERE / subqueries
# ---------------------------------------------------------------------------


def test_join_on_splits_keys_from_one_sided_filters():
    sql = """
    select o.order_id
    from orders as o
    join approvals as a on a.order_id = o.order_id and a.approved_flg = 'Y'
    left join periods as p on o.order_dt between p.valid_from and p.valid_to
    """
    rows = _rows(_usages(sql))
    assert ("join_key", "approvals.order_id", "<final>", "predicate") in rows
    assert ("join_key", "orders.order_id", "<final>", "predicate") in rows
    assert ("join_filter", "approvals.approved_flg", "<final>", "predicate") in rows
    # range join: every column of the predicate relates the two relations
    assert ("join_key", "periods.valid_from", "<final>", "predicate") in rows
    assert ("join_key", "orders.order_dt", "<final>", "predicate") in rows


def test_in_subquery_marks_outer_column_and_subquery_projection_as_filter():
    sql = """
    with run as (select max(run_dt) as run_dt from load_config where load_flg = 'Y')
    select d.contract_id
    from details as d
    where d.full_dt in (select r.run_dt from run as r)
    """
    usages = _usages(sql)
    rows = _rows(usages)
    assert ("filter", "details.full_dt", "<final>", "predicate") in rows
    assert ("filter", "load_config.run_dt", "<final>/subquery", "subquery") in rows
    assert ("filter", "load_config.load_flg", "run", "predicate") in rows
    subq = next(u for u in usages if u.detail == "subquery")
    assert subq.predicate_sql.startswith("d.full_dt IN")


def test_unreachable_cte_predicates_are_ignored():
    sql = """
    with unused as (select x from other_table where other_table.flag = 1)
    select t.a from t where t.b > 0
    """
    sources = {u.source for u in _usages(sql)}
    assert "other_table.flag" not in sources
    assert "t.b" in sources


def test_self_join_of_incremental_relation_is_reported():
    sql = """
    select n.run_dt from new_rows as n
    left join analytics.mart.my_model as existing on n.run_dt = existing.run_dt
    where existing.run_dt is null
    """
    rows = _rows(_usages(sql))
    assert ("filter", "my_model.run_dt", "<final>", "predicate") in rows
    assert ("join_key", "my_model.run_dt", "<final>", "predicate") in rows


# ---------------------------------------------------------------------------
# GROUP BY / HAVING
# ---------------------------------------------------------------------------


def test_group_by_all_expands_to_non_aggregate_projections():
    sql = """
    with src as (select s.run_dt, s.contract_id, s.amount from sales as s)
    select run_dt, contract_id, sum(amount) as amount
    from src
    group by all
    having sum(amount) != 0
    """
    usages = _usages(sql)
    rows = _rows(usages)
    assert ("group_by", "sales.run_dt", "<final>", "key") in rows
    assert ("group_by", "sales.contract_id", "<final>", "key") in rows
    assert ("having", "sales.amount", "<final>", "predicate") in rows
    assert not any(u.usage == "group_by" and u.source == "sales.amount" for u in usages)


def test_group_by_ordinal_maps_to_projection():
    rows = _rows(_usages("select t.region, count(*) from t group by 1"))
    assert ("group_by", "t.region", "<final>", "key") in rows


def test_group_by_all_marker_does_not_leak_into_lineage():
    parser = SQLColumnParser(dialect="snowflake")
    result = parser.parse_column_lineage("select a, sum(b) as b from t group by all")
    assert set(result.column_lineage) == {"a", "b"}


# ---------------------------------------------------------------------------
# Unqualified references
# ---------------------------------------------------------------------------


def test_unqualified_reference_resolves_via_parent_columns():
    sql = "select main.id, sum(proceeds_ht) as total from main join appr on appr.id = main.id where approved_flg = 'Y'"
    cols = {"main": {"id", "proceeds_ht"}, "appr": {"id", "approved_flg"}}
    usages = _usages(sql, table_columns=cols)
    flt = next(u for u in usages if u.usage == "filter")
    assert (flt.source, flt.resolution) == ("appr.approved_flg", "unqualified_resolved")
    # projected lineage benefits from the same column knowledge
    parser = SQLColumnParser(dialect="snowflake", table_columns=cols)
    lin = parser.parse_column_lineage(sql).column_lineage["total"][0]
    assert lin.source_columns == {"main.proceeds_ht"}


def test_unqualified_reference_resolves_via_column_lookup():
    sql = "select a.id from a join b on a.id = b.id where flag_cd = 'X'"
    lookup = {"a": {"id"}, "b": {"id", "flag_cd"}}
    parser = SQLColumnParser(dialect="snowflake", column_lookup=lookup.get)
    flt = next(u for u in parser.parse_column_lineage(sql).column_usages if u.usage == "filter")
    assert (flt.source, flt.resolution) == ("b.flag_cd", "unqualified_resolved")


def test_ambiguous_unqualified_reference_lists_every_candidate():
    sql = "select a.id from a join b on a.id = b.id where shared_col = 1"
    usages = _usages(sql, table_columns={"a": {"id", "shared_col"}, "b": {"id", "shared_col"}})
    flt = {(u.source, u.resolution) for u in usages if u.usage == "filter"}
    assert flt == {("a.shared_col", "ambiguous"), ("b.shared_col", "ambiguous")}


# ---------------------------------------------------------------------------
# Compile-time Jinja (queries that pick the SQL body)
# ---------------------------------------------------------------------------

RAW_WITH_LOAD_FLAG = """
{%- set load_flg = pkg.get_column_values(ref('LOAD_CONFIG')|lower,
                                         column = 'load_flg',
                                         order_by = 'MAX(created_ts) DESC',
                                         max_records = 1,
                                         default = '') -%}
{% if load_flg[0] == 'Y' %}
select * from {{ ref('FACTS') }} where run_dt = '{{ var('run_dt') }}'
{% else %}
select * from {{ this }} limit 0
{% endif %}
"""


def test_compile_time_query_is_reported_with_its_variable():
    usages = extract_compile_time_usages(RAW_WITH_LOAD_FLAG)
    rows = {(u.source, u.detail, tuple(u.dbt_vars), u.controls_branch) for u in usages}
    assert rows == {
        ("load_config.load_flg", "value_column", ("load_flg",), True),
        ("load_config.created_ts", "order_by", ("load_flg",), True),
    }
    assert all(u.usage == "compile_time" and u.scope == "<jinja>" for u in usages)


def test_var_names_and_predicate_tagging():
    assert extract_var_names(RAW_WITH_LOAD_FLAG) == {"run_dt": None}
    usages = _usages("select f.x from facts as f where f.region_cd in ('NORTH', 'SOUTH')")
    tagged = tag_var_predicates(usages, {"regions": ["NORTH", "SOUTH"], "other": "WEST"})
    assert tagged[0].dbt_vars == ["regions"]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_usage_rows_are_deterministic():
    first = [u.model_dump() for u in _usages(LATEST_CONTRACT_SQL)]
    second = [u.model_dump() for u in _usages(LATEST_CONTRACT_SQL)]
    assert first == second
    assert first == sorted(
        first,
        key=lambda r: (
            r["usage"],
            r["source"],
            r["scope"],
            r["detail"] or "",
            r["column_ref"],
            r["predicate_sql"] or "",
            r["resolution"],
        ),
    )


# ---------------------------------------------------------------------------
# API / CLI end-to-end
# ---------------------------------------------------------------------------


def _manifest_project(tmp_path: Path) -> Path:
    def node(name, deps, sql, raw=None):
        return {
            "name": name,
            "unique_id": f"model.pkg.{name}",
            "resource_type": "model",
            "language": "sql",
            "schema": "main",
            "database": "db",
            "columns": {},
            "depends_on": {"nodes": deps},
            "compiled_code": sql,
            "raw_code": raw or sql,
            "original_file_path": f"models/{name}.sql",
            "package_name": "pkg",
        }

    manifest = {
        "metadata": {"adapter_type": "snowflake"},
        "nodes": {
            "model.pkg.contract_sat": node(
                "contract_sat",
                ["source.pkg.raw.contracts"],
                "select c.contract_id, c.valid_from_ts, c.status_cd from raw.contracts as c",
            ),
            "model.pkg.load_config": node(
                "load_config", [], "select 'Y' as load_flg, current_timestamp as created_ts"
            ),
            "model.pkg.billing_base": node(
                "billing_base",
                ["model.pkg.contract_sat", "model.pkg.load_config"],
                LATEST_CONTRACT_SQL.replace("db.vault.contract_sat", "db.main.contract_sat"),
                raw=RAW_WITH_LOAD_FLAG,
            ),
        },
        "sources": {
            "source.pkg.raw.contracts": {
                "name": "contracts",
                "identifier": "contracts",
                "unique_id": "source.pkg.raw.contracts",
                "resource_type": "source",
                "source_name": "raw",
                "schema": "raw",
                "database": "db",
                "columns": {},
            }
        },
        "exposures": {},
    }
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return target / "manifest.json"


def test_get_column_usage_traces_origins_and_compile_time_rows(tmp_path):
    clear_lineage_caches()
    manifest = _manifest_project(tmp_path)
    reader = ManifestCatalogReader(manifest_path=str(manifest))
    reader.load()
    rows = get_column_usage(
        manifest_path=str(manifest),
        models=["billing_base"],
        _catalog_reader_override=reader,
    )
    by_key = {(r.usage, r.source_model, r.source_column): r for r in rows}
    qualify = by_key[("qualify", "contract_sat", "valid_from_ts")]
    assert qualify.scope == "contracts_latest"
    assert qualify.origins == [("contracts", "valid_from_ts")]
    compile_time = by_key[("compile_time", "load_config", "created_ts")]
    assert compile_time.controls_branch and compile_time.dbt_vars == ["load_flg"]
    assert compile_time.source_unique_id == "model.pkg.load_config"
    clear_lineage_caches()


def test_parse_cll_writes_usages_and_is_idempotent(tmp_path):
    clear_lineage_caches()
    manifest = _manifest_project(tmp_path)
    args = ["parse-cll", "-s", "billing_base", "--project-dir", str(tmp_path)]
    assert CliRunner().invoke(cli, args).exit_code == 0
    first = (manifest.parent / "cll-result.json").read_bytes()
    clear_lineage_caches()
    assert CliRunner().invoke(cli, args).exit_code == 0
    second = (manifest.parent / "cll-result.json").read_bytes()
    assert first == second
    payload = json.loads(first)
    assert payload["schema_version"] == 3
    usages = {(u["usage"], u["source_model"], u["source_column"]) for u in payload["usages"]}
    assert ("qualify", "contract_sat", "valid_from_ts") in usages
    assert ("filter", "contract_sat", "status_cd") in usages
    assert ("compile_time", "load_config", "load_flg") in usages
    clear_lineage_caches()


def test_empty_type_length_cast_parses():
    parser = SQLColumnParser(dialect="snowflake")
    result = parser.parse_column_lineage(
        "select coalesce(t.a, t.b::VARCHAR(), 'N') as c from t where t.d::NUMBER() > 0"
    )
    assert result.column_lineage["c"][0].source_columns == {"t.a", "t.b"}
    assert {u.source for u in result.column_usages} == {"t.d"}


def test_cte_named_like_its_base_table_resolves_to_the_base_table():
    sql = """
    with agent_pit as (
        select pit.agent_id from dc.vault.agent_pit as pit
        qualify row_number() over (partition by pit.agent_id order by pit.loaded_ts desc) = 1
    )
    select a.agent_id from agent_pit as a
    """
    usages = _usages(sql)
    rows = {(u.source, u.resolution) for u in usages if u.usage == "qualify"}
    assert rows == {("agent_pit.agent_id", "qualified"), ("agent_pit.loaded_ts", "qualified")}


def test_compile_time_column_expression_reports_its_columns():
    from dbt_osmosis_cll.cll_generator.parser.jinja_usage import extract_compile_time_usages

    raw = (
        "{%- set ts = get_column_values(ref('job_ref'), column='started_ts::VARCHAR', "
        "order_by='MAX(created_ts) DESC') -%}\n"
        "{%- set ids = get_column_values(ref('job_ref'), column=\"COALESCE(id_list, '0')\") -%}\n"
        "select 1"
    )
    usages = extract_compile_time_usages(raw, ["get_column_values"])
    values = sorted(u.source for u in usages if u.detail == "value_column")
    assert values == ["job_ref.id_list", "job_ref.started_ts"]
