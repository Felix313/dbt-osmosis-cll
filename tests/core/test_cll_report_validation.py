"""``lineage validate``: requirements checked by binding, against a real local warehouse."""

from __future__ import annotations

import pytest
from sqlglot import parse_one

from dbt_osmosis_cll.cll_generator.report.analysis import analyse, build_graph
from dbt_osmosis_cll.cll_generator.report.validation import (
    FAILED,
    NO_ACCESS,
    NOT_TESTABLE,
    PASSED,
    reduce_projections,
    render_markdown,
    validate,
)

duckdb = pytest.importorskip("duckdb")


def _row(model, column, progenitors=(), ttype="direct"):
    return {
        "model": model,
        "column": column,
        "transformation_type": ttype,
        "progenitors": [list(p) for p in progenitors],
        "in_sql": True,
    }


def _usage(model, usage, source_model, source_column):
    return {
        "model": model,
        "usage": usage,
        "source_model": source_model,
        "source_column": source_column,
        "scope": "main",
        "predicate_sql": "",
        "is_self_reference": False,
    }


def _node(name, parents, schema="sch"):
    return {
        "resource_type": "model",
        "name": name,
        "alias": name,
        "schema": schema,
        "original_file_path": f"models/{name}.sql",
        "config": {"materialized": "table"},
        "columns": {},
        "depends_on": {"nodes": [f"model.p.{d}" for d in parents]},
    }


MANIFEST = {
    "nodes": {
        "model.p.base": _node("base", []),
        "model.p.mid": _node("mid", ["base"]),
        "model.p.star_mid": _node("star_mid", ["base"]),
        "model.p.dp": _node("dp", ["mid", "star_mid"]),
    },
    "sources": {},
}
SQL = {
    "base": "SELECT 1 AS a, 2 AS b, 3 AS unused",
    "mid": "SELECT b.a FROM sch.base AS b WHERE b.b = 1",
    "star_mid": "SELECT * FROM sch.base",
    "dp": "SELECT m.a, s.a AS sa FROM sch.mid AS m JOIN sch.star_mid AS s ON s.a = m.a",
}
CLL = {
    "results": [
        _row("base", "a", ttype="literal"),
        _row("base", "b", ttype="literal"),
        _row("base", "unused", ttype="literal"),
        _row("mid", "a", [("base", "a")]),
        _row("star_mid", "a", [("base", "a")]),
        _row("star_mid", "b", [("base", "b")]),
        _row("star_mid", "unused", [("base", "unused")]),
        _row("dp", "a", [("mid", "a")]),
        _row("dp", "sa", [("star_mid", "a")]),
    ],
    "usages": [_usage("mid", "filter", "base", "b"), _usage("dp", "join_key", "star_mid", "a")],
    "coverage": {},
}


@pytest.fixture
def warehouse():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA sch")
    con.execute("CREATE TABLE sch.base AS SELECT 1 AS a, 2 AS b, 3 AS unused")
    con.execute("CREATE TABLE sch.mid AS SELECT 1 AS a")
    con.execute("CREATE TABLE sch.star_mid AS SELECT 1 AS a, 2 AS b, 3 AS unused")
    yield con
    con.close()


def _run(con):
    def run(sql):
        return [d[0] for d in con.execute(sql).description]

    return run


def _validate(cll, run, sql=SQL):
    graph = build_graph(cll, MANIFEST, ["dp"])
    result = analyse(graph, ["dp"])
    out = validate(graph, result, MANIFEST, lambda n: sql.get(n["name"]), run, "duckdb")
    return {c.model: c for c in out.checks}, out


def test_correct_requirements_are_confirmed_including_a_star_reader(warehouse):
    checks, out = _validate(CLL, _run(warehouse))
    assert {c.status for c in checks.values()} == {PASSED}
    # `unused` is droppable; `star_mid` reads base with SELECT * and loses it legitimately.
    assert set(checks) == {"base", "mid", "star_mid", "dp"}
    assert out.columns_checked > 0


def test_a_missing_requirement_is_found(warehouse):
    wrong = dict(CLL, usages=[])  # the filter on base.b is not known to the requirements
    checks, _ = _validate(wrong, _run(warehouse))
    assert checks["mid"].status == FAILED
    assert '"b"' in checks["mid"].detail or "b" in checks["mid"].detail


def test_required_output_lost_through_a_star_is_found(warehouse):
    # dp filters on star_mid.b, whose input is unknown to the lineage, and nothing else
    # needs base.b: reducing base removes it from what star_mid outputs.
    results = [r for r in CLL["results"] if (r["model"], r["column"]) != ("star_mid", "b")]
    cll = dict(
        CLL,
        results=[*results, _row("star_mid", "b")],
        usages=[_usage("dp", "join_key", "star_mid", "a"), _usage("dp", "filter", "star_mid", "b")],
    )
    sql = dict(SQL, mid="SELECT b.a FROM sch.base AS b")
    checks, _ = _validate(cll, _run(warehouse), sql)
    assert checks["star_mid"].status == FAILED
    assert "['b']" in checks["star_mid"].detail


def test_own_droppable_outputs_do_not_count_against_the_requirements(warehouse):
    # `extra` is an output nobody needs, through a CTE, reading a droppable upstream column.
    sql = dict(
        SQL,
        mid=(
            "WITH c AS (SELECT b.a, b.b, b.unused AS extra FROM sch.base AS b), "
            "d AS (SELECT * FROM c) "
            "SELECT d.a, d.extra FROM d WHERE d.b = 1"
        ),
    )
    cll = dict(CLL, results=[*CLL["results"], _row("mid", "extra", [("base", "unused")])])
    checks, _ = _validate(cll, _run(warehouse), sql)
    assert checks["mid"].status == PASSED, checks["mid"].detail


def test_projections_that_shape_rows_are_kept(warehouse):
    # DISTINCT over a column classified as droppable: removing it would change the rows.
    sql = dict(SQL, mid="SELECT DISTINCT b.a, b.unused AS extra FROM sch.base AS b WHERE b.b = 1")
    cll = dict(CLL, results=[*CLL["results"], _row("mid", "extra", [("base", "unused")])])
    checks, _ = _validate(cll, _run(warehouse), sql)
    assert checks["mid"].status == FAILED
    assert "unused" in checks["mid"].detail


def test_union_all_is_reduced_by_position(warehouse):
    sql = dict(
        SQL,
        mid=(
            "SELECT b.a, b.unused AS extra FROM sch.base AS b WHERE b.b = 1 "
            "UNION ALL SELECT b.a, b.unused FROM sch.base AS b"
        ),
    )
    cll = dict(CLL, results=[*CLL["results"], _row("mid", "extra", [("base", "unused")])])
    checks, _ = _validate(cll, _run(warehouse), sql)
    assert checks["mid"].status == PASSED, checks["mid"].detail


def test_union_branch_keeps_a_lateral_alias_it_reads(warehouse):
    sql = dict(
        SQL,
        mid=(
            "SELECT b.a AS spec, spec + 1 AS a FROM sch.base AS b WHERE b.b = 1 "
            "UNION ALL SELECT b.a AS spec, spec + 2 AS a FROM sch.base AS b"
        ),
    )
    checks, _ = _validate(CLL, _run(warehouse), sql)
    assert checks["mid"].status == PASSED, checks["mid"].detail


def test_lateral_alias_read_in_a_join_condition_stays():
    tree = parse_one(
        "SELECT b.a, b.a + 0 AS helper, b.x AS unused FROM sch.base AS b "
        "JOIN sch.other AS c ON c.a = helper",
        dialect="snowflake",
    )
    reduce_projections(tree, {"a"})
    assert [p.alias_or_name for p in tree.selects] == ["a", "helper"]


def test_aggregates_go_but_grouping_columns_stay():
    tree = parse_one(
        "SELECT t.k, t.j, MAX(t.v) AS m, MIN(t.w) AS n FROM sch.t GROUP BY 1, 2",
        dialect="snowflake",
    )
    reduce_projections(tree, {"k", "m"})
    assert tree.sql(dialect="snowflake") == (
        "SELECT t.k, MAX(t.v) AS m FROM sch.t GROUP BY t.k, t.j"
    )
    group_all = parse_one(
        "SELECT t.k, t.j, MAX(t.v) AS m, MIN(t.w) AS n FROM sch.t GROUP BY ALL", dialect="snowflake"
    )
    reduce_projections(group_all, {"k", "m"})
    assert [p.alias_or_name for p in group_all.selects] == ["k", "j", "m"]


def test_a_cte_nobody_reads_is_dropped():
    tree = parse_one(
        "WITH dead AS (SELECT DISTINCT d.x FROM sch.d AS d), live AS (SELECT l.a FROM sch.l AS l) "
        "SELECT live.a FROM live",
        dialect="snowflake",
    )
    reduce_projections(tree, {"a"})
    sql = tree.sql(dialect="snowflake")
    assert "dead" not in sql and "live" in sql


def test_a_cte_read_without_a_named_column_stays():
    tree = parse_one(
        "WITH used AS (SELECT d.x FROM sch.d AS d) "
        "SELECT t.a FROM sch.t AS t WHERE EXISTS (SELECT 1 FROM used)",
        dialect="snowflake",
    )
    reduce_projections(tree, {"a"})
    assert "WITH used AS" in tree.sql(dialect="snowflake")


def test_a_same_named_column_is_no_lateral_alias():
    tree = parse_one(
        "SELECT t.a, t.b AS b, t.c AS x FROM sch.t WHERE b = 1 AND x = 2", dialect="snowflake"
    )
    reduce_projections(tree, {"a"})
    assert [p.alias_or_name for p in tree.selects] == ["a", "x"]


def test_positional_group_by_is_not_shifted_by_the_reduction(warehouse):
    sql = dict(SQL, mid="SELECT 1 AS k, b.a FROM sch.base AS b WHERE b.b = 1 GROUP BY 1, 2")
    cll = dict(CLL, results=[*CLL["results"], _row("mid", "k", ttype="literal")])
    checks, _ = _validate(cll, _run(warehouse), sql)
    assert checks["mid"].status == PASSED, checks["mid"].detail


def test_schema_qualified_column_and_column_renaming_alias(warehouse):
    sql = dict(
        SQL,
        mid="SELECT sch.base.a FROM sch.base WHERE sch.base.b = 1",
        star_mid="SELECT d.x AS a FROM sch.base AS d (x, y, z)",
    )
    cll = dict(
        CLL,
        results=[
            r for r in CLL["results"] if not (r["model"] == "star_mid" and r["column"] != "a")
        ],
    )
    checks, _ = _validate(cll, _run(warehouse), sql)
    assert checks["mid"].status == PASSED, checks["mid"].detail
    assert checks["star_mid"].status == PASSED, checks["star_mid"].detail


def test_stub_column_types_are_retried_when_a_function_rejects_null():
    calls = []

    def run(sql):
        calls.append(sql)
        if "CAST(NULL AS" not in sql:
            raise RuntimeError("Function EXTRACT does not support NULL argument type")
        return ["a"]

    graph = build_graph(CLL, MANIFEST, ["dp"])
    out = validate(
        graph,
        analyse(graph, ["dp"]),
        MANIFEST,
        lambda n: SQL.get(n["name"]),
        run,
        "duckdb",
        visible=set(),
    )
    checks = {c.model: c for c in out.checks}
    assert checks["mid"].status == PASSED, checks["mid"].detail
    assert any("VARCHAR" in c or "TEXT" in c for c in calls)


def test_stub_offers_no_star_and_quotes_special_names():
    con = duckdb.connect()
    sql = dict(SQL, star_mid="""SELECT s.*, s."/BIC/X" AS odd FROM sch.base AS s""")
    graph = build_graph(CLL, MANIFEST, ["dp"])
    out = validate(
        graph,
        analyse(graph, ["dp"]),
        MANIFEST,
        lambda n: sql.get(n["name"]),
        _run(con),
        "duckdb",
        visible=set(),
    )
    check = {c.model: c for c in out.checks}["star_mid"]
    assert check.status in (PASSED, FAILED), check.detail
    assert "does not bind: " not in check.detail or "original" not in check.detail
    con.close()


def test_relations_read_through_an_ephemeral_model_are_upstream():
    from dbt_osmosis_cll.cll_generator.report.validation import _parents

    manifest = {
        "nodes": {
            "model.p.eph": {
                **_node("eph", []),
                "config": {"materialized": "ephemeral"},
                "depends_on": {"nodes": ["source.p.mig.raw_t"]},
            },
            "model.p.reader": _node("reader", ["eph"]),
        },
        "sources": {
            "source.p.mig.raw_t": {
                "name": "raw_t",
                "identifier": "raw_t",
                "schema": "mig",
                "source_name": "MIG",
                "columns": {},
            }
        },
    }
    parents = _parents(manifest["nodes"]["model.p.reader"], manifest, {})
    assert [(p["schema"], p["identifier"]) for p in parents] == [("mig", "raw_t")]


def test_relations_the_role_cannot_see_are_checked_against_a_stub():
    con = duckdb.connect()  # `sch` does not exist: nothing is visible
    graph = build_graph(CLL, MANIFEST, ["dp"])
    result = analyse(graph, ["dp"])
    out = validate(
        graph, result, MANIFEST, lambda n: SQL.get(n["name"]), _run(con), "duckdb", visible=set()
    )
    checks = {c.model: c for c in out.checks}
    assert checks["mid"].status == PASSED, checks["mid"].detail
    assert checks["mid"].stubbed == ["base"]
    assert out.stubbed_relations()["base"] == ["mid", "star_mid"]
    text = render_markdown(out, "de")
    assert "Die Rolle sieht BASE nicht" in text and "Leserecht" in text
    assert "The role cannot see BASE" in render_markdown(out, "en")

    # The stub offers required columns only: a missing requirement is still found.
    wrong = dict(CLL, usages=[])
    graph = build_graph(wrong, MANIFEST, ["dp"])
    out = validate(
        graph,
        analyse(graph, ["dp"]),
        MANIFEST,
        lambda n: SQL.get(n["name"]),
        _run(con),
        "duckdb",
        visible=set(),
    )
    assert {c.model: c for c in out.checks}["mid"].status == FAILED
    con.close()


def test_missing_access_names_the_relation_and_the_reason():
    def run(sql):
        raise RuntimeError(
            "002003 (42S02): SQL compilation error:\n"
            "Object 'EDW.SCH.BASE' does not exist or not authorized."
        )

    checks, out = _validate(CLL, run)
    assert checks["mid"].status == NO_ACCESS
    assert checks["mid"].relation == "EDW.SCH.BASE"
    assert "EDW.SCH.BASE" in out.blocked_relations()
    text = render_markdown(out, "de")
    assert "Ohne Zugriff auf EDW.SCH.BASE" in text and "SELECT" in text
    assert "Without access to EDW.SCH.BASE" in render_markdown(out, "en")


def test_sql_that_does_not_bind_as_compiled_is_not_blamed_on_the_requirements(warehouse):
    sql = dict(SQL, mid="SELECT b.not_there FROM sch.base AS b")
    checks, _ = _validate(CLL, _run(warehouse), sql)
    assert checks["mid"].status == NOT_TESTABLE
    assert "original does not bind" in checks["mid"].detail
    no_sql = dict(SQL)
    del no_sql["mid"]
    checks, _ = _validate(CLL, _run(warehouse), no_sql)
    assert checks["mid"].status == NOT_TESTABLE
