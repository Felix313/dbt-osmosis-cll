"""Data-product requirement analysis over a ``parse-cll`` result.

Models declared as *data products* are lineage anchors: tracing a column upstream
stops at another data product, which is then the column's origin. From the
one-hop lineage (``results[].progenitors``) and the column usages
(``usages[]``: join / filter / qualify / window / group-by / compile-time) this
module derives

- which columns every model must keep so that each data product can be rebuilt
  with all of its columns (a data product needs every column; an interim model only
  what some data product reaches),
- the dependencies between data products and the order to build them in (waves),
- the anchored origins of every data-product column.

The same algorithm runs in the HTML report (``static/report.js``) so data products
can be toggled interactively; this module is the reference and feeds the CSV /
Markdown outputs. Keep both in step — ``tests/core/test_cll_report.py`` compares them.

Requirement rules
-----------------
A column is required with category ``data`` when its values flow into a data
product's column, and ``logic`` when it is only needed to compute a predicate
(join keys, filters, QUALIFY, window partitions, GROUP BY, compile-time queries).
Every upstream input of a required column is required with the same category.
Once a model is needed at all, every column usage inside it is required as
``logic``: its joins and filters shape the rows of whatever it delivers. This is
deliberately conservative — a usage in a CTE that feeds none of the needed columns
still counts — so "droppable" never removes something a data product reads.
Self-references (an incremental model reading its own table) are ignored.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

DATA = "data"
LOGIC = "logic"
_CONSTANT_TYPES = ("literal", "generated")


@dataclass
class Graph:
    """The lineage graph a report is built on (also embedded into the HTML)."""

    models: dict[str, dict[str, Any]]
    """model name (lowercase) → metadata: kind, layer, path, materialized, analysed,
    columns (documented columns for relations outside the analysed scope)."""
    rows: list[list[Any]]
    """``[model, column, transformation_type, [[model, column], ...]]`` per output column."""
    usages: list[list[Any]]
    """``[model, usage, source_model, source_column, scope, predicate_sql]``."""
    deps: dict[str, list[str]]
    """model → direct parent models / sources (manifest ``depends_on``)."""
    gaps: list[list[str]] = field(default_factory=list)
    """``[model, column, kind]`` for columns whose lineage is not fully known:

    - ``unresolved``: the model's SQL could not be traced for it (no compiled SQL,
      ``SELECT *`` over a relation without known columns) — a real coverage gap;
    - ``no_input``: parsed, but computed without reading any column value
      (``COUNT(*)``, ``ROW_NUMBER()``, constants carried through CTEs) — complete,
      its row-shaping inputs are column usages.
    """
    documented_only: list[list[str]] = field(default_factory=list)
    """``[model, column]`` documented in YAML but not produced by the model's SQL.
    Not a column of the model; excluded from the analysis."""
    coverage: dict[str, Any] = field(default_factory=dict)
    selectors: list[str] = field(default_factory=list)
    data_products: list[str] = field(default_factory=list)
    """Data products preselected from configuration / CLI."""

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "models": self.models,
            "rows": self.rows,
            "usages": self.usages,
            "deps": self.deps,
            "gaps": self.gaps,
            "documented_only": self.documented_only,
            "coverage": self.coverage,
            "selectors": self.selectors,
            "data_products": self.data_products,
        }


def _in_sql(row: dict[str, Any]) -> bool:
    """Whether the parsed SQL produced lineage for the row (see ``in_sql`` in api.py).

    Results written before the flag existed are recognised by the signature of a
    column without lineage: ``direct`` with no input, expression or branch.
    """
    if "in_sql" in row:
        return bool(row["in_sql"])
    return bool(
        row.get("progenitors")
        or row.get("progenitor_model")
        or row.get("sql_expression")
        or row.get("union_branches")
        or (row.get("transformation_type") or "direct") != "direct"
    )


def _layer(path: str) -> str:
    parts = [p for p in path.replace("\\", "/").split("/") if p]
    return parts[1] if len(parts) > 2 else ""


def build_graph(
    cll: dict[str, Any], manifest: dict[str, Any], data_products: list[str] | None = None
) -> Graph:
    """Join a ``parse-cll`` result with manifest metadata into a :class:`Graph`."""
    uid_name: dict[str, str] = {}
    models: dict[str, dict[str, Any]] = {}
    for uid, node in (manifest.get("nodes") or {}).items():
        if node.get("resource_type") not in ("model", "seed", "snapshot"):
            continue
        name = str(node.get("name") or "").lower()
        uid_name[uid] = name
        path = str(node.get("original_file_path") or "").replace("\\", "/")
        models[name] = {
            "kind": node["resource_type"],
            "layer": _layer(path),
            "path": path,
            "materialized": (node.get("config") or {}).get("materialized"),
            "analysed": False,
            "columns": [str(c).lower() for c in (node.get("columns") or {})],
        }
    for uid, src in (manifest.get("sources") or {}).items():
        name = str(src.get("identifier") or src.get("name") or "").lower()
        uid_name[uid] = name
        models.setdefault(
            name,
            {
                "kind": "source",
                "layer": "source",
                "path": str(src.get("original_file_path") or "").replace("\\", "/"),
                "source_name": src.get("source_name"),
                "materialized": None,
                "analysed": False,
                "columns": [str(c).lower() for c in (src.get("columns") or {})],
            },
        )

    results = cll.get("results") or []
    traced = {str(r["model"]).lower() for r in results if _in_sql(r)}
    star_models = {
        str(m).lower() for m, *_ in (cll.get("coverage") or {}).get("star_over_unknown") or []
    }
    rows: list[list[Any]] = []
    gaps: list[list[str]] = []
    documented_only: list[list[str]] = []
    out_cols: dict[str, list[str]] = defaultdict(list)
    for r in results:
        model, col = str(r["model"]).lower(), str(r["column"]).lower()
        progs = [[str(m).lower(), str(c).lower()] for m, c in (r.get("progenitors") or [])]
        ttype = r.get("transformation_type") or "direct"
        if not _in_sql(r):
            if model in traced and model not in star_models:
                documented_only.append([model, col])
                continue
            gaps.append([model, col, "unresolved"])
        elif not progs and ttype not in _CONSTANT_TYPES:
            gaps.append([model, col, "no_input"])
        rows.append([model, col, ttype, progs])
        out_cols[model].append(col)
    for model, cols in out_cols.items():
        meta = models.setdefault(
            model,
            {"kind": "model", "layer": "", "path": "", "materialized": None, "columns": []},
        )
        meta["analysed"] = True
        meta["columns"] = cols

    usages: list[list[Any]] = []
    for u in cll.get("usages") or []:
        if u.get("is_self_reference") or not u.get("source_model"):
            continue
        usages.append([
            str(u["model"]).lower(),
            u["usage"],
            str(u["source_model"]).lower(),
            str(u["source_column"]).lower(),
            u.get("scope") or "",
            u.get("predicate_sql") or "",
        ])

    keep = set(out_cols) | {u[2] for u in usages} | {p[0] for r in rows for p in r[3]}
    deps: dict[str, list[str]] = {}
    for uid, node in (manifest.get("nodes") or {}).items():
        name = uid_name.get(uid)
        if name in out_cols:
            parents = (node.get("depends_on") or {}).get("nodes") or []
            deps[name] = sorted({uid_name[p] for p in parents if p in uid_name})
            keep.update(deps[name])

    for name in keep - set(models):
        # Referenced but unknown to the manifest (e.g. a hard-coded table name).
        models[name] = {
            "kind": "relation",
            "layer": "",
            "path": "",
            "materialized": None,
            "analysed": False,
            "columns": [],
        }
    return Graph(
        models={k: v for k, v in sorted(models.items()) if k in keep},
        rows=rows,
        usages=usages,
        deps=deps,
        gaps=gaps,
        documented_only=documented_only,
        coverage=dict(cll.get("coverage") or {}),
        selectors=list(cll.get("selectors") or []),
        data_products=[d.lower() for d in (data_products or [])],
    )


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


@dataclass
class Analysis:
    data_products: list[str]
    """Selected data products that are part of the analysed scope."""
    missing: list[str]
    """Selected data products the lineage graph does not cover."""
    required: dict[tuple[str, str], dict[str, Any]]
    """(model, column) → {categories: {data, logic}, usages: {...}, needed_by: {dp, ...}}"""
    anchors: dict[str, dict[str, dict[tuple[str, str], set[str]]]]
    """dp → upstream dp → {(upstream dp, column): categories}."""
    waves: dict[str, int]
    """dp → build wave (1 = only non-data-product inputs)."""
    cycles: list[str]
    dbt_only_deps: dict[str, list[str]]
    """dp → data products it depends on in the dbt DAG without reading any column."""
    origins: dict[str, dict[str, list[list[str]]]]
    """dp → column → ``[model, column, kind]`` with kind ``data_product`` / ``source``."""
    dangling: list[list[str]] = field(default_factory=list)
    """``[model, column]`` read downstream but not among the analysed model's outputs."""


def analyse(graph: Graph, data_products: list[str]) -> Analysis:
    rows = {(r[0], r[1]): r for r in graph.rows}
    cols_of: dict[str, list[str]] = defaultdict(list)
    for r in graph.rows:
        cols_of[r[0]].append(r[1])
    usages_of: dict[str, list[list[Any]]] = defaultdict(list)
    for u in graph.usages:
        usages_of[u[0]].append(u)

    selected = list(dict.fromkeys(d.lower() for d in data_products))
    dps = [d for d in selected if d in cols_of]
    missing = [d for d in selected if d not in cols_of]
    anchor_set = set(selected)

    required: dict[tuple[str, str], dict[str, Any]] = {}
    anchors: dict[str, dict[str, dict[tuple[str, str], set[str]]]] = {}
    dangling: set[tuple[str, str]] = set()
    for dp in dps:
        seen: dict[tuple[str, str], set[str]] = defaultdict(set)
        needed_models: set[str] = set()
        dp_anchors: dict[str, dict[tuple[str, str], set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        stack: list[tuple[tuple[str, str], str]] = []

        def need(model: str, col: str, category: str, usage: str | None = None) -> None:
            if model in anchor_set and model != dp:
                dp_anchors[model][(model, col)].add(category)
                return
            key = (model, col)
            entry = required.setdefault(
                key, {"categories": set(), "usages": set(), "needed_by": set()}
            )
            entry["needed_by"].add(dp)
            if usage:
                entry["usages"].add(usage)
            if category in seen[key]:
                return
            seen[key].add(category)
            entry["categories"].add(category)
            stack.append((key, category))

        for col in cols_of[dp]:
            need(dp, col, DATA)
        while stack:
            (model, col), category = stack.pop()
            if model not in cols_of:
                continue  # source / outside the analysed scope: terminal
            if model not in needed_models:
                needed_models.add(model)
                for u in usages_of[model]:
                    need(u[2], u[3], LOGIC, u[1])
            if (model, col) not in rows:
                dangling.add((model, col))
                continue
            for pm, pc in rows[(model, col)][3]:
                if pm != model or pc != col:
                    need(pm, pc, category)
        anchors[dp] = {k: dict(v) for k, v in dp_anchors.items()}

    # Build waves over column-level dependencies (longest path).
    waves: dict[str, int] = {}
    cycles: list[str] = []
    state: dict[str, int] = {}

    def wave(dp: str) -> int:
        if dp in waves:
            return waves[dp]
        if state.get(dp) == 1:
            cycles.append(dp)
            return 0
        state[dp] = 1
        ups = [u for u in anchors.get(dp, {}) if u in anchors]
        waves[dp] = 1 + max((wave(u) for u in ups), default=0)
        state[dp] = 2
        return waves[dp]

    for dp in dps:
        wave(dp)

    dbt_only: dict[str, list[str]] = {}
    for dp in dps:
        reach: set[str] = set()
        todo = list(graph.deps.get(dp, []))
        while todo:
            m = todo.pop()
            if m in reach:
                continue
            reach.add(m)
            if m not in anchor_set:
                todo.extend(graph.deps.get(m, []))
        extra = sorted((reach & anchor_set) - set(anchors[dp]) - {dp})
        if extra:
            dbt_only[dp] = extra

    memo: dict[tuple[str, str], frozenset[tuple[str, str, str]]] = {}

    def resolve(model: str, col: str, trail: frozenset) -> frozenset:
        key = (model, col)
        if model in anchor_set:
            return frozenset({(model, col, "data_product")})
        if key not in rows:
            return frozenset({(model, col, "source")})
        if key in memo:
            return memo[key]
        if key in trail:
            return frozenset()
        out: set[tuple[str, str, str]] = set()
        for pm, pc in rows[key][3]:
            out |= resolve(pm, pc, trail | {key})
        memo[key] = frozenset(out)
        return memo[key]

    origins: dict[str, dict[str, list[list[str]]]] = {}
    for dp in dps:
        per_col: dict[str, list[list[str]]] = {}
        for col in cols_of[dp]:
            found: set[tuple[str, str, str]] = set()
            for pm, pc in rows[(dp, col)][3]:
                if pm != dp:
                    found |= resolve(pm, pc, frozenset({(dp, col)}))
            per_col[col] = [list(o) for o in sorted(found)]
        origins[dp] = per_col

    return Analysis(
        data_products=dps,
        missing=missing,
        required=required,
        anchors=anchors,
        waves=waves,
        cycles=sorted(set(cycles)),
        dbt_only_deps=dbt_only,
        origins=origins,
        dangling=[list(k) for k in sorted(dangling)],
    )


def column_status(graph: Graph, result: Analysis) -> list[dict[str, Any]]:
    """One row per column of every model in scope: role and whether it is needed."""
    dps = set(result.data_products)
    out: list[dict[str, Any]] = []
    for model, meta in graph.models.items():
        if model in dps:
            role = "data_product"
        elif meta.get("analysed"):
            role = "interim"
        else:
            role = "source"
        cols = list(meta.get("columns") or [])
        extra = sorted(c for (m, c) in result.required if m == model and c not in cols)
        for col in cols + extra:
            req = result.required.get((model, col))
            if role == "data_product":
                status = "required"
            elif req:
                status = "required"
            else:
                status = "droppable" if role == "interim" else "unused"
            out.append({
                "model": model,
                "column": col,
                "role": role,
                "layer": meta.get("layer") or "",
                "status": status,
                "category": (
                    DATA
                    if role == "data_product" or (req and DATA in req["categories"])
                    else (LOGIC if req else "")
                ),
                "usages": sorted(req["usages"]) if req else [],
                "needed_by": sorted(req["needed_by"]) if req else [],
            })
    return out
