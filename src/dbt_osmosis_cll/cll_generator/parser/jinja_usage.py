"""Column usage that lives in a model's Jinja, not in its compiled SQL.

Some models query the warehouse *at compile time* and branch on the answer::

    {%- set load_flg = get_column_values(ref('LOAD_CONFIG'), column='load_flg',
                                         order_by='MAX(created_ts) DESC') -%}
    {% if load_flg[0] == 'Y' %} ...real model... {% else %} ...dummy... {% endif %}

The compiled SQL never mentions ``LOAD_CONFIG``, yet the model cannot be built without
``load_flg`` and ``created_ts``. This module reads the raw model code and reports such
calls as ``compile_time`` usages, plus the ``var()`` names the model reads.

Detection is static (Jinja AST, nothing is rendered) and limited to macros whose
signature is known — ``get_column_values`` in any namespace (dbt_utils, project
packages), which takes ``(table, column, order_by, max_records, default, where)``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from sqlglot import exp, parse_one

from dbt_osmosis_cll.cll_generator.models.schema import ColumnUsage

logger = logging.getLogger(__name__)

JINJA_SCOPE = "<jinja>"

# Introspective macro name → positional parameter names (dbt_utils signature).
_VALUE_MACRO_PARAMS: tuple[str, ...] = (
    "table",
    "column",
    "order_by",
    "max_records",
    "default",
    "where",
)
"""Positional parameters shared by get_column_values-style macros (dbt_utils signature)."""
DEFAULT_COMPILE_TIME_MACROS: tuple[str, ...] = ("get_column_values",)


def _dotted_name(node) -> str | None:
    from jinja2 import nodes

    if isinstance(node, nodes.Name):
        return node.name
    if isinstance(node, nodes.Getattr):
        base = _dotted_name(node.node)
        return f"{base}.{node.attr}" if base else node.attr
    return None


def _const(node) -> object | None:
    from jinja2 import nodes

    if isinstance(node, nodes.Const):
        return node.value
    return None


def _relation_of(node) -> str | None:
    """``ref('X')`` / ``source('s', 't')`` (optionally piped through filters) → name."""
    from jinja2 import nodes

    while isinstance(node, nodes.Filter):
        node = node.node
    if not isinstance(node, nodes.Call):
        return None
    name = _dotted_name(node.node)
    args = [_const(a) for a in node.args]
    if name == "ref" and args and isinstance(args[-1], str):
        return args[-1]
    if name == "source" and len(args) >= 2 and isinstance(args[1], str):
        return args[1]
    return None


def _call_args(call, params: tuple[str, ...]) -> dict[str, object]:
    bound: dict[str, object] = {}
    for param, arg in zip(params, call.args):
        bound[param] = arg
    for kw in call.kwargs:
        bound[kw.key] = kw.value
    return bound


def _sql_columns(fragment: str, clause: str) -> list[str]:
    """Column names referenced by an ORDER BY / WHERE fragment (best effort)."""
    sql = f"SELECT 1 FROM t {clause} {fragment}"
    try:
        tree = parse_one(sql)
    except Exception:  # noqa: BLE001 — malformed fragment: report nothing rather than fail
        return []
    return sorted({c.name.lower() for c in tree.find_all(exp.Column) if c.name})


def _names_in(node) -> set[str]:
    from jinja2 import nodes

    return {n.name for n in node.find_all(nodes.Name)}


def _assign_targets(template, macros: set[str]) -> list[tuple[str | None, object]]:
    """(target variable or None, call node) for every introspective macro call."""
    from jinja2 import nodes

    found: list[tuple[str | None, object]] = []
    claimed: set[int] = set()
    for assign in template.find_all(nodes.Assign):
        target = assign.target.name if isinstance(assign.target, nodes.Name) else None
        calls = [assign.node] if isinstance(assign.node, nodes.Call) else []
        calls += list(assign.node.find_all(nodes.Call))
        for call in calls:
            short = (_dotted_name(call.node) or "").split(".")[-1]
            if short in macros and id(call) not in claimed:
                claimed.add(id(call))
                found.append((target, call))
    for call in template.find_all(nodes.Call):
        short = (_dotted_name(call.node) or "").split(".")[-1]
        if short in macros and id(call) not in claimed:
            claimed.add(id(call))
            found.append((None, call))
    return found


def _parse_template(raw_code: str):
    from jinja2 import Environment

    env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
    return env.parse(raw_code)


def extract_compile_time_usages(
    raw_code: str, macros: Iterable[str] | None = None
) -> list[ColumnUsage]:
    """``compile_time`` usages for introspective macro calls in *raw_code*."""
    names = {m.split(".")[-1] for m in (macros or DEFAULT_COMPILE_TIME_MACROS)}
    if not raw_code or not any(n in raw_code for n in names):
        return []
    try:
        template = _parse_template(raw_code)
    except Exception as e:  # noqa: BLE001 — unparseable Jinja: no rows, never fail lineage
        logger.debug("Jinja parse failed for compile-time usage scan: %s", e)
        return []

    from jinja2 import nodes

    branch_names: set[str] = set()
    for if_node in template.find_all(nodes.If):
        branch_names |= _names_in(if_node.test)

    rows: dict[tuple, ColumnUsage] = {}
    for target, call in _assign_targets(template, names):
        macro = _dotted_name(call.node) or "get_column_values"
        args = _call_args(call, _VALUE_MACRO_PARAMS)
        relation = _relation_of(args.get("table"))
        column = _const(args.get("column")) if args.get("column") is not None else None
        if not relation or not isinstance(column, str):
            continue
        relation = relation.lower()
        order_by = _const(args.get("order_by")) if args.get("order_by") is not None else None
        where = _const(args.get("where")) if args.get("where") is not None else None
        call_sql = f"{macro}(ref('{relation}'), column='{column}'"
        if isinstance(order_by, str):
            call_sql += f", order_by='{order_by}'"
        if isinstance(where, str):
            call_sql += f", where='{where}'"
        call_sql += ")"
        dbt_vars = [target] if target else []
        controls = bool(target and target in branch_names)

        # The column argument is a SQL expression in general (`x::VARCHAR`,
        # `COALESCE(x, '0')`): report the columns it reads.
        value_cols = _sql_columns(column, "ORDER BY") or [column.lower()]
        entries: list[tuple[str, str]] = [(c, "value_column") for c in value_cols]
        if isinstance(order_by, str):
            entries += [(c, "order_by") for c in _sql_columns(order_by, "ORDER BY")]
        if isinstance(where, str):
            entries += [(c, "where") for c in _sql_columns(where, "WHERE")]
        for col, detail in entries:
            row = ColumnUsage(
                usage="compile_time",
                source=f"{relation}.{col}",
                column_ref=col,
                scope=JINJA_SCOPE,
                detail=detail,
                predicate_sql=call_sql,
                resolution="jinja",
                dbt_vars=dbt_vars,
                controls_branch=controls,
            )
            rows.setdefault(row.sort_key(), row)
    return sorted(rows.values(), key=lambda r: r.sort_key())


def extract_var_names(raw_code: str) -> dict[str, object | None]:
    """``{var name: literal default or None}`` for every ``var()`` call in *raw_code*."""
    if not raw_code or "var(" not in raw_code:
        return {}
    try:
        template = _parse_template(raw_code)
    except Exception:  # noqa: BLE001
        return {}
    from jinja2 import nodes

    found: dict[str, object | None] = {}
    for call in template.find_all(nodes.Call):
        if _dotted_name(call.node) != "var" or not call.args:
            continue
        name = _const(call.args[0])
        if not isinstance(name, str):
            continue
        default = _const(call.args[1]) if len(call.args) > 1 else None
        found.setdefault(name, default)
    return dict(sorted(found.items()))


def tag_var_predicates(
    usages: Iterable[ColumnUsage], var_values: dict[str, object]
) -> list[ColumnUsage]:
    """Attach ``var()`` names whose rendered value appears as a literal in a predicate.

    Compiled SQL carries rendered values only; a predicate ``x IN ('A', 'B')`` built from
    ``var('codes')`` is recognised by matching the literals against the known value.
    """
    out = []
    for usage in usages:
        if usage.predicate_sql and usage.usage != "compile_time" and var_values:
            literals = _predicate_literals(usage.predicate_sql)
            hits = [
                name
                for name, value in var_values.items()
                if value is not None
                and _value_literals(value)
                and _value_literals(value) <= literals
            ]
            if hits:
                usage = usage.model_copy(
                    update={"dbt_vars": sorted(set(usage.dbt_vars) | set(hits))}
                )
        out.append(usage)
    return out


def _predicate_literals(predicate_sql: str) -> set[str]:
    try:
        tree = parse_one(f"SELECT 1 WHERE {predicate_sql}")
    except Exception:  # noqa: BLE001
        return set()
    return {str(lit.this) for lit in tree.find_all(exp.Literal)}


def _value_literals(value: object) -> set[str]:
    """String literals a var value renders to. Numbers and one-character strings are
    skipped: matching ``-1`` or ``'Y'`` against arbitrary predicates is pure noise."""
    items = value if isinstance(value, (list, tuple, set)) else [value]
    if not all(isinstance(v, str) for v in items):
        return set()
    return {v for v in items if len(v) > 1}
