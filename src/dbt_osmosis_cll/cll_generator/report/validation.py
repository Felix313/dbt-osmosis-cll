"""Check the column requirements against the warehouse; no data row comes back.

``lineage report`` claims which columns each relation must keep. ``lineage validate`` tests
that claim for every needed model: the warehouse binds its compiled SQL, through
``SELECT * FROM (<sql>) WHERE 1 = 0``, once as it is and once reduced:

- the model outputs only what is required of it, and every CTE only what is still used;
- each upstream relation offers only its required columns (data products keep all).

Projections are kept where they shape rows (``DISTINCT``, ``UNION``, ``GROUP BY ALL``). If the
reduced query binds, the model needs no column classified as droppable. If only the reduced
query fails, a required column is missing from the requirements.

Only column names matter for this. A relation the role cannot see is replaced by a stub
(``SELECT NULL AS col, ...``), so the model is still checked; what stays unconfirmed is that
the column really exists there with a compatible type, which is reported with the reason.
Nothing is created. The warehouse may evaluate scalar subqueries while binding.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlglot import exp, parse_one

from dbt_osmosis_cll.cll_generator.artifacts.registry import _extract_source_select
from dbt_osmosis_cll.cll_generator.parser.sql_parser import (
    _EMPTY_TYPE_PARAMS_RE,
    _hoist_derived_tables,
    _with_of,
)
from dbt_osmosis_cll.cll_generator.parser.sql_parser_utils import UNKNOWN_COLUMNS_MARKER
from dbt_osmosis_cll.cll_generator.report.analysis import (
    Analysis,
    Graph,
    qualified_relations,
)
from dbt_osmosis_cll.config import get_config

PASSED = "passed"
FAILED = "failed"
NO_ACCESS = "no_access"
NOT_TESTABLE = "not_testable"

NO_COLUMNS = "cll_no_columns"
_NO_ACCESS = re.compile(
    r"does not exist or (?:is )?not authorized|insufficient privileges|permission denied"
    r"|unknown user-defined function",
    re.IGNORECASE,
)
_PLAIN_NAME = re.compile(r"[A-Za-z_][\w$]*")
_POSITIONAL_ALIAS = re.compile(r"number of columns produced by .* number of aliases", re.I)
_MISSING_OBJECT = re.compile(
    r"(?:Object|Database|Schema|Table|View)\s+'?\"?([\w$.\"]+)'?\"?\s+does not exist"
    r"|user-defined functions?\s+([\w$.\"]+)",
    re.IGNORECASE,
)


@dataclass
class ModelCheck:
    model: str
    status: str
    detail: str = ""
    relation: str = ""
    stubbed: list[str] = field(default_factory=list)
    """Upstream relations the role cannot see, replaced by a stub for this check."""


@dataclass
class ValidationResult:
    checks: list[ModelCheck] = field(default_factory=list)
    columns_checked: int = 0

    def by_status(self, status: str) -> list[ModelCheck]:
        return [c for c in self.checks if c.status == status]

    def blocked_relations(self) -> dict[str, list[str]]:
        """Object the role cannot use → models that could not be checked because of it."""
        out: dict[str, list[str]] = defaultdict(list)
        for check in self.by_status(NO_ACCESS):
            out[check.relation or "?"].append(check.model)
        return {k: sorted(v) for k, v in sorted(out.items())}

    def stubbed_relations(self) -> dict[str, list[str]]:
        """Relation replaced by a stub → models checked against that stub."""
        out: dict[str, list[str]] = defaultdict(list)
        for check in self.checks:
            for relation in check.stubbed:
                out[relation].append(check.model)
        return {k: sorted(v) for k, v in sorted(out.items())}

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 2,
            "summary": {
                status: len(self.by_status(status))
                for status in (PASSED, FAILED, NO_ACCESS, NOT_TESTABLE)
            },
            "confirmed_against_warehouse": sum(1 for c in self.by_status(PASSED) if not c.stubbed),
            "columns_checked": self.columns_checked,
            "blocked_relations": self.blocked_relations(),
            "stubbed_relations": self.stubbed_relations(),
            "checks": [c.__dict__ for c in self.checks],
        }


# ---------------------------------------------------------------------------
# Reducing a model's own projections
# ---------------------------------------------------------------------------


def _branches(body: Any) -> list[Any]:
    if isinstance(body, (exp.Union, exp.Intersect, exp.Except)):
        return _branches(body.this) + _branches(body.expression)
    if isinstance(body, exp.Subquery):
        return _branches(body.this)
    return [body]


def _shapes_rows(body: Any) -> bool:
    """Set operations other than UNION ALL compare whole rows."""
    if isinstance(body, exp.Union) and not isinstance(body, (exp.Intersect, exp.Except)):
        if not body.args.get("distinct"):
            return _shapes_rows(body.this) or _shapes_rows(body.expression)
        return True
    return isinstance(body, (exp.Intersect, exp.Except))


def _select_shapes_rows(select: exp.Select) -> bool:
    """DISTINCT compares whole rows: every projection stays."""
    return bool(select.args.get("distinct"))


def _expand_positions(select: exp.Select) -> bool:
    """Replace ``GROUP BY 1, 2`` / ``ORDER BY 1`` by the expressions; False if impossible."""
    projections = select.expressions
    clauses = [select.args.get("group"), select.args.get("order")]
    for clause in clauses:
        for key in list(clause.expressions) if clause is not None else []:
            target = key.this if isinstance(key, exp.Ordered) else key
            if not (isinstance(target, exp.Literal) and not target.is_string):
                continue
            index = int(target.this) - 1
            if any(_is_star(p) for p in projections) or not 0 <= index < len(projections):
                return False
            source = projections[index]
            target.replace((source.this if isinstance(source, exp.Alias) else source).copy())
    return True


def _grouping_projections(select: exp.Select) -> set[str]:
    """GROUP BY ALL groups by every projection without an aggregate: those stay."""
    group = select.args.get("group")
    if group is None or not group.args.get("all"):
        return set()
    return {
        _projection_name(p)
        for p in select.expressions
        if not _is_star(p) and not p.find(exp.AggFunc)
    }


def _is_alias(projection: Any) -> bool:
    """A real alias; ``t.x`` or ``t.x AS x`` read a column of that name, not an alias."""
    if not isinstance(projection, exp.Alias):
        return False
    inner = projection.this
    return not (isinstance(inner, exp.Column) and inner.name.lower() == projection.alias.lower())


def _lateral_references(select: exp.Select, projections: list[Any]) -> set[str]:
    """Unqualified names *select* reads in *projections* and its filters."""
    names = {c.name.lower() for p in projections for c in p.find_all(exp.Column) if not c.table}
    for arg in ("where", "qualify", "having", "group", "order"):
        part = select.args.get(arg)
        if part is not None:
            names |= {c.name.lower() for c in part.find_all(exp.Column) if not c.table}
    for join in select.args.get("joins") or []:
        names |= {c.name.lower() for c in join.find_all(exp.Column) if not c.table}
    return names


def _is_star(projection: Any) -> bool:
    return projection.is_star


def _projection_name(projection: Any) -> str:
    return projection.alias_or_name.lower()


def _prune_select(select: exp.Select, keep: set[str]) -> None:
    """Drop projections outside *keep*, except those the same SELECT reads as lateral aliases."""
    projections = list(select.expressions)
    while True:
        kept = [p for p in projections if _is_star(p) or _projection_name(p) in keep]
        referenced = _lateral_references(select, kept)
        added = {
            _projection_name(p)
            for p in projections
            if _is_alias(p)
            and _projection_name(p) in referenced
            and _projection_name(p) not in keep
        }
        if not added:
            break
        keep = keep | added
    if not kept:
        kept = [exp.alias_(exp.Literal.number(1), NO_COLUMNS)]
    select.set("expressions", kept)


def _prune_body(body: Any, keep: set[str] | None) -> None:
    """Reduce a query body's outputs to *keep* (None: everything stays)."""
    if keep is None or _shapes_rows(body):
        return
    branches = [b for b in _branches(body) if isinstance(b, exp.Select)]
    if not branches:
        return
    if len(branches) == 1:
        select = branches[0]
        if not _select_shapes_rows(select) and _expand_positions(select):
            _prune_select(select, keep | _grouping_projections(select))
        return
    if body.args.get("by_name"):
        for select in branches:
            if not _select_shapes_rows(select) and _expand_positions(select):
                _prune_select(select, keep | _grouping_projections(select))
        return
    # UNION ALL matches by position: only whole positions can go, and only if all are listed.
    if any(
        _select_shapes_rows(b)
        or _grouping_projections(b)
        or not _expand_positions(b)
        or any(_is_star(p) for p in b.expressions)
        for b in branches
    ):
        return
    width = min(len(b.expressions) for b in branches)
    names = [_projection_name(p) for p in branches[0].expressions]
    positions = {i for i in range(width) if names[i] in keep}
    while True:  # a kept position may read a lateral alias of its branch
        before = len(positions)
        for branch in branches:
            projections = branch.expressions
            referenced = _lateral_references(branch, [projections[i] for i in positions])
            positions |= {
                i
                for i in range(width)
                if _is_alias(projections[i]) and _projection_name(projections[i]) in referenced
            }
        if len(positions) == before:
            break
    for branch in branches:
        kept = [branch.expressions[i] for i in sorted(positions)]
        branch.set("expressions", kept or [exp.alias_(exp.Literal.number(1), NO_COLUMNS)])


def _own_selects(body: Any) -> list[exp.Select]:
    """All SELECTs of a query body, nested subqueries included, without nested CTE bodies."""
    return [
        s
        for s in body.find_all(exp.Select)
        if s.find_ancestor(exp.CTE) is body.find_ancestor(exp.CTE)
        or (isinstance(body, exp.CTE) and s.find_ancestor(exp.CTE) is body)
    ]


def _scope_tables(select: exp.Select) -> dict[str, str]:
    """alias (lowercase) → relation name, for the tables directly in *select*'s FROM / JOIN."""
    out: dict[str, str] = {}
    from_clause = select.args.get("from") or select.args.get("from_")
    sources = [from_clause.this] if from_clause is not None else []
    sources += [j.this for j in select.args.get("joins") or []]
    for source in sources:
        if isinstance(source, exp.Table):
            name = source.name.lower()
            out[(source.alias or source.name).lower()] = name
    return out


def _demand(
    body: Any,
    outputs: set[str] | None,
    top_level: list[exp.Select],
    ctes: set[str],
    need: dict[str, set[str] | None],
) -> None:
    """Record which columns of each CTE this query body still reads."""

    def add(cte: str, columns: set[str] | None) -> None:
        if cte not in ctes:
            return
        if columns is None or need.get(cte, set()) is None:
            need[cte] = None
        else:
            need[cte] = need.get(cte, set()) | columns

    for select in body.find_all(exp.Select):
        if select.find_ancestor(exp.CTE) is not (body if isinstance(body, exp.CTE) else None):
            continue
        scope = _scope_tables(select)
        relations = set(scope.values())
        for relation in relations:  # read, even if by no named column (COUNT(*), EXISTS)
            add(relation, set())
        for joined in select.args.get("joins") or []:
            for using in joined.args.get("using") or []:
                for relation in relations:
                    add(relation, {using.name.lower()})
        for table in select.find_all(exp.Table):
            alias = table.args.get("alias")
            renames = alias is not None and alias.columns
            if (table.args.get("pivots") or renames) and table.find_ancestor(exp.Select) is select:
                add(table.name.lower(), None)
        for projection in select.expressions:
            if not _is_star(projection):
                continue
            passed = outputs if select in top_level else None
            qualifier = projection.table.lower() if isinstance(projection, exp.Column) else ""
            targets = [scope[qualifier]] if qualifier in scope else list(relations)
            for relation in targets:
                add(relation, passed)
        for column in select.find_all(exp.Column):
            if isinstance(column.this, exp.Star):
                continue
            if column.find_ancestor(exp.Select) is not select:
                continue
            name = column.name.lower()
            qualifier = (column.table or "").lower()
            if qualifier:
                owner = select
                while owner is not None and qualifier not in _scope_tables(owner):
                    owner = owner.find_ancestor(exp.Select)
                if owner is not None:
                    add(_scope_tables(owner)[qualifier], {name})
                continue
            owner = select
            while owner is not None:  # unqualified: any relation in reach may hold it
                for relation in _scope_tables(owner).values():
                    add(relation, {name})
                owner = owner.find_ancestor(exp.Select)


def reduce_projections(tree: Any, required: set[str] | None) -> None:
    """Reduce the model's outputs to *required* and every CTE to what is still read."""
    _key, with_node = _with_of(tree)
    entries = list(with_node.expressions) if with_node is not None else []
    names = {cte.alias.lower() for cte in entries}
    need: dict[str, set[str] | None] = {}

    _prune_body(tree, required)
    final = [b for b in _branches(tree) if isinstance(b, exp.Select)]
    _demand(tree, required, final, names, need)
    for cte in reversed(entries):
        name = cte.alias.lower()
        if name not in need:  # never read: nothing depends on what it selects
            cte.pop()
            continue
        keep = need.get(name, set())
        _prune_body(cte.this, keep)
        top = [b for b in _branches(cte.this) if isinstance(b, exp.Select)]
        _demand(cte, keep, top, names, need)


# ---------------------------------------------------------------------------
# Reducing the upstream relations
# ---------------------------------------------------------------------------


def _parents(
    node: dict[str, Any], manifest: dict[str, Any], renamed: dict[tuple[str, str], str]
) -> list[dict[str, Any]]:
    """Physical relations a model reads, with their name in the lineage graph."""
    own = str(node.get("name") or "").lower()
    out = []
    for uid in (node.get("depends_on") or {}).get("nodes") or []:
        parent = (manifest.get("nodes") or {}).get(uid) or (manifest.get("sources") or {}).get(uid)
        if not parent:
            continue
        is_source = uid.startswith("source.")
        if (parent.get("config") or {}).get("materialized") == "ephemeral":
            out.extend(_parents(parent, manifest, renamed))  # compiled into this model's SQL
            continue
        identifier = str(parent.get("identifier") or parent.get("alias") or parent.get("name"))
        name = identifier.lower() if is_source else str(parent.get("name") or "").lower()
        out.append({
            "name": renamed.get((own, name), name) if is_source else name,
            "identifier": identifier.lower(),
            "schema": str(parent.get("schema") or "").lower(),
            "types": {
                str(c).lower(): col.get("data_type")
                for c, col in (parent.get("columns") or {}).items()
                if col.get("data_type")
            },
        })
    return out


STUB_KINDS = (
    (None, False),
    (None, True),
    ("VARCHAR", False),
    ("VARCHAR", True),
    ("VARIANT", False),
    ("VARIANT", True),
)
"""``(type, typed by name)`` of a stub column without a documented type: functions reject an
untyped NULL, and none of these fits all (``TRY_CAST`` wants text, ``EXTRACT`` a date)."""

_DATE_LIKE = re.compile(r"(_dt|_date|date|_day)(_|$)|(^|_)(dt|date)$", re.IGNORECASE)
_TIMESTAMP_LIKE = re.compile(r"(_ts|timestamp|_time)(_|$)", re.IGNORECASE)
"""Only for binding a stub: a column named like a date is offered as one."""


def _named_type(column: str) -> str | None:
    if _TIMESTAMP_LIKE.search(column):
        return "TIMESTAMP_NTZ"
    return "DATE" if _DATE_LIKE.search(column) else None


def _stub_value(dtype: str | None, kind: str | None, dialect: str | None) -> exp.Expression:
    for candidate in (dtype, kind):
        if candidate:
            try:
                return exp.Cast(this=exp.Null(), to=exp.DataType.build(candidate, dialect=dialect))
            except Exception:  # noqa: BLE001 — free-text YAML type: use the next candidate
                continue
    return exp.Null()


def _stub_select(
    columns: list[str], types: dict[str, Any], kind: tuple[Any, bool], dialect: str | None
) -> exp.Select:
    names = sorted(set(columns)) or [NO_COLUMNS]
    return exp.select(*[
        exp.alias_(
            _stub_value(types.get(c) or (_named_type(c) if kind[1] else None), kind[0], dialect),
            exp.to_identifier(c, quoted=not _PLAIN_NAME.fullmatch(c)),
        )
        for c in names
    ])


def _real_select(table: exp.Table, columns: list[str], case: dict[str, str]) -> exp.Select:
    inner = table.copy()
    for arg in ("alias", "pivots", "joins"):
        inner.set(arg, None)
    projections = [
        exp.column(exp.to_identifier(case[c], quoted=True)) if c in case else exp.column(c)
        for c in sorted(columns)
    ] or [exp.alias_(exp.Literal.number(1), NO_COLUMNS)]
    return exp.select(*projections).from_(inner)


def _replace_parents(
    tree: Any,
    parents: list[dict[str, Any]],
    columns_of: Callable[[str], list[str] | None],
    visible: set[str] | None,
    physical: dict[str, dict[str, str]],
    stub_kind: tuple[Any, bool],
    dialect: str | None,
) -> tuple[list[str], int]:
    """Read upstream relations as *columns_of* says (None: as they are).

    Relations outside *visible* become stubs, whatever *columns_of* says. Only
    schema-qualified references are replaced: an unqualified name may be a same-named CTE.
    Returns the stubbed relations and the number of columns offered.
    """
    by_physical = {(p["schema"], p["identifier"]): p for p in parents}
    for column in list(tree.find_all(exp.Column)):  # schema.table.column has no meaning below
        if (str(column.args.get("db") or "").lower(), column.table.lower()) in by_physical:
            column.set("db", None)
    stubbed: list[str] = []
    offered = 0
    for table in list(tree.find_all(exp.Table)):
        parent = by_physical.get((str(table.db).lower(), table.name.lower()))
        if parent is None:
            continue
        name = parent["name"]
        key = f"{parent['schema']}.{parent['identifier']}"
        alias = table.args.get("alias")
        renames = [c.name.lower() for c in alias.columns] if alias is not None else []
        columns = columns_of(name)
        hidden = visible is not None and key not in visible
        if (columns is None or renames) and not hidden:
            continue  # as it is: a column-renaming alias depends on all columns by position
        columns = renames or columns or []
        offered += len(columns)
        select = (
            _stub_select(columns, parent["types"], stub_kind, dialect)
            if hidden
            else _real_select(table, columns, physical.get(key) or {})
        )
        if hidden and name not in stubbed:
            stubbed.append(name)
        replacement = exp.Subquery(
            this=select,
            alias=exp.TableAlias(this=exp.to_identifier(table.alias or table.name)),
        )
        for arg in ("pivots", "joins"):
            if table.args.get(arg):
                replacement.set(arg, table.args[arg])
        table.replace(replacement)
    return stubbed, offered


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


def load_compiled_sql(project_dir: Path, node: dict[str, Any], dialect: str | None) -> str | None:
    """A model's compiled SQL, query only. A placeholder standing for a filter becomes FALSE,
    so scalar subqueries over it stay single-row; any other stands for a value: NULL."""
    sql = node.get("compiled_code")
    if not sql:
        path = (
            project_dir
            / "target"
            / "compiled"
            / str(node.get("package_name"))
            / str(node.get("original_file_path")).replace("\\", "/")
        )
        try:
            sql = path.read_text(encoding="utf-8")
        except OSError:
            return None
    if re.search(r"\b(?:MERGE|INSERT)\b", sql, re.IGNORECASE):
        sql = _extract_source_select(sql, dialect=dialect)
    for pattern in get_config(project_dir).compiled_sql_placeholder_patterns or []:
        sql = re.sub(rf"(\b(?:WHERE|AND|OR)\s+)(?:{pattern})", r"\1FALSE", sql, flags=re.IGNORECASE)
        sql = re.sub(pattern, "NULL", sql)
    return _EMPTY_TYPE_PARAMS_RE.sub(r"\1", sql)


def physical_columns(snapshot: dict[str, Any]) -> dict[str, dict[str, str]]:
    """``schema.identifier`` → {lowercase column: real column} from ``cll-columns.json``."""
    out: dict[str, dict[str, str]] = {}
    for entry in [
        *(snapshot.get("nodes") or {}).values(),
        *(snapshot.get("sources") or {}).values(),
    ]:
        meta = entry.get("metadata") or {}
        key = f"{str(meta.get('schema')).lower()}.{str(meta.get('name')).lower()}"
        out[key] = {str(c).lower(): str(c) for c in entry.get("columns") or {}}
    return out


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _bind(sql: str) -> str:
    return f"SELECT * FROM ({sql}) WHERE 1 = 0"


def _missing_relation(message: str) -> str:
    found = _MISSING_OBJECT.search(message)
    if not found:
        return ""
    return (found.group(1) or found.group(2) or "").replace('"', "").rstrip(".").upper()


def validate(
    graph: Graph,
    result: Analysis,
    manifest: dict[str, Any],
    compiled_sql: Callable[[dict[str, Any]], str | None],
    run: Callable[[str], list[str]],
    dialect: str | None,
    physical: dict[str, dict[str, str]] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    visible: set[str] | None = None,
) -> ValidationResult:
    """Bind every needed model as it is and reduced. *run* returns the output column names.

    Upstream relations outside *visible* (``schema.identifier``; None: all are visible) are
    replaced by stubs.
    """
    renamed = qualified_relations(manifest)
    physical = physical or {}
    required: dict[str, list[str]] = defaultdict(list)
    for model, column in result.required:
        required[model].append(column)
    data_products = set(result.data_products)
    needed = data_products | {m for m in required if (graph.models.get(m) or {}).get("analysed")}
    nodes = {
        str(n.get("name") or "").lower(): n
        for n in (manifest.get("nodes") or {}).values()
        if n.get("resource_type") == "model"
    }
    out = ValidationResult()
    todo = sorted(needed & set(nodes))

    def known_columns(name: str) -> list[str]:
        meta = graph.models.get(name) or {}
        return sorted({*(meta.get("columns") or []), *required.get(name, [])})

    for position, model in enumerate(todo, start=1):
        if on_progress:
            on_progress(position, len(todo))
        out.checks.append(
            _check_model(
                model,
                nodes[model],
                manifest,
                compiled_sql,
                run,
                dialect,
                required,
                data_products,
                physical,
                visible,
                known_columns,
                out,
                renamed,
            )  # fmt: skip
        )
    return out


def _check_model(
    model: str,
    node: dict[str, Any],
    manifest: dict[str, Any],
    compiled_sql: Callable[[dict[str, Any]], str | None],
    run: Callable[[str], list[str]],
    dialect: str | None,
    required: dict[str, list[str]],
    data_products: set[str],
    physical: dict[str, dict[str, str]],
    visible: set[str] | None,
    known_columns: Callable[[str], list[str]],
    out: ValidationResult,
    renamed: dict[tuple[str, str], str],
) -> ModelCheck:
    sql = compiled_sql(node)
    if not sql:
        return ModelCheck(model, NOT_TESTABLE, "no compiled SQL")
    try:
        parsed = _hoist_derived_tables(parse_one(sql, dialect=dialect))
    except Exception as exc:  # noqa: BLE001 — unparseable SQL cannot be rewritten
        return ModelCheck(model, NOT_TESTABLE, f"SQL not parseable: {str(exc)[:160]}")
    if not isinstance(parsed, exp.Query):
        return ModelCheck(model, NOT_TESTABLE, "not a query")
    parents = _parents(node, manifest, renamed)
    # A stub in the unreduced query offers anything this SQL could read from it.
    named_in_sql = sorted({
        c.name if c.this.args.get("quoted") else c.name.lower()
        for c in parsed.find_all(exp.Column)
        if not isinstance(c.this, exp.Star)
    })
    marker = [UNKNOWN_COLUMNS_MARKER] if UNKNOWN_COLUMNS_MARKER in named_in_sql else []

    stubbed: list[str] = []
    original_columns: list[str] = []
    stub_kind = STUB_KINDS[0]
    message = ""
    for stub_kind in STUB_KINDS:
        # A visible relation stays as it is in the unreduced query.
        original = parsed.copy()
        stubbed, _ = _replace_parents(
            original,
            [p for p in parents if _is_hidden(p, visible)],
            lambda name: sorted({*known_columns(name), *named_in_sql}),
            visible,
            physical,
            stub_kind,
            dialect,
        )
        try:
            original_columns = run(_bind(original.sql(dialect=dialect)))
            message = ""
            break
        except Exception as exc:  # noqa: BLE001 — every warehouse error is classified below
            message = str(exc)
            if not stubbed or _NO_ACCESS.search(message):
                break
    if message:
        if _NO_ACCESS.search(message):
            return ModelCheck(model, NO_ACCESS, message[:240], _missing_relation(message), stubbed)
        # Not the requirements' fault: this SQL does not bind even as compiled.
        return ModelCheck(
            model, NOT_TESTABLE, f"original does not bind: {message[:200]}", stubbed=stubbed
        )

    reduced = parsed.copy()
    own = None if model in data_products else {c.lower() for c in required.get(model, [])}
    reduce_projections(reduced, own)
    _, offered = _replace_parents(
        reduced,
        parents,
        lambda name: None if name in data_products else [*required.get(name, []), *marker],
        visible,
        physical,
        stub_kind,
        dialect,
    )
    try:
        reduced_columns = run(_bind(reduced.sql(dialect=dialect)))
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if _POSITIONAL_ALIAS.search(message):  # `(SELECT * ...) AS d (x, y)` counts columns
            return ModelCheck(
                model, NOT_TESTABLE, f"alias by position: {message[:200]}", stubbed=stubbed
            )
        return ModelCheck(
            model, FAILED, f"reduced query does not bind: {message[:300]}", stubbed=stubbed
        )
    # What is required of the model (everything, for a data product) must still come out.
    available = {c.lower() for c in original_columns}
    expected = available if own is None else own & available
    lost = sorted(expected - {c.lower() for c in reduced_columns})
    if lost:
        return ModelCheck(
            model, FAILED, f"required output columns lost: {lost[:20]}", stubbed=stubbed
        )
    out.columns_checked += offered
    return ModelCheck(model, PASSED, stubbed=stubbed)


def _is_hidden(parent: dict[str, Any], visible: set[str] | None) -> bool:
    return visible is not None and f"{parent['schema']}.{parent['identifier']}" not in visible


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


TEXT = {
    "en": {
        "title": "Validation of the column requirements",
        "method": (
            "Every needed model is bound by the warehouse as it is and reduced: it outputs only "
            "what is required of it, and its upstream relations offer only their required "
            "columns. No data row comes back and nothing is created."
        ),
        "summary": "Result",
        "passed": "bind with only the required columns: requirements confirmed",
        "against": "of them entirely against real relations, the others partly against stubs",
        "failed": "fail with only the required columns: requirements are wrong",
        "no_access": "not checkable: the role cannot use an object they need",
        "not_testable": "not checkable: the SQL does not bind or parse even as compiled",
        "columns": "required columns confirmed",
        "findings": "Findings",
        "blocked": "Warnings: not checkable for lack of access",
        "reason": (
            "Without access to {relation} the warehouse does not bind {count} model(s) using it. "
            "A role with USAGE/SELECT on it would allow checking them."
        ),
        "stubbed": "Warnings: checked against a stub for lack of access",
        "stub_reason": (
            "The role cannot see {relation}, so {count} model(s) were checked against its column "
            "names only. A role with read access would confirm that these columns really exist "
            "there with a compatible type."
        ),
        "untestable": "Not checkable",
        "unknown_relation": "(not named by the warehouse)",
    },
    "de": {
        "title": "Validierung des Spaltenbedarfs",
        "method": (
            "Jedes benötigte Modell wird vom Warehouse unverändert und reduziert gebunden: Es gibt "
            "nur aus, was von ihm benötigt wird, und seine Upstream-Relationen bieten nur ihre "
            "benötigten Spalten an. Es kommen keine Datenzeilen zurück und nichts wird angelegt."
        ),
        "summary": "Ergebnis",
        "passed": "binden nur mit den benötigten Spalten: Spaltenbedarf bestätigt",
        "against": "davon komplett gegen echte Relationen, die übrigen teils gegen Stubs",
        "failed": "scheitern nur mit den benötigten Spalten: Spaltenbedarf ist falsch",
        "no_access": "nicht prüfbar: die Rolle darf ein benötigtes Objekt nicht nutzen",
        "not_testable": "nicht prüfbar: das SQL bindet oder parst schon unverändert nicht",
        "columns": "benötigte Spalten bestätigt",
        "findings": "Befunde",
        "blocked": "Warnungen: wegen fehlenden Zugriffs nicht prüfbar",
        "reason": (
            "Ohne Zugriff auf {relation} bindet das Warehouse {count} Modell(e) nicht, die es "
            "nutzen. Eine Rolle mit USAGE/SELECT darauf würde deren Prüfung ermöglichen."
        ),
        "stubbed": "Warnungen: wegen fehlenden Zugriffs gegen Stub geprüft",
        "stub_reason": (
            "Die Rolle sieht {relation} nicht, deshalb wurden {count} Modell(e) nur gegen die "
            "Spaltennamen geprüft. Eine Rolle mit Leserecht würde bestätigen, dass die Spalten "
            "dort wirklich existieren und der Typ passt."
        ),
        "untestable": "Nicht prüfbar",
        "unknown_relation": "(vom Warehouse nicht genannt)",
    },
}


def blocked_reason(relation: str, count: int, lang: str = "en") -> str:
    text = TEXT[lang]
    return text["reason"].format(relation=relation or text["unknown_relation"], count=count)


def stub_reason(relation: str, count: int, lang: str = "en") -> str:
    return TEXT[lang]["stub_reason"].format(relation=relation.upper(), count=count)


def render_markdown(result: ValidationResult, lang: str = "en") -> str:
    text = TEXT[lang]
    lines = [f"# {text['title']}", "", text["method"], "", f"## {text['summary']}", ""]
    passed = result.by_status(PASSED)
    lines.append(f"- **{len(passed)}** {text[PASSED]}")
    lines.append(f"  - **{sum(1 for c in passed if not c.stubbed)}** {text['against']}")
    for status in (FAILED, NO_ACCESS, NOT_TESTABLE):
        lines.append(f"- **{len(result.by_status(status))}** {text[status]}")
    lines.append(f"- **{result.columns_checked}** {text['columns']}")
    failed = result.by_status(FAILED)
    if failed:
        lines += ["", f"## {text['findings']}", ""]
        lines += [f"- `{c.model.upper()}`: {' '.join(c.detail.split())}" for c in failed]
    blocked = result.blocked_relations()
    if blocked:
        lines += ["", f"## {text['blocked']}", ""]
        for relation, models in sorted(blocked.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            lines.append(f"- `{relation}`: {blocked_reason(relation, len(models), lang)}")
    stubbed = result.stubbed_relations()
    if stubbed:
        lines += ["", f"## {text['stubbed']}", ""]
        for relation, models in sorted(stubbed.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            lines.append(f"- `{relation.upper()}`: {stub_reason(relation, len(models), lang)}")
    untestable = result.by_status(NOT_TESTABLE)
    if untestable:
        lines += ["", f"## {text['untestable']}", ""]
        lines += [f"- `{c.model.upper()}`: {' '.join(c.detail.split())}" for c in untestable]
    return "\n".join(lines) + "\n"


def write_json(result: ValidationResult) -> str:
    return json.dumps(result.to_json(), indent=1, ensure_ascii=False) + "\n"
