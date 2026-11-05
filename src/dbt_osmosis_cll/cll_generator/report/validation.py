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
from dbt_osmosis_cll.cll_generator.report.analysis import Analysis, Graph
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
    group = select.args.get("group")
    return bool(select.args.get("distinct")) or bool(group and group.args.get("all"))


def _is_star(projection: Any) -> bool:
    return projection.is_star


def _projection_name(projection: Any) -> str:
    return projection.alias_or_name.lower()


def _prune_select(select: exp.Select, keep: set[str]) -> None:
    """Drop projections outside *keep*, except those the same SELECT reads as lateral aliases."""
    projections = list(select.expressions)
    while True:
        kept = [p for p in projections if _is_star(p) or _projection_name(p) in keep]
        referenced = {c.name.lower() for p in kept for c in p.find_all(exp.Column) if not c.table}
        for arg in ("where", "qualify", "having", "group", "order"):
            part = select.args.get(arg)
            if part is not None:
                referenced |= {c.name.lower() for c in part.find_all(exp.Column) if not c.table}
        added = {
            _projection_name(p)
            for p in projections
            if _projection_name(p) in referenced and _projection_name(p) not in keep
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
        if not _select_shapes_rows(branches[0]):
            _prune_select(branches[0], keep)
        return
    # UNION ALL matches by position: only whole positions can go, and only if all are listed.
    if any(_select_shapes_rows(b) or any(_is_star(p) for p in b.expressions) for b in branches):
        return
    width = min(len(b.expressions) for b in branches)
    names = [_projection_name(p) for p in branches[0].expressions]
    positions = [i for i in range(width) if i < len(names) and names[i] in keep]
    for branch in branches:
        kept = [branch.expressions[i] for i in positions]
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
        for joined in select.args.get("joins") or []:
            for using in joined.args.get("using") or []:
                for relation in relations:
                    add(relation, {using.name.lower()})
        for table in select.find_all(exp.Table):
            if table.args.get("pivots") and table.find_ancestor(exp.Select) is select:
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
        keep = need.get(name, set())
        _prune_body(cte.this, keep)
        top = [b for b in _branches(cte.this) if isinstance(b, exp.Select)]
        _demand(cte, keep, top, names, need)


# ---------------------------------------------------------------------------
# Reducing the upstream relations
# ---------------------------------------------------------------------------


def _parents(node: dict[str, Any], manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Physical relations a model reads, with their name in the lineage graph."""
    own = str(node.get("name") or "").lower()
    out = []
    for uid in (node.get("depends_on") or {}).get("nodes") or []:
        parent = (manifest.get("nodes") or {}).get(uid) or (manifest.get("sources") or {}).get(uid)
        if not parent:
            continue
        is_source = uid.startswith("source.")
        if (parent.get("config") or {}).get("materialized") == "ephemeral":
            continue
        identifier = str(parent.get("identifier") or parent.get("alias") or parent.get("name"))
        name = identifier.lower() if is_source else str(parent.get("name") or "").lower()
        if is_source and name == own:  # same-named source: see analysis.build_graph
            name = f"{str(parent.get('source_name')).lower()}.{name}"
        out.append({
            "name": name,
            "identifier": identifier.lower(),
            "schema": str(parent.get("schema") or "").lower(),
        })
    return out


def _stub_select(columns: list[str]) -> exp.Select:
    names = sorted(set(columns)) or [NO_COLUMNS]
    return exp.select(*[exp.alias_(exp.Null(), exp.to_identifier(c, quoted=False)) for c in names])


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
) -> tuple[list[str], int]:
    """Read upstream relations as *columns_of* says (None: as they are).

    Relations outside *visible* become stubs, whatever *columns_of* says. Only
    schema-qualified references are replaced: an unqualified name may be a same-named CTE.
    Returns the stubbed relations and the number of columns offered.
    """
    by_physical = {(p["schema"], p["identifier"]): p for p in parents}
    stubbed: list[str] = []
    offered = 0
    for table in list(tree.find_all(exp.Table)):
        parent = by_physical.get((str(table.db).lower(), table.name.lower()))
        if parent is None:
            continue
        name = parent["name"]
        columns = columns_of(name)
        hidden = visible is not None and name not in visible
        if columns is None and not hidden:
            continue
        columns = columns or []
        offered += len(columns)
        select = (
            _stub_select(columns)
            if hidden
            else _real_select(table, columns, physical.get(name) or {})
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
    """A model's compiled SQL, query only. Placeholder filters become FALSE, so scalar
    subqueries over them stay single-row."""
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
        sql = re.sub(pattern, "FALSE", sql)
    return _EMPTY_TYPE_PARAMS_RE.sub(r"\1", sql)


def physical_columns(
    snapshot: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, dict[str, str]]:
    """Graph name → {lowercase column: real column} from ``target/cll-columns.json``."""
    out: dict[str, dict[str, str]] = {}
    for uid, entry in (snapshot.get("nodes") or {}).items():
        node = (manifest.get("nodes") or {}).get(uid) or {}
        out[str(node.get("name") or uid.split(".")[-1]).lower()] = {
            str(c).lower(): str(c) for c in entry.get("columns") or {}
        }
    for uid, entry in (snapshot.get("sources") or {}).items():
        columns = {str(c).lower(): str(c) for c in entry.get("columns") or {}}
        source = (manifest.get("sources") or {}).get(uid) or {}
        name = str((entry.get("metadata") or {}).get("name") or "").lower()
        out.setdefault(name, columns)
        out[f"{str(source.get('source_name')).lower()}.{name}"] = columns
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
    return (found.group(1) or found.group(2) or "").replace('"', "").upper()


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

    Upstream relations outside *visible* (None: all are visible) are replaced by stubs.
    """
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
    parents = _parents(node, manifest)

    original = parsed.copy()
    hidden = visible is not None
    stubbed, _ = _replace_parents(
        original,
        parents,
        lambda name: known_columns(name) if hidden and name not in (visible or set()) else None,
        visible,
        physical,
    )
    try:
        original_columns = run(_bind(original.sql(dialect=dialect)))
    except Exception as exc:  # noqa: BLE001 — every warehouse error is classified below
        message = str(exc)
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
        lambda name: None if name in data_products else required.get(name, []),
        visible,
        physical,
    )
    try:
        reduced_columns = run(_bind(reduced.sql(dialect=dialect)))
    except Exception as exc:  # noqa: BLE001
        return ModelCheck(
            model, FAILED, f"reduced query does not bind: {str(exc)[:300]}", stubbed=stubbed
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
