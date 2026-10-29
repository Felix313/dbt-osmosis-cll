"""Non-projection column usage: joins, filters, QUALIFY, windows, GROUP BY, HAVING.

The lineage pass in :mod:`sql_parser` answers "where does each output column come
from". This module answers the complementary question "which upstream columns does
the SQL *read* without outputting them" — join keys, WHERE / ON / QUALIFY / HAVING
predicates, window specifications and grouping keys. A column that only filters is as
much a requirement of the model as one it outputs.

Every SELECT in the statement is a scope (CTE bodies, set-operation branches, nested
subqueries, the final SELECT). Each column is attributed to its innermost scope and
resolved through the CTE state the lineage pass already built, so a QUALIFY on a
column that a CTE never projects still lands on the base relation it reads.
CTEs the final SELECT never reaches are skipped: their predicates cannot affect the
result.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable
from typing import TYPE_CHECKING

from sqlglot import exp

from dbt_osmosis_cll.cll_generator.models.schema import ColumnUsage
from dbt_osmosis_cll.cll_generator.parser.sql_parser_utils import (
    get_scoped_table_aliases,
    get_scoped_tables_from_select,
    get_table_context,
    strip_sql_comments,
)

if TYPE_CHECKING:  # pragma: no cover
    from dbt_osmosis_cll.cll_generator.parser.sql_parser import ParserContext, SQLColumnParser

FINAL_SCOPE = "<final>"
GROUP_BY_ALL_MARKER = "cll_group_by_all_marker"
"""Identifier ``GROUP BY ALL`` is rewritten to before parsing (see sql_parser)."""

# Clause of the *enclosing* SELECT a nested subquery sits in → usage of its projections.
_SUBQUERY_CLAUSE_KIND = {
    "where": "filter",
    "having": "having",
    "qualify": "qualify",
}


def _flatten_and(node: exp.Expression | None) -> list[exp.Expression]:
    """Split a predicate into its top-level AND conjuncts (parentheses unwrapped)."""
    if node is None:
        return []
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.And):
        return _flatten_and(node.this) + _flatten_and(node.expression)
    return [node]


def _own_columns(node: exp.Expression, scope: exp.Select) -> list[exp.Column]:
    """Columns under *node* whose innermost SELECT is *scope* (nested scopes excluded)."""
    out = []
    candidates = [node] if isinstance(node, exp.Column) else node.find_all(exp.Column)
    for col in candidates:
        if not col.name or isinstance(col.this, exp.Star):
            continue
        if col.parent_select is scope:
            out.append(col)
    return out


def _sql(node: exp.Expression, dialect: str | None) -> str:
    try:
        return strip_sql_comments(node.sql(dialect=dialect, comments=False))
    except Exception:  # noqa: BLE001 — rendering is cosmetic, never fail the parse on it
        return strip_sql_comments(str(node))


class UsageCollector:
    """Collect :class:`ColumnUsage` rows for one parsed statement."""

    def __init__(
        self,
        parser: SQLColumnParser,
        base_context: ParserContext,
        ctes: list[exp.CTE],
    ) -> None:
        self._parser = parser
        self._base = base_context
        self._dialect = parser.dialect
        self._cte_map: dict[str, exp.CTE] = {c.alias.lower(): c for c in ctes}
        self._rows: dict[tuple, ColumnUsage] = {}
        self._expanding: set[tuple[str, str]] = set()
        # CTE whose body is being processed: inside its own definition the CTE's name
        # refers to the base relation (`x AS (SELECT ... FROM db.s.x)`), not to itself.
        self._current_cte: str | None = None

    # ------------------------------------------------------------------ scopes

    def collect(self, parsed: exp.Expression) -> list[ColumnUsage]:
        reachable = self._reachable_ctes(parsed)
        for select in parsed.find_all(exp.Select):
            cte_name = self._enclosing_cte(select)
            if cte_name is not None and cte_name not in reachable:
                continue
            self._current_cte = cte_name
            self._collect_scope(select, self._scope_name(select, cte_name))
        self._current_cte = None
        return sorted(self._rows.values(), key=lambda r: r.sort_key())

    def _reachable_ctes(self, parsed: exp.Expression) -> set[str]:
        """CTE names the non-CTE part of the statement reads, transitively."""
        if not self._cte_map:
            return set()

        def referenced(node: exp.Expression, skip_with: bool) -> set[str]:
            names = set()
            for table in node.find_all(exp.Table):
                if skip_with and self._enclosing_cte(table) is not None:
                    continue
                name = str(table.name).lower()
                if name in self._cte_map:
                    names.add(name)
            return names

        frontier = referenced(parsed, skip_with=True)
        seen: set[str] = set()
        while frontier:
            name = frontier.pop()
            if name in seen:
                continue
            seen.add(name)
            frontier |= referenced(self._cte_map[name].this, skip_with=False) - seen
        # CTEs nested inside subqueries (WITH inside a derived table) are rare and not
        # reachable from the top-level walk above; keep them rather than drop predicates.
        for name, cte in self._cte_map.items():
            if not (isinstance(cte.parent, exp.With) and cte.parent.parent is parsed):
                seen.add(name)
        return seen

    @staticmethod
    def _enclosing_cte(node: exp.Expression) -> str | None:
        parent = node.parent
        while parent is not None:
            if isinstance(parent, exp.CTE):
                return parent.alias.lower()
            parent = parent.parent
        return None

    @staticmethod
    def _scope_name(select: exp.Select, cte_name: str | None) -> str:
        base = cte_name or FINAL_SCOPE
        parent = select.parent
        while parent is not None and not isinstance(parent, exp.CTE):
            if isinstance(parent, exp.Select):
                return f"{base}/subquery"
            parent = parent.parent
        return base

    def _context_for(self, select: exp.Select) -> ParserContext:
        """Scope context: own aliases, plus enclosing scopes' aliases for correlation."""
        aliases: dict[str, str] = {}
        chain = []
        parent = select.parent
        while parent is not None:
            if isinstance(parent, exp.Select):
                chain.append(parent)
            if isinstance(parent, exp.CTE):
                break
            parent = parent.parent
        for outer in reversed(chain):
            aliases.update(get_scoped_table_aliases(outer))
        aliases.update(get_scoped_table_aliases(select))
        column_definitions = {
            strip_sql_comments(e.alias_or_name.lower()): e for e in select.expressions
        }
        return dataclasses.replace(
            self._base,
            aliases=aliases,
            table_context=get_table_context(select),
            scope_tables=get_scoped_tables_from_select(select),
            column_definitions=column_definitions,
        )

    # ----------------------------------------------------------------- clauses

    def _collect_scope(self, select: exp.Select, scope: str) -> None:
        ctx = self._context_for(select)
        derived = self._derived_tables(select)

        inherited = self._inherited_kind(select)
        if inherited is not None:
            kind, predicate = inherited
            for expr in select.expressions:
                for col in _own_columns(expr, select):
                    self._emit(kind, col, select, ctx, derived, scope, "subquery", predicate)

        where = select.args.get("where")
        if where is not None:
            for conj in _flatten_and(where.this):
                for col in _own_columns(conj, select):
                    self._emit("filter", col, select, ctx, derived, scope, "predicate", conj)

        for join in select.args.get("joins") or []:
            for conj in _flatten_and(join.args.get("on")):
                cols = _own_columns(conj, select)
                kind = "join_key" if self._relates_relations(cols, ctx) else "join_filter"
                for col in cols:
                    self._emit(kind, col, select, ctx, derived, scope, "predicate", conj)
            for ident in join.args.get("using") or []:
                name = ident.name if hasattr(ident, "name") else str(ident)
                for table in ctx.scope_tables:
                    self._emit_resolved(
                        "join_key",
                        f"{table}.{name}".lower(),
                        scope,
                        "key",
                        _sql(join, self._dialect),
                        self._sources_for_table(table, name.lower(), ctx, derived),
                    )

        group = select.args.get("group")
        if group is not None:
            for key in self._group_keys(group, select):
                for col in _own_columns(key, select):
                    self._emit("group_by", col, select, ctx, derived, scope, "key", key)

        having = select.args.get("having")
        if having is not None:
            for conj in _flatten_and(having.this):
                for col in _own_columns(conj, select):
                    self._emit("having", col, select, ctx, derived, scope, "predicate", conj)

        qualify = select.args.get("qualify")
        if qualify is not None:
            for conj in _flatten_and(qualify.this):
                for col in _own_columns(conj, select):
                    self._emit(
                        "qualify",
                        col,
                        select,
                        ctx,
                        derived,
                        scope,
                        self._window_position(col, conj),
                        conj,
                    )

        for expr in select.expressions:
            for window in expr.find_all(exp.Window):
                if window.parent_select is not select:
                    continue
                for part, detail in (
                    (window.args.get("partition_by") or [], "partition_by"),
                    ([window.args.get("order")] if window.args.get("order") else [], "order_by"),
                ):
                    for node in part:
                        for col in _own_columns(node, select):
                            self._emit(
                                "window",
                                col,
                                select,
                                ctx,
                                derived,
                                scope,
                                detail,
                                None,
                                predicate_sql=self._window_spec_sql(window),
                            )

    @staticmethod
    def _window_position(col: exp.Column, root: exp.Expression) -> str:
        """``partition_by`` / ``order_by`` / ``window_function`` / ``predicate`` of *col*."""
        child, node = col, col.parent
        while node is not None and node is not root.parent:
            if isinstance(node, exp.Window):
                if any(child is p for p in node.args.get("partition_by") or []):
                    return "partition_by"
                if child is node.args.get("order"):
                    return "order_by"
                return "window_function"
            child, node = node, node.parent
        return "predicate"

    def _group_keys(self, group: exp.Group, select: exp.Select) -> list[exp.Expression]:
        """Grouping expressions; ``GROUP BY ALL`` and ordinals map onto the projections."""
        exprs = list(group.expressions)
        is_all = bool(group.args.get("all")) or any(
            isinstance(e, exp.Column) and e.name.lower() == GROUP_BY_ALL_MARKER for e in exprs
        )
        if is_all:
            return [
                e
                for e in select.expressions
                if not e.find(exp.AggFunc)
                and not e.find(exp.Window)
                and not isinstance(e.this if isinstance(e, exp.Alias) else e, exp.Star)
            ]
        keys: list[exp.Expression] = []
        projections = list(select.expressions)
        for key in exprs:
            if isinstance(key, exp.Literal) and key.is_int:
                idx = int(key.this) - 1
                if 0 <= idx < len(projections):
                    keys.append(projections[idx])
                continue
            keys.append(key)
        for extra in ("rollup", "cube", "grouping_sets"):
            for node in group.args.get(extra) or []:
                keys.append(node)
        return keys

    def _inherited_kind(self, select: exp.Select) -> tuple[str, exp.Expression] | None:
        """(usage, conjunct) when *select* is a subquery inside a filtering clause."""
        child: exp.Expression = select
        node = select.parent
        while node is not None and not isinstance(node, exp.Select):
            child, node = node, node.parent
        if node is None:
            return None
        outer: exp.Select = node
        for arg, kind in _SUBQUERY_CLAUSE_KIND.items():
            clause = outer.args.get(arg)
            if clause is not None and (clause is child or select in clause.find_all(exp.Select)):
                return kind, self._conjunct_containing(clause.this, select)
        for join in outer.args.get("joins") or []:
            on = join.args.get("on")
            if on is not None and select in on.find_all(exp.Select):
                return "join_filter", self._conjunct_containing(on, select)
        return None

    @staticmethod
    def _conjunct_containing(clause: exp.Expression, node: exp.Expression) -> exp.Expression:
        for conj in _flatten_and(clause):
            if conj is node or any(n is node for n in conj.find_all(type(node))):
                return conj
        return clause

    def _relates_relations(self, cols: list[exp.Column], ctx: ParserContext) -> bool:
        """True when the ON conjunct references columns of two or more relations."""
        relations = set()
        for col in cols:
            if col.table:
                relations.add(str(col.table).lower())
            else:
                relations.add(f"?{col.name.lower()}")
        return len(relations) >= 2

    # -------------------------------------------------------------- resolution

    def _derived_tables(self, select: exp.Select) -> dict[str, exp.Select]:
        """Alias → inner SELECT for subqueries used as FROM / JOIN relations."""
        derived: dict[str, exp.Select] = {}
        sources = []
        frm = select.args.get("from") or select.args.get("from_")
        if frm is not None:
            sources.append(frm.this)
        sources.extend(j.this for j in select.args.get("joins") or [])
        for src in sources:
            if isinstance(src, exp.Subquery) and src.alias:
                inner = src.this
                while isinstance(inner, exp.Subquery):
                    inner = inner.this
                if isinstance(inner, exp.Select):
                    derived[src.alias.lower()] = inner
        return derived

    def _known_columns(
        self, table: str, ctx: ParserContext, derived: dict[str, exp.Select]
    ) -> set[str] | None:
        if table in derived:
            return {strip_sql_comments(e.alias_or_name.lower()) for e in derived[table].expressions}
        if table in ctx.cte_sources and table != self._current_cte:
            cols = {c.lower() for c in ctx.cte_sources[table]}
            # A CTE built on `SELECT *` over a base table has columns we cannot list.
            return None if (ctx.cte_base_tables.get(table) and not cols) else cols
        return self._parser._known_table_columns(table)

    def _sources_for_table(
        self, table: str, name: str, ctx: ParserContext, derived: dict[str, exp.Select]
    ) -> tuple[list[str], str]:
        """Resolve ``table.name`` to base ``relation.column`` strings through CTEs."""
        if table in derived:
            inner = derived[table]
            for expr in inner.expressions:
                if strip_sql_comments(expr.alias_or_name.lower()) == name:
                    sub_ctx = self._context_for(inner)
                    srcs = self._parser._extract_source_columns(expr, sub_ctx)
                    srcs = {s for s in srcs if s}
                    return (sorted(srcs), "qualified") if srcs else ([], "no_upstream")
            return [f"{table}.{name}"], "unresolved"
        if ctx.ephemeral_cte_names and table in ctx.ephemeral_cte_names:
            return [f"{table}.{name}"], "qualified"
        if table in ctx.cte_sources and table != self._current_cte:
            cte_cols = ctx.cte_sources[table]
            key = next((k for k in sorted(cte_cols) if k.lower() == name), None)
            if key is not None:
                resolved = cte_cols[key]
                if resolved:
                    return [resolved.lower()], "qualified"
                multi = self._parser._get_source_set(
                    table, key, ctx
                ) or self._parser._get_union_branches(table, key, ctx)
                multi = [m for m in multi if m]
                if multi:
                    return sorted(set(multi)), "qualified"
                # Computed without a value source (ROW_NUMBER(), RANK(), ...): ordering or
                # filtering on it reads whatever its expression reads, window spec included.
                expanded = self._expand_computed_cte_column(table, key)
                return (expanded, "qualified") if expanded else ([], "no_upstream")
            bases = sorted(ctx.cte_base_tables.get(table) or ())
            if len(bases) == 1:
                return [f"{bases[0]}.{name}"], "qualified"
            if bases:
                hits = [b for b in bases if name in (self._parser._known_table_columns(b) or ())]
                if len(hits) == 1:
                    return [f"{hits[0]}.{name}"], "qualified"
                return [f"{b}.{name}" for b in (hits or bases)], "ambiguous"
            return [f"{table}.{name}"], "unresolved"
        return [f"{table}.{name}"], "qualified"

    def _expand_computed_cte_column(self, table: str, name: str) -> list[str]:
        """Every base column the projection *name* of CTE *table* reads (window spec too)."""
        key = (table, name.lower())
        cte = self._cte_map.get(table)
        if cte is None or key in self._expanding or not isinstance(cte.this, exp.Select):
            return []
        body: exp.Select = cte.this
        expr = next(
            (e for e in body.expressions if strip_sql_comments(e.alias_or_name.lower()) == key[1]),
            None,
        )
        if expr is None:
            return []
        self._expanding.add(key)
        outer_cte, self._current_cte = self._current_cte, table
        try:
            ctx = self._context_for(body)
            derived = self._derived_tables(body)
            out: set[str] = set()
            for col in _own_columns(expr, body):
                srcs, _ = self._resolve(col, ctx, derived)
                out.update(s for s in srcs if s)
            return sorted(out)
        finally:
            self._current_cte = outer_cte
            self._expanding.discard(key)

    def _resolve(
        self, col: exp.Column, ctx: ParserContext, derived: dict[str, exp.Select]
    ) -> tuple[list[str], str]:
        name = strip_sql_comments(col.name).lower()
        if col.table:
            qualifier = str(col.table).lower()
            table = ctx.aliases.get(qualifier, ctx.aliases.get(str(col.table), qualifier)).lower()
            if (
                table not in derived
                and table not in ctx.cte_sources
                and table not in ctx.scope_tables
                and qualifier not in ctx.aliases
                and self._parser._known_table_columns(table) is None
                and ctx.scope_tables
            ):
                # Unknown qualifier that is neither an alias, a CTE nor a relation of
                # this scope — usually a correlated reference we could not map.
                srcs, _ = self._sources_for_table(table, name, ctx, derived)
                return srcs, "unresolved"
            return self._sources_for_table(table, name, ctx, derived)

        scope_tables = list(dict.fromkeys(ctx.scope_tables + list(derived)))
        candidates = []
        unknown = []
        for table in scope_tables:
            known = self._known_columns(table, ctx, derived)
            if known is None:
                unknown.append(table)
            elif name in known:
                candidates.append(table)
        if len(candidates) == 1:
            srcs, res = self._sources_for_table(candidates[0], name, ctx, derived)
            if res != "qualified":
                return srcs, res
            return srcs, "unqualified_resolved" if len(scope_tables) > 1 else "single_table"
        alias_expr = (ctx.column_definitions or {}).get(name)
        alias_inner = alias_expr.this if isinstance(alias_expr, exp.Alias) else alias_expr
        is_passthrough = isinstance(alias_inner, exp.Column) and alias_inner.name.lower() == name
        if not candidates and alias_expr is not None and not is_passthrough:
            expr = alias_expr
            srcs = self._parser._extract_source_columns(expr, ctx, visited_forward_refs={name})
            srcs = {s for s in srcs if s}
            return (sorted(srcs), "select_alias") if srcs else ([], "no_upstream")
        if len(scope_tables) == 1 and not candidates:
            srcs, res = self._sources_for_table(scope_tables[0], name, ctx, derived)
            return srcs, "single_table" if res == "qualified" else res
        pool = candidates or unknown
        if not pool:
            return [], "unresolved"
        if len(pool) == 1 and not candidates:
            srcs, res = self._sources_for_table(pool[0], name, ctx, derived)
            return srcs, "unqualified_resolved" if res == "qualified" else res
        out: list[str] = []
        for table in pool:
            srcs, _ = self._sources_for_table(table, name, ctx, derived)
            out.extend(s for s in srcs if s not in out)
        return out, "ambiguous"

    # ------------------------------------------------------------------- emit

    def _emit(
        self,
        kind: str,
        col: exp.Column,
        select: exp.Select,
        ctx: ParserContext,
        derived: dict[str, exp.Select],
        scope: str,
        detail: str,
        predicate: exp.Expression | None,
        predicate_sql: str | None = None,
    ) -> None:
        sources, resolution = self._resolve(col, ctx, derived)
        if predicate_sql is None and predicate is not None:
            predicate_sql = _sql(predicate, self._dialect)
        self._emit_resolved(
            kind,
            _sql(col, self._dialect).lower(),
            scope,
            detail,
            predicate_sql,
            (sources, resolution),
        )

    def _window_spec_sql(self, window: exp.Window) -> str:
        """``PARTITION BY … ORDER BY …`` of *window* — the part a window usage depends on.

        The windowed function is projected lineage; leaving it out makes every function
        over the same specification collapse into one usage row.
        """
        parts = []
        partition = window.args.get("partition_by") or []
        if partition:
            parts.append("PARTITION BY " + ", ".join(_sql(p, self._dialect) for p in partition))
        order = window.args.get("order")
        if order is not None:
            parts.append(_sql(order, self._dialect))
        return f"OVER ({' '.join(parts)})"

    def _emit_resolved(
        self,
        kind: str,
        column_ref: str,
        scope: str,
        detail: str,
        predicate_sql: str | None,
        resolved: tuple[Iterable[str], str],
    ) -> None:
        sources, resolution = resolved
        sources = list(sources)
        if not sources and resolution == "no_upstream":
            # Literal / constant-only column: it adds no upstream requirement.
            return
        sources = sources or [""]
        for source in sources:
            row = ColumnUsage(
                usage=kind,  # type: ignore[arg-type]
                source=source.lower(),
                column_ref=column_ref,
                scope=scope,
                detail=detail,
                predicate_sql=predicate_sql,
                resolution=resolution,  # type: ignore[arg-type]
            )
            self._rows.setdefault(row.sort_key(), row)
