import re
from sqlglot import exp
from typing import Dict, List, Optional, Any


def strip_sql_comments(text: str) -> str:
    """Remove SQL comments from a string.

    Removes both /* ... */ and -- style comments.
    Normalizes whitespace (multiple spaces become single space).
    """
    if not text:
        return text

    # Remove /* ... */ style comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

    # Remove -- style comments (everything after -- until end of line)
    text = re.sub(r"--.*?$", "", text, flags=re.MULTILINE)

    # Normalize whitespace (multiple spaces/tabs/newlines become single space)
    text = re.sub(r"\s+", " ", text)

    # Clean up any extra whitespace that might be left
    return text.strip()


def get_table_aliases(parsed: Any) -> Dict[str, str]:
    aliases = {}
    for table in parsed.find_all((exp.Table, exp.From, exp.Join)):
        if table.alias:
            aliases[table.alias] = str(table.name).lower()
    return aliases


def get_scoped_table_aliases(scope_root: Any) -> Dict[str, str]:
    """Collect table aliases belonging to *scope_root*'s own SQL scope.

    Unlike :func:`get_table_aliases`, aliases declared inside nested scopes —
    CTE definitions and subqueries below *scope_root* — are skipped, so an
    inner ``FROM customers o`` can neither shadow nor leak into the outer
    SELECT's alias map (the "global alias map" wrong-edge bug).
    """
    aliases: Dict[str, str] = {}
    for table in scope_root.find_all((exp.Table, exp.From, exp.Join)):
        if not table.alias:
            continue
        parent = table.parent
        nested = False
        while parent is not None and parent is not scope_root:
            # Any intermediate SELECT (a WHERE/SELECT-clause subquery is a bare
            # exp.Select under e.g. exp.Exists), derived table, or CTE definition
            # opens a nested scope whose aliases must not leak out.
            if isinstance(parent, (exp.CTE, exp.Subquery, exp.Select)):
                nested = True
                break
            parent = parent.parent
        if not nested:
            aliases[table.alias] = str(table.name).lower()
    return aliases


def get_table_context(select: Any) -> str:
    from_clause = select.find(exp.From)
    if from_clause:
        table = from_clause.find(exp.Table)
        if table:
            return str(table.name).lower()

        subquery = from_clause.find(exp.Subquery)
        if subquery:
            subquery_select = subquery.find(exp.Select)
            if subquery_select:
                return get_table_context(subquery_select)
    return ""


def get_all_tables_from_select(select: Any) -> List[str]:
    tables = []
    from_clause = select.find(exp.From)
    if from_clause:
        table = from_clause.find(exp.Table)
        if table:
            tables.append(str(table.name).lower())

    for join in select.find_all(exp.Join):
        if hasattr(join, "this"):
            join_table = join.this
            if isinstance(join_table, exp.Table):
                tables.append(str(join_table.name).lower())
            elif hasattr(join_table, "name"):
                tables.append(str(join_table.name).lower())

    return tables


def get_scoped_tables_from_select(select: Any) -> List[str]:
    """FROM + JOIN table names belonging to *select*'s own scope, in order.

    Reads the select's own ``from`` / ``joins`` args directly instead of
    ``find_all``, so tables inside CTE definitions or subqueries are excluded.
    """
    tables: List[str] = []
    from_clause = select.args.get("from")
    if from_clause is not None and isinstance(from_clause.this, exp.Table):
        tables.append(str(from_clause.this.name).lower())
    for join in select.args.get("joins") or []:
        join_table = join.this
        if isinstance(join_table, exp.Table):
            tables.append(str(join_table.name).lower())
        elif hasattr(join_table, "name"):
            tables.append(str(join_table.name).lower())
    return tables


def get_final_select(parsed: Any) -> Optional[Any]:
    # UNION / UNION ALL: both branches contribute columns — return None so the caller
    # handles them as a union-type (multi-source). exp.Union covers both variants.
    if isinstance(parsed, exp.Union):
        return None
    # INTERSECT / EXCEPT: result columns are defined by the first (left) branch;
    # the right branch is a filter. Walk into .this to return that first SELECT.
    # (exp.Intersect and exp.Except are NOT subclasses of exp.Union in sqlglot.)

    query = parsed
    while hasattr(query, "this") and query.this:
        query = query.this

    if isinstance(query, exp.Select):
        return query

    if isinstance(query, exp.Query):
        return query.this if isinstance(query.this, exp.Select) else None

    return None


def split_qualified_name(qualified_name: str) -> tuple[str, str]:
    """Split a qualified name into table and column parts, stripping SQL comments."""
    if "." not in qualified_name:
        return ("", strip_sql_comments(qualified_name))
    qualified_name = strip_sql_comments(qualified_name)
    parts = qualified_name.split(".")
    table_part = ".".join(parts[:-1])
    column_part = strip_sql_comments(parts[-1])
    return (table_part, column_part)
