"""Clean public API for column lineage resolution.

This is the stable contract consumed by dbt-osmosis and other integrators.
Do not break the signatures of ``ColumnLineageResult`` or ``get_column_lineage``.
"""
from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass, field
from typing import List, Literal, Optional, Tuple

from dbt_osmosis_cll.cll_generator.artifacts.exceptions import CompiledSqlMissingError

logger = logging.getLogger(__name__)

# dbt Jinja context objects that can appear as table qualifiers in compiled SQL
# (e.g. `{{ target.schema }}.TABLE` compiles to a raw table reference whose alias
# may be left as `target` or `tar`).  These are never dbt model names, so any
# progenitor that resolves to one of these strings is a phantom node and must be
# dropped.
_JINJA_RESERVED: frozenset[str] = frozenset(
    {"target", "this", "model", "config", "var", "env_var", "builtins", "flags"}
)

# Process-level cache of fully-loaded registries.  Building a registry parses the
# compiled SQL of EVERY model in the project, so without this cache a per-model
# caller (e.g. dbt-osmosis resolving lineage one node at a time) re-parses the
# whole project on every call — O(N^2) over the model count.  Safe to cache for
# the lifetime of the process because the manifest and compiled SQL are immutable
# during a run.  Each entry is (registry, terminal_node_names).
_REGISTRY_CACHE: dict = {}


def clear_lineage_caches() -> None:
    """Clear the process-level registry cache.

    Call this if the manifest or compiled SQL changes within a single long-lived
    process (e.g. between test cases that rebuild artifacts at the same path).
    """
    _REGISTRY_CACHE.clear()


@dataclass
class ColumnLineageResult:
    """Fully-resolved lineage for a single model column."""

    model: str
    """Short model name (e.g. ``stg_orders``)."""

    column: str
    """Column name (lowercase)."""

    progenitor_model: Optional[str]
    """Direct upstream model that provides this column's value, or None for source columns."""

    progenitor_column: Optional[str]
    """Column name in *progenitor_model*, or None."""

    is_rename: bool
    """True when this column is a pure alias of a single upstream column with no transformation."""

    source_column: Optional[str]
    """Original column name before the rename, or None when *is_rename* is False."""

    is_computed: bool = False
    """True when the column is a multi-source or opaque expression (COALESCE(a,b), CASE with
    multiple source columns, etc.) with no single traceable upstream column."""

    is_aggregate: bool = False
    """True when the column is produced by an aggregate function (SUM, COUNT, AVG, MAX, MIN…)."""

    is_window: bool = False
    """True when the column is produced by a window function (ROW_NUMBER, RANK, SUM OVER…)."""

    is_literal: bool = False
    """True when the column is a hardcoded constant ('SAP', 42, TRUE, NULL)."""

    is_union: bool = False
    """True when the column originates from a top-level UNION ALL / UNION / INTERSECT / EXCEPT."""

    is_generated: bool = False
    """True when the column is produced by a zero-argument system function with no column inputs
    (CURRENT_DATE, SYSDATE, UUID_STRING, RANDOM, SEQ4, etc.)."""

    literal_value: Optional[str] = None
    """The string representation of the constant when ``is_literal`` is True, else None."""

    generated_value: Optional[str] = None
    """The expression string when ``is_generated`` is True (e.g. 'CURRENT_DATE', 'UUID_STRING()')."""

    is_first_in_chain: bool = False
    """True when this column has no traceable upstream dbt model and is not computed.

    This marks columns that sit at the origin of the lineage graph — typically staging
    models that pull directly from raw source tables.  Useful for visualisation to
    distinguish "we traced it all the way back" from "lineage stops here".

    A column is first-in-chain when ``progenitor_model is None`` and ``is_computed`` is
    False (pure unknowns from parse failures also land here, so treat as a soft signal).
    """

    unique_id: Optional[str] = None
    """Manifest unique_id of *model* (e.g. ``model.my_pkg.stg_orders``), when known.

    Lets consumers disambiguate results when two manifest entries share a short
    name (cross-package models, model vs source identifier). Additive — older
    cached results deserialize with ``None``."""

    union_branches: List[Tuple[str, str]] = field(default_factory=list)
    """When ``is_union`` is True: one ``(progenitor_model, progenitor_column)`` tuple per
    UNION / INTERSECT / EXCEPT branch in declaration order. Empty when the column is
    not produced by a set operation.

    Consumers that need to make a decision about which upstream description to inherit
    (dbt-osmosis: agreement-based propagation) iterate this list and apply their own
    conflict-resolution policy. The single-progenitor fields (``progenitor_model``,
    ``progenitor_column``) are intentionally left as ``None`` for union columns since
    no single branch can claim canonical status."""

    progenitors: List[Tuple[str, str]] = field(default_factory=list)
    """ALL direct ``(model, column)`` inputs of this column — the generalization of
    ``union_branches`` to every transformation kind:

    - single-source columns: ``[(progenitor_model, progenitor_column)]``
    - multi-source computed columns (``COALESCE(a.x, b.y)``, arithmetic over two
      tables, …): one pair per contributing source column, sorted by source string
    - union columns: identical to ``union_branches`` (declaration order)

    Lets consumers state what feeds a computed endpoint column ("Computed here from
    A.X, B.Y") instead of a bare "no single progenitor". Additive — older cached
    results deserialize with an empty list."""


def get_column_lineage(
    manifest_path: str,
    catalog_path: Optional[str] = None,
    live_db: bool = False,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
    target: Optional[str] = None,
    models: Optional[List[str]] = None,
    dialect: Optional[str] = None,
    compiled_sql_source: Literal["manifest", "target_dir", "auto_compile"] = "manifest",
    include_ephemeral: bool = False,
    _catalog_reader_override: Optional[object] = None,
    placeholder_patterns: Optional[List[str]] = None,
) -> List[ColumnLineageResult]:
    """Resolve column-level lineage for dbt models.

    Exactly one of *catalog_path* or *live_db=True* must be supplied to
    provide column schema information.  Both cannot be set simultaneously.

    Args:
        manifest_path: Path to ``manifest.json``.
        catalog_path: Path to ``catalog.json`` (mutually exclusive with *live_db*).
        live_db: When True, query the live database for column schemas via the
            dbt adapter (requires *profiles_dir* and a dbt project at *project_dir*).
        project_dir: dbt project root directory.  Required when *live_db=True* or
            *compiled_sql_source="auto_compile"*.
        profiles_dir: Directory containing ``profiles.yml``.  Defaults to the
            current directory when *live_db=True*.
        target: dbt target profile name.  Uses profile default when omitted.
        models: Optional list of model **names** (not node IDs) to restrict
            results to.  When None, all models in the manifest are included.
        dialect: SQL dialect override (e.g. ``"snowflake"``).  When None, the
            adapter type from the manifest is used automatically.
        compiled_sql_source: Controls how compiled SQL is obtained when the
            manifest does not contain inline ``compiled_code``.

            ``"manifest"`` *(default)* — only use inline compiled SQL embedded
            by ``dbt compile`` or ``dbt run``.  Raises
            :exc:`~dbt_osmosis_cll.cll_generator.artifacts.exceptions.CompiledSqlMissingError`
            if the manifest was produced by ``dbt parse`` and contains no
            compiled SQL.

            ``"target_dir"`` — fall back to ``.sql`` files in
            ``target/compiled/`` when inline compiled SQL is absent.

            .. warning::
                These files may be **stale** if models have changed since the
                last ``dbt compile`` run.  Column lineage may be inaccurate.

            ``"auto_compile"`` — run ``dbt compile`` automatically before
            resolving lineage.  Requires *project_dir*.  The freshly compiled
            manifest is then used, guaranteeing accuracy.

    Returns:
        List of :class:`ColumnLineageResult` — one entry per (model, column) pair
        with resolved lineage.  Columns whose lineage cannot be parsed are omitted
        silently (the parser logs a warning for each).

    Raises:
        ValueError: When both *catalog_path* and *live_db* are supplied, or
            neither is supplied; or when *compiled_sql_source="auto_compile"*
            but *project_dir* is not provided.
        CompiledSqlMissingError: When *compiled_sql_source="manifest"* and the
            manifest contains no inline compiled SQL.
        RuntimeError: When the dbt adapter cannot be bootstrapped (live_db mode)
            or when ``dbt compile`` fails (auto_compile mode).
        FileNotFoundError: When *manifest_path* or *catalog_path* do not exist.
    """
    if _catalog_reader_override is None:
        if catalog_path and live_db:
            raise ValueError("Provide either catalog_path or live_db=True, not both.")
        if not catalog_path and not live_db:
            raise ValueError("Either catalog_path or live_db=True is required.")

    if compiled_sql_source == "auto_compile":
        if not project_dir:
            raise ValueError(
                "project_dir is required when compiled_sql_source='auto_compile'."
            )
        _run_dbt_compile(project_dir=project_dir, profiles_dir=profiles_dir, target=target)

    # Pre-flight: check that the manifest has inline compiled SQL (unless caller
    # opted into a fallback strategy).
    if compiled_sql_source == "manifest":
        _assert_compiled_sql_present(manifest_path)

    use_target_dir = compiled_sql_source in ("target_dir", "auto_compile")

    registry, terminal_node_names = _load_registry_cached(
        manifest_path=manifest_path,
        catalog_path=catalog_path,
        live_db=live_db,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        dialect=dialect,
        use_target_dir=use_target_dir,
        stop_at_ephemeral=include_ephemeral,
        placeholder_patterns=placeholder_patterns,
        catalog_reader_override=_catalog_reader_override,
    )

    all_nodes = registry.get_models_by_id()

    model_filter = {m.lower() for m in models} if models else None
    results: List[ColumnLineageResult] = []

    # When a model filter is supplied, iterate only the requested models instead of
    # every node in the project — keeps per-call work proportional to the request.
    # Names that collide across packages match EVERY model with that name; each
    # result row carries its unique_id so consumers can disambiguate.
    iter_items = sorted(
        (
            (model_obj.name.lower(), model_obj)
            for model_obj in all_nodes.values()
            if model_filter is None or model_obj.name.lower() in model_filter
        ),
        key=lambda kv: (kv[0], kv[1].unique_id or ""),
    )

    for model_name, model_obj in iter_items:
        if model_obj.resource_type not in ("model",):
            continue

        for col_name, col_obj in sorted(model_obj.columns.items()):
            if not col_obj.lineage:
                results.append(
                    ColumnLineageResult(
                        model=model_name,
                        column=col_name,
                        progenitor_model=None,
                        progenitor_column=None,
                        is_rename=False,
                        source_column=None,
                        is_computed=False,
                        is_first_in_chain=True,
                        unique_id=model_obj.unique_id,
                    )
                )
                continue

            # Take the first lineage entry (highest precedence after parsing)
            lin = col_obj.lineage[0]
            progenitor_model, progenitor_column = _resolve_progenitor(lin)
            ttype = lin.transformation_type
            is_computed   = ttype == "derived"
            is_aggregate  = ttype == "aggregate"
            is_window     = ttype == "window"
            is_literal    = ttype == "literal"
            is_union      = ttype == "union"
            is_generated  = ttype == "generated"
            literal_value   = lin.sql_expression if is_literal  else None
            generated_value = lin.sql_expression if is_generated else None
            # Multiple source columns → no single traceable progenitor
            if len(lin.source_columns) > 1:
                progenitor_model, progenitor_column = None, None
            # Union branches: split each "table.column" into a (model, col) tuple
            # so downstream consumers can iterate without re-parsing strings.
            union_branches: List[Tuple[str, str]] = []
            if is_union:
                for branch in lin.union_branches:
                    if "." in branch:
                        m, c = branch.rsplit(".", 1)
                        union_branches.append((m.lower(), c.lower()))
            # progenitors: ALL direct (model, column) inputs (generalized
            # union_branches). Unions reuse their branch pairs; everything else
            # derives from the full source-column set (preserved through CTE
            # hops by the parser).
            if is_union:
                progenitors: List[Tuple[str, str]] = list(union_branches)
            else:
                progenitors = []
                for src in sorted(lin.source_columns):
                    if src and "." in src:
                        m, c = src.rsplit(".", 1)
                        m = m.lower()
                        # Same ephemeral-prefix strip as _resolve_progenitor.
                        if m.startswith("__dbt__cte__"):
                            m = m[len("__dbt__cte__"):]
                        # Drop phantom nodes from Jinja context leakage.
                        if m in _JINJA_RESERVED:
                            continue
                        progenitors.append((m, c.lower()))
            # first-in-chain: only for pure passthroughs (direct/renamed) that reach a terminal node
            is_passthrough = ttype in ("direct", "renamed")
            is_first = (
                progenitor_model is None or progenitor_model in terminal_node_names
            ) and is_passthrough

            results.append(
                ColumnLineageResult(
                    model=model_name,
                    column=col_name,
                    progenitor_model=progenitor_model,
                    progenitor_column=progenitor_column,
                    is_rename=lin.is_rename,
                    source_column=lin.source_column,
                    is_computed=is_computed,
                    is_aggregate=is_aggregate,
                    is_window=is_window,
                    is_literal=is_literal,
                    is_union=is_union,
                    is_generated=is_generated,
                    literal_value=literal_value,
                    generated_value=generated_value,
                    is_first_in_chain=is_first,
                    union_branches=union_branches,
                    progenitors=progenitors,
                    unique_id=model_obj.unique_id,
                )
            )

    # Emit ephemeral model rows when include_ephemeral=True.
    # Each __dbt__cte__<model_name> is stripped of its prefix to surface as
    # a visible intermediate node in the lineage graph.
    if include_ephemeral:
        for cte_name, cte_cols in sorted(registry.get_ephemeral_lineage().items()):
            # Strip dbt's injection prefix: __dbt__cte__model_name → model_name
            ephemeral_model_name = cte_name.lstrip("_").replace("dbt__cte__", "", 1)
            if model_filter and ephemeral_model_name not in model_filter:
                continue
            for col_name, lin in sorted(cte_cols.items()):
                progenitor_model, progenitor_column = _resolve_progenitor(lin)
                is_computed = lin.transformation_type == "derived"
                if is_computed and len(lin.source_columns) > 1:
                    progenitor_model, progenitor_column = None, None
                is_first = (
                    progenitor_model is None or progenitor_model in terminal_node_names
                ) and not is_computed
                results.append(
                    ColumnLineageResult(
                        model=ephemeral_model_name,
                        column=col_name,
                        progenitor_model=progenitor_model,
                        progenitor_column=progenitor_column,
                        is_rename=lin.is_rename,
                        source_column=lin.source_column,
                        is_computed=is_computed,
                        is_first_in_chain=is_first,
                    )
                )

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_registry_cached(
    *,
    manifest_path: str,
    catalog_path: Optional[str],
    live_db: bool,
    project_dir: Optional[str],
    profiles_dir: Optional[str],
    target: Optional[str],
    dialect: Optional[str],
    use_target_dir: bool,
    stop_at_ephemeral: bool,
    placeholder_patterns: Optional[List[str]],
    catalog_reader_override: Optional[object],
):
    """Return a fully-loaded (registry, terminal_node_names), using the process cache.

    Loading parses every model's compiled SQL, so the result is cached and reused
    across calls for the same project/parameters within a process.
    """
    # Discriminate the catalog source so different inputs never share a registry.
    if catalog_reader_override is not None:
        catalog_disc: tuple = ("override", id(catalog_reader_override))
    elif live_db:
        catalog_disc = ("livedb", project_dir, profiles_dir, target)
    else:
        catalog_disc = ("catalog", catalog_path)

    cache_key = (
        manifest_path,
        dialect,
        use_target_dir,
        stop_at_ephemeral,
        tuple(placeholder_patterns) if placeholder_patterns else None,
        catalog_disc,
    )
    cached = _REGISTRY_CACHE.get(cache_key)
    if cached is not None:
        return cached

    catalog_reader = catalog_reader_override or _build_catalog_reader(
        manifest_path=manifest_path,
        catalog_path=catalog_path,
        live_db=live_db,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
    )

    from dbt_osmosis_cll.cll_generator.artifacts.registry import ModelRegistry

    registry = ModelRegistry(
        catalog_path=None,       # type: ignore[arg-type]  # overridden below
        manifest_path=manifest_path,
        adapter_override=dialect,
        _catalog_reader_override=catalog_reader,
        use_target_dir_fallback=use_target_dir,
        stop_at_ephemeral=stop_at_ephemeral,
        placeholder_patterns=placeholder_patterns,
    )
    registry.load()

    # Terminal node names (sources + seeds) — columns whose progenitor is one of
    # these sit at the origin of the lineage graph (first-in-chain).  Computed once
    # here so per-call result construction does not re-scan every node.
    all_nodes = registry.get_models()
    terminal_node_names = frozenset(
        name for name, node in all_nodes.items()
        if node.resource_type in ("source", "seed")
    )

    _REGISTRY_CACHE[cache_key] = (registry, terminal_node_names)
    return registry, terminal_node_names


def _build_catalog_reader(
    manifest_path: str,
    catalog_path: Optional[str],
    live_db: bool,
    project_dir: Optional[str],
    profiles_dir: Optional[str],
    target: Optional[str],
):
    if live_db:
        from dbt_osmosis_cll.cll_generator.artifacts.live_db import LiveDbCatalogReader

        return LiveDbCatalogReader(
            manifest_path=manifest_path,
            project_dir=project_dir or ".",
            profiles_dir=profiles_dir or ".",
            target=target,
        )

    from dbt_osmosis_cll.cll_generator.artifacts.catalog import CatalogReader

    return CatalogReader(catalog_path=catalog_path)  # type: ignore[arg-type]


def _assert_compiled_sql_present(manifest_path: str) -> None:
    """Raise CompiledSqlMissingError if the manifest has no inline compiled SQL.

    Inspects the manifest before loading the registry so the error fires early
    with a clear, actionable message instead of silently producing empty lineage.
    """
    from dbt_osmosis_cll.cll_generator.artifacts.manifest import ManifestReader

    reader = ManifestReader(manifest_path)
    reader.load()
    if not reader.has_inline_compiled_sql():
        raise CompiledSqlMissingError(
            "The manifest at '{}' was generated by 'dbt parse' and contains no "
            "compiled SQL. Column lineage cannot be resolved without compiled SQL.\n\n"
            "Fix options:\n"
            "  1. Run 'dbt compile' in your project, then retry with the default "
            "compiled_sql_source='manifest'.\n"
            "  2. Pass compiled_sql_source='auto_compile' and project_dir=<path> "
            "to let dbt-column-lineage run 'dbt compile' automatically.\n"
            "  3. Pass compiled_sql_source='target_dir' to use previously compiled "
            "files under target/compiled/ (may be stale).".format(manifest_path)
        )


def _run_dbt_compile(
    project_dir: str,
    profiles_dir: Optional[str],
    target: Optional[str],
) -> None:
    """Run ``dbt compile`` as a subprocess and raise RuntimeError on failure."""
    cmd = [sys.executable, "-m", "dbt", "compile", "--project-dir", project_dir]
    if profiles_dir:
        cmd += ["--profiles-dir", profiles_dir]
    if target:
        cmd += ["--target", target]

    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"'dbt compile' failed (exit {result.returncode}).\n"
            f"stdout: {result.stdout[-2000:]}\n"
            f"stderr: {result.stderr[-2000:]}"
        )
    logger.info("'dbt compile' completed successfully.")


def _resolve_progenitor(lin) -> tuple[Optional[str], Optional[str]]:
    """Extract (model, column) from the first source_column entry of a ColumnLineage.

    Strips the ``__dbt__cte__`` prefix dbt injects for ephemeral models so that
    progenitor_model refers to the human-readable model name.
    """
    if not lin.source_columns:
        return None, None

    src = next(iter(sorted(lin.source_columns)))
    if "." not in src:
        return None, src.lower()

    parts = src.rsplit(".", 1)
    model_part = parts[0].lower()
    # Strip dbt's ephemeral CTE injection prefix
    if model_part.startswith("__dbt__cte__"):
        model_part = model_part[len("__dbt__cte__"):]
    # Reject Jinja context objects that leak into compiled SQL as table qualifiers
    # (e.g. `target`, `this`).  They are never real dbt model names.
    if model_part in _JINJA_RESERVED:
        return None, None
    return model_part, parts[1].lower()
