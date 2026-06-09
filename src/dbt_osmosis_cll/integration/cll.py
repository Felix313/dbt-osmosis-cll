"""Shared column-level lineage (CLL) integration for dbt-osmosis.

All CLL calls funnel through :func:`get_cll_results`, which provides a
two-tier cache and graceful fallback:

  1. In-memory ``_LINEAGE_CACHE`` — process-scoped, instant on repeated access.
  2. Persistent ``target/cll_cache.json`` — keyed by compiled SQL hash, survives
     across osmosis invocations so unchanged models skip re-analysis.
  3. CLL computation via ``ManifestCatalogReader`` — no live DB connection;
     column lists come from source/model YMLs already in the manifest.

Both ``inheritance.py`` (disambiguation) and ``transforms.py`` (origin
enrichment) import from here.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import types
import typing as t
from pathlib import Path

logger = logging.getLogger(__name__)

from dbt_osmosis_cll.config import get_config

if t.TYPE_CHECKING:
    from dbt.contracts.graph.nodes import ResultNode
    from dbt_osmosis_cll.osmosis_propagation.dbt_protocols import YamlRefactorContextProtocol

_CACHE_SCHEMA_VERSION = 3

# (project_dir, model_name) → List[result]  in-memory, process-scoped
_LINEAGE_CACHE: dict[tuple[str, str], list[t.Any]] = {}

# project_dir → ManifestCatalogReader  (warmed once per run; cheap — just JSON)
_READER_CACHE: dict[str, t.Any] = {}

# project_dir → {name_lower: node}  for O(1) manifest lookups in get_column_origin
_SOURCE_INDEX: dict[str, dict[str, t.Any]] = {}
_NODE_INDEX: dict[str, dict[str, t.Any]] = {}

# project_dir → {(db_upper, schema_upper, identifier_upper): source_node}
# Built once per project from the manifest. Used by Phase 4 CLL-driven inheritance
# to resolve compiled-SQL database references back to dbt source nodes.
_SOURCE_REVERSE_INDEX: dict[str, dict[tuple[str, str, str], t.Any]] = {}

# (project_dir, model_name_lower, column_name_lower) → origin tuple or None
_ORIGIN_CACHE: dict[tuple[str, str, str], tuple[str, str, str, str] | None] = {}

# project_dir → {model_name: {"compiled_sql_hash": str, "results": [dict]}}
_DISK_CACHE: dict[str, dict[str, t.Any]] = {}

# project_dir → set of model names for which CLL failed during this run
_CLL_FAILURES: dict[str, set[str]] = {}

# project_dir → {reason: set of "MODEL.COLUMN"} for origin-walk soft-fails (cycle / max-depth)
_CLL_WALK_SOFT_FAILS: dict[str, dict[str, set[str]]] = {}

_CACHE_LOCK = threading.Lock()
_FAILURES_LOCK = threading.Lock()

# Fields we read from ColumnLineageResult — serialised/deserialised for disk cache
_RESULT_FIELDS = (
    "model", "column", "is_computed", "progenitor_model", "progenitor_column",
    "is_first_in_chain", "is_rename", "source_column",
    "is_aggregate", "is_window", "is_literal", "is_union", "is_generated",
    "literal_value", "generated_value",
)


# ---------------------------------------------------------------------------
# Disk cache helpers
# ---------------------------------------------------------------------------

def _disk_cache_path(project_dir: str) -> Path:
    cfg_path = get_config().cll_cache_path
    p = Path(cfg_path)
    return p if p.is_absolute() else Path(project_dir) / p


def _load_disk_cache(project_dir: str) -> dict[str, t.Any]:
    """Return the in-memory disk cache for *project_dir*, loading from disk on first access."""
    if project_dir in _DISK_CACHE:
        return _DISK_CACHE[project_dir]
    path = _disk_cache_path(project_dir)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("schema_version") == _CACHE_SCHEMA_VERSION:
                entries = data.get("entries", {})
                _DISK_CACHE[project_dir] = entries
                logger.info("CLL cache — warm (%d entries)", len(entries))
                return _DISK_CACHE[project_dir]
        except Exception as exc:
            logger.debug("CLL disk cache load failed (will rebuild): %s", exc)
    logger.info("CLL cache — cold (no cache found, first run will be slow)")
    _DISK_CACHE[project_dir] = {}
    return _DISK_CACHE[project_dir]


def _save_disk_cache(project_dir: str) -> None:
    """Atomically flush the in-memory disk cache for *project_dir* to disk."""
    path = _disk_cache_path(project_dir)
    tmp = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": _CACHE_SCHEMA_VERSION,
            "entries": _DISK_CACHE.get(project_dir, {}),
        }
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:
        logger.debug("CLL disk cache write failed: %s", exc)


def _compiled_sql_hash(project_dir: str, node: t.Any) -> str | None:
    """Return sha256 hex of *node*'s source SQL file, or None if absent.

    We hash the source .sql file (not the compiled artifact) because compiled
    SQL changes on every dbt compile when Jinja renders dynamic values such as
    batch timestamps — causing false cache invalidation. Column-level lineage
    depends only on SELECT structure, which only changes when the source SQL
    actually changes.
    """
    original = getattr(node, "original_file_path", None)
    if original and original.endswith(".sql"):
        full = Path(project_dir) / original
        if full.exists():
            return hashlib.sha256(full.read_bytes()).hexdigest()
    return None


def _is_compiled_sql_stale(project_dir: str, node: t.Any, target_base: "Path | None" = None) -> bool:
    """Return True if the compiled SQL artifact is older than the source SQL file,
    or if no compiled artifact exists yet.

    Uses file mtime only — does not check manifest compiled_code, which is
    unreliable (wiped by dbt parse, overwritten by per-model compiles).
    """
    original: str = getattr(node, "original_file_path", "") or ""
    if not original.endswith(".sql"):
        return False
    source_path = Path(project_dir) / original
    if not source_path.exists():
        return False

    # Locate compiled artifact — try with and without package-name nesting.
    # Use the configured target base (respects DBT_TARGET_PATH) when provided.
    compiled_dir = (target_base or Path(project_dir) / "target") / "compiled"
    package_name: str = getattr(node, "package_name", "") or ""
    candidates = [compiled_dir / package_name / original, compiled_dir / original]
    for compiled_path in candidates:
        if compiled_path.exists():
            return source_path.stat().st_mtime > compiled_path.stat().st_mtime
    # Compiled file doesn't exist at all — also stale
    return True




def _deserialize_results(raw: list[dict[str, t.Any]]) -> list[t.Any]:
    return [types.SimpleNamespace(**r) for r in raw]


def _serialize_results(results: list[t.Any]) -> list[dict[str, t.Any]]:
    return [{f: getattr(r, f, None) for f in _RESULT_FIELDS} for r in results]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cll_results(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
) -> list[t.Any]:
    """Return CLL results for *node*, using a two-tier cache.

    Cache hit order:
      1. In-memory _LINEAGE_CACHE (process-scoped, instant).
      2. Disk cache (target/cll_cache.json) keyed by compiled SQL hash —
         hits skip CLL computation for unchanged models across invocations.
      3. CLL via ManifestCatalogReader (no live DB; reads column lists from manifest).

    Returns [] on any failure so callers fall back to name-based inheritance.
    """
    from dbt.artifacts.resources.types import NodeType

    # Sources and seeds are leaf/terminal nodes with no compiled SQL — CLL has nothing to trace.
    if node.resource_type in (NodeType.Source, NodeType.Seed):
        return []

    runtime_cfg = context.project.runtime_cfg
    project_dir: str = str(runtime_cfg.project_root)
    # Use the configured target directory (respects DBT_TARGET_PATH env var).
    # project_target_path is the full absolute path to the target folder.
    target_base: Path = Path(getattr(runtime_cfg, "project_target_path", None) or Path(project_dir) / "target")
    cache_key = (project_dir, node.name)

    # 1. In-memory
    if cache_key in _LINEAGE_CACHE:
        return _LINEAGE_CACHE[cache_key]

    manifest_path = str(target_base / "manifest.json")
    sql_hash = _compiled_sql_hash(project_dir, node)

    # 2. Disk cache
    if sql_hash is not None:
        disk_cache = _load_disk_cache(project_dir)
        entry = disk_cache.get(node.name)
        if entry and entry.get("compiled_sql_hash") == sql_hash:
            results = _deserialize_results(entry["results"])
            _LINEAGE_CACHE[cache_key] = results
            logger.debug("CLL disk cache hit for %s", node.name)
            return results

    # 3. Compute via CLL.
    # Always read compiled SQL from target/compiled/ rather than manifest.compiled_code:
    # - manifest.compiled_code is wiped by dbt parse and overwritten by each per-model
    #   dbt compile call, causing race conditions in multi-model runs.
    # - target/compiled/ files are stable — dbt parse never touches them, and each
    #   compile only writes the models it was asked to compile.
    # Freshness is guaranteed by maybe_bulk_compile() running upfront before CLL.
    try:
        from dbt_osmosis_cll.cll_generator.api import get_column_lineage
        from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader

        if project_dir not in _READER_CACHE:
            reader = ManifestCatalogReader(manifest_path=manifest_path)
            reader.load()
            _READER_CACHE[project_dir] = reader

        adapter_type: str | None = getattr(
            getattr(runtime_cfg, "credentials", None), "type", None
        )

        from dbt_osmosis_cll.config import get_config
        _cfg = get_config()
        results = get_column_lineage(
            manifest_path=manifest_path,
            models=[node.name],
            compiled_sql_source="target_dir",
            dialect=adapter_type,
            _catalog_reader_override=_READER_CACHE[project_dir],
            placeholder_patterns=_cfg.compiled_sql_placeholder_patterns,
        )
    except Exception as exc:
        logger.warning(
            ":warning: CLL unavailable for %s: %s",
            node.unique_id,
            exc,
        )
        results = []
        with _FAILURES_LOCK:
            _CLL_FAILURES.setdefault(project_dir, set()).add(node.name)

    # Update both caches.
    # Never persist empty results: an empty list means CLL failed or the model
    # had no traceable columns at the time of the run (e.g. wrong package version,
    # transient SQL error).  Skipping empty entries forces a fresh CLL invocation
    # on the next run so transient failures don't poison the cache permanently.
    _LINEAGE_CACHE[cache_key] = results
    if sql_hash is not None and results:
        with _CACHE_LOCK:
            disk_cache = _load_disk_cache(project_dir)
            disk_cache[node.name] = {
                "compiled_sql_hash": sql_hash,
                "results": _serialize_results(results),
            }
            _save_disk_cache(project_dir)

    return results


def maybe_bulk_compile(context: "YamlRefactorContextProtocol") -> None:
    """Compile all in-scope models with stale compiled SQL in one ``dbt compile`` call.

    Runs upfront before CLL analysis so ``get_cll_results`` never needs to fall back
    to per-model compiles (each of which carries ~2-4 s of dbt startup overhead).
    When no models are stale this is a no-op.
    """
    import subprocess
    import sys

    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    try:
        from dbt.artifacts.resources import ModelNode  # dbt ≥ 1.8
    except ImportError:
        from dbt.contracts.graph.nodes import ModelNode  # type: ignore[no-redef]

    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    target_name: str | None = getattr(runtime_cfg, "target_name", None)
    target_base = Path(project_dir) / getattr(runtime_cfg, "target_path", "target")

    logger.info("Checking compiled SQL freshness for candidate nodes...")
    stale = [
        node
        for _, node in _iter_candidate_nodes(context)
        if isinstance(node, ModelNode)
        and _is_compiled_sql_stale(project_dir, node, target_base)
    ]

    if not stale:
        logger.info("All compiled SQL is up-to-date — skipping bulk compile.")
        return

    logger.info("%d stale model(s) — running bulk dbt compile upfront.", len(stale))

    select_str = " ".join(n.name for n in stale)
    # Windows CreateProcess has a ~32 767 char command-line limit; stay well below it.
    # When the select string is too long just compile everything (no --select).
    _MAX_SELECT_LEN = 8_000
    cmd = [sys.executable, "-m", "dbt.cli.main", "compile", "--project-dir", project_dir]
    if len(select_str) <= _MAX_SELECT_LEN:
        cmd += ["--select", select_str]
    else:
        logger.debug("select string too long (%d chars) — compiling all models.", len(select_str))
    if target_name:
        cmd += ["--target", target_name]

    logger.debug("$ %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_dir)
    if result.returncode != 0:
        logger.warning(
            ":warning: Bulk dbt compile failed (exit %d):\n%s",
            result.returncode,
            result.stderr or result.stdout,
        )
    else:
        logger.info("Bulk compile done (%d models).", len(stale))
        # Invalidate reader cache so ManifestCatalogReader picks up the refreshed manifest.
        _READER_CACHE.pop(project_dir, None)


def get_model_columns_from_cll(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
) -> dict[str, t.Any] | None:
    """Return ``{column_name: meta}`` for *node* from CLL, or ``None`` if unavailable.

    The returned dict is compatible with ``get_columns()`` output — values expose
    ``.comment``, ``.type``, and ``.index`` attributes so callers can use it as a
    drop-in replacement.  Column names are lowercased (as CLL returns them);
    ``inject_missing_columns`` / ``remove_columns_not_in_database`` handle
    case conversion via their ``output-to-upper`` / ``output-to-lower`` settings.

    Returns ``None`` (not empty dict) when CLL has no results so callers can
    distinguish "CLL unavailable, fall back to DB" from "model has zero columns".
    """
    results = get_cll_results(context, node)
    node_lower = node.name.lower()
    cols = [r for r in results if r.model.lower() == node_lower]
    if not cols:
        return None
    return {
        r.column: types.SimpleNamespace(comment="", type=None, index=i)
        for i, r in enumerate(cols)
    }


_CLL_COMPUTED_SENTINEL = "__computed__"
"""Sentinel value placed in the parent map for columns with no traceable upstream.

Two cases both receive this sentinel:

1. ``is_computed=True, progenitor_model=None`` — the SQL parser explicitly detected a
   derived expression (aggregate, multi-source COALESCE, etc.) with no single progenitor.

2. ``is_computed=False, progenitor_model=None, lineage=[]`` — the column exists in the
   catalog but the SQL parser found no lineage entry for it (complex multi-CTE joins,
   parser limitation).  CLL emits ``is_computed=False`` for this case even though the
   column is effectively "born" in this model.

In both cases there is no single ancestor to inherit documentation from.  The sentinel
tells ``_build_column_knowledge_graph`` to skip name-matching inheritance entirely —
``annotate_column_origins`` will attach a "computed in" annotation instead.
"""


def build_parent_map(results: list[t.Any], node_name: str) -> dict[str, str]:
    """Build ``{column_lower: direct_parent_model_lower}`` from CLL results.

    Columns where CLL resolved a concrete direct parent (``progenitor_model`` is set
    and ``is_computed`` is False) map to the parent model name (lowercase).

    Columns with no traceable upstream — whether explicitly computed (``is_computed=True``,
    ``progenitor_model=None``) or simply unresolvable (``is_computed=False``,
    ``progenitor_model=None``) — map to ``_CLL_COMPUTED_SENTINEL`` so callers skip
    name-matching inheritance for them rather than accidentally inheriting from an
    unrelated upstream column.
    """
    parent_map: dict[str, str] = {}
    node_name_lower = node_name.lower()
    for r in results:
        if r.model.lower() != node_name_lower:
            continue
        if r.progenitor_model is None:
            # No traceable upstream (computed or unresolvable) — sentinel either way.
            parent_map[r.column.lower()] = _CLL_COMPUTED_SENTINEL
            continue
        if r.is_computed:
            # Single-source computed (e.g. COALESCE(a, 0)) — parent model is known but the
            # column is derived. Still map it so a "derived from" annotation can fire.
            pass
        parent_map[r.column.lower()] = r.progenitor_model.lower()
    return parent_map


# ---------------------------------------------------------------------------
# Origin tracing
# ---------------------------------------------------------------------------

def _ensure_manifest_index(context: YamlRefactorContextProtocol) -> None:
    """Build name→node lookup dicts for sources and model nodes (once per project)."""
    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    if project_dir not in _SOURCE_INDEX:
        src_idx: dict[str, t.Any] = {}
        rev_idx: dict[tuple[str, str, str], t.Any] = {}
        for src_node in context.project.manifest.sources.values():
            identifier = (getattr(src_node, "identifier", None) or "").lower()
            name = (getattr(src_node, "name", None) or "").lower()
            # Index by identifier first (canonical); also by name when it differs,
            # using setdefault so identifier entries take priority on collision.
            if identifier:
                src_idx[identifier] = src_node
            if name and name != identifier:
                src_idx.setdefault(name, src_node)
            # Reverse index: (database, schema, identifier) → source_node.
            # Used by Phase 4 CLL-driven inheritance to resolve compiled-SQL
            # database references (e.g. EDW_DB_PROD.AE_AML.SOME_TABLE) back
            # to dbt source nodes for description lookup.
            db  = (getattr(src_node, "database",   None) or "").upper()
            sch = (getattr(src_node, "schema",     None) or "").upper()
            idf = (getattr(src_node, "identifier", None) or "").upper()
            if db and sch and idf:
                rev_idx[(db, sch, idf)] = src_node
        _SOURCE_INDEX[project_dir] = src_idx
        _SOURCE_REVERSE_INDEX[project_dir] = rev_idx
    if project_dir not in _NODE_INDEX:
        node_idx: dict[str, t.Any] = {}
        for n in context.project.manifest.nodes.values():
            name = getattr(n, "name", "").lower()
            if name:
                node_idx[name] = n
        _NODE_INDEX[project_dir] = node_idx


def get_source_node_by_relation(
    context: YamlRefactorContextProtocol,
    database: str,
    schema: str,
    identifier: str,
) -> t.Any | None:
    """Resolve a compiled-SQL database reference to a dbt source node.

    CLL progenitors for source tables are expressed as ``DATABASE.SCHEMA.TABLE``
    strings extracted from compiled SQL.  This function maps them back to the
    manifest source node so Phase 4 CLL-driven inheritance can look up column
    descriptions directly.

    Returns ``None`` when no matching source node exists (e.g. the table is an
    external reference not declared as a dbt source).
    """
    _ensure_manifest_index(context)
    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    key = (database.upper(), schema.upper(), identifier.upper())
    return _SOURCE_REVERSE_INDEX.get(project_dir, {}).get(key)


def get_cll_failures(context: YamlRefactorContextProtocol) -> frozenset[str]:
    """Return the set of model names for which CLL failed during this run."""
    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    with _FAILURES_LOCK:
        return frozenset(_CLL_FAILURES.get(project_dir, set()))


def clear_cll_failures(context: YamlRefactorContextProtocol) -> None:
    """Clear the CLL failure set for this project (call after emitting summary)."""
    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    with _FAILURES_LOCK:
        _CLL_FAILURES.pop(project_dir, None)


def record_cll_walk_soft_fail(
    context: YamlRefactorContextProtocol, reason: str, ref: str
) -> None:
    """Record a soft-fail from the origin/description walk for the end-of-run summary.

    Unlike a CLL *failure* (a model whose lineage could not be computed), a walk soft-fail is
    a column whose description/origin could not be fully resolved because the walk bailed out
    — it hit the depth limit (``reason="max-depth"``) or a genuine multi-node lineage cycle
    (``reason="cycle"``). The column simply resolves to no inherited description rather than
    erroring; collecting these lets the run end with one actionable summary instead of silence.
    ``ref`` is the ``MODEL.COLUMN`` where the guard tripped.
    """
    project_dir = str(context.project.runtime_cfg.project_root)
    with _FAILURES_LOCK:
        _CLL_WALK_SOFT_FAILS.setdefault(project_dir, {}).setdefault(reason, set()).add(ref)


def get_cll_walk_soft_fails(
    context: YamlRefactorContextProtocol,
) -> dict[str, frozenset[str]]:
    """Return ``{reason: {MODEL.COLUMN, ...}}`` of origin-walk soft-fails for this project."""
    project_dir = str(context.project.runtime_cfg.project_root)
    with _FAILURES_LOCK:
        return {
            reason: frozenset(refs)
            for reason, refs in _CLL_WALK_SOFT_FAILS.get(project_dir, {}).items()
        }


def clear_cll_walk_soft_fails(context: YamlRefactorContextProtocol) -> None:
    """Clear the origin-walk soft-fail registry for this project (after emitting summary)."""
    project_dir = str(context.project.runtime_cfg.project_root)
    with _FAILURES_LOCK:
        _CLL_WALK_SOFT_FAILS.pop(project_dir, None)


def is_computation_wall(result: t.Any) -> bool:
    """True when *result* describes a column whose value is BORN in its own model.

    A union, aggregate, window, literal or generated column, or a multi-source
    expression (``is_computed`` with no single progenitor column). The value is
    produced *here*, so no single upstream column is its origin and the lineage walk
    must not trace through it. BOTH lineage walkers stop at this same wall set:
    ``_resolve_cll_description`` returns the locally-owned description (its step 6),
    and ``get_column_origin`` returns the ``computed in: SCHEMA.MODEL`` sentinel.
    Sharing one predicate is what keeps the description tracer and the annotation
    tracer from drifting on *where a column is computed* (the bug this replaces:
    the annotation tracer instead stopped at the first described ancestor).
    """
    return bool(
        getattr(result, "is_union", False)
        or getattr(result, "is_aggregate", False)
        or getattr(result, "is_window", False)
        or getattr(result, "is_literal", False)
        or getattr(result, "is_generated", False)
        or (
            getattr(result, "is_computed", False)
            and not (getattr(result, "progenitor_column", None) or "")
        )
    )


def get_column_origin(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
    column_name: str,
    _depth: int = 0,
) -> tuple[str, str, str, str] | None:
    """Trace *column_name* in *node* back to its ultimate source-system origin.

    Recursively follows ``progenitor_model → progenitor_column`` through CLL
    results until a source node or a first-in-chain model is reached.

    Single-source computed columns (e.g. TRY_TO_NUMBER(col)) are followed
    through to their origin. Only truly multi-source columns (progenitor_column
    is None while is_computed is True) return None.

    Returns ``(SCHEMA, MODEL_NAME, COLUMN_NAME)`` all uppercased, or ``None``
    when the chain cannot be resolved.
    """
    max_depth = get_config().cll_max_origin_depth
    if _depth > max_depth:
        logger.warning(
            "CLL origin depth %d exceeded for %s.%s — treating as computed "
            "(no description will be inherited). Raise cll-max-origin-depth in "
            "the .osmosis [osmosis] section if this column has legitimate deep lineage.",
            max_depth,
            node.name,
            column_name,
        )
        return None

    # Origin is deterministic — cache to avoid recomputing across downstream models
    if _depth == 0:
        _ensure_manifest_index(context)
        runtime_cfg = context.project.runtime_cfg
        project_dir = str(runtime_cfg.project_root)
        origin_key = (project_dir, node.name.lower(), column_name.lower())
        if origin_key in _ORIGIN_CACHE:
            return _ORIGIN_CACHE[origin_key]
        result = _compute_column_origin(context, node, column_name, project_dir)
        _ORIGIN_CACHE[origin_key] = result
        return result

    return _compute_column_origin(context, node, column_name, _depth=_depth)


def _compute_column_origin(
    context: YamlRefactorContextProtocol,
    node: t.Any,
    column_name: str,
    project_dir: str | None = None,
    _depth: int = 0,
) -> tuple[str, str, str, str] | None:
    """Internal recursive implementation for :func:`get_column_origin`."""
    max_depth = get_config().cll_max_origin_depth
    if _depth > max_depth:
        logger.warning(
            "CLL origin depth %d exceeded for %s.%s — treating as computed "
            "(no description will be inherited). Raise cll-max-origin-depth in "
            "the .osmosis [osmosis] section if this column has legitimate deep lineage.",
            max_depth,
            node.name,
            column_name,
        )
        return None

    node_lower = node.name.lower()

    results = get_cll_results(context, node)
    col_lower = column_name.lower()

    result = next(
        (r for r in results if r.model.lower() == node_lower and r.column.lower() == col_lower),
        None,
    )

    if result is None:
        return None

    # Computation wall — the value is BORN in this model (union / aggregate / window /
    # literal / generated / multi-source expression). Return the "computed in:
    # SCHEMA.MODEL" sentinel (empty origin column) so the caller writes a model-level
    # annotation rather than tracing through into the computation's inputs or crediting an
    # arbitrary branch. This is the SAME wall set ``_resolve_cll_description`` stops at
    # (its step 6), so the annotation tracer and the desc-source tracer agree on where a
    # column is computed. Description inheritance for unions is handled separately by the
    # agreement-aware walker in ``inherit_upstream_column_knowledge_cll``.
    if is_computation_wall(result):
        schema = (
            getattr(getattr(node, "unrendered_config", None), "schema", None)
            or getattr(node, "schema", None)
            or ""
        )
        return (str(schema).upper(), node.name.upper(), "", column_name.upper())

    # Column originates here (source-layer or seed — first in dbt chain)
    if result.progenitor_model is None:
        if result.is_first_in_chain:
            schema = (
                getattr(getattr(node, "unrendered_config", None), "schema", None)
                or getattr(node, "schema", None)
                or ""
            )
            return (str(schema).upper(), node.name.upper(), column_name.upper(), column_name.upper())
        return None

    progenitor_lower = result.progenitor_model.lower()
    # Strip adapter.quote() wrapping (e.g. '"COLUMN"' → 'COLUMN')
    progenitor_col = (result.progenitor_column or column_name).strip('"').strip("'")

    if project_dir is None:
        _ensure_manifest_index(context)
        runtime_cfg = context.project.runtime_cfg
        project_dir = str(runtime_cfg.project_root)

    # Check if progenitor is a source node (terminal in dbt lineage)
    src_node = _SOURCE_INDEX[project_dir].get(progenitor_lower)
    if src_node is not None:
        schema = (getattr(src_node, "schema", None) or "").upper()
        return (schema, progenitor_lower.upper(), progenitor_col.upper(), progenitor_col.upper())

    # Pure rename or single-source derivation: the value is RELOCATED or type-transformed,
    # not born here — trace through to where it is computed (a wall) or to its source.
    # We deliberately do NOT stop at the first ancestor that merely carries a description:
    # a described passthrough is still a passthrough, and stopping there credited the wrong
    # model and drifted from the desc-source walker, which passes transitively through
    # inherited copies to the origin. Being purely structural (CLL flags only), this walk is
    # deterministic regardless of which descriptions are populated during the run.
    model_node = _NODE_INDEX[project_dir].get(progenitor_lower)
    if model_node is not None:
        return _compute_column_origin(context, model_node, progenitor_col, project_dir, _depth + 1)

    return None


def get_origin_source_description(
    context: YamlRefactorContextProtocol,
    schema: str,
    model_name: str,
    column_name: str,
) -> str | None:
    """Look up the description for an origin column from the manifest.

    Searches both sources and model nodes; case-insensitive on all names.
    Returns None if no non-placeholder description is found.
    """
    model_lower = model_name.lower()
    col_lower = column_name.lower()

    _ensure_manifest_index(context)
    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)

    upstream_node = _SOURCE_INDEX[project_dir].get(model_lower) or _NODE_INDEX[project_dir].get(model_lower)
    if upstream_node is not None:
        cols: dict[str, t.Any] = getattr(upstream_node, "columns", {})
        col_info = next(
            (v for k, v in cols.items() if k.lower() == col_lower), None
        )
        if col_info is None:
            return None
        desc = getattr(col_info, "description", None) or ""
        if desc and desc not in context.placeholders:
            return desc
        return None

    return None
