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

from dbt_osmosis.config import get_config

if t.TYPE_CHECKING:
    from dbt.contracts.graph.nodes import ResultNode
    from dbt_osmosis.core.dbt_protocols import YamlRefactorContextProtocol

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
                _DISK_CACHE[project_dir] = data.get("entries", {})
                return _DISK_CACHE[project_dir]
        except Exception as exc:
            logger.debug("CLL disk cache load failed (will rebuild): %s", exc)
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
        from dbt_column_lineage.api import get_column_lineage
        from dbt_column_lineage.artifacts.manifest_catalog import ManifestCatalogReader

        if project_dir not in _READER_CACHE:
            reader = ManifestCatalogReader(manifest_path=manifest_path)
            reader.load()
            _READER_CACHE[project_dir] = reader

        adapter_type: str | None = getattr(
            getattr(runtime_cfg, "credentials", None), "type", None
        )

        from dbt_osmosis.config import get_config
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

    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    try:
        from dbt.artifacts.resources import ModelNode  # dbt ≥ 1.8
    except ImportError:
        from dbt.contracts.graph.nodes import ModelNode  # type: ignore[no-redef]

    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    target_name: str | None = getattr(runtime_cfg, "target_name", None)
    target_base = Path(project_dir) / getattr(runtime_cfg, "target_path", "target")

    logger.info(":mag: Checking compiled SQL freshness for candidate nodes...")
    stale = [
        node
        for _, node in _iter_candidate_nodes(context)
        if isinstance(node, ModelNode)
        and _is_compiled_sql_stale(project_dir, node, target_base)
    ]

    if not stale:
        logger.info(":white_check_mark: All compiled SQL is up-to-date — skipping bulk compile.")
        return

    logger.info(":hammer: %d stale model(s) — running bulk dbt compile upfront.", len(stale))

    select_str = " ".join(n.name for n in stale)
    # Windows CreateProcess has a ~32 767 char command-line limit; stay well below it.
    # When the select string is too long just compile everything (no --select).
    _MAX_SELECT_LEN = 8_000
    cmd = [sys.executable, "-m", "dbt.cli.main", "compile", "--project-dir", project_dir]
    if len(select_str) <= _MAX_SELECT_LEN:
        cmd += ["--select", select_str]
    else:
        logger.debug(":information: select string too long (%d chars) — compiling all models.", len(select_str))
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
        logger.info(":white_check_mark: Bulk compile done (%d models).", len(stale))
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

    # Union column: column is born in this model from multiple branches.
    # Return the "computed in: SCHEMA.MODEL" sentinel so annotate writes the
    # union-marker annotation rather than picking an arbitrary branch and
    # silently inheriting its description. Description inheritance for unions
    # is handled separately by the agreement-aware walker in
    # ``inherit_upstream_column_knowledge_cll`` / ``_find_cll_description``.
    if getattr(result, "is_union", False):
        schema = (
            getattr(getattr(node, "unrendered_config", None), "schema", None)
            or getattr(node, "schema", None)
            or ""
        )
        return (str(schema).upper(), node.name.upper(), "", column_name.upper())

    # Multi-source computed: column is born in this model (multi-arg expression…).
    # Return (schema, model, "") as a sentinel so the caller can write the "computed in: SCHEMA.MODEL"
    # annotation rather than silently dropping it.
    if (result.progenitor_column is None or result.progenitor_column == "") and result.is_computed:
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

    # Walker semantics: if the progenitor already has a meaningful description
    # (in either the YAML buffer or the in-memory manifest), stop there instead
    # of recursing all the way to the source. This keeps annotate's "deep
    # tracer" consistent with inherit's chain walker, eliminating a class of
    # non-idempotency bugs where intermediate models populated during the run
    # change which upstream gets credited as the origin.
    model_node = _NODE_INDEX[project_dir].get(progenitor_lower)
    if model_node is not None and _node_has_real_description(
        context, model_node, progenitor_col
    ):
        schema = (
            getattr(getattr(model_node, "unrendered_config", None), "schema", None)
            or getattr(model_node, "schema", None)
            or ""
        )
        return (
            str(schema).upper(),
            progenitor_lower.upper(),
            progenitor_col.upper(),
            progenitor_col.upper(),
        )

    # Recurse into the progenitor dbt model
    if model_node is not None:
        return _compute_column_origin(context, model_node, progenitor_col, project_dir, _depth + 1)

    return None


def _node_has_real_description(
    context: YamlRefactorContextProtocol, node: t.Any, column_name: str
) -> bool:
    """True iff the column has a non-placeholder description after annotation strip.

    Used by ``_compute_column_origin`` so its walking semantics match the
    inherit walker (``_find_cll_description``): both stop at the first node in
    the chain that carries a real description, instead of one consumer racing
    to the leaf source while the other stops at the first populated ancestor.
    Reads the YAML buffer first (parallel-safe, reflects pre-pipeline state)
    then falls back to the in-memory manifest (live during the run).
    """
    # In-memory manifest first — see comment in `_find_cll_description` for
    # the topological-waves rationale.
    cols = getattr(node, "columns", {})
    col_info = next(
        (v for k, v in cols.items() if k.lower() == column_name.lower()), None
    )
    if col_info is not None:
        raw = getattr(col_info, "description", None) or ""
        cleaned = strip_annotation_tags(raw).strip()
        if cleaned and cleaned not in context.placeholders:
            return True

    # YAML buffer fallback — covers nodes outside this run's candidate set.
    try:
        from dbt_osmosis.core.inheritance import _read_ancestor_yaml_description
    except Exception:  # noqa: BLE001 — defensive: missing dep means treat as no description
        return False
    variants = [column_name, column_name.upper(), column_name.lower()]
    yaml_desc = _read_ancestor_yaml_description(context, node, variants)
    if yaml_desc:
        cleaned = strip_annotation_tags(yaml_desc).strip()
        if cleaned and cleaned not in context.placeholders:
            return True
    return False


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




def _wrap_annotation(tag: str) -> str:
    """Wrap a raw tag string with the configured separator and namespace prefix."""
    cfg = get_config()
    return f"{cfg.annotation_separator}\n{cfg.annotation_namespace} -> {tag}"


def format_origin_tag(origin_col: str, origin_table: str, source_description: str | None) -> str:
    """Return the full annotation block for a **renamed** column."""
    base = f"{get_config().annotation_renamed} {origin_table}.{origin_col}"
    if source_description:
        base = f"{base} — {source_description}"
    return _wrap_annotation(base)


def format_computed_origin_tag(origin_col: str, origin_table: str, source_description: str | None) -> str:
    """Return the full annotation block for a **passthrough / computed** column."""
    base = f"{get_config().annotation_derived} {origin_table}.{origin_col}"
    if source_description:
        base = f"{base} — {source_description}"
    return _wrap_annotation(base)


def format_derived_tag(schema: str, model: str, entry_col: str | None = None) -> str:
    """Return the annotation block for a **multi-source / computed** column.

    When *entry_col* is supplied and differs from the queried column name, it is
    appended as ``(as ENTRY_COL)`` so the reader knows what name to search for
    in the referenced model.
    """
    tag = f"{get_config().annotation_computed} {schema}.{model}"
    if entry_col:
        tag = f"{tag} (as {entry_col})"
    return _wrap_annotation(tag)


def format_aggregate_from_tag(progenitor_col: str, progenitor_model: str) -> str:
    """Return annotation for a single-source aggregate: ``Aggregated from MODEL.COL``.

    Consistent with renamed/derived: ``from`` points to a column (MODEL.COL),
    while ``in:`` (the *_in variants) points to a model (SCHEMA.MODEL).
    """
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_aggregate_from} {progenitor_model}.{progenitor_col}")


def format_aggregate_in_tag(schema: str, model: str) -> str:
    """Return annotation for an aggregate with no traceable source: ``Aggregated in: SCHEMA.MODEL``."""
    return _wrap_annotation(f"{get_config().annotation_aggregate_in} {schema}.{model}")


def format_window_from_tag(progenitor_col: str, progenitor_model: str) -> str:
    """Return annotation for a window function with a traceable value column: ``Window from MODEL.COL``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_window_from} {progenitor_model}.{progenitor_col}")


def format_window_in_tag(schema: str, model: str) -> str:
    """Return annotation for a window function with no traceable source: ``Window in: SCHEMA.MODEL``."""
    return _wrap_annotation(f"{get_config().annotation_window_in} {schema}.{model}")


def format_union_tag(schema: str, model: str) -> str:
    """Return annotation for a top-level UNION / UNION ALL column: ``UNION in: SCHEMA.MODEL``.

    CLL does not distinguish UNION from UNION ALL, so a single label covers both.
    """
    return _wrap_annotation(f"{get_config().annotation_union} {schema}.{model}")


def format_literal_tag(literal_value: str, schema: str, model: str) -> str:
    """Return annotation for a hardcoded constant: ``Literal 'SAP' set in: SCHEMA.MODEL``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_literal} {literal_value} set in: {schema}.{model}")


def format_generated_tag(generated_expr: str, schema: str, model: str) -> str:
    """Return annotation for a zero-arg system function: ``Generated in: SCHEMA.MODEL``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_generated} in: {schema}.{model}")


_WS_NORMALIZE_RE = re.compile(r"\s+")


def descriptions_equivalent(a: str | None, b: str | None) -> bool:
    """True when two descriptions differ only in whitespace / line-wrap.

    Database comments come back as single-line strings while YAML stores them
    wrapped at a fixed width — they describe the same content but are not
    byte-equal. Collapsing internal whitespace before compare makes the
    "is this a real change?" check robust to wrap differences and prevents
    spurious source refresh updates that would in turn flap the annotation
    layer on the very next run (idempotency bug).
    """
    if not a and not b:
        return True
    if not a or not b:
        return False
    return _WS_NORMALIZE_RE.sub(" ", a).strip() == _WS_NORMALIZE_RE.sub(" ", b).strip()


def strip_annotation_tags(description: str) -> str:
    """Remove any annotation block from a description string.

    Strips from the configured separator onwards so a fresh annotation can be
    appended on the next run.  Also handles legacy bare prefixes — both the
    current annotation verbs and any project-specific ``legacy-strip-markers``
    configured in ``.osmosis`` — for backward compatibility.
    """
    cfg = get_config()
    sep_idx = description.find(cfg.annotation_separator)
    if sep_idx != -1:
        return description[:sep_idx].rstrip()
    # Legacy bare prefixes (pre-separator format) — always strip regardless of config.
    # Namespace-prefixed variants are checked first so that old single-line annotations
    # like "NAMESPACE -> VERB TABLE.COL" strip to "" rather than leaving
    # the orphaned "NAMESPACE ->" prefix behind.
    _ns = cfg.annotation_namespace
    for marker in (
        # Namespace-prefixed forms (legacy "NAMESPACE -> VERB ..." on a single line)
        f"{_ns} -> {cfg.annotation_renamed}",
        f"{_ns} -> {cfg.annotation_derived}",
        f"{_ns} -> {cfg.annotation_computed}",
        f"{_ns} -> {cfg.annotation_aggregate_from}",
        f"{_ns} -> {cfg.annotation_aggregate_in}",
        f"{_ns} -> {cfg.annotation_window_from}",
        f"{_ns} -> {cfg.annotation_window_in}",
        f"{_ns} -> {cfg.annotation_union}",
        f"{_ns} -> {cfg.annotation_literal}",
        f"{_ns} -> {cfg.annotation_generated}",
        # Bare verb forms (even older format, or when namespace was different)
        cfg.annotation_renamed, cfg.annotation_derived, cfg.annotation_computed,
        cfg.annotation_aggregate_from, cfg.annotation_aggregate_in,
        cfg.annotation_window_from, cfg.annotation_window_in,
        cfg.annotation_union, cfg.annotation_literal, cfg.annotation_generated,
        # Project-specific legacy markers configured in .osmosis (legacy-strip-markers)
        *cfg.legacy_strip_markers,
    ):
        idx = description.find(marker)
        if idx != -1:
            description = description[:idx].rstrip()
    return description
