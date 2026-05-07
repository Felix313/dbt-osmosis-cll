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
import threading
import types
import typing as t
from pathlib import Path

logger = logging.getLogger(__name__)

from dbt_osmosis.config import get_config

if t.TYPE_CHECKING:
    from dbt.contracts.graph.nodes import ResultNode
    from dbt_osmosis.core.dbt_protocols import YamlRefactorContextProtocol

_CACHE_SCHEMA_VERSION = 2

# (project_dir, model_name) → List[result]  in-memory, process-scoped
_LINEAGE_CACHE: dict[tuple[str, str], list[t.Any]] = {}

# project_dir → ManifestCatalogReader  (warmed once per run; cheap — just JSON)
_READER_CACHE: dict[str, t.Any] = {}

# project_dir → {name_lower: node}  for O(1) manifest lookups in get_column_origin
_SOURCE_INDEX: dict[str, dict[str, t.Any]] = {}
_NODE_INDEX: dict[str, dict[str, t.Any]] = {}

# (project_dir, model_name_lower, column_name_lower) → origin tuple or None
_ORIGIN_CACHE: dict[tuple[str, str, str], tuple[str, str, str] | None] = {}

# project_dir → {model_name: {"compiled_sql_hash": str, "results": [dict]}}
_DISK_CACHE: dict[str, dict[str, t.Any]] = {}

_CACHE_LOCK = threading.Lock()

# Fields we read from ColumnLineageResult — serialised/deserialised for disk cache
_RESULT_FIELDS = ("model", "column", "is_computed", "progenitor_model", "progenitor_column", "is_first_in_chain", "is_rename", "source_column")


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


def _is_compiled_sql_stale(project_dir: str, node: t.Any) -> bool:
    """Return True if the compiled SQL artifact is older than the source SQL file.

    When a developer edits a model, the source .sql mtime advances past the
    compiled artifact's mtime. Running CLL on the stale compiled file would
    produce wrong column lists and store them under the new source hash —
    corrupting the cache for subsequent runs.
    """
    original: str = getattr(node, "original_file_path", "") or ""
    if not original.endswith(".sql"):
        return False
    source_path = Path(project_dir) / original
    if not source_path.exists():
        return False

    # Locate compiled artifact — try with and without package-name nesting
    target_dir = Path(project_dir) / "target" / "compiled"
    package_name: str = getattr(node, "package_name", "") or ""
    candidates = [target_dir / package_name / original, target_dir / original]
    for compiled_path in candidates:
        if compiled_path.exists():
            return source_path.stat().st_mtime > compiled_path.stat().st_mtime
    # Compiled file doesn't exist at all — also stale
    return True


def _compile_node(project_dir: str, node: t.Any, target: str | None) -> bool:
    """Run ``dbt compile --select <model>`` scoped to *node*. Returns True on success."""
    import subprocess
    import sys

    cmd = [sys.executable, "-m", "dbt.cli.main", "compile", "--select", node.name, "--project-dir", project_dir]
    if target:
        cmd += ["--target", target]

    logger.info(":hammer: Running dbt compile for stale compiled SQL => %s", node.name)
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_dir)
    if result.returncode != 0:
        logger.warning(
            ":warning: dbt compile failed for %s (exit %d):\n%s",
            node.name,
            result.returncode,
            result.stderr or result.stdout,
        )
        return False
    logger.info(":white_check_mark: dbt compile succeeded for %s", node.name)
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

    # Sources are leaf nodes with no compiled SQL — CLL has nothing to trace.
    if node.resource_type == NodeType.Source:
        return []

    runtime_cfg = context.project.runtime_cfg
    project_dir: str = str(runtime_cfg.project_root)
    cache_key = (project_dir, node.name)

    # 1. In-memory
    if cache_key in _LINEAGE_CACHE:
        return _LINEAGE_CACHE[cache_key]

    manifest_path = str(Path(project_dir) / "target" / "manifest.json")
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

    # 3. Compute via CLL — ensure compiled SQL is fresh first
    try:
        from dbt_column_lineage.api import get_column_lineage
        from dbt_column_lineage.artifacts.manifest_catalog import ManifestCatalogReader

        # If the source SQL is newer than target/compiled/, CLL would read stale SQL
        # and cache wrong column lists under the new source hash. Compile first.
        compile_on_failure: bool = getattr(
            getattr(context, "settings", None), "compile_on_cll_failure", True
        )
        if compile_on_failure and _is_compiled_sql_stale(project_dir, node):
            target_name: str | None = getattr(runtime_cfg, "target_name", None)
            if _compile_node(project_dir, node, target_name):
                # Recompute source hash after compile (manifest may have been rewritten)
                sql_hash = _compiled_sql_hash(project_dir, node)
            # Always invalidate reader cache so ManifestCatalogReader picks up the new manifest
            _READER_CACHE.pop(project_dir, None)

        if project_dir not in _READER_CACHE:
            reader = ManifestCatalogReader(manifest_path=manifest_path)
            reader.load()
            _READER_CACHE[project_dir] = reader

        # Pass the adapter type as dialect so CLL uses the correct SQL parser.
        adapter_type: str | None = getattr(
            getattr(runtime_cfg, "credentials", None), "type", None
        )

        results = get_column_lineage(
            manifest_path=manifest_path,
            models=[node.name],
            compiled_sql_source="target_dir",
            dialect=adapter_type,
            _catalog_reader_override=_READER_CACHE[project_dir],
        )
    except Exception as exc:
        logger.warning(
            ":warning: CLL unavailable for %s: %s — falling back to name-matching",
            node.unique_id,
            exc,
        )
        results = []

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


def invalidate_cll_for_node(project_dir: str, node: t.Any) -> None:
    """Evict *node* from the in-memory lineage and reader caches.

    Call this after running ``dbt compile`` for a model so the next
    ``get_cll_results`` call re-reads the freshly written compiled SQL.
    ``_READER_CACHE`` is also cleared so ManifestCatalogReader reloads the
    updated manifest.json (compile rewrites it).
    """
    cache_key = (project_dir, node.name)
    _LINEAGE_CACHE.pop(cache_key, None)
    _READER_CACHE.pop(project_dir, None)
    logger.debug("CLL cache invalidated for %s", node.name)


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


_CLL_COMPUTED_SENTINEL = "__cbm_computed__"
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
``enrich_rename_descriptions`` will attach a "Berechnet in:" annotation instead.
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
            # Single-source computed (e.g. COALESCE(a, 0)) — parent is known but derived.
            # Still map to progenitor so "Abgeleitet aus:" annotation can fire.
            pass
        parent_map[r.column.lower()] = r.progenitor_model.lower()
    return parent_map


# ---------------------------------------------------------------------------
# Origin tracing
# ---------------------------------------------------------------------------

_CBM_ORIGIN_TAG = "CBM_ORIGIN:"


def _ensure_manifest_index(context: YamlRefactorContextProtocol) -> None:
    """Build name→node lookup dicts for sources and model nodes (once per project)."""
    runtime_cfg = context.project.runtime_cfg
    project_dir = str(runtime_cfg.project_root)
    if project_dir not in _SOURCE_INDEX:
        src_idx: dict[str, t.Any] = {}
        for src_node in context.project.manifest.sources.values():
            identifier = (getattr(src_node, "identifier", None) or "").lower()
            name = (getattr(src_node, "name", None) or "").lower()
            # Index by identifier first (canonical); also by name when it differs,
            # using setdefault so identifier entries take priority on collision.
            if identifier:
                src_idx[identifier] = src_node
            if name and name != identifier:
                src_idx.setdefault(name, src_node)
        _SOURCE_INDEX[project_dir] = src_idx
    if project_dir not in _NODE_INDEX:
        node_idx: dict[str, t.Any] = {}
        for n in context.project.manifest.nodes.values():
            name = getattr(n, "name", "").lower()
            if name:
                node_idx[name] = n
        _NODE_INDEX[project_dir] = node_idx


def get_column_origin(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
    column_name: str,
    _depth: int = 0,
) -> tuple[str, str, str] | None:
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
            "[tool.dbt-osmosis-cll] if this column has legitimate deep lineage.",
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
) -> tuple[str, str, str] | None:
    """Internal recursive implementation for :func:`get_column_origin`."""
    max_depth = get_config().cll_max_origin_depth
    if _depth > max_depth:
        logger.warning(
            "CLL origin depth %d exceeded for %s.%s — treating as computed "
            "(no description will be inherited). Raise cll-max-origin-depth in "
            "[tool.dbt-osmosis-cll] if this column has legitimate deep lineage.",
            max_depth,
            node.name,
            column_name,
        )
        return None
    results = get_cll_results(context, node)
    col_lower = column_name.lower()
    node_lower = node.name.lower()

    result = next(
        (r for r in results if r.model.lower() == node_lower and r.column.lower() == col_lower),
        None,
    )

    if result is None:
        return None

    # Multi-source computed: no single traceable origin
    if result.progenitor_column is None and result.is_computed:
        return None

    # Column originates here (source-layer or seed — first in dbt chain)
    if result.progenitor_model is None:
        if result.is_first_in_chain:
            schema = (
                getattr(getattr(node, "unrendered_config", None), "schema", None)
                or getattr(node, "schema", None)
                or ""
            )
            return (str(schema).upper(), node.name.upper(), column_name.upper())
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
        return (schema, progenitor_lower.upper(), progenitor_col.upper())

    # Recurse into the progenitor dbt model
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


_CBM_DERIVED_TAG = "CBM_DERIVED_IN:"    # legacy tag kept for backward-compat stripping only


def _wrap_annotation(tag: str) -> str:
    """Wrap a raw tag string with the configured separator and namespace prefix."""
    cfg = get_config()
    return f"{cfg.annotation_separator}\n{cfg.annotation_namespace} -> {tag}"


def format_origin_tag(origin_col: str, origin_table: str, source_description: str | None) -> str:
    """Return the full annotation block for a **renamed** column."""
    base = f"{get_config().annotation_renamed} {origin_table} -> {origin_col}"
    if source_description:
        base = f"{base} — {source_description}"
    return _wrap_annotation(base)


def format_computed_origin_tag(origin_col: str, origin_table: str, source_description: str | None) -> str:
    """Return the full annotation block for a **computed/transformed** column."""
    base = f"{get_config().annotation_derived} {origin_table} -> {origin_col}"
    if source_description:
        base = f"{base} — {source_description}"
    return _wrap_annotation(base)


def format_derived_tag(schema: str, model: str) -> str:
    """Return the annotation block for a **multi-source derived** column."""
    return _wrap_annotation(f"{get_config().annotation_computed} {schema}.{model}")


def strip_origin_tag(description: str) -> str:
    """Remove any annotation block from a description string.

    Strips from the configured separator onwards so a fresh annotation can be
    appended on the next run.  Also handles legacy bare prefixes for backward compat.
    """
    cfg = get_config()
    sep_idx = description.find(cfg.annotation_separator)
    if sep_idx != -1:
        return description[:sep_idx].rstrip()
    # Legacy bare prefixes (pre-separator format) — always strip regardless of config
    for marker in (
        cfg.annotation_renamed, cfg.annotation_derived, cfg.annotation_computed,
        _CBM_ORIGIN_TAG, _CBM_DERIVED_TAG,
        # Hard-coded originals for backward compat if user changed the config strings
        "Umbenannt von:", "Abgeleitet aus:", "Berechnet in:",
    ):
        idx = description.find(marker)
        if idx != -1:
            description = description[:idx].rstrip()
    return description


def strip_all_cbm_tags(description: str) -> str:
    """Remove all CBM annotation tags (current and legacy variants)."""
    return strip_origin_tag(description)
