from __future__ import annotations

import atexit
import time
import typing as t
from collections import ChainMap
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path  # used by callers that import from this module
from types import MappingProxyType

from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.nodes import ResultNode, ColumnInfo  # pyright: ignore[reportPrivateImportUsage]

if t.TYPE_CHECKING:
    from dbt_osmosis.core.dbt_protocols import (
        YamlRefactorContextProtocol,
    )

from dbt_osmosis.core import logger
from dbt_osmosis.core.inheritance import _safe_column_replace
from dbt_osmosis.core.settings import get_managed_meta_keys
from dbt_osmosis.config import get_config



__all__ = [
    "TransformOperation",
    "TransformPipeline",
    "_transform_op",
    "annotate_column_origins",
    "inherit_upstream_column_knowledge",
    "inherit_upstream_column_knowledge_cll",
    "inject_missing_columns",
    "remove_columns_not_in_database",
    "sort_columns_alphabetically",
    "sort_columns_as_configured",
    "sort_columns_as_in_database",
    "synchronize_data_types",
    "synthesize_missing_documentation_with_openai",
    "apply_semantic_analysis",
    "suggest_improved_documentation",
]


@dataclass
class TransformOperation:
    """An operation to be run on a dbt manifest node."""

    func: t.Callable[..., t.Any]
    name: str

    _result: t.Any | None = field(init=False, default=None)
    _context: t.Any | None = field(init=False, default=None)  # YamlRefactorContext
    _node: ResultNode | None = field(init=False, default=None)
    _metadata: dict[str, t.Any] = field(init=False, default_factory=dict)

    @property
    def result(self) -> t.Any:
        """The result of the operation or None."""
        return self._result

    @property
    def metadata(self) -> MappingProxyType[str, t.Any]:
        """Metadata about the operation."""
        return MappingProxyType(self._metadata)

    def __call__(
        self,
        context: YamlRefactorContextProtocol,
        node: ResultNode | None = None,  # YamlRefactorContextProtocol
    ) -> TransformOperation:
        """Run the operation and store the result."""
        self._context = context
        self._node = node
        self._metadata["started"] = True
        try:
            self.func(context, node)
            self._metadata["success"] = True
        except Exception as e:
            self._metadata["error"] = str(e)
            raise
        return self

    def __rshift__(self, next_op: TransformOperation) -> TransformPipeline:
        """Chain operations together."""
        return TransformPipeline([self]) >> next_op

    def __repr__(self) -> str:
        return f"<Operation: {self.name} (success={self.metadata.get('success', False)})>"


@dataclass
class TransformPipeline:
    """A pipeline of transform operations to be run on a dbt manifest node."""

    operations: list[TransformOperation] = field(default_factory=list)
    commit_mode: t.Literal["none", "batch", "atomic", "defer"] = "batch"

    _metadata: dict[str, t.Any] = field(init=False, default_factory=dict)

    @property
    def metadata(self) -> MappingProxyType[str, t.Any]:
        """Metadata about the pipeline."""
        return MappingProxyType(self._metadata)

    def __rshift__(self, next_op: TransformOperation | t.Callable[..., t.Any]) -> TransformPipeline:
        """Chain operations together."""
        if isinstance(next_op, TransformOperation):
            self.operations.append(next_op)
        elif callable(next_op):
            self.operations.append(TransformOperation(next_op, next_op.__name__))
        else:
            raise ValueError(f"Cannot chain non-callable: {next_op}")
        return self

    def __call__(
        self,
        context: YamlRefactorContextProtocol,
        node: ResultNode | None = None,  # YamlRefactorContextProtocol
    ) -> TransformPipeline:
        """Run all operations in the pipeline."""
        logger.info(
            "\n:gear: [b]Running pipeline[/b] with => %s operations %s \n",
            len(self.operations),
            [op.name for op in self.operations],
        )

        self._metadata["started_at"] = (pipeline_start := time.time())
        for op in self.operations:
            logger.info(
                ":gear:  [b]Starting to[/b] [yellow]%s[/yellow]",
                op.name,
            )
            step_start = time.time()
            _ = op(context, node)
            step_end = time.time()
            logger.info(
                ":sparkles: [b]Done with[/b] [green]%s[/green] in %.2fs \n",
                op.name,
                step_end - step_start,
            )
            self._metadata.setdefault("steps", []).append({
                **op.metadata,
                "duration": step_end - step_start,
            })
            if self.commit_mode == "atomic":
                logger.info(
                    ":hourglass: [b]Committing[/b] Operation => [green]%s[/green]",
                    op.name,
                )
                from dbt_osmosis.core.sync_operations import sync_node_to_yaml

                sync_node_to_yaml(context, node, commit=True)
                logger.info(":checkered_flag: [b]Committed[/b] \n")
        self._metadata["completed_at"] = (pipeline_end := time.time())

        logger.info(
            ":checkered_flag: [b]Manifest transformation pipeline [green]completed[/green] in => %.2fs[/b]",
            pipeline_end - pipeline_start,
        )

        def _commit() -> None:
            """Commit changes to YAML files. Designed for use as an atexit handler."""
            logger.info(":hourglass: Committing all changes to YAML files in batch.")
            _commit_start = time.time()
            try:
                from dbt_osmosis.core.sync_operations import sync_node_to_yaml

                sync_node_to_yaml(context, node, commit=True)
                _commit_end = time.time()
                logger.info(
                    ":checkered_flag: YAML commits completed in => %.2fs",
                    _commit_end - _commit_start,
                )
            except Exception as e:
                # Log error but don't raise during atexit (prevents shutdown issues)
                logger.error(":boom: Failed to commit YAML changes during shutdown: %s", e)

        if self.commit_mode == "batch":
            _commit()
        elif self.commit_mode == "defer":
            logger.warning(
                ":warning: Using 'defer' commit mode with atexit.register. "
                "This may cause issues if locks are held during shutdown. "
                "Consider using 'batch' or 'atomic' mode instead.",
            )
            _ = atexit.register(_commit)

        # Emit CLL failure summary after all operations and YAML writes are done.
        # Failures are tracked per-model throughout the run; a consolidated summary
        # here is more actionable than individual inline warnings buried in the log.
        if context is not None:
            try:
                from dbt_osmosis.core.cll import clear_cll_failures, get_cll_failures
                failures = get_cll_failures(context)
                if failures:
                    logger.warning(
                        ":warning: CLL failed for %d model(s) — these were skipped during "
                        "annotation and will not have updated lineage tags. "
                        "Run [bold]dbt compile --select %s[/bold] to fix.\n  %s",
                        len(failures),
                        " ".join(sorted(failures)),
                        "\n  ".join(sorted(failures)),
                    )
                clear_cll_failures(context)
            except Exception:
                pass

        return self

    def __repr__(self) -> str:
        steps = [op.name for op in self.operations]
        return f"<OperationPipeline: {len(self.operations)} operations, steps={steps!r}>"


def _transform_op(
    name: str | None = None,
) -> t.Callable[[t.Callable[[t.Any, ResultNode | None], None]], TransformOperation]:
    """Decorator to create a TransformOperation from a function."""

    def decorator(
        func: t.Callable[[t.Any, ResultNode | None], None],  # YamlRefactorContext
    ) -> TransformOperation:
        return TransformOperation(func, name=name or func.__name__)

    return decorator


@_transform_op("Inherit Upstream Column Knowledge")
def inherit_upstream_column_knowledge(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,  # YamlRefactorContext
) -> None:
    """Inherit column level knowledge from the ancestors of a dbt model or source node."""
    if node is None:
        logger.info(":wave: Inheriting column knowledge across all matched nodes.")
        from dbt_osmosis.core.node_filters import _iter_candidate_nodes

        # Must process sequentially in topological order so that upstream in-memory
        # state is already updated when downstream nodes inherit from it.
        # pool.map would process concurrently and break the cascade (requires 2 passes).
        nodes = list(_iter_candidate_nodes(context))
        total = len(nodes)
        for i, (_, n) in enumerate(nodes, start=1):
            inherit_upstream_column_knowledge(context, n)
            if i % 25 == 0 or i == total:
                logger.info(":hourglass: Inherit Upstream Column Knowledge progress => %d / %d", i, total)
        return

    logger.info(":dna: Inheriting column knowledge for => %s", node.unique_id)

    from dbt_osmosis.core.inheritance import _build_column_knowledge_graph
    from dbt_osmosis.core.introspection import _get_setting_for_node

    column_knowledge_graph = _build_column_knowledge_graph(context, node)
    kwargs = None
    for name, node_column in node.columns.items():
        kwargs = column_knowledge_graph.get(name)
        if kwargs is None:
            continue
        inheritable = ["description"]
        if not _get_setting_for_node(
            "skip-add-tags",
            node,
            name,
            fallback=context.settings.skip_add_tags,
        ):
            inheritable.append("tags")
        if not _get_setting_for_node(
            "skip-merge-meta",
            node,
            name,
            fallback=context.settings.skip_merge_meta,
        ):
            inheritable.append("meta")
        for extra in _get_setting_for_node(
            "add-inheritance-for-specified-keys",
            node,
            name,
            fallback=context.settings.add_inheritance_for_specified_keys,
        ):
            if extra not in inheritable:
                inheritable.append(extra)

        # desc-owner controls who owns the column description.
        # "upstream" → upstream always overwrites (old force-inherit: true behaviour).
        # Any other value ("this", "aml", or custom) → preserve the existing description;
        # upstream only fills gaps.  Column-level meta wins over model/layer defaults via
        # _get_setting_for_node's standard resolution order.
        #
        # Exception: a description that consists ONLY of an annotation block (no real
        # business content) is treated as if the column has no description — it is always
        # refreshed from upstream regardless of desc-owner.
        desc_authority = _get_setting_for_node("desc-owner", node, name, fallback="this")
        force_inherit = str(desc_authority).lower() == "upstream"
        existing_desc = node_column.description.strip()
        if not force_inherit and existing_desc:
            from dbt_osmosis.core.cll import strip_annotation_tags
            if not strip_annotation_tags(existing_desc).strip():
                force_inherit = True  # annotation-only ⟹ treat as no real description
        if (
            "description" in inheritable
            and not force_inherit
            and existing_desc
        ):
            inheritable.remove("description")

        updated_metadata = {k: v for k, v in kwargs.items() if v is not None and k in inheritable}

        # Strip annotation blocks from inherited descriptions.
        # Annotations are layer-specific — each layer's annotate_column_origins writes its
        # own annotation based on that layer's config.  Letting them bleed downstream via
        # inheritance causes intermediate layers (prom__aa, kwm_aa, etc.) to show staging-level
        # annotations that belong only in the staging YAML.
        # Note: annotation always runs AFTER inheritance in the pipeline, so this stripping
        # never removes freshly-written annotation from the current node — it only cleans
        # stale annotation carried on the upstream node's in-memory description.
        if "description" in updated_metadata and isinstance(updated_metadata["description"], str):
            from dbt_osmosis.core.cll import strip_annotation_tags
            _clean_desc = strip_annotation_tags(updated_metadata["description"]).strip()
            updated_metadata = {**updated_metadata, "description": _clean_desc}

        # Strip osmosis-internal protection markers from inherited meta.
        # MANAGED keys (anchor_meta_key, meta_key_renamed_from, meta_key_derived_from,
        # meta_key_computed_in) must not
        # propagate downstream, but ARE re-applied when the column locally owns them.
        # Both top-level meta AND config.meta are filtered (fusion_compat stores
        # anchor-description in config.meta).
        _managed = get_managed_meta_keys()
        if "meta" in updated_metadata and isinstance(updated_metadata["meta"], dict):
            local_meta = dict(node_column.meta or {})
            filtered_meta = {k: v for k, v in updated_metadata["meta"].items() if k not in _managed}
            # Re-apply managed keys the column itself owns (e.g. anchor-description set by AML injection)
            for key in _managed:
                if key in local_meta:
                    filtered_meta[key] = local_meta[key]
            updated_metadata = {**updated_metadata, "meta": filtered_meta}

        if isinstance(updated_metadata.get("config"), dict):
            config_meta = updated_metadata["config"].get("meta", {})
            if config_meta:
                local_config_meta = dict((getattr(node_column, "config", None) or {}).get("meta", {}))
                filtered_config_meta = {k: v for k, v in config_meta.items() if k not in _managed}
                for key in _managed:
                    if key in local_config_meta:
                        filtered_config_meta[key] = local_config_meta[key]
                updated_metadata["config"]["meta"] = filtered_config_meta

        logger.debug(
            ":star2: Inheriting updated metadata => %s for column => %s",
            updated_metadata,
            name,
        )
        node.columns[name] = _safe_column_replace(node_column, **updated_metadata)


def _find_cll_description(
    context: YamlRefactorContextProtocol,
    parent_model_name: str,
    parent_col_name: str,
    depth: int = 0,
    max_depth: int | None = None,
) -> str | None:
    """Walk the CLL progenitor chain to find the closest ancestor with a real description.

    Reads from stable YAML buffers (not in-memory manifest) — safe for parallel execution.
    Stops at: computed walls, aggregate/window/union/literal/generated columns, source nodes,
    unresolvable nodes, depth limit.
    """
    from dbt_osmosis.core.cll import (
        _SOURCE_INDEX,
        _NODE_INDEX,
        _ensure_manifest_index,
        get_cll_results,
        strip_annotation_tags,
    )
    from dbt_osmosis.core.inheritance import _read_ancestor_yaml_description

    if max_depth is None:
        max_depth = get_config().cll_max_origin_depth

    # 1. Depth guard — protects against cyclic or pathological lineage chains.
    if depth > max_depth:
        logger.warning(
            ":warning: CLL description search exceeded max depth (%d) at => %s.%s",
            max_depth,
            parent_model_name,
            parent_col_name,
        )
        return None

    # 2. Resolve the parent model name to a manifest node (source or model).
    _ensure_manifest_index(context)
    project_dir = str(context.project.runtime_cfg.project_root)
    src_node = _SOURCE_INDEX[project_dir].get(parent_model_name.lower())
    model_node = _NODE_INDEX[project_dir].get(parent_model_name.lower())
    upstream_node = src_node or model_node
    if upstream_node is None:
        return None

    # 3. Prefer the stable YAML buffer description (reflects pre-run enrichment, parallel-safe).
    variants = [parent_col_name, parent_col_name.upper(), parent_col_name.lower()]
    yaml_desc = _read_ancestor_yaml_description(context, upstream_node, variants)
    if yaml_desc:
        cleaned = strip_annotation_tags(yaml_desc).strip()
        if cleaned and cleaned not in context.placeholders:
            return cleaned

    # 4. Fall back to the in-memory manifest description.
    cols = getattr(upstream_node, "columns", {})
    col_info = next(
        (v for k, v in cols.items() if k.lower() == parent_col_name.lower()), None
    )
    if col_info:
        raw = getattr(col_info, "description", None) or ""
        cleaned = strip_annotation_tags(raw).strip()
        if cleaned and cleaned not in context.placeholders:
            return cleaned

    # 5. Sources are terminal — CLL cannot recurse further (no compiled SQL).
    if src_node is not None:
        return None

    # 6. Ask CLL for this upstream column's own progenitor to continue the walk.
    parent_results = get_cll_results(context, model_node)
    parent_result = next(
        (
            r
            for r in parent_results
            if r.model.lower() == parent_model_name.lower()
            and r.column.lower() == parent_col_name.lower()
        ),
        None,
    )
    if parent_result is None:
        return None

    # 7. Stop at computed walls — no single traceable upstream column.
    _is_aggregate = getattr(parent_result, "is_aggregate", False)
    _is_window = getattr(parent_result, "is_window", False)
    _is_union = getattr(parent_result, "is_union", False)
    _is_literal = getattr(parent_result, "is_literal", False)
    _is_generated = getattr(parent_result, "is_generated", False)
    if (
        _is_aggregate
        or _is_window
        or _is_union
        or _is_literal
        or _is_generated
        or (parent_result.is_computed and parent_result.progenitor_column is None)
    ):
        return None

    # 8. Stop if there is no progenitor to recurse into.
    if parent_result.progenitor_model is None:
        return None

    # 9. Recurse into the grandparent (always follow the chain past intermediate renames).
    progenitor_col = (parent_result.progenitor_column or parent_col_name).strip('"').strip("'")
    return _find_cll_description(
        context,
        parent_result.progenitor_model.lower(),
        progenitor_col,
        depth + 1,
        max_depth,
    )


@_transform_op("Inherit Upstream Column Knowledge (CLL)")
def inherit_upstream_column_knowledge_cll(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """CLL-driven column description inheritance — runs in parallel, no name-matching.

    Replaces inherit_upstream_column_knowledge for projects using CLL.

    For each column in the node:
    - Queries CLL for the immediate upstream progenitor.
    - Skips computed / aggregate / window / union / literal / generated columns.
    - If inherit-through-renames is False (default) and CLL detected a rename → skips description.
    - Otherwise: walks the CLL progenitor chain to find the closest ancestor with a description.
    - Applies desc-owner logic: "upstream" always overwrites; any other value only fills gaps.
    - Tags and meta from the immediate CLL progenitor are inherited (not from the full chain).
    - Managed meta keys (desc-owner, anchor-description, etc.) are filtered from inherited meta
      and re-applied only from the local column's own meta.
    - No name-matching fallback. CLL failure = column skipped, existing description preserved.
    """
    if node is None:
        logger.info(":wave: CLL-driven column inheritance across all matched nodes.")
        from dbt_osmosis.core.cll import _ensure_manifest_index

        # Build the index dicts once, up front, so the parallel pool only reads them.
        _ensure_manifest_index(context)

        from dbt_osmosis.core.node_filters import _iter_candidate_nodes

        nodes = list(_iter_candidate_nodes(context))
        total = len(nodes)
        for i, _ in enumerate(
            context.pool.map(
                partial(inherit_upstream_column_knowledge_cll, context),
                (n for _, n in nodes),
            ),
            start=1,
        ):
            if i % 25 == 0 or i == total:
                logger.info(":hourglass: CLL Inherit progress => %d / %d", i, total)
        return

    logger.info(":dna: CLL-driven inheritance for => %s", node.unique_id)

    from dbt_osmosis.core.cll import (
        _ensure_manifest_index,
        _SOURCE_INDEX,
        _NODE_INDEX,
        get_cll_results,
        strip_annotation_tags,
    )
    from dbt_osmosis.core.introspection import _get_setting_for_node

    _ensure_manifest_index(context)
    project_dir = str(context.project.runtime_cfg.project_root)
    _cfg = get_config()
    _managed = get_managed_meta_keys()

    # Sources have no SQL → CLL cannot run; their descriptions come from the DB.
    if node.resource_type == NodeType.Source:
        return

    results = get_cll_results(context, node)
    if not results:
        # CLL failed or produced no results → skip entirely, preserve existing state.
        # The failure (if any) was already recorded in _CLL_FAILURES by get_cll_results.
        logger.debug(":warning: CLL unavailable for %s — skipping CLL inheritance.", node.name)
        return

    node_lower = node.name.lower()
    result_by_col: dict[str, t.Any] = {
        r.column.lower(): r for r in results if r.model.lower() == node_lower
    }

    # Per-node config (resolved once, can still be overridden per column below).
    inherit_renames_node = _get_setting_for_node(
        "inherit-through-renames", node, fallback=_cfg.inherit_through_renames
    )

    for col_name, node_col in node.columns.items():
        result = result_by_col.get(col_name.lower())
        if result is None:
            # CLL has no entry for this column in this model → skip.
            continue

        # --- Skip columns with no single traceable upstream (annotate step handles them) ---
        _is_aggregate = getattr(result, "is_aggregate", False)
        _is_window = getattr(result, "is_window", False)
        _is_union = getattr(result, "is_union", False)
        _is_literal = getattr(result, "is_literal", False)
        _is_generated = getattr(result, "is_generated", False)
        _is_multi_src = result.is_computed and result.progenitor_column is None

        if (
            _is_aggregate
            or _is_window
            or _is_union
            or _is_literal
            or _is_generated
            or _is_multi_src
        ):
            continue

        if result.is_first_in_chain or result.progenitor_model is None:
            # Column originates here; no upstream to inherit from.
            continue

        # --- Rename check ---
        # Strip adapter quote characters before comparing names — progenitor_column may carry
        # SQL-quoted identifiers like '"COLUMN_NAME"' that are the same logical name.
        _prog_col_stripped = (result.progenitor_column or "").strip('"').strip("'").lower()
        is_rename = bool(result.is_rename) or (
            result.progenitor_column is not None and col_name.lower() != _prog_col_stripped
        )

        # Per-column override of inherit-through-renames is allowed.
        inherit_renames = _get_setting_for_node(
            "inherit-through-renames",
            node,
            col_name,
            fallback=inherit_renames_node,
        )

        if is_rename and not inherit_renames:
            # Rename detected and not configured to follow → skip description, still do tags/meta.
            desc_to_apply = None
        else:
            # Walk the CLL progenitor chain for the closest ancestor with a real description.
            progenitor_col = (result.progenitor_column or col_name).strip('"').strip("'")
            desc_to_apply = _find_cll_description(
                context,
                result.progenitor_model.lower(),
                progenitor_col,
            )

        # --- desc-owner: should we overwrite the existing description? ---
        desc_authority = _get_setting_for_node("desc-owner", node, col_name, fallback="this")
        force_inherit = str(desc_authority).lower() == "upstream"
        existing_desc = node_col.description.strip() if node_col.description else ""
        if not force_inherit and existing_desc:
            # An annotation-only description (no real content after tag strip) counts as empty.
            if not strip_annotation_tags(existing_desc).strip():
                force_inherit = True

        update_kwargs: dict[str, t.Any] = {}

        if desc_to_apply is not None:
            # Only write the description if allowed (force_inherit or the column is empty).
            if force_inherit or not existing_desc:
                clean_desc = strip_annotation_tags(desc_to_apply).strip()
                if clean_desc and clean_desc not in context.placeholders:
                    update_kwargs["description"] = clean_desc

        # --- Inherit tags and meta from the immediate CLL progenitor (not the full chain) ---
        prog_lower = result.progenitor_model.lower()
        progenitor_node = _SOURCE_INDEX[project_dir].get(prog_lower) or _NODE_INDEX[
            project_dir
        ].get(prog_lower)
        if progenitor_node is not None:
            prog_col_name = (result.progenitor_column or col_name).strip('"').strip("'")
            prog_cols = getattr(progenitor_node, "columns", {})
            prog_col = next(
                (v for k, v in prog_cols.items() if k.lower() == prog_col_name.lower()), None
            )
            if prog_col is not None:
                skip_tags = _get_setting_for_node(
                    "skip-add-tags",
                    node,
                    col_name,
                    fallback=context.settings.skip_add_tags,
                )
                skip_meta = _get_setting_for_node(
                    "skip-merge-meta",
                    node,
                    col_name,
                    fallback=context.settings.skip_merge_meta,
                )

                if not skip_tags:
                    upstream_tags = list(getattr(prog_col, "tags", None) or [])
                    if upstream_tags:
                        existing_tags = list(getattr(node_col, "tags", None) or [])
                        merged_tags = list(set(existing_tags) | set(upstream_tags))
                        if merged_tags != existing_tags:
                            update_kwargs["tags"] = merged_tags

                if not skip_meta:
                    upstream_meta = dict(getattr(prog_col, "meta", None) or {})
                    local_meta = dict(node_col.meta or {})
                    # Filter managed keys from upstream meta — they must not propagate.
                    filtered_upstream = {
                        k: v for k, v in upstream_meta.items() if k not in _managed
                    }
                    # Merge: local wins on collision, then re-apply local-owned managed keys.
                    merged_meta = {**filtered_upstream, **local_meta}
                    for key in _managed:
                        if key in local_meta:
                            merged_meta[key] = local_meta[key]
                        else:
                            merged_meta.pop(key, None)
                    if merged_meta != local_meta:
                        update_kwargs["meta"] = merged_meta

        if update_kwargs:
            logger.debug(
                ":star2: CLL inherit => %s.%s: %s",
                node.name,
                col_name,
                list(update_kwargs.keys()),
            )
            node.columns[col_name] = _safe_column_replace(node_col, **update_kwargs)


@_transform_op("Inject Missing Columns")
def inject_missing_columns(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Add missing columns to a dbt node and it's corresponding yaml section. Changes are implicitly buffered until commit_yamls is called."""
    from dbt_osmosis.core.introspection import _get_setting_for_node, get_columns
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if _get_setting_for_node("skip-add-columns", node, fallback=context.settings.skip_add_columns):
        logger.debug(":no_entry_sign: Skipping column injection (skip_add_columns=True).")
        return
    if node is None:
        logger.info(":wave: Injecting missing columns for all matched nodes.")
        # Batch-prefetch source columns in a single DB round trip before per-node processing.
        # For source nodes, get_columns always hits the DB (no CLL) — batching here eliminates
        # the N sequential round trips that make large source refreshes slow.
        from dbt_osmosis.core.introspection import prefetch_columns

        source_nodes = [
            n for _, n in _iter_candidate_nodes(context) if n.resource_type == NodeType.Source
        ]
        if source_nodes:
            prefetch_columns(context, source_nodes)
        nodes = list(_iter_candidate_nodes(context))
        total = len(nodes)
        for i, _ in enumerate(context.pool.map(
            partial(inject_missing_columns, context),
            (n for _, n in nodes),
        ), start=1):
            if i % 25 == 0 or i == total:
                logger.info(":hourglass: Inject Missing Columns progress => %d / %d", i, total)
        return
    if (
        _get_setting_for_node(
            "skip-add-source-columns",
            node,
            fallback=context.settings.skip_add_source_columns,
        )
        and node.resource_type == NodeType.Source
    ):
        logger.debug(":no_entry_sign: Skipping column injection (skip_add_source_columns=True).")
        return

    from dbt_osmosis.core.cll import get_model_columns_from_cll
    from dbt_osmosis.core.introspection import normalize_column_name

    incoming_columns: dict[str, t.Any] | None = None

    if node.resource_type == NodeType.Source:
        # Sources have no SQL — DB is always the source of truth
        incoming_columns = get_columns(context, node)
    else:
        # Models: CLL is the primary source (reads compiled SQL, no DB needed)
        cll_columns = get_model_columns_from_cll(context, node)

        if cll_columns:
            incoming_columns = cll_columns
        elif not _get_setting_for_node(
            "introspect-sources-only",
            node,
            fallback=context.settings.introspect_sources_only,
        ):
            incoming_columns = get_columns(context, node)
        elif _get_setting_for_node(
            "db-fallback-on-cll-failure",
            node,
            fallback=context.settings.db_fallback_on_cll_failure,
        ):
            logger.warning(
                ":warning: CLL yielded no columns for => %s after compile attempt, falling back to DB",
                node.unique_id,
            )
            incoming_columns = get_columns(context, node)
        else:
            logger.error(
                ":x: CLL yielded no columns for => %s. "
                "Run 'dbt compile --select %s' to fix, or set db_fallback_on_cll_failure=True to allow DB fallback.",
                node.unique_id,
                node.name,
            )
            return

    if not incoming_columns:
        return

    output_to_upper = _get_setting_for_node(
        "output-to-upper", node, fallback=context.settings.output_to_upper
    )
    output_to_lower = _get_setting_for_node(
        "output-to-lower", node, fallback=context.settings.output_to_lower
    )
    case_insensitive = output_to_upper or output_to_lower
    current_columns = {
        normalize_column_name(c.name, context.project.runtime_cfg.credentials.type).lower()
        if case_insensitive
        else normalize_column_name(c.name, context.project.runtime_cfg.credentials.type)
        for c in node.columns.values()
    }

    for incoming_name, incoming_meta in incoming_columns.items():
        compare_name = incoming_name.lower() if case_insensitive else incoming_name
        if compare_name not in current_columns:
            logger.info(
                ":heavy_plus_sign: Reconciling missing column => %s in node => %s",
                incoming_name,
                node.unique_id,
            )
            final_name = incoming_name
            if output_to_upper:
                final_name = incoming_name.upper()
            elif output_to_lower:
                final_name = incoming_name.lower()

            gen_col = {"name": final_name, "description": incoming_meta.comment or ""}
            if (dtype := incoming_meta.type) and not _get_setting_for_node(
                "skip-add-data-types",
                node,
                fallback=context.settings.skip_add_data_types,
            ):
                if output_to_upper:
                    gen_col["data_type"] = dtype.upper()
                elif output_to_lower:
                    gen_col["data_type"] = dtype.lower()
                else:
                    gen_col["data_type"] = dtype
            node.columns[final_name] = ColumnInfo.from_dict(gen_col)
            if hasattr(node.columns[final_name], "config"):
                delattr(node.columns[final_name], "config")
        elif (
            node.resource_type == NodeType.Source
            and incoming_meta.comment
            and not _get_setting_for_node(
                "prefer-yaml-values",
                node,
                incoming_name,
                fallback=context.settings.prefer_yaml_values,
            )
        ):
            # For sources, Snowflake prod is the source of truth: overwrite existing YAML
            # descriptions with the DB comment whenever it is non-empty and
            # --prefer-yaml-values is not set.  This ensures manual edits in the source
            # YAML are never silently preserved when prod has an authoritative description.
            existing_col = next(
                (c for c in node.columns.values()
                 if normalize_column_name(c.name, context.project.runtime_cfg.credentials.type).lower()
                 == compare_name.lower()),
                None,
            )
            if existing_col is not None and existing_col.description.strip() != incoming_meta.comment.strip():
                logger.info(
                    ":pencil: Updating source column description from prod => %s in node => %s",
                    incoming_name,
                    node.unique_id,
                )
                node.columns[existing_col.name] = _safe_column_replace(existing_col, description=incoming_meta.comment)


@_transform_op("Remove Extra Columns")
def remove_columns_not_in_database(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Remove columns from a dbt node and it's corresponding yaml section that are not present in the database. Changes are implicitly buffered until commit_yamls is called."""
    from dbt_osmosis.core.introspection import (
        _get_setting_for_node,
        get_columns,
        normalize_column_name,
    )
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Removing columns not in DB across all matched nodes.")
        for _ in context.pool.map(
            partial(remove_columns_not_in_database, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    output_to_upper = _get_setting_for_node(
        "output-to-upper", node, fallback=context.settings.output_to_upper
    )
    output_to_lower = _get_setting_for_node(
        "output-to-lower", node, fallback=context.settings.output_to_lower
    )
    case_insensitive = output_to_upper or output_to_lower
    current_columns = {
        (
            normalize_column_name(c.name, context.project.runtime_cfg.credentials.type).lower()
            if case_insensitive
            else normalize_column_name(c.name, context.project.runtime_cfg.credentials.type)
        ): key
        for key, c in node.columns.items()
    }
    from dbt_osmosis.core.cll import get_model_columns_from_cll

    incoming_columns: dict[str, t.Any] | None = None

    if node.resource_type == NodeType.Source:
        incoming_columns = get_columns(context, node)
    else:
        cll_columns = get_model_columns_from_cll(context, node)
        if cll_columns:
            incoming_columns = cll_columns
        elif not _get_setting_for_node(
            "introspect-sources-only",
            node,
            fallback=context.settings.introspect_sources_only,
        ):
            incoming_columns = get_columns(context, node)
        elif _get_setting_for_node(
            "db-fallback-on-cll-failure",
            node,
            fallback=context.settings.db_fallback_on_cll_failure,
        ):
            logger.warning(
                ":warning: CLL yielded no columns for => %s, falling back to DB",
                node.unique_id,
            )
            incoming_columns = get_columns(context, node)
        else:
            logger.error(
                ":x: CLL yielded no columns for => %s. "
                "Run 'dbt compile --select %s' to fix, or set db_fallback_on_cll_failure=True.",
                node.unique_id,
                node.name,
            )
            return
    if not incoming_columns:
        logger.info(
            ":no_entry_sign: No columns discovered for node => %s, skipping cleanup.",
            node.unique_id,
        )
        return
    incoming_keys = (
        {k.lower() for k in incoming_columns} if case_insensitive else set(incoming_columns.keys())
    )
    extra_columns = set(current_columns.keys()) - incoming_keys
    for extra_column in extra_columns:
        logger.info(
            ":heavy_minus_sign: Removing extra column => %s in node => %s",
            extra_column,
            node.unique_id,
        )
        _ = node.columns.pop(current_columns[extra_column], None)


@_transform_op("Sort Columns in DB Order")
def sort_columns_as_in_database(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Sort columns in a dbt node and it's corresponding yaml section as they appear in the database. Changes are implicitly buffered until commit_yamls is called."""
    from dbt_osmosis.core.introspection import _get_setting_for_node, get_columns, normalize_column_name
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Sorting columns as they appear in DB across all matched nodes.")
        for _ in context.pool.map(
            partial(sort_columns_as_in_database, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    logger.info(":1234: Sorting columns by warehouse order => %s", node.unique_id)
    from dbt_osmosis.core.cll import get_model_columns_from_cll
    if node.resource_type == NodeType.Source:
        incoming_columns = get_columns(context, node)
    else:
        cll_columns = get_model_columns_from_cll(context, node)
        if cll_columns:
            incoming_columns = cll_columns
        elif not _get_setting_for_node(
            "introspect-sources-only",
            node,
            fallback=context.settings.introspect_sources_only,
        ):
            incoming_columns = get_columns(context, node)
        elif _get_setting_for_node(
            "db-fallback-on-cll-failure",
            node,
            fallback=context.settings.db_fallback_on_cll_failure,
        ):
            incoming_columns = get_columns(context, node)
        else:
            logger.warning(
                ":warning: Skipping sort for model => %s (CLL unavailable, introspect_sources_only=True)",
                node.unique_id,
            )
            return
    if not incoming_columns:
        logger.info(
            ":no_entry_sign: No columns discovered for node => %s, skipping db order sorting.",
            node.unique_id,
        )
        return

    def _position(column: str) -> int:
        inc = incoming_columns.get(
            normalize_column_name(column, context.project.runtime_cfg.credentials.type),
        )
        if inc is None or inc.index is None:
            return 99_999
        return inc.index

    node.columns = {k: v for k, v in sorted(node.columns.items(), key=lambda i: _position(i[0]))}


@_transform_op("Sort Columns Alphabetically")
def sort_columns_alphabetically(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Sort columns in a dbt node and it's corresponding yaml section alphabetically. Changes are implicitly buffered until commit_yamls is called."""
    from dbt_osmosis.core.introspection import _get_setting_for_node
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Sorting columns alphabetically across all matched nodes.")
        for _ in context.pool.map(
            partial(sort_columns_alphabetically, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    logger.info(":abcd: Sorting columns alphabetically => %s", node.unique_id)

    # Determine the case conversion setting for sorting
    # We need to sort based on the FINAL case of the column names, not the original case
    output_to_lower = _get_setting_for_node(
        "output-to-lower",
        node,
        fallback=context.settings.output_to_lower,
    )
    output_to_upper = _get_setting_for_node(
        "output-to-upper",
        node,
        fallback=context.settings.output_to_upper,
    )

    def sort_key(item: tuple[str, t.Any]) -> str:
        """Generate a sort key based on the final case of the column name."""
        column_name = item[0]
        if output_to_upper:
            return column_name.upper()
        elif output_to_lower:
            return column_name.lower()
        else:
            return column_name

    node.columns = {k: v for k, v in sorted(node.columns.items(), key=sort_key)}


@_transform_op("Sort Columns")
def sort_columns_as_configured(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    from dbt_osmosis.core.introspection import _get_setting_for_node
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Sorting columns as configured across all matched nodes.")
        for _ in context.pool.map(
            partial(sort_columns_as_configured, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    sort_by = _get_setting_for_node("sort-by", node, fallback="database")
    if sort_by == "database":
        _ = sort_columns_as_in_database(context, node)
    elif sort_by == "alphabetical":
        _ = sort_columns_alphabetically(context, node)
    else:
        raise ValueError(f"Invalid sort-by value: {sort_by} for node: {node.unique_id}")


@_transform_op("Synchronize Data Types")
def synchronize_data_types(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Populate data types for columns in a dbt node and it's corresponding yaml section. Changes are implicitly buffered until commit_yamls is called."""
    from dbt_osmosis.core.introspection import (
        _get_setting_for_node,
        get_columns,
        normalize_column_name,
    )
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Populating data types across all matched nodes.")
        for _ in context.pool.map(
            partial(synchronize_data_types, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    logger.info(":1234: Synchronizing data types => %s", node.unique_id)
    from dbt_osmosis.core.cll import get_model_columns_from_cll
    if node.resource_type == NodeType.Source:
        incoming_columns = get_columns(context, node)
    else:
        cll_columns = get_model_columns_from_cll(context, node)
        if cll_columns:
            incoming_columns = cll_columns
        elif not _get_setting_for_node(
            "introspect-sources-only",
            node,
            fallback=context.settings.introspect_sources_only,
        ):
            incoming_columns = get_columns(context, node)
        elif _get_setting_for_node(
            "db-fallback-on-cll-failure",
            node,
            fallback=context.settings.db_fallback_on_cll_failure,
        ):
            logger.warning(
                ":warning: CLL unavailable for type sync => %s, falling back to DB",
                node.unique_id,
            )
            incoming_columns = get_columns(context, node)
        else:
            logger.warning(
                ":warning: Skipping data type sync for model => %s (CLL types unavailable, introspect_sources_only=True)",
                node.unique_id,
            )
            return
    incoming_columns_lower = {k.lower(): v for k, v in incoming_columns.items()}
    if _get_setting_for_node("skip-add-data-types", node, fallback=False):
        return
    for name, column in node.columns.items():
        if _get_setting_for_node(
            "skip-add-data-types",
            node,
            name,
            fallback=context.settings.skip_add_data_types,
        ):
            continue
        lowercase = _get_setting_for_node(
            "output-to-lower",
            node,
            name,
            fallback=context.settings.output_to_lower,
        )
        uppercase = _get_setting_for_node(
            "output-to-upper",
            node,
            name,
            fallback=context.settings.output_to_upper,
        )
        normalized = normalize_column_name(name, context.project.runtime_cfg.credentials.type)
        inc_c = incoming_columns.get(normalized)
        if inc_c is None and (lowercase or uppercase):
            inc_c = incoming_columns_lower.get(normalized.lower())
        if inc_c:
            is_lower = column.data_type and column.data_type.islower()
            if inc_c.type:
                if uppercase:
                    column.data_type = inc_c.type.upper()
                elif lowercase or is_lower:
                    column.data_type = inc_c.type.lower()
                else:
                    column.data_type = inc_c.type





@_transform_op("Annotate Column Origins")
def annotate_column_origins(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Annotate columns with their CLL-traced origin via description blocks and optional meta tags.

    Appends an annotation block (preceded by the configured ``annotation-separator``)
    to each column's description, describing how the column was produced: renamed,
    derived, aggregated, windowed, unioned, literal, generated, or computed from
    multiple sources.  Stale annotation blocks from prior runs are stripped first.

    Annotation is controlled by ``annotate-column-origin-infos`` (per node/layer):
      - ``if_altered``: annotate only renamed / derived / computed columns.
      - ``always``: annotate every column, including passthrough (intended for DP
        endpoint layers where origins are non-obvious).
      - absent / empty: no annotation appended to descriptions.
      In all cases: source description is omitted from the annotation when it is
      identical to the column's own description (deduplication).

    When ``write-cll-tags-to-meta: true`` is set in ``.osmosis``, machine-readable
    origin tags are also written to column meta (key names are configurable):
      - Pure renames: ``col-renamed-from`` (``TABLE.COLUMN``).
      - Single-source computed: ``col-derived-from`` (``TABLE.COLUMN``).
      - Multi-source / opaque columns: ``col-computed-in`` (``SCHEMA.MODEL``).
      Off by default — enable when querying the dbt manifest for lineage.
    Sources are always skipped (no SQL to trace through).
    """
    from dbt.artifacts.resources.types import NodeType
    from dbt_osmosis.core.cll import (
        format_aggregate_from_tag,
        format_aggregate_in_tag,
        format_computed_origin_tag,
        format_derived_tag,
        format_generated_tag,
        format_literal_tag,
        format_origin_tag,
        format_union_tag,
        format_window_from_tag,
        format_window_in_tag,
        get_column_origin,
        get_cll_results,
        get_origin_source_description,
        strip_annotation_tags,
    )
    from dbt_osmosis.core.introspection import _get_setting_for_node
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Enriching column origins across all matched nodes.")
        nodes = list(_iter_candidate_nodes(context))
        total = len(nodes)
        for i, _ in enumerate(context.pool.map(
            partial(annotate_column_origins, context),
            (n for _, n in nodes),
        ), start=1):
            if i % 25 == 0 or i == total:
                logger.info(":hourglass: Annotate Column Origins progress => %d / %d", i, total)
        return

    if node.resource_type == NodeType.Source:
        return

    from dbt_osmosis.config import get_column_docs as _get_col_docs_fn
    _col_docs: dict[str, str] = _get_col_docs_fn()
    # Columns in the reference file are implicitly ignored by CLL annotation.
    _ignore_cols: frozenset[str] = frozenset(_col_docs.keys())

    # annotate-column-origin-infos controls annotation depth per node/layer:
    #   "if_altered" → annotate renamed / derived / computed columns only
    #   "always"     → also annotate passthrough columns (use on DP endpoint layers)
    #   "never" / false / absent → no annotation (but CLL still runs to fill renamed
    #                              column descriptions and strip stale annotation tags)
    _raw_annotate = _get_setting_for_node("annotate-column-origin-infos", node, fallback=None)
    _annotate_mode: str = "" if (not _raw_annotate or _raw_annotate == "never") else str(_raw_annotate)

    results = get_cll_results(context, node)
    node_lower = node.name.lower()
    result_by_col: dict[str, t.Any] = {
        r.column.lower(): r for r in results if r.model.lower() == node_lower
    }

    node_schema = (
        getattr(getattr(node, "unrendered_config", None), "schema", None)
        or getattr(node, "schema", None)
        or ""
    ).upper()
    node_ref = f"{node_schema}.{node.name.upper()}"

    _cfg = get_config()
    _managed = get_managed_meta_keys()
    _key_renamed = _cfg.meta_key_renamed_from
    _key_derived = _cfg.meta_key_derived_from
    _key_computed = _cfg.meta_key_computed_in
    # Node-level: enable per layer via +dbt-osmosis-options (e.g. only on the data
    # product layer), falling back to the package-level .osmosis default.
    _write_meta = _get_setting_for_node(
        "write-cll-tags-to-meta", node, fallback=_cfg.write_cll_tags_to_meta
    )

    for col_name, node_col in node.columns.items():
        # Columns in the ignore list are never annotated. If stale tags from a
        # previous run are present, strip them now so the YAML stays clean.
        # Central docs provide a canonical description when the column has none.
        if _ignore_cols and col_name.lower() in _ignore_cols:
            # Strip ALL managed keys from both top-level meta and config.meta.
            stale_meta = dict(node_col.meta or {})
            removed_meta = {stale_meta.pop(k, None) for k in _managed} - {None}

            raw_config_meta: dict[str, t.Any] = {}
            node_config = getattr(node_col, "config", None)
            if node_config is not None:
                raw_config_meta = dict(
                    node_config.get("meta", {}) if isinstance(node_config, dict)
                    else getattr(node_config, "meta", None) or {}
                )
            removed_config = {raw_config_meta.pop(k, None) for k in _managed} - {None}

            cleaned_config: dict[str, t.Any] | None = None
            if removed_config and node_config is not None:
                from dbt_osmosis.core.inheritance import _column_to_dict
                col_as_dict = _column_to_dict(node_col, omit_none=True)
                config_base = col_as_dict.get("config") or {}
                cleaned_config = {**config_base, "meta": raw_config_meta}

            stripped_desc = strip_annotation_tags((node_col.description or "").strip())
            if _col_docs:
                central_doc = _col_docs.get(col_name.lower())
                if central_doc and (not stripped_desc or stripped_desc in context.placeholders):
                    stripped_desc = central_doc

            any_removed = bool(removed_meta) or bool(removed_config)
            if any_removed or stripped_desc != (node_col.description or "").strip():
                replace_kwargs: dict[str, t.Any] = {"meta": stale_meta, "description": stripped_desc}
                if cleaned_config is not None:
                    replace_kwargs["config"] = cleaned_config
                node.columns[col_name] = _safe_column_replace(node_col, **replace_kwargs)
            continue

        result = result_by_col.get(col_name.lower())
        if result is None:
            # CLL has no trace for this column.
            # Still clean any stale managed meta keys left by previous runs.
            stale_meta = dict(node_col.meta or {})
            removed_any = bool({stale_meta.pop(k, None) for k in _managed} - {None})

            raw_config_meta: dict[str, t.Any] = {}
            node_config = getattr(node_col, "config", None)
            if node_config is not None:
                raw_config_meta = dict(
                    node_config.get("meta", {}) if isinstance(node_config, dict)
                    else getattr(node_config, "meta", None) or {}
                )
            removed_cfg = bool({raw_config_meta.pop(k, None) for k in _managed} - {None})

            cleaned_config: dict[str, t.Any] | None = None
            if removed_cfg and node_config is not None:
                from dbt_osmosis.core.inheritance import _column_to_dict
                col_as_dict = _column_to_dict(node_col, omit_none=True)
                config_base = col_as_dict.get("config") or {}
                cleaned_config = {**config_base, "meta": raw_config_meta}

            replace_kwargs: dict[str, t.Any] = {}
            if removed_any:
                replace_kwargs["meta"] = stale_meta
            if cleaned_config is not None:
                replace_kwargs["config"] = cleaned_config

            # Strip stale annotation tags from the description even when CLL has
            # no result for this column.  Ensures annotations from previous runs are
            # removed when annotation mode is turned off or CLL fails to trace the column.
            raw_desc = (node_col.description or "").strip()
            stripped_desc = strip_annotation_tags(raw_desc)
            if stripped_desc in context.placeholders:
                stripped_desc = ""
            if stripped_desc != raw_desc:
                replace_kwargs["description"] = stripped_desc

            # Try central docs for a description when column has none.
            if _col_docs:
                central_doc = _col_docs.get(col_name.lower())
                if central_doc and (not stripped_desc or stripped_desc in context.placeholders):
                    replace_kwargs["description"] = central_doc

            if replace_kwargs:
                node.columns[col_name] = _safe_column_replace(node_col, **replace_kwargs)
            continue

        new_meta = dict(node_col.meta or {})
        progenitor_col_raw = result.progenitor_column
        is_multi_source = result.is_computed and progenitor_col_raw is None

        # New semantic types — use getattr for backward compat with old SimpleNamespace disk cache entries
        _is_union      = getattr(result, "is_union",      False)
        _is_literal    = getattr(result, "is_literal",    False)
        _is_aggregate  = getattr(result, "is_aggregate",  False)
        _is_window     = getattr(result, "is_window",     False)
        _is_generated  = getattr(result, "is_generated",  False)
        _literal_val   = getattr(result, "literal_value",   None)
        _generated_val = getattr(result, "generated_value", None)

        # Clean managed meta keys from config.meta for all columns that have a CLL result.
        # (Columns without results and ignore-list columns handle this in their own branches.)
        _raw_cfg_meta: dict[str, t.Any] = {}
        _node_config = getattr(node_col, "config", None)
        if _node_config is not None:
            _raw_cfg_meta = dict(
                _node_config.get("meta", {}) if isinstance(_node_config, dict)
                else getattr(_node_config, "meta", None) or {}
            )
        _removed_cfg = bool({_raw_cfg_meta.pop(k, None) for k in _managed} - {None})
        _cleaned_config: dict[str, t.Any] | None = None
        if _removed_cfg and _node_config is not None:
            from dbt_osmosis.core.inheritance import _column_to_dict
            _col_as_dict = _column_to_dict(node_col, omit_none=True)
            _config_base = _col_as_dict.get("config") or {}
            _cleaned_config = {**_config_base, "meta": _raw_cfg_meta}
        _config_kwarg: dict[str, t.Any] = {"config": _cleaned_config} if _cleaned_config is not None else {}

        if _is_union or _is_literal or _is_generated or _is_aggregate or _is_window:
            new_meta.pop(_key_computed, None)
            new_meta.pop(_key_renamed, None)
            new_meta.pop(_key_derived, None)
            base_desc = strip_annotation_tags((node_col.description or "").strip())
            has_real_desc = bool(base_desc) and base_desc not in context.placeholders
            central_doc = _col_docs.get(col_name.lower()) if _col_docs else None

            if central_doc and not has_real_desc:
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=central_doc, **_config_kwarg)
            elif _annotate_mode:
                if _is_union:
                    tag = format_union_tag(node_schema, node.name.upper())
                elif _is_literal:
                    tag = format_literal_tag(_literal_val or "", node_schema, node.name.upper())
                elif _is_generated:
                    tag = format_generated_tag(_generated_val or "", node_schema, node.name.upper())
                elif _is_aggregate:
                    if result.progenitor_column is not None and result.progenitor_model is not None:
                        tag = format_aggregate_from_tag(
                            result.progenitor_column.upper(), result.progenitor_model.upper()
                        )
                    else:
                        tag = format_aggregate_in_tag(node_schema, node.name.upper())
                else:  # _is_window
                    if result.progenitor_column is not None and result.progenitor_model is not None:
                        tag = format_window_from_tag(
                            result.progenitor_column.upper(), result.progenitor_model.upper()
                        )
                    else:
                        tag = format_window_in_tag(node_schema, node.name.upper())

                new_desc = f"{base_desc}\n\n{tag}".strip() if has_real_desc else tag
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=new_desc, **_config_kwarg)
            else:
                # Non-annotation mode: still propagate the immediate progenitor description.
                # Without this, wrong descriptions written by a previous (buggy) CLL run
                # persist forever because no annotation tag ever replaces them.
                if result.progenitor_column is not None:
                    _raw_prog = get_origin_source_description(
                        context, "", result.progenitor_model or "", result.progenitor_column
                    )
                    if _raw_prog:
                        _prog_desc = strip_annotation_tags(_raw_prog).strip() or None
                        if _prog_desc:
                            _desc_auth = _get_setting_for_node(
                                "desc-owner", node, col_name, fallback="this"
                            )
                            _force_here = str(_desc_auth).lower() == "upstream"
                            if not has_real_desc or _force_here:
                                base_desc = _prog_desc
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)
            continue

        if is_multi_source:
            if _write_meta:
                new_meta[_key_computed] = node_ref
            else:
                new_meta.pop(_key_computed, None)
            new_meta.pop(_key_renamed, None)
            new_meta.pop(_key_derived, None)

            # Always strip old annotation tags; preserves real descriptions unchanged.
            base_desc = strip_annotation_tags((node_col.description or "").strip())
            has_real_desc = bool(base_desc) and base_desc not in context.placeholders

            # Use central docs as the description when the column has none.
            central_doc = _col_docs.get(col_name.lower()) if _col_docs else None
            if central_doc and not has_real_desc:
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=central_doc, **_config_kwarg)
            elif _annotate_mode:
                # Multi-source: "computed in: SCHEMA.MODEL" — can't trace to a single column
                derived_tag = format_derived_tag(node_schema, node.name.upper())
                new_desc = f"{base_desc}\n\n{derived_tag}".strip() if has_real_desc else derived_tag
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=new_desc, **_config_kwarg)
            else:
                # No annotation mode: still write stripped description to remove any old tags.
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)

            logger.debug(":pencil: %s.%s => derived in %s", node.name, col_name, node_ref)

        else:
            origin = get_column_origin(context, node, col_name)

            if origin is None:
                # Deep trace failed (broken chain, depth limit, unknown node).
                # Fall back to immediate CLL progenitor so the annotation is never
                # silently dropped when the deep chain is unresolvable.
                imm_model = result.progenitor_model
                imm_col = result.progenitor_column or ""
                if not imm_model:
                    # Truly unresolvable — strip stale tags, keep description.
                    base_desc = strip_annotation_tags((node_col.description or "").strip())
                    if not base_desc or base_desc in context.placeholders:
                        base_desc = ""
                    node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)
                    continue
                origin = ("", imm_model.upper(), imm_col.upper(), imm_col.upper())

            schema, origin_model, origin_col, entry_col = origin

            if not origin_col:
                # Computed column found upstream — origin_model is where it was computed.
                # Write "computed in: SCHEMA.MODEL" rather than a column-level reference.
                # If entry_col differs from col_name, a rename occurred along the chain —
                # append "(as ENTRY_COL)" so the reader knows what to search for in that model.
                if _write_meta:
                    new_meta[_key_computed] = f"{schema}.{origin_model}" if schema else origin_model
                else:
                    new_meta.pop(_key_computed, None)
                new_meta.pop(_key_renamed, None)
                new_meta.pop(_key_derived, None)
                base_desc = strip_annotation_tags((node_col.description or "").strip())
                if not base_desc or base_desc in context.placeholders:
                    base_desc = ""
                if _annotate_mode:
                    renamed_entry = entry_col if entry_col and entry_col.upper() != col_name.upper() else None
                    derived_tag = format_derived_tag(schema, origin_model, entry_col=renamed_entry)
                    new_desc = f"{base_desc}\n\n{derived_tag}".strip() if base_desc else derived_tag
                    node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=new_desc, **_config_kwarg)
                else:
                    node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)
                continue

            # Use CLL's is_rename (pure alias, no expression) rather than a name-comparison
            # heuristic. is_computed covers casts, aggregations, CASE, arithmetic, etc.
            is_pure_rename = result.is_rename
            is_name_changed = col_name.upper() != origin_col.upper()
            new_meta.pop(_key_computed, None)

            if _write_meta:
                if is_pure_rename:
                    new_meta[_key_renamed] = f"{origin_model}.{origin_col}"
                    new_meta.pop(_key_derived, None)
                else:
                    new_meta[_key_derived] = f"{origin_model}.{origin_col}"
                    new_meta.pop(_key_renamed, None)
            else:
                new_meta.pop(_key_renamed, None)
                new_meta.pop(_key_derived, None)

            raw_source_desc = get_origin_source_description(context, schema, origin_model, origin_col)
            # Strip any annotation tags that may have been written to the upstream
            # manifest by a concurrent annotate_column_origins call in the same run.
            # Without this, the annotation of the upstream leaks into the source_desc
            # embedded in this node's annotation.
            source_desc: str | None = strip_annotation_tags(raw_source_desc).strip() or None if raw_source_desc else None

            # Always strip old annotation tags from the current description — ensures annotations
            # from previous runs don't accumulate when annotate mode is "never".
            base_desc = strip_annotation_tags((node_col.description or "").strip())
            if not base_desc or base_desc in context.placeholders:
                base_desc = ""

            # When the column has no own description, promote source_desc as base_desc.
            # This bridges the rename gap: name-based inheritance cannot propagate
            # descriptions across renamed columns, so CLL-resolved source_desc fills the
            # role instead. As a side-effect the dedup check below will suppress the
            # source description from the annotation (no duplication).
            if not base_desc and source_desc:
                base_desc = source_desc

            # Determine annotation block.
            # annotation-include-source-description is a per-node option (default true):
            #   true  → inject source desc into annotation when it differs from base_desc
            #   false → write origin reference (TABLE.COL) only (DP layers)
            origin_annotation: str | None = None
            annotation_src_desc: str | None = None
            if _get_setting_for_node("annotation-include-source-description", node, fallback=True):
                annotation_src_desc = source_desc if source_desc and source_desc.strip() != base_desc else None
            if is_name_changed:
                # Renamed: always annotate (shows the original col name — not obvious from YAML).
                if is_pure_rename:
                    origin_annotation = format_origin_tag(origin_col, origin_model, annotation_src_desc)
                else:
                    origin_annotation = format_computed_origin_tag(origin_col, origin_model, annotation_src_desc)
            elif _annotate_mode == "always":
                # Passthrough: annotate only when layer is set to "always".
                origin_annotation = format_computed_origin_tag(origin_col, origin_model, annotation_src_desc)

            if _annotate_mode and origin_annotation is not None:
                new_desc = f"{base_desc}\n\n{origin_annotation}".strip() if base_desc else origin_annotation
            else:
                # No annotation: write stripped description so old tags are removed.
                new_desc = base_desc

            node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=new_desc, **_config_kwarg)

            if is_name_changed:
                logger.debug(":pencil: %s.%s => renamed from %s.%s", node.name, col_name, schema, origin_col)


def _collect_upstream_documents(
    node: ResultNode,
    context: YamlRefactorContextProtocol,
) -> list[str]:
    """Collect upstream documentation from dependency nodes.

    Args:
        node: The dbt node to collect upstream docs for
        context: The YamlRefactorContext instance

    Returns:
        List of strings containing upstream documentation

    """
    import textwrap

    node_map = ChainMap(
        t.cast("dict[str, ResultNode]", context.project.manifest.nodes),
        t.cast("dict[str, ResultNode]", context.project.manifest.sources),
    )
    upstream_docs: list[str] = ["# The following is not exhaustive, but provides some context."]
    depends_on_nodes = t.cast("list[str]", node.depends_on_nodes)

    for i, uid in enumerate(depends_on_nodes):
        dep = node_map.get(uid)
        if dep is not None:
            oneline_desc = dep.description.replace("\n", " ")
            upstream_docs.append(f"{uid}: # {oneline_desc}")
            for j, (name, meta) in enumerate(dep.columns.items()):
                if meta.description and meta.description not in context.placeholders:
                    upstream_docs.append(f"- {name}: |\n{textwrap.indent(meta.description, '  ')}")
                if j > 20:
                    # just a small amount of this supplementary context is sufficient
                    upstream_docs.append("- (omitting additional columns for brevity)")
                    break
        # ensure our context window is bounded, semi-arbitrary
        if len(upstream_docs) > 100 and i < len(depends_on_nodes) - 1:
            upstream_docs.append(f"# remaining nodes are: {', '.join(depends_on_nodes[i:])}")
            break

    if len(upstream_docs) == 1:
        upstream_docs[0] = "(no upstream documentation found)"

    return upstream_docs


def _synthesize_bulk_documentation(
    node: ResultNode,
    upstream_docs: list[str],
    context: YamlRefactorContextProtocol,
) -> None:
    """Synthesize documentation in bulk for multiple columns.

    Args:
        node: The dbt node to synthesize documentation for
        upstream_docs: List of upstream documentation strings
        context: The YamlRefactorContext instance

    """
    from dbt_osmosis.core.llm import generate_model_spec_as_json

    logger.info(
        ":robot: Synthesizing bulk documentation for => %s columns in node => %s",
        len(node.columns)
        - len([
            c
            for c in node.columns.values()
            if c.description and c.description not in context.placeholders
        ]),
        node.unique_id,
    )

    spec = generate_model_spec_as_json(
        getattr(
            node,
            "compiled_sql",
            f"SELECT {', '.join(node.columns)} FROM {node.schema}.{node.name}",
        ),
        upstream_docs=upstream_docs,
        existing_context=f"NodeId={node.unique_id}\nTableDescription={node.description}",
        temperature=0.4,
    )

    if not node.description or node.description in context.placeholders:
        node.description = spec.get("description", node.description)

    for synth_col in spec.get("columns", []):
        usr_col = node.columns.get(synth_col["name"])
        if usr_col and (not usr_col.description or usr_col.description in context.placeholders):
            usr_col.description = synth_col.get("description", usr_col.description)


def _synthesize_node_documentation(
    node: ResultNode,
    upstream_docs: list[str],
    context: YamlRefactorContextProtocol,
) -> None:
    """Synthesize documentation for the node itself.

    Args:
        node: The dbt node to synthesize documentation for
        upstream_docs: List of upstream documentation strings
        context: The YamlRefactorContext instance

    """
    from dbt_osmosis.core.llm import generate_table_doc

    if not node.description or node.description in context.placeholders:
        logger.info(
            ":robot: Synthesizing documentation for node => %s",
            node.unique_id,
        )
        node.description = generate_table_doc(
            getattr(
                node,
                "compiled_sql",
                f"SELECT {', '.join(node.columns)} FROM {node.schema}.{node.name}",
            ),
            table_name=node.relation_name or node.name,
            upstream_docs=upstream_docs,
        )


def _synthesize_individual_column_documentation(
    node: ResultNode,
    upstream_docs: list[str],
    context: YamlRefactorContextProtocol,
) -> None:
    """Synthesize documentation for individual columns.

    Args:
        node: The dbt node to synthesize documentation for
        upstream_docs: List of upstream documentation strings
        context: The YamlRefactorContext instance

    """
    from dbt_osmosis.core.llm import generate_column_doc

    for column_name, column in node.columns.items():
        if not column.description or column.description in context.placeholders:
            logger.info(
                ":robot: Synthesizing documentation for column => %s in node => %s",
                column_name,
                node.unique_id,
            )
            column.description = generate_column_doc(
                column_name,
                existing_context=f"DataType={column.data_type or 'unknown'}>\nColumnParent={node.unique_id}\nTableDescription={node.description}",
                table_name=node.relation_name or node.name,
                upstream_docs=upstream_docs,
                temperature=0.7,
            )


def synthesize_missing_documentation_with_openai(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Synthesize missing documentation for a dbt node using OpenAI's GPT-4o API."""
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    try:
        import importlib.util

        importlib.util.find_spec("dbt_osmosis.core.llm")
    except ImportError:
        raise ImportError(
            "Please install the 'dbt-osmosis[openai]' extra to use this feature.",
        ) from None
    if node is None:
        logger.info(":wave: Synthesizing missing documentation across all matched nodes.")
        for _ in context.pool.map(
            partial(synthesize_missing_documentation_with_openai, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return

    # since we are topologically sorted, we continually pass down synthesized knowledge leveraging our inheritance system
    # which minimizes synthesis requests -- in some cases by an order of magnitude while increasing accuracy
    _ = inherit_upstream_column_knowledge(context, node)
    total = len(node.columns)
    if total == 0:
        logger.info(
            ":no_entry_sign: No columns to synthesize documentation for => %s",
            node.unique_id,
        )
        return

    documented = len([
        column
        for column in node.columns.values()
        if column.description and column.description not in context.placeholders
    ])

    # Collect upstream documentation
    upstream_docs = _collect_upstream_documents(node, context)

    # Choose synthesis strategy based on number of missing columns
    if total - documented > 10:  # Use bulk synthesis for many missing columns
        _synthesize_bulk_documentation(node, upstream_docs, context)
    else:  # Use individual synthesis for few missing columns
        _synthesize_node_documentation(node, upstream_docs, context)
        _synthesize_individual_column_documentation(node, upstream_docs, context)


@_transform_op("Apply Semantic Analysis")
def apply_semantic_analysis(
    context: YamlRefactorContextProtocol, node: ResultNode | None = None
) -> None:
    """Apply AI semantic analysis to infer business meaning and relationships for columns.

    Uses LLM to analyze column names, data types, and context to:
    - Infer semantic types (primary_key, foreign_key, metric, dimension, etc.)
    - Detect relationships between columns (e.g., foreign keys)
    - Generate contextual descriptions based on semantic understanding
    - Suggest tags and metadata based on business meaning

    This transform enhances documentation by providing deeper business context
    beyond what traditional inheritance can provide.

    Args:
        context: The YAML refactor context
        node: The node to analyze. If None, analyzes all matched nodes.
    """
    from dbt_osmosis.core.inheritance import _build_column_knowledge_graph
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Applying semantic analysis across all matched nodes.")
        for _ in context.pool.map(
            partial(apply_semantic_analysis, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return

    logger.info(":robot: Analyzing semantics for => %s", node.unique_id)

    # Check if LLM is configured
    try:
        from dbt_osmosis.core.llm import analyze_column_semantics, generate_semantic_description

        # Verify LLM client can be created (will raise if not configured)
        _ = analyze_column_semantics.__globals__["get_llm_client"]()
    except Exception as e:
        logger.warning(
            ":warning: LLM not configured or accessible. Skipping semantic analysis: %s",
            e,
        )
        return

    # Build column knowledge graph to get upstream context
    column_knowledge_graph = _build_column_knowledge_graph(context, node)

    # Collect upstream columns for relationship inference
    upstream_columns: list[dict[str, str]] = []
    for name, meta in column_knowledge_graph.items():
        if "description" in meta:
            upstream_columns.append({"name": name, "description": meta["description"]})

    # Build model context (description or SQL)
    model_context = node.description or ""
    raw_sql = getattr(node, "raw_sql", None)
    if isinstance(raw_sql, str) and raw_sql:
        # Include a snippet of SQL for context
        model_context = f"{model_context}\n\nSQL: {raw_sql[:500]}..."

    # Apply semantic analysis to each column
    for column_name, column_info in node.columns.items():
        # Skip columns that already have comprehensive documentation
        if column_info.description and len(column_info.description) > 50:
            logger.debug(
                ":page_with_curl: Skipping semantic analysis for column => %s (already documented)",
                column_name,
            )
            continue

        try:
            logger.info(":mag: Analyzing semantics for column => %s", column_name)

            # Perform semantic analysis
            semantic_result = analyze_column_semantics(
                column_name=column_name,
                data_type=column_info.data_type,
                table_name=node.name,
                model_context=model_context,
                upstream_columns=upstream_columns[:20],  # Limit for context
                temperature=0.3,
            )

            # Generate or enhance description using semantic analysis
            new_description = generate_semantic_description(
                column_name=column_name,
                semantic_analysis=semantic_result,
                table_name=node.name,
                upstream_description=column_info.description,
                temperature=0.5,
            )

            # Update column description
            node.columns[column_name] = column_info.replace(description=new_description)

            # Apply suggested tags if present
            if semantic_result.get("tags"):
                existing_tags = list(column_info.tags) if column_info.tags else []
                new_tags = semantic_result["tags"]
                merged_tags = list(set(existing_tags + new_tags))
                if merged_tags != existing_tags:
                    node.columns[column_name] = column_info.replace(tags=merged_tags)
                    logger.debug(
                        ":label: Added tags to column %s: %s",
                        column_name,
                        new_tags,
                    )

            # Apply suggested meta if present
            if semantic_result.get("meta"):
                existing_meta = dict(column_info.meta) if column_info.meta else {}
                # Merge meta, prioritizing existing values
                merged_meta = {**semantic_result["meta"], **existing_meta}
                if merged_meta != existing_meta:
                    node.columns[column_name] = column_info.replace(meta=merged_meta)
                    logger.debug(
                        ":wrench: Added meta to column %s: %s",
                        column_name,
                        semantic_result["meta"],
                    )

            logger.info(
                ":sparkles: Applied semantic analysis to column => %s: %s",
                column_name,
                semantic_result.get("semantic_type", "unknown"),
            )

        except Exception as e:
            logger.warning(
                ":warning: Failed to analyze semantics for column %s: %s",
                column_name,
                e,
            )
            # Continue with other columns even if one fails
            continue


@_transform_op("Suggest Improved Documentation")
def suggest_improved_documentation(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
    threshold: float = 0.7,
    learning_mode: bool = True,
) -> None:
    """Suggest improved documentation using AI co-pilot with voice learning.

    This transform analyzes the project's documentation style and suggests
    improvements for model and column descriptions. It learns from existing
    documentation to match the team's voice and terminology.

    Args:
        context: The YamlRefactorContext instance
        node: The dbt node to suggest improvements for (None = all nodes)
        threshold: Confidence threshold for applying suggestions (0.0-1.0)
        learning_mode: Whether to analyze project style for voice learning

    Behavior:
        - For models with no documentation: generates new descriptions
        - For models with poor documentation: suggests improvements
        - Uses project style analysis to match team's voice
        - Only applies suggestions above confidence threshold
    """
    from dbt_osmosis.core.introspection import _get_setting_for_node
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes
    from dbt_osmosis.core.voice_learning import (
        ProjectStyleProfile,
        analyze_project_documentation_style,
        extract_style_examples,
    )

    try:
        import importlib.util

        importlib.util.find_spec("dbt_osmosis.core.llm")
    except ImportError:
        raise ImportError(
            "Please install the 'dbt-osmosis[openai]' extra to use this feature."
        ) from None

    if node is None:
        logger.info(":wave: Suggesting improved documentation across all matched nodes.")
        for _ in context.pool.map(
            partial(
                suggest_improved_documentation,
                context,
                threshold=threshold,
                learning_mode=learning_mode,
            ),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return

    # Check if AI co-pilot is disabled for this node
    if _get_setting_for_node("skip-ai-suggestions", node, fallback=False):
        logger.debug(":no_entry_sign: Skipping AI suggestions (skip_ai_suggestions=True).")
        return

    logger.info(":robot: Generating AI documentation suggestions for => %s", node.unique_id)

    # Analyze project style for voice learning
    style_profile: ProjectStyleProfile | None = None
    style_examples: list[str] | None = None

    if learning_mode:
        logger.debug(":books: Analyzing project documentation style...")
        style_profile = analyze_project_documentation_style(
            context,
            max_nodes=50,
            max_columns_per_node=10,
        )
        logger.debug(
            ":mag: Found %d model examples, %d column examples",
            len(style_profile.model_description_samples),
            len(style_profile.column_description_samples),
        )
    else:
        # Extract targeted examples from similar nodes
        examples = extract_style_examples(context, node, max_examples=3)
        style_examples = []
        style_examples.extend(examples.get("model_descriptions", []))
        style_examples.extend(examples.get("column_descriptions", []))

    # Collect upstream documentation
    upstream_docs = _collect_upstream_documents(node, context)

    # Track statistics
    suggestions_made = 0
    suggestions_applied = 0

    # Suggest model description
    needs_model_doc = not node.description or node.description in context.placeholders
    has_poor_model_doc = node.description and len(node.description.split()) < 5

    if needs_model_doc or has_poor_model_doc:
        from dbt_osmosis.core.llm import suggest_documentation_improvements

        suggestion = suggest_documentation_improvements(
            target="table",
            current_description=node.description if not needs_model_doc else None,
            table_name=node.relation_name or node.name,
            sql_content=getattr(node, "compiled_sql", f"SELECT * FROM {node.name}"),
            upstream_docs=upstream_docs,
            style_profile=style_profile,
            style_examples=style_examples,
            temperature=0.5,
        )

        suggestions_made += 1

        if suggestion.confidence >= threshold:
            node.description = suggestion.text
            suggestions_applied += 1
            logger.info(
                ":sparkles: Applied model description suggestion (confidence: %.2f): %s",
                suggestion.confidence,
                suggestion.reason,
            )
        else:
            logger.debug(
                ":heavy_check_mark: Model suggestion below threshold (confidence: %.2f): %s",
                suggestion.confidence,
                suggestion.reason,
            )

    # Suggest column descriptions
    for column_name, column in node.columns.items():
        needs_col_doc = not column.description or column.description in context.placeholders
        has_poor_col_doc = column.description and len(column.description.split()) < 3

        if needs_col_doc or has_poor_col_doc:
            from dbt_osmosis.core.llm import suggest_documentation_improvements

            suggestion = suggest_documentation_improvements(
                target="column",
                current_description=column.description if not needs_col_doc else None,
                column_name=column_name,
                table_name=node.relation_name or node.name,
                existing_context=f"DataType={column.data_type or 'unknown'}",
                upstream_docs=upstream_docs,
                style_profile=style_profile,
                style_examples=style_examples,
                temperature=0.5,
            )

            suggestions_made += 1

            if suggestion.confidence >= threshold:
                column.description = suggestion.text
                suggestions_applied += 1
                logger.info(
                    ":sparkles: Applied column suggestion for '%s' (confidence: %.2f)",
                    column_name,
                    suggestion.confidence,
                )
            else:
                logger.debug(
                    ":heavy_check_mark: Column '%s' suggestion below threshold (confidence: %.2f)",
                    column_name,
                    suggestion.confidence,
                )

    logger.info(
        ":bar_chart: Generated %d suggestions, applied %d for node => %s",
        suggestions_made,
        suggestions_applied,
        node.unique_id,
    )

