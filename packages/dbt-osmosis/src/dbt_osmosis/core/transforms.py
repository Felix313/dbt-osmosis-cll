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


def _run_dbt_compile_for_node(context: t.Any, node: t.Any) -> None:
    """Run ``dbt compile --select <model>`` for *node* and invalidate the CLL cache."""
    from dbt_osmosis.core.cll import _compile_node, invalidate_cll_for_node

    project_dir = str(context.project.runtime_cfg.project_root)
    target: str | None = getattr(context.project.runtime_cfg, "target_name", None)
    _compile_node(project_dir, node, target)
    invalidate_cll_for_node(project_dir, node)


__all__ = [
    "TransformOperation",
    "TransformPipeline",
    "_transform_op",
    "enrich_rename_descriptions",
    "inherit_upstream_column_knowledge",
    "inject_missing_columns",
    "remove_columns_not_in_database",
    "report_name_match_fallbacks",
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
        for _, n in _iter_candidate_nodes(context):
            inherit_upstream_column_knowledge(context, n)
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

        # Special case: osmosis_progenitor should always be inherited if add-progenitor-to-meta is enabled,
        # regardless of skip-merge-meta setting. This ensures progenitor tracking works independently.
        if _get_setting_for_node(
            "add-progenitor-to-meta",
            node,
            name,
            fallback=context.settings.add_progenitor_to_meta,
        ):
            # Check if meta has osmosis_progenitor in kwargs (check both top-level and config.meta for dbt 1.10)
            meta_progenitor = kwargs.get("meta", {}).get("osmosis_progenitor")
            if not meta_progenitor:
                meta_progenitor = kwargs.get("config", {}).get("meta", {}).get("osmosis_progenitor")
            if meta_progenitor:
                # Ensure meta is in inheritable if not already present
                if "meta" not in inheritable:
                    inheritable.append("meta")

        # Special case: if force_inherit_descriptions is False and the local column already has
        # a description, don't inherit the description from upstream (preserve local description).
        # If anchor-description is set on a column via meta, it is exempt from
        # force-inherit-descriptions — the manually curated description is preserved even during
        # a forced re-propagation pass. The CLL annotation step (enrich_rename_descriptions) runs
        # as a separate transform and is unaffected by anchoring.
        #
        # Exception: a column whose description consists ONLY of a CBM-ODP annotation (no real
        # business content) is treated as effectively un-anchored, even if anchor-description is
        # set at folder level.  Annotation-only descriptions are derived, not manually curated,
        # so they should always be refreshed from upstream.
        force_inherit = _get_setting_for_node(
            "force-inherit-descriptions",
            node,
            name,
            fallback=context.settings.force_inherit_descriptions,
        )
        is_anchored = bool(_get_setting_for_node("anchor-description", node, name, fallback=False))
        existing_desc = node_column.description.strip()
        if is_anchored and existing_desc:
            from dbt_osmosis.core.cll import strip_all_cbm_tags
            annotation_only = not strip_all_cbm_tags(existing_desc).strip()
            if annotation_only:
                is_anchored = False  # annotation-only ⟹ treat as un-anchored
        if (
            "description" in inheritable
            and (not force_inherit or is_anchored)
            and existing_desc
        ):
            inheritable.remove("description")

        updated_metadata = {k: v for k, v in kwargs.items() if v is not None and k in inheritable}
        logger.debug(
            ":star2: Inheriting updated metadata => %s for column => %s",
            updated_metadata,
            name,
        )
        node.columns[name] = node_column.replace(**updated_metadata)


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
        for _ in context.pool.map(
            partial(inject_missing_columns, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
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

    from dbt_osmosis.core.cll import get_model_columns_from_cll, invalidate_cll_for_node
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
                node.columns[existing_col.name] = existing_col.replace(description=incoming_meta.comment)


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





@_transform_op("Enrich Column Origins")
def enrich_rename_descriptions(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Annotate columns with CLL-traced origin via meta tags and optional description injection.

    For each column that was **renamed** relative to its ultimate source column:
      - Writes ``cbm_source_name: SCHEMA.MODEL.COL`` to column meta.
      - Writes ``cbm_source_description: <text>`` when the source YAML has a description.
      - Removes stale tags from passthrough columns (same name end-to-end).
    For each column derived from multiple sources (unresolvable progenitor):
      - Writes ``cbm_derived_col: SCHEMA.MODEL`` to column meta.

    When ``append-col-origin-to-description: true`` is also set:
      - Renamed/transformed columns (col name differs from ultimate source col):
        appends ``CBM_ORIGIN: SCHEMA.MODEL.COL`` to description.
      - Multi-source columns: appends ``CBM_DERIVED_IN: SCHEMA.MODEL`` to description.
      - Passthrough columns (same name end-to-end): no description injection.

    Controlled by ``add-col-origin-to-meta: true`` in ``+dbt-osmosis-options``.
    Sources are always skipped (no SQL to trace through).
    """
    from dbt.artifacts.resources.types import NodeType
    from dbt_osmosis.core.cll import (
        format_computed_origin_tag,
        format_derived_tag,
        format_origin_tag,
        get_column_origin,
        get_cll_results,
        get_origin_source_description,
        strip_all_cbm_tags,
    )
    from dbt_osmosis.core.introspection import _get_setting_for_node
    from dbt_osmosis.core.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info(":wave: Enriching column origins across all matched nodes.")
        for _ in context.pool.map(
            partial(enrich_rename_descriptions, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return

    if not _get_setting_for_node("add-col-origin-to-meta", node, fallback=False):
        return
    if node.resource_type == NodeType.Source:
        return

    append_to_desc = _get_setting_for_node("append-col-origin-to-description", node, fallback=False)

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

    for col_name, node_col in node.columns.items():
        result = result_by_col.get(col_name.lower())
        if result is None:
            continue

        new_meta = dict(node_col.meta or {})
        progenitor_col_raw = result.progenitor_column
        is_multi_source = result.is_computed and progenitor_col_raw is None

        if is_multi_source:
            new_meta["cbm_derived_col"] = node_ref
            new_meta.pop("cbm_source_name", None)

            if append_to_desc:
                # Multi-source: "Abgeleitet aus: SCHEMA.MODEL" — can't trace to a single column
                derived_tag = format_derived_tag(node_schema, node.name.upper())
                base_desc = strip_all_cbm_tags((node_col.description or "").strip())
                new_desc = f"{base_desc}\n\n{derived_tag}".strip() if base_desc else derived_tag
                node.columns[col_name] = node_col.replace(meta=new_meta, description=new_desc)
            else:
                node.columns[col_name] = node_col.replace(meta=new_meta)

            logger.debug(":pencil: %s.%s => derived in %s", node.name, col_name, node_ref)

        else:
            origin = get_column_origin(context, node, col_name)
            if origin is None:
                continue

            schema, origin_model, origin_col = origin
            # Use CLL's is_rename (pure alias, no expression) rather than a name-comparison
            # heuristic. is_computed covers casts, aggregations, CASE, arithmetic, etc.
            is_pure_rename = result.is_rename
            is_name_changed = col_name.upper() != origin_col.upper()
            new_meta.pop("cbm_derived_col", None)

            if is_name_changed:
                new_meta["cbm_source_name"] = f"{schema}.{origin_model}.{origin_col}"
                source_desc = get_origin_source_description(context, schema, origin_model, origin_col)
                if source_desc:
                    new_meta["cbm_source_description"] = source_desc
                else:
                    new_meta.pop("cbm_source_description", None)
            else:
                new_meta.pop("cbm_source_name", None)
                new_meta.pop("cbm_source_description", None)

            if append_to_desc and is_name_changed:
                # Pure alias rename → "Basiert auf:"; computed (cast/agg/expr) → "Abgeleitet aus:"
                if is_pure_rename:
                    origin_annotation = format_origin_tag(
                        origin_col, origin_model, new_meta.get("cbm_source_description")
                    )
                else:
                    origin_annotation = format_computed_origin_tag(
                        origin_col, origin_model, new_meta.get("cbm_source_description")
                    )
                base_desc = strip_all_cbm_tags((node_col.description or "").strip())
                if not base_desc or base_desc in context.placeholders:
                    base_desc = ""
                new_desc = f"{base_desc}\n\n{origin_annotation}".strip() if base_desc else origin_annotation
                node.columns[col_name] = node_col.replace(meta=new_meta, description=new_desc)
            else:
                node.columns[col_name] = node_col.replace(meta=new_meta)

            if is_name_changed:
                logger.debug(":pencil: %s.%s => renamed from %s", node.name, col_name, new_meta["cbm_source_name"])


@_transform_op("Report Name-Match Fallbacks")
def report_name_match_fallbacks(
    context: YamlRefactorContextProtocol,
    node: ResultNode | None = None,
) -> None:
    """Emit a warning summary of columns that fell back to name-matching.

    When CLL has no lineage data for a column (parser failure, SQL construct CLL
    can't trace, or CLL never ran for a model), osmosis falls back to name-matching
    to find a description.  This transform collects those cases from the
    inheritance accumulator and logs a single summary at the end of the pipeline
    so developers know which models may have inherited descriptions via heuristics
    rather than deterministic column-level lineage.
    """
    if node is not None:
        # Only meaningful as a full-run (context-level) step; skip per-node calls.
        return

    from dbt_osmosis.core.inheritance import get_and_clear_name_match_fallback

    fallbacks = get_and_clear_name_match_fallback()
    if not fallbacks:
        logger.info(
            ":white_check_mark: All column lineage resolved via CLL — no name-matching fallbacks used."
        )
        return

    lines = [
        f":warning: CLL had no lineage data for {sum(len(v) for v in fallbacks.values())} "
        f"column(s) across {len(fallbacks)} model(s) — name-matching was used as fallback:"
    ]
    for uid, cols in sorted(fallbacks.items()):
        # Shorten "model.project.schema.name" to "schema.name" for readability
        parts = uid.split(".")
        short = ".".join(parts[-2:]) if len(parts) >= 2 else uid
        lines.append(f"  {short}: {', '.join(cols)}")
    lines.append(
        "  → Verify these columns inherited the correct description. "
        "CLL could not trace their lineage (unsupported SQL construct or parser failure)."
    )
    logger.warning("\n".join(lines))


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

