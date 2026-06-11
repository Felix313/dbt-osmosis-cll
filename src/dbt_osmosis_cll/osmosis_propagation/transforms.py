from __future__ import annotations

import atexit
import time
import typing as t
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path  # used by callers that import from this module
from types import MappingProxyType

from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.nodes import ResultNode, ColumnInfo  # pyright: ignore[reportPrivateImportUsage]

if t.TYPE_CHECKING:
    from dbt_osmosis_cll.osmosis_propagation.dbt_protocols import (
        YamlRefactorContextProtocol,
    )

from dbt_osmosis_cll.osmosis_propagation import logger
from dbt_osmosis_cll.osmosis_propagation.inheritance import _safe_column_replace
from dbt_osmosis_cll.osmosis_propagation.settings import get_managed_meta_keys
from dbt_osmosis_cll.config import get_config



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
                from dbt_osmosis_cll.osmosis_propagation.sync_operations import sync_node_to_yaml

                sync_node_to_yaml(context, node, commit=True)
                logger.info("[b]Committed[/b] \n")
        self._metadata["completed_at"] = (pipeline_end := time.time())

        logger.info(
            ":checkered_flag: [b]Manifest transformation pipeline [green]completed[/green] in => %.2fs[/b]",
            pipeline_end - pipeline_start,
        )

        def _commit() -> None:
            """Commit changes to YAML files. Designed for use as an atexit handler.

            Per-node errors are collected (not raised) and surfaced as a single
            run-end summary so the user does not have to scroll the log to find
            them.
            """
            logger.info("Committing all changes to YAML files in batch.")
            _commit_start = time.time()
            failures: list[tuple[str, str]] = []
            try:
                from dbt_osmosis_cll.osmosis_propagation.sync_operations import sync_node_to_yaml

                sync_node_to_yaml(context, node, commit=True, failures=failures)
            except Exception as e:
                # Catch-all so atexit shutdown is never interrupted.
                logger.error("Batch commit aborted: %s", e)
                failures.append(("<batch>", f"{type(e).__name__}: {e}"))

            _commit_end = time.time()
            logger.info(
                ":checkered_flag: YAML commits completed in => %.2fs (%d failure(s))",
                _commit_end - _commit_start,
                len(failures),
            )
            self._metadata["commit_failures"] = failures
            if failures:
                sep = "=" * 72
                lines = [
                    "",
                    sep,
                    f":boom: [bold red]YAML COMMIT FAILURES — {len(failures)} node(s) NOT written[/bold red]",
                    sep,
                ]
                for unique_id, msg in failures:
                    lines.append(f"  - {unique_id}: {msg}")
                lines.append(sep)
                logger.error("\n".join(lines))

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
                from dbt_osmosis_cll.integration.cll import clear_cll_failures, get_cll_failures
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

            # Emit the origin-walk soft-fail summary (cycle / max-depth). These columns
            # resolved to no inherited description/origin because the walk bailed — not hard
            # errors, but worth surfacing once at the end rather than silently dropping.
            try:
                from dbt_osmosis_cll.integration.cll import (
                    clear_cll_walk_soft_fails,
                    get_cll_walk_soft_fails,
                )
                soft_fails = get_cll_walk_soft_fails(context)
                _labels = {
                    "max-depth": "exceeded the max lineage depth",
                    "cycle": "hit a lineage cycle",
                }
                for reason, refs in sorted(soft_fails.items()):
                    if not refs:
                        continue
                    logger.warning(
                        ":warning: CLL origin walk %s for %d column(s) — these kept their "
                        "existing description and got no desc-source tag:\n  %s",
                        _labels.get(reason, reason),
                        len(refs),
                        "\n  ".join(sorted(refs)),
                    )
                clear_cll_walk_soft_fails(context)
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
        logger.info("Inheriting column knowledge across all matched nodes.")
        from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

        # Must process sequentially in topological order so that upstream in-memory
        # state is already updated when downstream nodes inherit from it.
        # pool.map would process concurrently and break the cascade (requires 2 passes).
        nodes = list(_iter_candidate_nodes(context))
        total = len(nodes)
        for i, (_, n) in enumerate(nodes, start=1):
            inherit_upstream_column_knowledge(context, n)
            if i % 25 == 0 or i == total:
                logger.info("Inherit Upstream Column Knowledge progress => %d / %d", i, total)
        return

    logger.debug("Inheriting column knowledge for => %s", node.unique_id)

    from dbt_osmosis_cll.osmosis_propagation.inheritance import _build_column_knowledge_graph
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node

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
            from dbt_osmosis_cll.osmosis_propagation.annotations import strip_annotation_tags
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
            from dbt_osmosis_cll.osmosis_propagation.annotations import strip_annotation_tags
            _clean_desc = strip_annotation_tags(updated_metadata["description"]).strip()
            updated_metadata = {**updated_metadata, "description": _clean_desc}

        # Strip osmosis-internal protection markers from inherited meta.
        # MANAGED keys (desc-owner, meta_key_renamed_from, meta_key_derived_from,
        # meta_key_computed_in) must not
        # propagate downstream, but ARE re-applied when the column locally owns them.
        # Both top-level meta AND config.meta are filtered (fusion_compat stores
        # desc-owner in config.meta).
        _managed = get_managed_meta_keys()
        if "meta" in updated_metadata and isinstance(updated_metadata["meta"], dict):
            local_meta = dict(node_column.meta or {})
            filtered_meta = {k: v for k, v in updated_metadata["meta"].items() if k not in _managed}
            # Re-apply managed keys the column itself owns (e.g. desc-owner: aml set by AML injection)
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


def _owns_description(
    context: YamlRefactorContextProtocol, node: t.Any, column_name: str
) -> bool:
    """True if *column_name* on *node* OWNS its description rather than inheriting it.

    ``desc-owner`` is the single ownership key: ``upstream`` means force-inherit
    (the column holds a *copy* of its upstream description); any other value
    (``this``, ``aml``, …) anchors the description at this model. An owning column
    defines the authoritative "new truth" from here downstream and walls off
    force-inheritance.

    A non-owning column (``desc-owner: upstream``) merely holds a copy of its
    upstream's description; the walker must resolve it transitively instead of
    trusting the stored copy, otherwise a stale/incorrect copy gets laundered
    downstream and propagation stops being idempotent.
    """
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node

    cols = getattr(node, "columns", {})
    actual = next((k for k in cols if k.lower() == column_name.lower()), column_name)

    authority = _get_setting_for_node("desc-owner", node, actual, fallback="this")
    return str(authority).lower() != "upstream"


def _resolve_cll_description(
    context: YamlRefactorContextProtocol,
    parent_model_name: str,
    parent_col_name: str,
    depth: int = 0,
    max_depth: int | None = None,
    visited: set[tuple[str, str]] | None = None,
) -> tuple[str | None, str | None]:
    """Resolve a column's authoritative description AND its true origin via CLL lineage.

    Returns ``(description, origin_ref)``:

    - ``description`` — the authoritative text to inherit. A node's *own stored* description
      is used only when that node OWNS the value: a source, an anchored column that
      **re-defines** the text (``desc-owner`` != ``upstream`` *and* its text differs from
      upstream), a column originating here (no progenitor), or a computed wall
      (aggregate/window/literal/generated/multi-source) where the value is born. A
      non-anchored force-inherit *copy* is never returned — the walk recurses past it so a
      stale copy is never laundered downstream.

    - ``origin_ref`` — ``MODEL.COLUMN`` (uppercase; source table name for sources) of the
      node where that text was **first defined**, i.e. the single place to edit it to change
      every downstream copy. The walk passes *through* same-text copies (force-inherit or
      gap-filled) and stops only where the text changes (a re-definition / anchor), at a
      source, computed wall, union, or chain end. ``origin_ref`` is ``None`` when no
      description resolves, and for unions (no single origin).

    Because the walk resolves from origins/anchors via the stable buffer rather than from
    intermediate copies, propagation is a pure function of {origins, anchors, config} and
    idempotent in a single pass. The owner the walk stops at IS the origin, so the inherited
    text and its ``desc-source`` pointer always agree.

    Stops at: anchors that re-define, computed walls, source nodes, origins, unresolvable
    nodes, depth limit, or a self-referencing cycle (e.g. incremental models that select from
    `{{ this }}` produce M.col → M.col in CLL).
    """
    from dbt_osmosis_cll.integration.cll import (
        _SOURCE_INDEX,
        _NODE_INDEX,
        _ensure_manifest_index,
        get_cll_results,
        is_computation_wall,
        record_cll_walk_soft_fail,
    )
    from dbt_osmosis_cll.osmosis_propagation.annotations import strip_annotation_tags
    from dbt_osmosis_cll.osmosis_propagation.inheritance import _read_ancestor_yaml_description
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node

    if max_depth is None:
        max_depth = get_config().cll_max_origin_depth

    if visited is None:
        visited = set()
    key = (parent_model_name.lower(), parent_col_name.lower())
    here_ref = f"{parent_model_name.upper()}.{parent_col_name.upper()}"
    # Cycle guard — a node already on THIS walk path is re-entered. Direct self-refs
    # (incremental {{ this }}, M.col → M.col) are short-circuited at step 8 before they get
    # here, and union branches each get their own `visited` copy so a diamond (two branches
    # reconverging) is not mistaken for a cycle — so reaching this point means a genuine
    # multi-node lineage loop. Record it as a soft-fail and stop (the column resolves to no
    # inherited description rather than erroring).
    if key in visited:
        record_cll_walk_soft_fail(context, "cycle", here_ref)
        logger.debug(":repeat: CLL walk hit a lineage cycle at => %s", here_ref)
        return None, None
    visited.add(key)

    # 1. Depth guard — protects against cyclic or pathological lineage chains. Soft-fail:
    # the column resolves to no inherited description; surfaced in the end-of-run summary.
    if depth > max_depth:
        record_cll_walk_soft_fail(context, "max-depth", here_ref)
        logger.debug(
            ":warning: CLL walk exceeded max depth (%d) at => %s",
            max_depth,
            here_ref,
        )
        return None, None

    # 2. Resolve the parent model name to a manifest node (source or model).
    _ensure_manifest_index(context)
    project_dir = str(context.project.runtime_cfg.project_root)
    src_node = _SOURCE_INDEX[project_dir].get(parent_model_name.lower())
    model_node = _NODE_INDEX[project_dir].get(parent_model_name.lower())
    upstream_node = src_node or model_node
    if upstream_node is None:
        return None, None

    # Read this node's OWN stored description (in-memory first — surfaces anchors/origins
    # enriched earlier in THIS run — then the stable YAML buffer for nodes outside the
    # candidate set).
    def _own_description() -> str | None:
        cols = getattr(upstream_node, "columns", {})
        col_info = next(
            (v for k, v in cols.items() if k.lower() == parent_col_name.lower()), None
        )
        if col_info:
            raw = getattr(col_info, "description", None) or ""
            cleaned = strip_annotation_tags(raw).strip()
            if cleaned and cleaned not in context.placeholders:
                return cleaned
        variants = [parent_col_name, parent_col_name.upper(), parent_col_name.lower()]
        yaml_desc = _read_ancestor_yaml_description(context, upstream_node, variants)
        if yaml_desc:
            cleaned = strip_annotation_tags(yaml_desc).strip()
            if cleaned and cleaned not in context.placeholders:
                return cleaned
        return None

    # This node IS the origin of its own description (terminal: source/computed/origin).
    def _own() -> tuple[str | None, str | None]:
        own = _own_description()
        return (own, here_ref) if own is not None else (None, None)

    # 3. Sources are terminal origins — their DB/authored description is authoritative.
    if src_node is not None:
        return _own()

    # 4. Ask CLL how this column is produced.
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
        # No lineage for this column — best effort: trust whatever it stores.
        return _own()

    _is_union = getattr(parent_result, "is_union", False)

    # 5. Union column — agreement-aware: recurse into every branch, then accept the
    # description iff at most one distinct non-empty answer is found (single populated
    # branch, or all branches agree). Two or more different descriptions = real semantic
    # conflict; fall back to a locally authored description at the union node. A union has
    # no single origin, so origin_ref is None whenever the text comes from the branches.
    if _is_union:
        union_branches = getattr(parent_result, "union_branches", []) or []
        if not union_branches:
            return _own()
        from dbt_osmosis_cll.osmosis_propagation.annotations import descriptions_equivalent

        branch_descs: list[str] = []
        for branch_model, branch_col in union_branches:
            # Each branch is an independent path → give it its own visited copy so two
            # branches reconverging on a shared ancestor (a diamond) is not mistaken for a
            # cycle. A real loop within a branch is still caught by that branch's copy.
            branch_desc, _ = _resolve_cll_description(
                context,
                branch_model,
                branch_col,
                depth + 1,
                max_depth,
                set(visited),
            )
            if branch_desc:
                branch_descs.append(branch_desc)
        if not branch_descs:
            return _own()
        first = branch_descs[0]
        if all(descriptions_equivalent(first, other) for other in branch_descs[1:]):
            return first, None
        # Real conflict — must be authored at the union node itself.
        return _own()

    # 6. Computed wall — the value is BORN here (aggregate / window / literal / generated
    # / multi-source expression). No single upstream column's description transfers, so
    # return only a description authored HERE; never recurse into the computation inputs.
    # Shares one predicate with the annotation tracer (``get_column_origin``) so both stop
    # at exactly the same set of computation walls. (Union is handled above in step 5 with
    # branch-agreement semantics and never reaches here.)
    if is_computation_wall(parent_result):
        return _own()

    # 7. Column originates in this model with no upstream — nothing to inherit from.
    # Note: is_first_in_chain=True with progenitor_model set means "first dbt model
    # in the chain, source is the progenitor" (e.g. staging referencing a source).
    # In that case we DO want to recurse to the source, so only skip when progenitor is None.
    if parent_result.progenitor_model is None:
        return _own()

    # 8. Single-progenitor column. Resolve the upstream FIRST, then decide whether THIS
    # node re-defines the text or merely carries a copy of it:
    #   • anchor that re-defines (owns it AND its text differs from upstream) → wall: the
    #     text and the origin are HERE; the walk stops (new truth from here downstream).
    #   • otherwise, if the upstream resolved a description → pass through: take the upstream
    #     text AND its (deeper) origin. This walks transitively through same-text copies —
    #     force-inherited or gap-filled — to the node where the text was first defined, so a
    #     stale intermediate copy is never laundered and the origin is the true source.
    #   • upstream resolved nothing → an owner with its own text is the origin here; a
    #     non-anchored copy with no resolvable upstream yields nothing (never laundered).
    progenitor_col = (parent_result.progenitor_column or parent_col_name).strip('"').strip("'")
    # Rename / derivation boundary. When the output name differs from its single upstream
    # source name, THIS hop renames or derives the column — a new identity/value begins here
    # (a pure rename OR a single-source computed column such as a cast or
    # ``CASE WHEN src IS NULL ...``; step 6 only walls *multi-source* computed columns, so
    # single-source derived columns reach this point). Per inherit-through-renames semantics
    # the upstream column's description must NOT cross this boundary unless the node opts in.
    # A same-name passthrough/cast ("aliased back to the same name") is never gated and walks
    # through normally. Checking this at every hop — not just the top node the caller gates —
    # is what stops e.g. META_PREP.FLG_SR_ERSTELLT (a same-name passthrough) from laundering
    # CONTRACT_ACCOUNT_ID's description across the derive boundary at REPORTING_BASE.
    if progenitor_col.lower() != parent_col_name.lower():
        _inherit_renames = _get_setting_for_node(
            "inherit-through-renames",
            model_node,
            parent_col_name,
            fallback=get_config().inherit_through_renames,
        )
        if not _inherit_renames:
            # The text originates HERE (returns this node's own authored text, or nothing).
            return _own()
    if (parent_result.progenitor_model.lower(), progenitor_col.lower()) == (
        parent_model_name.lower(),
        parent_col_name.lower(),
    ):
        # Direct self-reference (e.g. an incremental model selecting from {{ this }} produces
        # M.col → M.col): the column is its own progenitor, so there is nothing upstream to
        # inherit. Treat as no-upstream — it resolves to its own description below — and do
        # NOT recurse, so this expected pattern never trips the cycle guard / soft-fail log.
        parent_text, parent_origin = None, None
    else:
        parent_text, parent_origin = _resolve_cll_description(
            context,
            parent_result.progenitor_model.lower(),
            progenitor_col,
            depth + 1,
            max_depth,
            visited,
        )
    owns = _owns_description(context, model_node, parent_col_name)
    own = _own_description()
    if owns and own is not None and own != parent_text:
        # Anchored re-definition — the new truth begins here (origin = here), even if the
        # upstream is empty/different. An anchor re-authoring text identical to upstream is
        # NOT a re-definition; it falls through and the deeper origin is kept.
        return own, here_ref
    if parent_text is not None:
        return parent_text, parent_origin
    if owns and own is not None:
        # Owner with its own text and an empty upstream → the text originates here.
        return own, here_ref
    # Non-anchored copy with no resolvable upstream → do not launder the copy.
    return None, None


def _find_cll_description(
    context: YamlRefactorContextProtocol,
    parent_model_name: str,
    parent_col_name: str,
    depth: int = 0,
    max_depth: int | None = None,
    visited: set[tuple[str, str]] | None = None,
) -> str | None:
    """Resolved description only — back-compat wrapper around :func:`_resolve_cll_description`.

    Use ``_resolve_cll_description`` directly when the origin (``desc-source``) is also needed.
    """
    return _resolve_cll_description(
        context, parent_model_name, parent_col_name, depth, max_depth, visited
    )[0]


def _read_column_config_meta(node_col: t.Any) -> dict[str, t.Any]:
    """Return a copy of a column's ``config.meta`` dict (empty if absent).

    ``config`` may be a plain dict or a ColumnConfig object depending on dbt version;
    this normalizes both to a dict copy that is safe to mutate.
    """
    node_config = getattr(node_col, "config", None)
    if node_config is None:
        return {}
    raw = (
        node_config.get("meta", {}) if isinstance(node_config, dict)
        else getattr(node_config, "meta", None) or {}
    )
    return dict(raw or {})


def _build_column_config_with_meta(node_col: t.Any, new_meta: dict[str, t.Any]) -> dict[str, t.Any]:
    """Build a ``config`` dict for ``_safe_column_replace`` carrying *new_meta*.

    Preserves any other keys already on the column's config block.
    """
    from dbt_osmosis_cll.osmosis_propagation.inheritance import _column_to_dict

    col_as_dict = _column_to_dict(node_col, omit_none=True)
    config_base = col_as_dict.get("config") or {}
    return {**config_base, "meta": new_meta}


def _drop_stale_desc_source(node: t.Any, col_name: str, node_col: t.Any, key: str) -> None:
    """Remove a ``desc-source`` provenance tag from a column that is no longer CLL-inherited.

    Used on the early-skip paths (aggregate / window / literal / generated / multi-source /
    originates-here) where a column that previously inherited a description — and so carries a
    provenance tag — has since become non-inheritable.  Keeping the old tag there would make it
    stale, so it is dropped.

    The tag is a managed meta key, so the authoritative removal is from the column's TOP-LEVEL
    ``meta``: the YAML sync writer re-adds managed keys to ``config.meta`` ONLY from top-level
    meta, so leaving a stale tag there would re-persist it.  A stale copy sitting directly in
    ``config.meta`` is also stripped for completeness (though the writer drops managed keys from
    existing ``config.meta`` anyway).  No-op when the key is disabled or the tag is absent.
    """
    if not key:
        return
    top_meta = dict(getattr(node_col, "meta", None) or {})
    cfg_meta = _read_column_config_meta(node_col)
    if key not in top_meta and key not in cfg_meta:
        return
    replace_kwargs: dict[str, t.Any] = {}
    if key in top_meta:
        replace_kwargs["meta"] = {k: v for k, v in top_meta.items() if k != key}
    if key in cfg_meta:
        replace_kwargs["config"] = _build_column_config_with_meta(
            node_col, {k: v for k, v in cfg_meta.items() if k != key}
        )
    node.columns[col_name] = _safe_column_replace(node_col, **replace_kwargs)


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
    - Managed meta keys (desc-owner, CLL origin tags, etc.) are filtered from inherited meta
      and re-applied only from the local column's own meta.
    - No name-matching fallback. CLL failure = column skipped, existing description preserved.
    """
    if node is None:
        logger.info("CLL-driven column inheritance across all matched nodes.")
        from dbt_osmosis_cll.integration.cll import _ensure_manifest_index

        # Build the index dicts once, up front, so the parallel pool only reads them.
        _ensure_manifest_index(context)

        from dbt_osmosis_cll.osmosis_propagation.node_filters import (
            _iter_candidate_nodes,
            _topological_waves,
        )

        nodes = [n for _, n in _iter_candidate_nodes(context)]
        waves = _topological_waves(context, nodes)
        total = len(nodes)
        # Topological waves: every node sees the fully-enriched in-memory state
        # of all its upstream dependencies before its own walker runs. Without
        # this, downstream nodes processed early read pre-pipeline YAML buffer
        # state for upstreams this run is about to populate — and the enrichment
        # cascade requires N additional runs for an N-deep DAG.
        processed = 0
        for wave_idx, wave in enumerate(waves):
            for _ in context.pool.map(
                partial(inherit_upstream_column_knowledge_cll, context),
                wave,
            ):
                processed += 1
                if processed % 25 == 0 or processed == total:
                    logger.info(
                        ":hourglass: CLL Inherit progress => %d / %d (wave %d/%d)",
                        processed, total, wave_idx + 1, len(waves),
                    )
        return

    logger.debug("CLL-driven inheritance for => %s", node.unique_id)

    from dbt_osmosis_cll.integration.cll import (
        _ensure_manifest_index,
        _SOURCE_INDEX,
        _NODE_INDEX,
        get_cll_results,
    )
    from dbt_osmosis_cll.osmosis_propagation.annotations import strip_annotation_tags
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node

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
        logger.debug("CLL unavailable for %s — skipping CLL inheritance.", node.name)
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

        # --- Classify the column ---
        _is_aggregate = getattr(result, "is_aggregate", False)
        _is_window = getattr(result, "is_window", False)
        _is_union = getattr(result, "is_union", False)
        _is_literal = getattr(result, "is_literal", False)
        _is_generated = getattr(result, "is_generated", False)
        _is_multi_src = result.is_computed and result.progenitor_column is None

        if (
            _is_aggregate
            or _is_window
            or _is_literal
            or _is_generated
            or _is_multi_src
        ):
            # No single traceable progenitor → drop any stale provenance tag, then skip.
            _drop_stale_desc_source(node, col_name, node_col, _cfg.desc_source_key)
            continue

        if result.progenitor_model is None and not _is_union:
            # Column originates here with no traceable upstream — nothing to inherit from.
            # Note: is_first_in_chain=True with progenitor_model set means "first dbt model
            # in chain, source is progenitor" (staging→source). Allow inheritance in that case.
            _drop_stale_desc_source(node, col_name, node_col, _cfg.desc_source_key)
            continue

        # True-origin reference for the desc-source provenance tag — the node where the
        # description was first defined (resolved by _resolve_cll_description on the
        # single-progenitor path). Unions have no single origin, so they never get one.
        desc_source_ref: str | None = None

        if _is_union:
            # Agreement-aware union resolution: every branch's description is
            # collected via the walker; the result is accepted iff at most one
            # distinct non-empty answer is found. Empty parents stay silent;
            # only true semantic disagreement between two-or-more populated
            # parents stops inheritance.
            union_branches = getattr(result, "union_branches", []) or []
            desc_to_apply: str | None = None
            if union_branches:
                from dbt_osmosis_cll.osmosis_propagation.annotations import descriptions_equivalent

                branch_descs: list[str] = []
                for branch_model, branch_col in union_branches:
                    bd = _find_cll_description(context, branch_model, branch_col)
                    if bd:
                        branch_descs.append(bd)
                if branch_descs:
                    first = branch_descs[0]
                    if all(descriptions_equivalent(first, other) for other in branch_descs[1:]):
                        desc_to_apply = first
        else:
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
                # Walk the CLL progenitor chain for the authoritative description AND the
                # node where it was first defined. desc-source points at that true origin —
                # the single place to edit for downstream consistency — which the walk reaches
                # by passing through same-text copies and stopping where the text is re-defined.
                progenitor_col = (result.progenitor_column or col_name).strip('"').strip("'")
                desc_to_apply, desc_source_ref = _resolve_cll_description(
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

        # Track whether THIS column's description was gap-filled (was empty before and a
        # CLL description is now applied). Force-inherit overwrites of an existing,
        # already-populated description are NOT gap-fills — those columns are owned
        # upstream and must not receive a desc-source provenance tag.
        _gap_filled = False

        if desc_to_apply is not None:
            # Only write the description if allowed (force_inherit or the column is empty).
            if force_inherit or not existing_desc:
                clean_desc = strip_annotation_tags(desc_to_apply).strip()
                if clean_desc and clean_desc not in context.placeholders:
                    update_kwargs["description"] = clean_desc
                    if not existing_desc:
                        _gap_filled = True

        # --- Inherit tags and meta from the immediate CLL progenitor (not the full chain) ---
        # Unions have no single progenitor — skip tag/meta merge for them. Their
        # branches' tags/meta would have to be unioned which has no clean
        # interpretation (set semantics for tags would lose origin; meta keys
        # could collide). Tags/meta at union nodes are expected to be authored
        # locally, like the description itself when branches disagree.
        if result.progenitor_model is None:
            progenitor_node = None
        else:
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

        # --- desc-source provenance (managed meta key) ---
        # Recomputed from current CLL state on EVERY run rather than written once and left in
        # place, so the tag can never go stale: it always equals the column's CURRENT true
        # origin, or is absent when the column is no longer CLL-inherited. This makes the pass
        # idempotent (a second run with no upstream change is a no-op) and self-correcting (if
        # the lineage moves to a different origin the pointer updates; if the column stops being
        # inherited the tag is dropped).
        #
        # IMPORTANT — write to the column's TOP-LEVEL ``meta``, not ``config.meta`` directly.
        # desc-source is a managed meta key, and the YAML sync writer treats top-level ``meta``
        # as the single source of truth for managed keys: it strips them from any existing
        # ``config.meta`` (to avoid stale accumulation) and re-adds them from top-level ``meta``
        # — placing them under ``config.meta`` in fusion mode, or keeping them top-level in
        # classic mode. A value written straight to ``config.meta`` here is therefore dropped on
        # write in BOTH modes. Routing through ``meta`` lets the writer persist it correctly.
        #
        # A column counts as CLL-inherited — and therefore carries the tag, pointing at
        # ``desc_source_ref`` — when it is not owned upstream (force_inherit), has a single
        # resolvable origin, has a non-empty final description, AND any of:
        #   • it was gap-filled this run; or
        #   • its description still matches the resolved upstream text — covers the idempotent
        #     re-match on later runs and backfills descriptions that were inherited before this
        #     feature existed (no tag yet, but content proves the inheritance); or
        #   • it already carried the tag — keeps provenance alive when ``desc-owner: this``
        #     freezes the child while the upstream description text later drifts. The frozen
        #     text no longer matches upstream, but the column is still sourced from the same
        #     origin, so the pointer is correct, not stale.
        # Everything else — owned upstream, unions, the early-skipped computed/aggregate kinds
        # (handled above), an unresolved/empty description, or a genuinely locally authored
        # description that was never inherited — carries no tag, and any existing tag is dropped.
        # To claim local ownership of a previously inherited description, remove its desc-source
        # tag; while the tag is present the column is treated as inherited.
        _desc_source_key = _cfg.desc_source_key
        if _desc_source_key:
            _orig_meta = dict(node_col.meta or {})
            # Detect a prior tag wherever it loaded from: top-level meta (classic) or a
            # fusion-written config.meta. Either counts as "already inherited".
            _had_tag = (
                _desc_source_key in _orig_meta
                or _desc_source_key in _read_column_config_meta(node_col)
            )

            _final_desc_raw = update_kwargs.get("description", existing_desc)
            _final_desc = (
                strip_annotation_tags(_final_desc_raw).strip() if _final_desc_raw else ""
            )
            _resolved_upstream = (
                strip_annotation_tags(desc_to_apply).strip() if desc_to_apply else ""
            )

            # A column never cites itself as its own source: when the origin resolves back to
            # this very column (e.g. an incremental model selecting from {{ this }}), the text
            # originates here, so it is authored, not inherited → no tag.
            _self_ref = f"{node.name.upper()}.{col_name.upper()}"

            # desc-source is for GAP-FILL columns only — those whose description is inherited via
            # CLL because the layer owns descriptions but gap-fills empties (desc-owner: this,
            # the default). It is NOT written for:
            #   • desc-owner: upstream (force-inherit) — transparent passthroughs, covered by
            #     CBM-ODP annotations, owned upstream;
            #   • a NAMED anchor (desc-owner set to anything else, e.g. "aml") — the description
            #     is explicitly authored/owned at that column (e.g. injected by AML enrichment),
            #     so the column is an ORIGIN; a CLL-lineage pointer would mischaracterize it even
            #     when its text happens to match an upstream source.
            _is_gap_fill_owner = str(desc_authority).lower() == "this"

            _is_inherited = (
                not force_inherit
                and _is_gap_fill_owner
                and desc_source_ref is not None
                and desc_source_ref != _self_ref
                and bool(_final_desc)
                and (_gap_filled or _final_desc == _resolved_upstream or _had_tag)
            )

            # Apply onto the meta the writer will persist: the tag/meta block above may already
            # have staged update_kwargs["meta"]; otherwise start from the column's current meta.
            _new_meta = dict(update_kwargs["meta"]) if "meta" in update_kwargs else dict(_orig_meta)
            if _is_inherited:
                _new_meta[_desc_source_key] = desc_source_ref
            else:
                _new_meta.pop(_desc_source_key, None)
            if _new_meta != _orig_meta:
                update_kwargs["meta"] = _new_meta
            else:
                # desc-source reverted the meta block's change to a no-op → don't write meta.
                update_kwargs.pop("meta", None)

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
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node, get_columns
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if _get_setting_for_node("skip-add-columns", node, fallback=context.settings.skip_add_columns):
        logger.debug("Skipping column injection (skip_add_columns=True).")
        return
    if node is None:
        logger.info("Injecting missing columns for all matched nodes.")
        # Batch-prefetch source columns in a single DB round trip before per-node processing.
        # For source nodes, get_columns always hits the DB (no CLL) — batching here eliminates
        # the N sequential round trips that make large source refreshes slow.
        from dbt_osmosis_cll.osmosis_propagation.introspection import prefetch_columns

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
                logger.info("Inject Missing Columns progress => %d / %d", i, total)
        return
    if (
        _get_setting_for_node(
            "skip-add-source-columns",
            node,
            fallback=context.settings.skip_add_source_columns,
        )
        and node.resource_type == NodeType.Source
    ):
        logger.debug("Skipping column injection (skip_add_source_columns=True).")
        return

    from dbt_osmosis_cll.integration.cll import get_model_columns_from_cll
    from dbt_osmosis_cll.osmosis_propagation.annotations import descriptions_equivalent
    from dbt_osmosis_cll.osmosis_propagation.introspection import normalize_column_name

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
            if existing_col is not None and not descriptions_equivalent(
                existing_col.description, incoming_meta.comment,
            ):
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
    from dbt_osmosis_cll.osmosis_propagation.introspection import (
        _get_setting_for_node,
        get_columns,
        normalize_column_name,
    )
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info("Removing columns not in DB across all matched nodes.")
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
    from dbt_osmosis_cll.integration.cll import get_model_columns_from_cll

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
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node, get_columns, normalize_column_name
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info("Sorting columns as they appear in DB across all matched nodes.")
        for _ in context.pool.map(
            partial(sort_columns_as_in_database, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    logger.debug("Sorting columns by warehouse order => %s", node.unique_id)
    from dbt_osmosis_cll.integration.cll import get_model_columns_from_cll
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
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info("Sorting columns alphabetically across all matched nodes.")
        for _ in context.pool.map(
            partial(sort_columns_alphabetically, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    logger.debug("Sorting columns alphabetically => %s", node.unique_id)

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
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info("Sorting columns as configured across all matched nodes.")
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
    from dbt_osmosis_cll.osmosis_propagation.introspection import (
        _get_setting_for_node,
        get_columns,
        normalize_column_name,
    )
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info("Populating data types across all matched nodes.")
        for _ in context.pool.map(
            partial(synchronize_data_types, context),
            (n for _, n in _iter_candidate_nodes(context)),
        ):
            ...
        return
    logger.debug("Synchronizing data types => %s", node.unique_id)
    from dbt_osmosis_cll.integration.cll import get_model_columns_from_cll
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
    from dbt_osmosis_cll.integration.cll import (
        get_column_origin,
        get_cll_results,
        get_origin_source_description,
    )
    from dbt_osmosis_cll.osmosis_propagation.annotations import (
        descriptions_equivalent,
        format_aggregate_from_tag,
        format_aggregate_in_tag,
        format_computed_here_tag,
        format_computed_origin_tag,
        format_derived_tag,
        format_generated_tag,
        format_literal_tag,
        format_origin_tag,
        format_union_tag,
        format_window_from_tag,
        format_window_in_tag,
        strip_annotation_tags,
    )
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    if node is None:
        logger.info("Enriching column origins across all matched nodes.")
        from dbt_osmosis_cll.osmosis_propagation.node_filters import _topological_waves

        nodes = [n for _, n in _iter_candidate_nodes(context)]
        waves = _topological_waves(context, nodes)
        total = len(nodes)
        # Same topological-wave reasoning as the inherit pass: annotate's deep
        # tracer reads upstream descriptions; running upstream before downstream
        # within a single pass keeps that read consistent run-to-run.
        processed = 0
        for wave_idx, wave in enumerate(waves):
            for _ in context.pool.map(
                partial(annotate_column_origins, context),
                wave,
            ):
                processed += 1
                if processed % 25 == 0 or processed == total:
                    logger.info(
                        ":hourglass: Annotate Column Origins progress => %d / %d (wave %d/%d)",
                        processed, total, wave_idx + 1, len(waves),
                    )
        return

    if node.resource_type == NodeType.Source:
        return

    from dbt_osmosis_cll.config import get_column_docs as _get_col_docs_fn
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
                from dbt_osmosis_cll.osmosis_propagation.inheritance import _column_to_dict
                col_as_dict = _column_to_dict(node_col, omit_none=True)
                config_base = col_as_dict.get("config") or {}
                cleaned_config = {**config_base, "meta": raw_config_meta}

            stripped_desc = strip_annotation_tags((node_col.description or "").strip())
            if _col_docs:
                central_doc = _col_docs.get(col_name.lower())
                # Glossary columns are CLL-ignored and centrally owned, so the
                # glossary is authoritative: its description always wins (overwrite,
                # not just gap-fill). This lets edits to the central glossary
                # propagate to already-documented columns on the next run.
                if central_doc:
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
                from dbt_osmosis_cll.osmosis_propagation.inheritance import _column_to_dict
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
            from dbt_osmosis_cll.osmosis_propagation.inheritance import _column_to_dict
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
                # Non-annotation mode: annotation is disabled for this layer, so just strip any
                # stale CBM-ODP tags from the column's OWN description and write it back.
                # annotate_column_origins never SETS a description — descriptions are owned
                # exclusively by propagation (inherit_upstream_column_knowledge_cll), which walls
                # aggregate / window / literal / generated / union columns (their value is born
                # here, so no single upstream description transfers). (Previously this path copied
                # the immediate progenitor's description onto these columns — propagation in the
                # annotation function — which laundered e.g. SERVICE_CONTRACT_TYPE's text onto the
                # MAX(CASE ...) HAS_* flags. Propagation now owns that decision.)
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)
            continue

        if is_multi_source:
            if _write_meta:
                # Bare model name (no schema) — consistent with renamed_from / derived_from.
                new_meta[_key_computed] = node.name.upper()
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
                # Multi-source: born in this model — "Computed here", listing the
                # direct inputs when CLL preserved them (roadmap #5): the endpoint
                # reader sees what feeds the expression without opening the SQL.
                _progenitor_pairs = getattr(result, "progenitors", None) or []
                _inputs = [
                    f"{str(m).upper()}.{str(c).upper()}"
                    for m, c in _progenitor_pairs
                    if m and c
                ]
                computed_tag = format_computed_here_tag(inputs=_inputs or None)
                new_desc = f"{base_desc}\n\n{computed_tag}".strip() if has_real_desc else computed_tag
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=new_desc, **_config_kwarg)
            else:
                # No annotation mode: still write stripped description to remove any old tags.
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)

            logger.debug("%s.%s => derived in %s", node.name, col_name, node_ref)

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

            # Self-referencing origin: the deep trace looped back to this node
            # (e.g. an incremental model joined with {{ this }}, or a within-model
            # rename that CLL cannot resolve past). Treat as unresolvable — strip
            # stale tags and keep the column's own description unchanged.
            if origin_model.upper() == node.name.upper():
                base_desc = strip_annotation_tags((node_col.description or "").strip())
                node.columns[col_name] = _safe_column_replace(node_col, meta=new_meta, description=base_desc, **_config_kwarg)
                continue

            if not origin_col:
                # Computed column found upstream — origin_model is where it was computed.
                # Write "computed in: SCHEMA.MODEL" rather than a column-level reference.
                # If entry_col differs from col_name, a rename occurred along the chain —
                # append "(as ENTRY_COL)" so the reader knows what to search for in that model.
                if _write_meta:
                    # Bare model name (no schema) — consistent with renamed_from / derived_from
                    # meta and the unqualified annotation tags.
                    new_meta[_key_computed] = origin_model
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

            # NOTE: annotate_column_origins never SETS a column description. Descriptions are
            # owned exclusively by propagation (inherit_upstream_column_knowledge_cll +
            # _resolve_cll_description), which respects inherit-through-renames per hop. This
            # function only appends the CBM-ODP provenance tag. (Previously a
            # "base_desc = source_desc" promotion bridged the rename gap here by copying the
            # deep-origin description onto empty columns — that laundered descriptions across
            # rename/derivation boundaries regardless of inherit-through-renames, e.g.
            # FLG_SR_ERSTELLT absorbing CONTRACT_ACCOUNT_ID's description. Propagation now
            # owns that decision.) source_desc below feeds only the annotation text.

            # Determine annotation block.
            # annotation-include-source-description is a per-node option (default true):
            #   true  → inject source desc into annotation when it differs from base_desc
            #   false → write origin reference (TABLE.COL) only (DP layers)
            origin_annotation: str | None = None
            annotation_src_desc: str | None = None
            if _get_setting_for_node("annotation-include-source-description", node, fallback=True):
                annotation_src_desc = (
                    source_desc
                    if source_desc and not descriptions_equivalent(source_desc, base_desc)
                    else None
                )
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
                logger.debug("%s.%s => renamed from %s.%s", node.name, col_name, schema, origin_col)
