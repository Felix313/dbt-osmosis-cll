from __future__ import annotations

import typing as t
from collections import defaultdict, deque
from itertools import chain
from pathlib import Path

from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.nodes import ResultNode

from dbt_osmosis.core import logger

__all__ = [
    "_is_file_match",
    "_is_fqn_match",
    "_iter_candidate_nodes",
    "_topological_sort",
]


def _is_fqn_match(node: ResultNode, fqns: list[str]) -> bool:
    """Filter models based on the provided fully qualified name matching on partial segments."""
    logger.debug(":mag_right: Checking if node => %s matches any FQNs => %s", node.unique_id, fqns)
    for fqn_str in fqns:
        parts = fqn_str.split(".")
        segment_match = len(node.fqn[1:]) >= len(parts) and all(
            left == right for left, right in zip(parts, node.fqn[1:])
        )
        if segment_match:
            logger.debug(":white_check_mark: FQN matched => %s", fqn_str)
            return True
    return False


def _resolve_select_to_ids(context: t.Any) -> frozenset[str]:
    """Resolve dbt selector strings to a set of unique node IDs using dbt's graph engine.

    Uses Linker.get_graph() to build the full DAG from the manifest, then NodeSelector
    to evaluate the selector expression. Supports all dbt selector methods (source:, tag:,
    path:, +, @, unions, intersections) except state: and result: which require run artifacts.
    """
    from dbt.compilation import Linker
    from dbt.graph.cli import parse_union
    from dbt.graph.selector import NodeSelector
    from dbt.graph.selector_spec import IndirectSelection

    manifest = context.project.manifest
    select_strs = list(context.settings.select)

    logger.debug(":mag_right: Resolving dbt selectors => %s", select_strs)

    graph = Linker().get_graph(manifest)
    spec = parse_union(select_strs, expect_exists=False)
    selector = NodeSelector(graph=graph, manifest=manifest, previous_state=None)

    try:
        selected_ids = selector.get_selected(spec)
    except Exception as exc:
        # state: and result: selectors raise here — surface a clear error
        raise ValueError(
            f"dbt selector failed: {exc}\n"
            "Note: 'state:' and 'result:' selectors are not supported in osmosis "
            "(no run artifacts available)."
        ) from exc

    logger.debug(":white_check_mark: Selector resolved => %d node(s)", len(selected_ids))
    return frozenset(selected_ids)


def _is_file_match(node: ResultNode, paths: list[Path | str], root: Path | str) -> bool:
    """Check if a node's file path matches any of the provided file paths or names."""
    node_path = Path(root, node.original_file_path).resolve()
    yaml_path = None
    if node.patch_path:
        absolute_patch_path = Path(root, node.patch_path.partition("://")[-1]).resolve()
        if absolute_patch_path.exists():
            yaml_path = absolute_patch_path
    for model_or_dir in paths:
        model_or_dir = Path(model_or_dir).resolve()
        if node.name == model_or_dir.stem:
            logger.debug(":white_check_mark: Name match => %s", model_or_dir)
            return True
        if model_or_dir.is_dir():
            if model_or_dir in node_path.parents or (
                yaml_path and model_or_dir in yaml_path.parents
            ):
                logger.debug(":white_check_mark: Directory path match => %s", model_or_dir)
                return True
        if model_or_dir.is_file():
            if model_or_dir.samefile(node_path) or (yaml_path and model_or_dir.samefile(yaml_path)):
                logger.debug(":white_check_mark: File path match => %s", model_or_dir)
                return True
    return False


def _topological_sort(
    candidate_nodes: list[tuple[str, ResultNode]],
) -> list[tuple[str, ResultNode]]:
    """Perform a topological sort on the given candidate_nodes (uid, node) pairs
    based on their dependencies. If a cycle is detected, raise a ValueError.

    Kahn's Algorithm:
      1) Build adjacency list: parent -> {child, child, ...}
         (Because if node 'child' depends on 'parent', we have an edge parent->child).
      2) Compute in-degrees for all nodes.
      3) Collect all nodes with in-degree == 0 into a queue.
      4) Repeatedly pop from queue and 'visit' that node,
         then decrement the in-degree of its children.
         If any child's in-degree becomes 0, push it into the queue.
      5) If we visited all nodes, we have a valid topological order.
         Otherwise, a cycle exists.
    """
    adjacency: defaultdict[str, set[str]] = defaultdict(set)
    in_degree: defaultdict[str, int] = defaultdict(int)

    all_uids = {uid for uid, _ in candidate_nodes}

    for uid, _ in candidate_nodes:
        in_degree[uid] = 0

    for uid, node in candidate_nodes:
        for dep_uid in node.depends_on_nodes:
            if dep_uid in all_uids:
                adjacency[dep_uid].add(uid)
                in_degree[uid] += 1

    queue: deque[str] = deque([uid for uid, deg in in_degree.items() if deg == 0])
    sorted_uids: list[str] = []

    while queue:
        parent_uid = queue.popleft()
        sorted_uids.append(parent_uid)

        for child_uid in adjacency[parent_uid]:
            in_degree[child_uid] -= 1
            if in_degree[child_uid] == 0:
                queue.append(child_uid)

    if len(sorted_uids) < len(candidate_nodes):
        raise ValueError(
            "Cycle detected in node dependencies. Cannot produce a valid topological order.",
        )

    uid_to_node = dict(candidate_nodes)
    return [(uid, uid_to_node[uid]) for uid in sorted_uids]




def _iter_candidate_nodes(
    context: t.Any,  # YamlRefactorContext type will be imported
) -> t.Iterator[tuple[str, ResultNode]]:
    """Iterate over candidate nodes using the context's single selection contract."""
    logger.debug(
        ":mag: Filtering nodes (models/sources/seeds) with user-specified settings => %s",
        context.settings,
    )

    include_external = context.settings.include_external

    # --select path: resolve node IDs via dbt's graph engine, then filter to those IDs.
    # --fqn and --select are mutually exclusive; the CLI enforces this before we get here.
    selected_ids: frozenset[str] | None = None
    if context.settings.select:
        selected_ids = _resolve_select_to_ids(context)

    def f(node: ResultNode) -> bool:
        """Closure to filter models based on the context settings."""
        if node.resource_type not in (NodeType.Model, NodeType.Source, NodeType.Seed):
            return False
        if node.package_name != context.project.runtime_cfg.project_name and not include_external:
            return False
        if node.resource_type == NodeType.Model and node.config.materialized == "ephemeral":
            return False
        if selected_ids is not None:
            # --select: membership check against dbt-resolved IDs (all other filters bypassed)
            return node.unique_id in selected_ids
        if context.settings.models:
            if not _is_file_match(
                node,
                context.settings.models,
                context.project.runtime_cfg.project_root,
            ):
                return False
        if context.settings.fqn:
            if not _is_fqn_match(node, context.settings.fqn):
                return False
        logger.debug(":white_check_mark: Node => %s passed filtering logic.", node.unique_id)
        return True

    candidate_nodes: list[t.Any] = []
    items = chain(context.project.manifest.nodes.items(), context.project.manifest.sources.items())
    for uid, dbt_node in items:
        if f(dbt_node):
            candidate_nodes.append((uid, dbt_node))

    for uid, node in _topological_sort(candidate_nodes):
        yield uid, node
