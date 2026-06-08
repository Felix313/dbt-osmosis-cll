from __future__ import annotations

import typing as t
from importlib import import_module
from types import MappingProxyType

from dbt.contracts.graph.nodes import ModelNode, ResultNode, SeedNode, SourceDefinition

if t.TYPE_CHECKING:
    from dbt_osmosis_cll.osmosis_propagation.dbt_protocols import YamlRefactorContextProtocol

from dbt_osmosis_cll.osmosis_propagation import logger

__all__ = [
    "_build_column_knowledge_graph",
    "_build_graph_edge",
    "_build_node_ancestor_tree",
    "_clean_graph_edge",
    "_collect_column_variants",
    "_column_to_dict",
    "_ensure_column_config_attr",
    "_find_matching_column",
    "_get_node_yaml",
    "_get_unrendered",
    "_merge_graph_node_data",
    "_read_ancestor_yaml_description",
    "_safe_column_replace",
]


def _ensure_column_config_attr(column: t.Any) -> None:
    """Ensure ColumnInfo has a 'config' attribute set.

    In dbt-core 1.10+, ColumnInfo objects may be missing the 'config' attribute,
    which causes mashumaro serialization (to_dict / replace) to fail. This helper
    sets a default ColumnConfig() on the instance when the attribute is absent.
    Safe to call repeatedly.
    """
    if hasattr(column, "config"):
        return
    try:
        module = import_module("dbt.artifacts.resources.v1.components")
        column_config = getattr(module, "ColumnConfig", None)
        if column_config is not None:
            t.cast("t.Any", column).config = column_config()
    except (ImportError, AttributeError):
        pass  # Older dbt version, attribute should already exist


def _column_to_dict(column: t.Any, **kwargs: t.Any) -> dict[str, t.Any]:
    """Convert a ColumnInfo to dict, handling missing config attribute in dbt-core 1.10+."""
    _ensure_column_config_attr(column)
    return column.to_dict(**kwargs)


def _safe_column_replace(column: t.Any, **kwargs: t.Any) -> t.Any:
    """ColumnInfo.replace() guarded against the dbt 1.10+ missing-config crash."""
    _ensure_column_config_attr(column)
    return column.replace(**kwargs)


def _initialize_column_knowledge(column: t.Any, node: ResultNode) -> dict[str, t.Any]:
    """Normalize one local column into the knowledge-graph representation."""
    column_data = _column_to_dict(column, omit_none=True)

    # Match the existing graph-builder behaviorby dropping empty/whitespace-only strings and empty lists.
    return {
        k: v for k, v in column_data.items()
        if not (isinstance(v, str) and not v.strip()) and v not in ([], ())
    }


def _build_node_ancestor_tree(
    manifest: t.Any,
    node: ResultNode,
    tree: dict[str, list[str]] | None = None,
    visited: set[str] | None = None,
    depth: int = 1,
    max_depth: int = 100,
) -> dict[str, list[str]]:
    """Build a flat graph of a node and it's ancestors."""
    logger.debug("Building ancestor tree/branch for => %s", node.unique_id)
    if tree is None or visited is None:
        visited = {node.unique_id}  # set literal — NOT set(str) which iterates characters
        tree = {"generation_0": [node.unique_id]}
        depth = 1

    if not hasattr(node, "depends_on"):
        return tree

    # Prevent unbounded recursion
    if depth > max_depth:
        logger.warning(
            "Ancestor tree depth %d exceeded for node %s — truncating here. "
            "Descriptions from deeper ancestors will not propagate to this model. "
            "This is unexpected in normal dbt projects; check for unusual graph depth.",
            max_depth,
            node.unique_id,
        )
        return tree

    for dep in getattr(node.depends_on, "nodes", []):
        if not dep.startswith(("model.", "seed.", "source.")):
            continue

        # Cycle guard: if this dep is already in the visited set it was reached via another
        # path in the DAG (diamond dependency pattern).  dbt validates the graph is cycle-free
        # so this is never a true cycle — just log at debug level and skip re-traversal.
        if dep in visited:
            logger.debug(
                "Already visited %s while building ancestor tree for %s — "
                "diamond dependency, skipping re-traversal.",
                dep,
                node.unique_id,
            )
            continue

        visited.add(dep)
        member = manifest.nodes.get(dep, manifest.sources.get(dep))
        if member:
            tree.setdefault(f"generation_{depth}", []).append(dep)
            _ = _build_node_ancestor_tree(manifest, member, tree, visited, depth + 1, max_depth)

    for generation in tree.values():
        generation.sort()  # For deterministic ordering

    return tree


def _get_node_yaml(
    context: YamlRefactorContextProtocol,
    member: ResultNode,
) -> MappingProxyType[str, t.Any] | None:
    """Get a read-only view of the parsed YAML for a dbt model or source node."""
    from pathlib import Path

    from dbt_osmosis_cll.osmosis_propagation.introspection import _find_first
    from dbt_osmosis_cll.osmosis_propagation.schema.reader import _read_yaml

    project_root = context.project.runtime_cfg.project_root
    if not project_root:
        return None
    project_dir = Path(project_root)

    if isinstance(member, SourceDefinition):
        if not member.original_file_path:
            return None
        path = project_dir.joinpath(member.original_file_path)
        sources = t.cast(
            "list[dict[str, t.Any]]",
            _read_yaml(context.yaml_handler, context.yaml_handler_lock, path).get("sources", []),
        )
        source = _find_first(sources, lambda s: s["name"] == member.source_name, {})
        tables = source.get("tables", [])
        maybe_doc = _find_first(tables, lambda tbl: tbl["name"] == member.name)
        if maybe_doc is not None:
            return MappingProxyType(maybe_doc)

    elif isinstance(member, (ModelNode, SeedNode)):
        if not member.patch_path:
            return None
        path = project_dir.joinpath(member.patch_path.split("://")[-1])
        section = f"{member.resource_type}s"
        models = t.cast(
            "list[dict[str, t.Any]]",
            _read_yaml(context.yaml_handler, context.yaml_handler_lock, path).get(section, []),
        )
        maybe_doc = _find_first(models, lambda model: model["name"] == member.name)
        if maybe_doc is not None:
            return MappingProxyType(maybe_doc)

    return None


def _collect_column_variants(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
) -> dict[str, list[str]]:
    """Collect column variants from node columns and plugins."""
    from dbt_osmosis_cll.osmosis_propagation.plugins import get_plugin_manager

    pm = get_plugin_manager()
    node_column_variants: dict[str, list[str]] = {}
    for column_name, _ in node.columns.items():
        variants = node_column_variants.setdefault(column_name, [column_name])
        for v in pm.hook.get_candidates(name=column_name, node=node, context=context.project):
            variants.extend(t.cast("list[str]", v))

    return node_column_variants


def _get_unrendered(
    context: YamlRefactorContextProtocol,
    k: str,
    name: str,
    ancestor: ResultNode,
    node_column_variants: dict[str, list[str]],
) -> t.Any:
    """Get unrendered value for a column from ancestor YAML."""
    raw_yaml: t.Mapping[str, t.Any] = _get_node_yaml(context, ancestor) or {}
    raw_columns = t.cast("list[dict[str, t.Any]]", raw_yaml.get("columns", []))
    from dbt_osmosis_cll.osmosis_propagation.introspection import _find_first, normalize_column_name

    raw_column_metadata = _find_first(
        raw_columns,
        lambda c: (
            normalize_column_name(c["name"], context.project.runtime_cfg.credentials.type)
            in node_column_variants[name]
        ),
        {},
    )
    return raw_column_metadata.get(k)


def _build_graph_edge(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
    name: str,
    incoming: t.Any,
    ancestor: ResultNode,
    node_column_variants: dict[str, list[str]],
) -> dict[str, t.Any]:
    """Build a graph edge from incoming column with inheritance applied."""
    graph_edge = _column_to_dict(incoming, omit_none=True)

    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node

    # Use unrendered descriptions if configured
    if _get_setting_for_node(
        "use-unrendered-descriptions",
        node,
        name,
        fallback=context.settings.use_unrendered_descriptions,
    ):
        if unrendered_description := _get_unrendered(
            context,
            "description",
            name,
            ancestor,
            node_column_variants,
        ):
            graph_edge["description"] = unrendered_description

    # Handle inheritance for specified keys
    for inheritable in _get_setting_for_node(
        "add-inheritance-for-specified-keys",
        node,
        name,
        fallback=context.settings.add_inheritance_for_specified_keys,
    ):
        current_val = graph_edge.get(inheritable)
        if incoming_unrendered_val := _get_unrendered(
            context,
            inheritable,
            name,
            ancestor,
            node_column_variants,
        ):
            graph_edge[inheritable] = incoming_unrendered_val
        elif incoming_val := graph_edge.pop(inheritable, current_val):
            graph_edge[inheritable] = incoming_val

    return graph_edge


def _clean_graph_edge(
    context: YamlRefactorContextProtocol,
    graph_edge: dict[str, t.Any],
    generation: str,
    node: ResultNode,
    name: str,
) -> None:
    """Clean up empty values and placeholder descriptions from graph edge."""
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node
    from dbt_osmosis_cll.osmosis_propagation.settings import EMPTY_STRING

    # Remove placeholder descriptions or force inherit if direct ancestor
    if graph_edge.get("description", EMPTY_STRING) in context.placeholders or (
        generation == "generation_0"
        and _get_setting_for_node(
            "force_inherit_descriptions",
            node,
            name,
            fallback=context.settings.force_inherit_descriptions,
        )
    ):
        graph_edge.pop("description", None)

    # Remove empty/whitespace-only descriptions (that weren't caught by placeholder check)
    if not graph_edge.get("description", "").strip():
        graph_edge.pop("description", None)

    # Remove empty tags and meta objects
    if graph_edge.get("tags") == []:
        del graph_edge["tags"]
    if graph_edge.get("meta") == {}:
        del graph_edge["meta"]

    # Clean up empty nested config entries (handles data from fusion_compat mode)
    if isinstance(graph_edge.get("config"), dict):
        config = graph_edge["config"]
        if config.get("meta", {}) == {}:
            config.pop("meta", None)
        if config.get("tags", []) == []:
            config.pop("tags", None)
        if not config:
            graph_edge.pop("config", None)

    # Remove None values
    for k in list(graph_edge.keys()):
        if graph_edge[k] is None:
            graph_edge.pop(k)


def _find_matching_column(ancestor: ResultNode, column_variants: list[str]) -> t.Any | None:
    """Find a matching column in ancestor from the given variants."""
    for variant in column_variants:
        incoming = ancestor.columns.get(variant)
        if incoming is not None:
            return incoming
    return None


def _merge_graph_node_data(
    graph_node: dict[str, t.Any],
    graph_edge: dict[str, t.Any],
) -> None:
    """Merge graph edge data into existing graph node, handling tags and meta merging."""
    # Merge top-level tags
    current_tags = graph_node.get("tags", [])
    if merged_tags := (set(graph_edge.pop("tags", [])) | set(current_tags)):
        graph_edge["tags"] = list(merged_tags)

    # Merge top-level meta
    current_meta = graph_node.get("meta", {})
    edge_meta = graph_edge.pop("meta", {})
    if merged_meta := {**current_meta, **edge_meta}:
        graph_edge["meta"] = merged_meta

    # Merge config-level meta and tags (handles data from fusion_compat mode)
    current_config = graph_node.get("config")
    edge_config = graph_edge.pop("config", None)
    if isinstance(current_config, dict) or isinstance(edge_config, dict):
        current_config = current_config if isinstance(current_config, dict) else {}
        edge_config = edge_config if isinstance(edge_config, dict) else {}
        # Merge config.meta
        current_config_meta = current_config.get("meta", {})
        edge_config_meta = edge_config.pop("meta", {})
        merged_config_meta = {**current_config_meta, **edge_config_meta}
        if merged_config_meta:
            edge_config["meta"] = merged_config_meta
        # Merge config.tags
        current_config_tags = current_config.get("tags", [])
        edge_config_tags = edge_config.pop("tags", [])
        if merged_config_tags := (set(edge_config_tags) | set(current_config_tags)):
            edge_config["tags"] = list(merged_config_tags)
        # Merge remaining config keys
        for k, v in current_config.items():
            if k not in edge_config:
                edge_config[k] = v
        if edge_config:
            graph_edge["config"] = edge_config

    # Update graph node with merged data
    graph_node.update(graph_edge)


def _read_ancestor_yaml_description(
    context: "YamlRefactorContextProtocol",
    ancestor: ResultNode,
    column_variants: list[str],
) -> str | None:
    """Read a column's description directly from the YAML buffer for an ancestor node.

    The YAML buffer reflects the on-disk state at osmosis startup — including any
    pre-run enrichments (e.g. AML injection into staging YAMLs) — rather than the
    in-memory manifest which is mutated by ``inherit_upstream_column_knowledge`` as
    it processes earlier nodes in topological order.

    This is the core of CLL-guided description inheritance: CLL pins the correct
    ancestor (avoiding multi-join ambiguity) and we read that ancestor's description
    from the stable YAML buffer, not from the potentially-stale manifest node.

    Returns the description string if the column is found in the YAML, else ``None``.
    When ``None`` is returned the caller falls back to the manifest-based description.
    """
    from dbt_osmosis_cll.osmosis_propagation.introspection import _find_first, normalize_column_name

    ancestor_yaml = _get_node_yaml(context, ancestor)
    if ancestor_yaml is None:
        return None

    yaml_cols: list[dict[str, t.Any]] = list(ancestor_yaml.get("columns", []))
    if not yaml_cols:
        return None

    db_type = context.project.runtime_cfg.credentials.type
    col_variants_set = set(column_variants)

    yaml_col = _find_first(
        yaml_cols,
        lambda c: normalize_column_name(c.get("name", ""), db_type) in col_variants_set,
        None,
    )
    if yaml_col is None:
        return None

    desc = yaml_col.get("description")
    if not desc:
        return None
    return str(desc) if not isinstance(desc, str) else desc


def _build_column_knowledge_graph(
    context: YamlRefactorContextProtocol,
    node: ResultNode,
) -> dict[str, dict[str, t.Any]]:
    """Generate a column knowledge graph for a dbt model or source node."""
    tree = _build_node_ancestor_tree(context.project.manifest, node)
    logger.debug("Node ancestor tree => %s", tree)

    node_yaml = _get_node_yaml(context, node)
    node_column_variants = _collect_column_variants(context, node)

    # CLL-first parent resolution: ask CLL which direct parent provides each column.
    # When CLL succeeds the result disambiguates multi-parent joins deterministically.
    # Multi-source computed columns carry _CLL_COMPUTED_SENTINEL — inheritance is skipped
    # entirely for them; annotate_column_origins will add a "computed in:" annotation.
    from dbt_osmosis_cll.integration.cll import _CLL_COMPUTED_SENTINEL, build_parent_map, get_cll_results
    from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node
    cll_parent_map = build_parent_map(get_cll_results(context, node), node.name)

    # Initialize the column knowledge graph with the local node's column data
    # This ensures local metadata is preserved and merged with inherited metadata
    column_knowledge_graph: dict[str, dict[str, t.Any]] = {}
    for name, column in node.columns.items():
        column_knowledge_graph[name] = _initialize_column_knowledge(column, node)

    # Process ancestors from farthest to closest
    for generation in reversed(sorted(tree.keys())):
        ancestors = tree[generation]
        seen_in_gen: set[str] = set()

        for ancestor_uid in ancestors:
            ancestor = context.project.manifest.nodes.get(
                ancestor_uid,
                context.project.manifest.sources.get(ancestor_uid),
            )
            if not isinstance(ancestor, (SourceDefinition, SeedNode, ModelNode)):
                continue

            # Skip the target node itself — it has no upstream to inherit from.
            if ancestor_uid == node.unique_id:
                continue

            # Process each column in the target node
            for name, _ in node.columns.items():
                cll_parent = cll_parent_map.get(name.lower())
                if cll_parent == _CLL_COMPUTED_SENTINEL:
                    # CLL confirmed: multi-source computed column — no single progenitor.
                    # Skip inheritance for all ancestors; the "computed in:" annotation
                    # will be attached by annotate_column_origins instead.
                    continue
                elif cll_parent is not None:
                    # CLL identified the direct parent deterministically.
                    # Skip every ancestor that is NOT the CLL-confirmed parent so
                    # multi-parent ambiguity (same column name in two joined models)
                    # is resolved without arbitrary alphabetical ordering.
                    if ancestor.name.lower() != cll_parent:
                        continue
                    # CLL confirmed — process this ancestor regardless of the
                    # generation guard (another ancestor in same generation may have
                    # already set the guard for a different column).
                elif name in seen_in_gen:
                    # No CLL data for this column: first ancestor in this generation wins.
                    continue

                # Find matching column in ancestor
                incoming = _find_matching_column(ancestor, node_column_variants[name])
                if incoming is None:
                    continue

                # Mark this column as processed in this generation
                seen_in_gen.add(name)

                # Build graph edge with inheritance applied
                graph_edge = _build_graph_edge(
                    context,
                    node,
                    name,
                    incoming,
                    ancestor,
                    node_column_variants,
                )

                # CLL-guided YAML-buffer description override:
                # _build_graph_edge reads the description from the in-memory manifest
                # column (``incoming.description``), which may be stale — an earlier
                # inherit_upstream_column_knowledge pass (for a shallower node processed
                # before this one topologically) could have mutated the manifest.
                # The YAML buffer, by contrast, is populated from the on-disk files at
                # osmosis startup and is NOT updated until the final sync phase, so it
                # always reflects the enriched state written by pre-run scripts (e.g.
                # AML injection into staging YAMLs).
                # When use_unrendered_descriptions is True the buffer is already the
                # source because _build_graph_edge calls _get_unrendered internally.
                if not _get_setting_for_node(
                    "use-unrendered-descriptions",
                    node,
                    name,
                    fallback=context.settings.use_unrendered_descriptions,
                ):
                    yaml_desc = _read_ancestor_yaml_description(
                        context, ancestor, node_column_variants[name]
                    )
                    if yaml_desc is not None:
                        graph_edge["description"] = yaml_desc

                # Clean up empty values and placeholders
                _clean_graph_edge(context, graph_edge, generation, node, name)

                # Merge with existing graph node (which already has local column data)
                graph_node = column_knowledge_graph.setdefault(name, {})
                _merge_graph_node_data(graph_node, graph_edge)

    return column_knowledge_graph
