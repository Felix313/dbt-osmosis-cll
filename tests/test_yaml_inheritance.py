# pyright: reportAny=false, reportUnknownMemberType=false, reportPrivateUsage=false
import typing as t
from unittest import mock

import pytest

from dbt_osmosis_cll.osmosis_propagation.inheritance import (
    _build_node_ancestor_tree,
    _get_node_yaml,
)
from dbt_osmosis_cll.osmosis_propagation.settings import YamlRefactorContext
from dbt_osmosis_cll.osmosis_propagation.sync_operations import sync_node_to_yaml
from dbt_osmosis_cll.osmosis_propagation.transforms import inherit_upstream_column_knowledge


@pytest.mark.parametrize(
    "node_id, expected_tree",
    [
        (
            "model.jaffle_shop_duckdb.customers",
            {
                "generation_0": ["model.jaffle_shop_duckdb.customers"],
                "generation_1": [
                    "model.jaffle_shop_duckdb.stg_customers.v1",
                    "model.jaffle_shop_duckdb.stg_orders",
                    "model.jaffle_shop_duckdb.stg_payments",
                ],
                "generation_2": [
                    "seed.jaffle_shop_duckdb.raw_customers",
                    "seed.jaffle_shop_duckdb.raw_orders",
                    "seed.jaffle_shop_duckdb.raw_payments",
                ],
            },
        ),
    ],
)
def test_build_node_ancestor_tree(
    yaml_context: YamlRefactorContext,
    node_id: str,
    expected_tree: dict[str, list[str]],
):
    """Test the build node ancestor tree functionality."""
    manifest = yaml_context.project.manifest
    target_node = manifest.nodes[node_id]
    assert _build_node_ancestor_tree(manifest, target_node) == expected_tree


# NOTE: downstream node has the following set in the test body, keep these in mind when creating cases
# local_column.description = "I was steadfast and unyielding"
# local_column.tags = ["baz"]
# local_column.meta = {"c": 3}
@pytest.mark.parametrize(
    "settings, upstream_mutations, downstream_metadata",
    [
        # Case 2: Skip add tags and merge meta
        (
            {"skip_add_tags": True, "skip_merge_meta": True},
            {
                "stg_customers.v1.customer_id": {
                    "description": "I will not be inherited, since the customer table documents me",
                    "meta": {"a": 1},
                    "tags": ["foo", "bar"],
                },
            },
            {
                "description": "I was steadfast and unyielding",
                "meta": {"c": 3},
                "tags": ["baz"],
            },
        ),
        # Case 4: Skip add data types but inherit specified keys
        (
            {"skip_add_data_types": True, "add_inheritance_for_specified_keys": ["quote"]},
            {
                "stg_customers.v1.customer_id": {
                    "description": "Keep on, keeping on",
                    "meta": {"e": 5},
                    "tags": ["constrainted"],
                    "quote": True,
                },
            },
            {
                "description": "I was steadfast and unyielding",
                "meta": {"c": 3, "e": 5},
                "tags": ["constrainted", "baz"],
                "quote": True,
            },
        ),
        # Case 5: Output to lowercase
        (
            {"output_to_lower": True},
            {
                "stg_customers.v1.customer_id": {
                    "name": "WTF",
                },
            },
            {
                "name": "wtf",
                "description": "I was steadfast and unyielding",
                "meta": {"c": 3},
                "tags": ["baz"],
            },
        ),
    ],
)
def test_inherit_upstream_column_knowledge_with_various_settings(
    yaml_context: YamlRefactorContext,
    settings: dict[str, t.Any],
    upstream_mutations: dict[str, t.Any],
    downstream_metadata: dict[str, t.Any],
):
    """Test inherit_upstream_column_knowledge with various settings and configurations."""
    manifest = yaml_context.project.manifest
    target_node = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    local_column = target_node.columns["customer_id"]
    local_column.description = "I was steadfast and unyielding"
    local_column.tags = ["baz"]
    local_column.meta = {"c": 3}

    # Apply settings; disable fusion_compat to test classic YAML output format
    yaml_context.settings.fusion_compat = False
    for key, value in settings.items():
        setattr(yaml_context.settings, key, value)

    # Modify upstream column data
    for column_path, mods in upstream_mutations.items():
        components = column_path.split(".")

        if len(components) > 2:
            node_id, version, column_name = components
            node = f"model.jaffle_shop_duckdb.{node_id}.{version}"
        else:
            node_id, column_name = components
            node = f"model.jaffle_shop_duckdb.{node_id}"

        upstream_col = manifest.nodes[node].columns[column_name]
        for attr, attr_value in mods.items():
            setattr(upstream_col, attr, attr_value)

    # Perform inheritance
    with (
        mock.patch("dbt_osmosis_cll.osmosis_propagation.schema.reader._YAML_BUFFER_CACHE", {}),
        mock.patch("dbt_osmosis_cll.osmosis_propagation.introspection._COLUMN_LIST_CACHE", {}),
    ):
        _ = inherit_upstream_column_knowledge(yaml_context, target_node)
        sync_node_to_yaml(yaml_context, target_node, commit=False)
        yaml_slice = _get_node_yaml(yaml_context, target_node)

    # Assert metadata, description, and tags
    cid = target_node.columns["customer_id"]
    assert cid.description == downstream_metadata["description"]
    assert cid.meta == downstream_metadata["meta"]
    assert sorted(cid.tags) == sorted(downstream_metadata["tags"])

    # Validate YAML output
    assert yaml_slice
    yaml_column = yaml_slice["columns"][0]
    assert yaml_column["description"] == downstream_metadata["description"]
    assert yaml_column["meta"] == downstream_metadata["meta"]
    assert sorted(yaml_column["tags"]) == sorted(downstream_metadata["tags"])


@pytest.mark.parametrize(
    "use_unrendered_descriptions, expected_start",
    [
        (False, "Orders can be one of the following statuses:"),
    ],
)
def test_use_unrendered_descriptions(
    yaml_context: YamlRefactorContext,
    use_unrendered_descriptions: bool,
    expected_start: str,
):
    """Test the handling of unrendered descriptions."""
    manifest = yaml_context.project.manifest
    target_node = manifest.nodes["model.jaffle_shop_duckdb.orders"]
    yaml_context.settings.use_unrendered_descriptions = use_unrendered_descriptions
    yaml_context.settings.force_inherit_descriptions = True

    with (
        mock.patch("dbt_osmosis_cll.osmosis_propagation.schema.reader._YAML_BUFFER_CACHE", {}),
        mock.patch("dbt_osmosis_cll.osmosis_propagation.introspection._COLUMN_LIST_CACHE", {}),
    ):
        _ = inherit_upstream_column_knowledge(yaml_context, target_node)
        sync_node_to_yaml(yaml_context, target_node, commit=False)

    assert target_node.columns["status"].description.startswith(expected_start)
