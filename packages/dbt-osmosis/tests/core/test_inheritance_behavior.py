"""Behavior-focused tests for the legacy name-match column inheritance.

These tests exercise ``inherit_upstream_column_knowledge`` (the original
name-match transform, still used as a pre-pass before LLM synthesis). The
CLL-driven inheritance that the pipeline uses by default is covered separately
in ``test_inheritance_cll.py``.

Tests for behaviours the fork removed or replaced have been dropped:
- ``osmosis_progenitor`` / ``add_progenitor_to_meta`` (progenitor tracking removed)
- ``default_progenitor`` / ``column_default_progenitor`` overrides (removed)
- multi-hop propagation through an empty intermediate and settings-level
  ``force_inherit_descriptions`` overwrite (now governed by CLL + ``desc-owner``)
"""

from __future__ import annotations

from unittest import mock

import pytest

from dbt_osmosis.core.transforms import inherit_upstream_column_knowledge


@pytest.fixture(scope="function")
def fresh_caches():
    """Patches the internal caches so each test starts with a fresh state."""
    with (
        mock.patch("dbt_osmosis.core.introspection._COLUMN_LIST_CACHE", {}),
        mock.patch("dbt_osmosis.core.schema.reader._YAML_BUFFER_CACHE", {}),
    ):
        yield


def test_partial_documentation_propagation(yaml_context, fresh_caches):
    """Test that only undocumented columns are inherited, not already documented ones.

    When a model has some documented and some undocumented columns:
    - Documented columns should keep their descriptions
    - Undocumented columns should inherit from upstream
    """
    manifest = yaml_context.project.manifest

    # Set up: stg_customers has documentation for all columns
    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["first_name"].description = "First name from source"
    stg_customers.columns["last_name"].description = "Last name from source"

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["first_name"].description = ""  # Empty - should inherit
    customers.columns[
        "last_name"
    ].description = "Customer family name (custom description)"  # Has doc - keep it

    # Execute
    yaml_context.settings.force_inherit_descriptions = False  # Don't override existing docs
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Assert: first_name inherited, last_name kept original
    assert customers.columns["first_name"].description == "First name from source"
    assert customers.columns["last_name"].description == "Customer family name (custom description)"


def test_tag_and_meta_inheritance(yaml_context, fresh_caches):
    """Test that tags and meta fields propagate through inheritance.

    Tags and metadata are as important as descriptions for data governance.
    This test verifies they flow downstream correctly.
    """
    manifest = yaml_context.project.manifest

    # Set up: upstream has tags and meta
    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["customer_id"].tags = ["pk", "identifier"]
    stg_customers.columns["customer_id"].meta = {
        "sensitivity": "public",
        "governance": "customer_key",
    }

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["customer_id"].tags = []
    customers.columns["customer_id"].meta = {}

    # Execute
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Assert: tags and meta should be inherited (merged with local)
    assert "pk" in customers.columns["customer_id"].tags
    assert "identifier" in customers.columns["customer_id"].tags
    assert customers.columns["customer_id"].meta.get("sensitivity") == "public"
    assert customers.columns["customer_id"].meta.get("governance") == "customer_key"


def test_diamond_pattern_inheritance(yaml_context, fresh_caches):
    """Test inheritance when a column is provided by an upstream model.

    Scenario: customers depends on stg_customers (among others). A documented
    first_name in stg_customers should flow into an undocumented customers.first_name.
    """
    manifest = yaml_context.project.manifest

    # Set up: first_name documented in stg_customers
    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["first_name"].description = "First name from customers staging"

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["first_name"].description = ""  # Empty - should inherit

    # Execute
    yaml_context.settings.force_inherit_descriptions = True
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Assert: Should inherit from stg_customers
    assert customers.columns["first_name"].description == "First name from customers staging"


def test_skip_add_tags_preserves_local_tags(yaml_context, fresh_caches):
    """Test that skip_add_tags=true prevents upstream tags from being added.

    When skip_add_tags is enabled, local tags should be preserved and upstream
    tags should NOT be merged in.
    """
    manifest = yaml_context.project.manifest

    # Set up
    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["customer_id"].tags = ["upstream_tag"]

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["customer_id"].tags = ["local_tag"]

    # Execute with skip_add_tags = True
    yaml_context.settings.skip_add_tags = True
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Assert: Should keep local tags only
    assert customers.columns["customer_id"].tags == ["local_tag"]


def test_skip_merge_meta_preserves_local_meta(yaml_context, fresh_caches):
    """Test that skip_merge_meta=true prevents upstream meta from being merged.

    When skip_merge_meta is enabled, local meta should be preserved and upstream
    meta should NOT be merged in.
    """
    manifest = yaml_context.project.manifest

    # Set up
    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["customer_id"].meta = {"upstream_key": "upstream_value"}

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["customer_id"].meta = {"local_key": "local_value"}

    # Execute with skip_merge_meta = True
    yaml_context.settings.skip_merge_meta = True
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Assert: Should keep local meta only
    assert customers.columns["customer_id"].meta == {"local_key": "local_value"}


def test_inheritance_with_placeholder_descriptions(yaml_context, fresh_caches):
    """Test that empty descriptions are treated as undocumented.

    This test verifies that columns with empty descriptions inherit from upstream.
    The default placeholder list includes empty string.
    """
    manifest = yaml_context.project.manifest

    # Set up: upstream has good description, downstream has empty description
    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["first_name"].description = "Customer first name"

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["first_name"].description = ""  # Empty - should inherit

    # Execute - should inherit because empty string is a placeholder
    yaml_context.settings.force_inherit_descriptions = False  # Don't override existing docs
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Assert: Should inherit because empty description is treated as undocumented
    assert customers.columns["first_name"].description == "Customer first name"


def test_whitespace_only_description_inherits_from_upstream(yaml_context, fresh_caches):
    """Whitespace-only local descriptions are treated as empty and inherit from upstream.

    A column with description "   " should behave identically to one with description ""
    — the whitespace is not considered real documentation, so upstream docs flow in.
    """
    manifest = yaml_context.project.manifest

    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["first_name"].description = "Customer first name"

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["first_name"].description = "   "  # whitespace-only — should inherit

    yaml_context.settings.force_inherit_descriptions = False
    inherit_upstream_column_knowledge(yaml_context, customers)

    assert customers.columns["first_name"].description == "Customer first name"


def test_whitespace_only_upstream_description_does_not_propagate(yaml_context, fresh_caches):
    """A whitespace-only description on an upstream column is not propagated downstream.

    When the upstream source has a whitespace-only description it is stripped from the
    graph edge during _clean_graph_edge, so the downstream column stays undocumented
    rather than inheriting meaningless whitespace.
    """
    manifest = yaml_context.project.manifest

    stg_customers = manifest.nodes["model.jaffle_shop_duckdb.stg_customers.v1"]
    stg_customers.columns["first_name"].description = "   "  # whitespace-only upstream

    customers = manifest.nodes["model.jaffle_shop_duckdb.customers"]
    customers.columns["first_name"].description = ""

    yaml_context.settings.force_inherit_descriptions = True
    inherit_upstream_column_knowledge(yaml_context, customers)

    # Whitespace-only upstream doc must not propagate; description stays empty/unchanged
    assert not customers.columns["first_name"].description.strip()
