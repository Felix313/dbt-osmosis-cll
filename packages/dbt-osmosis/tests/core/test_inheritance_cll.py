"""Behavior tests for CLL-driven inheritance (Phase 4).

These tests target ``inherit_upstream_column_knowledge_cll`` and its helper
``_find_cll_description`` in isolation.  The Phase-4 logic is *pure* given its
CLL inputs, so rather than building a full dbt project we mock the boundaries it
reads from — ``get_cll_results``, the source/node indexes, the YAML-buffer reader
and ``_get_setting_for_node`` — and assert the decision branches:

- passthrough columns inherit the closest upstream description
- desc-owner: "this" preserves existing docs; "upstream" overwrites
- renames are skipped unless inherit-through-renames is enabled
- aggregate / window / union / literal / generated / multi-source columns are skipped
- annotation-only descriptions are treated as empty
- CLL failure (no results) and source nodes are skipped, state preserved
- tags/meta inherit from the immediate progenitor; managed meta keys are filtered
- _find_cll_description walks the progenitor chain and stops at computed walls
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest
from dbt.artifacts.resources.types import NodeType

from dbt_osmosis.config import get_config, reset_config
from dbt_osmosis.core.transforms import (
    _find_cll_description,
    inherit_upstream_column_knowledge_cll,
)

PROJECT = "/proj"


class FakeColumn:
    """Minimal stand-in for dbt ColumnInfo supporting attribute reads + replace()."""

    def __init__(self, name, description="", tags=None, meta=None):
        self.name = name
        self.description = description
        self.tags = list(tags or [])
        self.meta = dict(meta or {})

    def replace(self, **kwargs):
        new = FakeColumn(self.name, self.description, list(self.tags), dict(self.meta))
        for key, value in kwargs.items():
            setattr(new, key, value)
        return new


class FakeNode:
    def __init__(self, name, columns, resource_type=NodeType.Model, settings=None):
        self.name = name
        self.columns = columns
        self.resource_type = resource_type
        self.unique_id = f"model.test.{name}"
        self._settings = settings or {}


def make_context():
    return SimpleNamespace(
        placeholders=("",),
        settings=SimpleNamespace(skip_add_tags=False, skip_merge_meta=False),
        project=SimpleNamespace(runtime_cfg=SimpleNamespace(project_root=PROJECT)),
    )


def cll(model, column, **kwargs):
    base = {
        "progenitor_model": None,
        "progenitor_column": None,
        "is_computed": False,
        "is_rename": False,
        "is_first_in_chain": False,
        "is_aggregate": False,
        "is_window": False,
        "is_union": False,
        "is_literal": False,
        "is_generated": False,
    }
    base.update(kwargs)
    return SimpleNamespace(model=model, column=column, **base)


@contextlib.contextmanager
def patched(
    *,
    results=None,
    yaml_descs=None,
    source_index=None,
    node_index=None,
    managed=None,
):
    """Patch every boundary the Phase-4 functions read from."""
    results = results or {}
    yaml_descs = yaml_descs or {}
    source_index = source_index or {}
    node_index = node_index or {}

    def fake_read_yaml(_ctx, ancestor, variants):
        name = getattr(ancestor, "name", "").lower()
        for variant in variants:
            if (name, variant.lower()) in yaml_descs:
                return yaml_descs[(name, variant.lower())]
        return None

    def fake_get_setting(key, node, col_name=None, fallback=None):
        node_settings = getattr(node, "_settings", {})
        if (key, col_name) in node_settings:
            return node_settings[(key, col_name)]
        if (key, None) in node_settings:
            return node_settings[(key, None)]
        return fallback

    def fake_get_cll_results(_ctx, node):
        return results.get(node.name, [])

    stack = contextlib.ExitStack()
    stack.enter_context(
        mock.patch("dbt_osmosis.core.cll._ensure_manifest_index", lambda _ctx: None)
    )
    stack.enter_context(mock.patch("dbt_osmosis.core.cll.get_cll_results", fake_get_cll_results))
    stack.enter_context(mock.patch("dbt_osmosis.core.cll._SOURCE_INDEX", {PROJECT: source_index}))
    stack.enter_context(mock.patch("dbt_osmosis.core.cll._NODE_INDEX", {PROJECT: node_index}))
    stack.enter_context(
        mock.patch("dbt_osmosis.core.inheritance._read_ancestor_yaml_description", fake_read_yaml)
    )
    stack.enter_context(
        mock.patch("dbt_osmosis.core.introspection._get_setting_for_node", fake_get_setting)
    )
    if managed is not None:
        stack.enter_context(
            mock.patch(
                "dbt_osmosis.core.transforms.get_managed_meta_keys",
                lambda: frozenset(managed),
            )
        )
    with stack:
        yield


@pytest.fixture(autouse=True)
def _reset_config():
    """Force default .osmosis config so strip/separator behaviour is deterministic."""
    reset_config()
    yield
    reset_config()


# ---------------------------------------------------------------------------
# inherit_upstream_column_knowledge_cll
# ---------------------------------------------------------------------------


def test_passthrough_fills_empty_description():
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Parent desc"


def test_existing_description_preserved_with_owner_this():
    child = FakeNode("child", {"COL": FakeColumn("COL", description="Local desc")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Local desc"


def test_owner_upstream_overwrites_existing_description():
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Local desc")},
        settings={("desc-owner", None): "upstream"},
    )
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Parent desc"


def test_rename_skips_description_but_inherits_tags_when_renames_disabled():
    child = FakeNode("child", {"NEW_NAME": FakeColumn("NEW_NAME", description="")})
    ctx = make_context()
    parent = FakeNode("parent", {"OLD_NAME": FakeColumn("OLD_NAME", "Old desc", tags=["pii"])})
    with patched(
        results={
            "child": [
                cll(
                    "child",
                    "new_name",
                    progenitor_model="parent",
                    progenitor_column="old_name",
                    is_rename=True,
                )
            ]
        },
        yaml_descs={("parent", "old_name"): "Old desc"},
        node_index={"parent": parent},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    # Rename: description NOT inherited (default inherit-through-renames False) ...
    assert child.columns["NEW_NAME"].description == ""
    # ... but tags from the immediate progenitor still flow through.
    assert "pii" in child.columns["NEW_NAME"].tags


def test_rename_follows_description_when_renames_enabled():
    child = FakeNode(
        "child",
        {"NEW_NAME": FakeColumn("NEW_NAME", description="")},
        settings={("inherit-through-renames", None): True},
    )
    ctx = make_context()
    parent = FakeNode("parent", {"OLD_NAME": FakeColumn("OLD_NAME", "Old desc")})
    with patched(
        results={
            "child": [
                cll(
                    "child",
                    "new_name",
                    progenitor_model="parent",
                    progenitor_column="old_name",
                    is_rename=True,
                )
            ]
        },
        yaml_descs={("parent", "old_name"): "Old desc"},
        node_index={"parent": parent},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["NEW_NAME"].description == "Old desc"


@pytest.mark.parametrize(
    "flag",
    ["is_aggregate", "is_window", "is_union", "is_literal", "is_generated"],
)
def test_non_traceable_column_types_are_skipped(flag):
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()
    with patched(
        results={
            "child": [
                cll(
                    "child",
                    "col",
                    progenitor_model="parent",
                    progenitor_column="col",
                    **{flag: True},
                )
            ]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == ""


def test_multi_source_wall_is_skipped():
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()
    with patched(
        results={
            "child": [
                cll(
                    "child",
                    "col",
                    progenitor_model="parent",
                    progenitor_column=None,
                    is_computed=True,
                )
            ]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == ""


def test_first_in_chain_is_skipped():
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()
    with patched(
        results={"child": [cll("child", "col", is_first_in_chain=True)]},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == ""


def test_annotation_only_description_is_treated_as_empty():
    cfg = get_config()
    annotation_only = (
        f"{cfg.annotation_separator}\n{cfg.annotation_namespace} -> "
        f"{cfg.annotation_renamed} parent.col"
    )
    child = FakeNode("child", {"COL": FakeColumn("COL", description=annotation_only)})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Parent desc"


def test_no_cll_results_preserves_state():
    child = FakeNode("child", {"COL": FakeColumn("COL", description="Local desc")})
    ctx = make_context()
    with patched(results={}):  # CLL failure → empty list
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Local desc"


def test_source_node_is_skipped():
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Local desc")},
        resource_type=NodeType.Source,
    )
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Local desc"


def test_tags_and_meta_inherited_with_managed_keys_filtered():
    child = FakeNode("child", {"COL": FakeColumn("COL", description="Local desc")})
    ctx = make_context()
    parent = FakeNode(
        "parent",
        {
            "COL": FakeColumn(
                "COL",
                "Parent desc",
                tags=["pii"],
                meta={"sensitivity": "high", "desc-owner": "upstream"},
            )
        },
    )
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        node_index={"parent": parent},
        managed={"desc-owner"},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert "pii" in child.columns["COL"].tags
    assert child.columns["COL"].meta == {"sensitivity": "high"}  # desc-owner filtered out


# ---------------------------------------------------------------------------
# _find_cll_description
# ---------------------------------------------------------------------------


def test_find_cll_description_walks_chain_to_grandparent():
    ctx = make_context()
    with patched(
        results={"p": [cll("p", "col", progenitor_model="gp", progenitor_column="col")]},
        yaml_descs={("gp", "col"): "Grandparent desc"},
        node_index={"p": FakeNode("p", {}), "gp": FakeNode("gp", {})},
    ):
        result = _find_cll_description(ctx, "p", "col")
    assert result == "Grandparent desc"


def test_find_cll_description_stops_at_computed_wall():
    ctx = make_context()
    with patched(
        results={
            "p": [cll("p", "col", progenitor_model="gp", progenitor_column=None, is_computed=True)]
        },
        node_index={"p": FakeNode("p", {}), "gp": FakeNode("gp", {})},
    ):
        result = _find_cll_description(ctx, "p", "col")
    assert result is None


def test_find_cll_description_returns_none_for_unresolvable_node():
    ctx = make_context()
    with patched(results={}, node_index={}):
        result = _find_cll_description(ctx, "missing", "col")
    assert result is None
