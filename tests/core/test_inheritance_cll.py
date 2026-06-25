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

from dbt_osmosis_cll.config import get_config, reset_config
from dbt_osmosis_cll.integration.cll import (
    clear_cll_walk_soft_fails,
    get_cll_walk_soft_fails,
    get_column_origin,
)
from dbt_osmosis_cll.osmosis_propagation.transforms import (
    _find_cll_description,
    _resolve_cll_description,
    inherit_upstream_column_knowledge_cll,
)

PROJECT = "/proj"


class FakeColumn:
    """Minimal stand-in for dbt ColumnInfo supporting attribute reads + replace()."""

    def __init__(self, name, description="", tags=None, meta=None, config=None):
        self.name = name
        self.description = description
        self.tags = list(tags or [])
        self.meta = dict(meta or {})
        # config mirrors dbt ColumnInfo.config — a dict that may carry a nested 'meta'.
        self.config = dict(config or {})

    def to_dict(self, **kwargs):
        return {
            "name": self.name,
            "description": self.description,
            "tags": list(self.tags),
            "meta": dict(self.meta),
            "config": dict(self.config),
        }

    def replace(self, **kwargs):
        new = FakeColumn(
            self.name,
            self.description,
            list(self.tags),
            dict(self.meta),
            dict(self.config),
        )
        for key, value in kwargs.items():
            setattr(new, key, value)
        return new

    def config_meta(self):
        """Convenience accessor for the nested config.meta dict in assertions."""
        return dict((self.config or {}).get("meta", {}))


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
        mock.patch("dbt_osmosis_cll.integration.cll._ensure_manifest_index", lambda _ctx: None)
    )
    stack.enter_context(
        mock.patch("dbt_osmosis_cll.integration.cll.get_cll_results", fake_get_cll_results)
    )
    stack.enter_context(
        mock.patch("dbt_osmosis_cll.integration.cll._SOURCE_INDEX", {PROJECT: source_index})
    )
    stack.enter_context(
        mock.patch("dbt_osmosis_cll.integration.cll._NODE_INDEX", {PROJECT: node_index})
    )
    stack.enter_context(
        mock.patch(
            "dbt_osmosis_cll.osmosis_propagation.inheritance._read_ancestor_yaml_description",
            fake_read_yaml,
        )
    )
    stack.enter_context(
        mock.patch(
            "dbt_osmosis_cll.osmosis_propagation.introspection._get_setting_for_node",
            fake_get_setting,
        )
    )
    if managed is not None:
        stack.enter_context(
            mock.patch(
                "dbt_osmosis_cll.osmosis_propagation.transforms.get_managed_meta_keys",
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


# ---------------------------------------------------------------------------
# Idempotency: stale inherited copies must not be laundered downstream
# ---------------------------------------------------------------------------


def test_find_cll_description_skips_inherited_copy_on_forced_passthrough():
    """A non-anchored force-inherit passthrough holds only a *copy* of its upstream.

    The walk must resolve transitively (recurse past it) instead of returning the
    stored copy, so a stale value sitting below a computed wall is never laundered
    downstream. Regression for the one-hop-per-run idempotency bug.
    """
    ctx = make_context()
    # mid: force-inherit passthrough of an aggregate, carrying a STALE copy of the
    # aggregate input's description (written by older tool code before the wall existed).
    mid = FakeNode(
        "mid",
        {"COL": FakeColumn("COL", "Stale source desc")},
        settings={("desc-owner", None): "upstream"},
    )
    agg = FakeNode("agg", {"COL": FakeColumn("COL", "")})  # aggregate, no own description
    with patched(
        results={
            "mid": [cll("mid", "col", progenitor_model="agg", progenitor_column="col")],
            "agg": [
                cll(
                    "agg",
                    "col",
                    progenitor_model="src",
                    progenitor_column="src_col",
                    is_aggregate=True,
                )
            ],
        },
        node_index={"mid": mid, "agg": agg},
    ):
        result = _find_cll_description(ctx, "mid", "col")
    # The aggregate wall yields no inheritable description; the stale copy on `mid`
    # must NOT be returned.
    assert result is None


def test_forced_child_below_aggregate_keeps_local_description():
    """REPORTING scenario: a force-inherit child below an aggregate wall, whose parent
    holds a stale inherited copy, must keep its own authored description (idempotent)."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", "letztes Auszugsdatum des Vertragskontos")},
        settings={("desc-owner", None): "upstream"},
    )
    mid = FakeNode(
        "mid",
        {"COL": FakeColumn("COL", "Stale source desc")},
        settings={("desc-owner", None): "upstream"},
    )
    agg = FakeNode("agg", {"COL": FakeColumn("COL", "")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="mid", progenitor_column="col")],
            "mid": [cll("mid", "col", progenitor_model="agg", progenitor_column="col")],
            "agg": [
                cll(
                    "agg",
                    "col",
                    progenitor_model="src",
                    progenitor_column="src_col",
                    is_aggregate=True,
                )
            ],
        },
        node_index={"mid": mid, "agg": agg},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "letztes Auszugsdatum des Vertragskontos"


def test_anchor_acts_as_wall_for_downstream_inheritance():
    """An anchored (desc-owner: this) intermediate defines the new truth: downstream
    force-inherit takes the anchor's description and the walk stops there, even though
    the anchor is itself a passthrough of a deeper origin."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", "")},
        settings={("desc-owner", None): "upstream"},
    )
    # mid is anchored (desc-owner: this) with its own authored description.
    mid = FakeNode(
        "mid",
        {"COL": FakeColumn("COL", "Anchored truth")},
        settings={("desc-owner", None): "this"},
    )
    deep = FakeNode("deep", {"COL": FakeColumn("COL", "Deep origin desc")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="mid", progenitor_column="col")],
            "mid": [cll("mid", "col", progenitor_model="deep", progenitor_column="col")],
        },
        yaml_descs={("deep", "col"): "Deep origin desc"},
        node_index={"mid": mid, "deep": deep},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    assert child.columns["COL"].description == "Anchored truth"


# ---------------------------------------------------------------------------
# desc-owner: upstream injection
# ---------------------------------------------------------------------------
#
# NOTE: ``desc-owner: upstream`` is written to the column's TOP-LEVEL ``meta`` (FakeColumn.meta).
# The YAML sync writer then places it under ``config.meta`` (fusion) or keeps it top-level
# (classic). These unit tests call the inherit transform directly, so they assert on ``meta``.


def test_gap_fill_injects_desc_owner_upstream():
    """An empty column gap-filled via CLL receives desc-owner: upstream in its meta."""
    child = FakeNode("child", {"BPARTNER_ID": FakeColumn("BPARTNER_ID", description="")})
    ctx = make_context()
    with patched(
        results={
            "child": [
                cll(
                    "child",
                    "bpartner_id",
                    progenitor_model="stg_edw__ae_aml__aml_t_bpartner_position",
                    progenitor_column="bpartner_id",
                )
            ]
        },
        yaml_descs={
            ("stg_edw__ae_aml__aml_t_bpartner_position", "bpartner_id"): "Geschaeftspartner-Nr."
        },
        node_index={
            "stg_edw__ae_aml__aml_t_bpartner_position": FakeNode(
                "stg_edw__ae_aml__aml_t_bpartner_position",
                {"BPARTNER_ID": FakeColumn("BPARTNER_ID", "Geschaeftspartner-Nr.")},
            )
        },
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["BPARTNER_ID"]
    assert col.description == "Geschaeftspartner-Nr."
    assert col.meta.get("desc-owner") == "upstream"
    assert "desc-source" not in col.meta


def test_same_text_copy_injects_desc_owner_upstream():
    """A column whose text already equals the upstream text gets desc-owner: upstream injected.
    A column that re-defines the text (different from upstream) is an origin — no injection.

    Chain: src "V" → base "X" → base2 "Y" → base3 "Y".
    base3 carries the SAME text as base2 (a copy) → injection.
    base2 re-defines the text (Y != X) → it IS an origin → no injection.
    """
    src = FakeNode("src", {"COL": FakeColumn("COL", "V")}, resource_type=NodeType.Source)
    base = FakeNode("base", {"COL": FakeColumn("COL", "X")})
    base2 = FakeNode("base2", {"COL": FakeColumn("COL", "Y")})
    base3 = FakeNode("base3", {"COL": FakeColumn("COL", "Y")})
    ctx = make_context()
    chain = dict(
        results={
            "base3": [cll("base3", "col", progenitor_model="base2", progenitor_column="col")],
            "base2": [cll("base2", "col", progenitor_model="base", progenitor_column="col")],
            "base": [cll("base", "col", progenitor_model="src", progenitor_column="col")],
        },
        source_index={"src": src},
        node_index={"base": base, "base2": base2, "base3": base3},
    )

    # base3 is a same-text copy of base2 → injected with desc-owner: upstream.
    with patched(**chain):
        inherit_upstream_column_knowledge_cll(ctx, base3)
    assert base3.columns["COL"].description == "Y"
    assert base3.columns["COL"].meta.get("desc-owner") == "upstream"

    # base2 re-defines the text (Y != X) → it IS an origin → no injection.
    with patched(**chain):
        inherit_upstream_column_knowledge_cll(ctx, base2)
    assert base2.columns["COL"].description == "Y"
    assert "desc-owner" not in base2.columns["COL"].meta


def test_force_inherit_overwrite_does_not_inject_again():
    """An existing description overwritten via desc-owner: upstream at layer/node level is
    already owned upstream — no additional column-level injection needed."""
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
    col = child.columns["COL"]
    assert col.description == "Parent desc"  # overwritten by force-inherit
    # No column-level injection needed (already covered by node/layer setting).
    assert "desc-owner" not in col.meta
    assert "desc-source" not in col.meta


def test_no_description_resolved_writes_no_ownership_tag():
    """When CLL resolves no description, no gap-fill happens → no desc-owner: upstream written."""
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        # No yaml_descs entry for parent.col → _find_cll_description returns None.
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == ""
    assert "desc-owner" not in col.meta
    assert "desc-source" not in col.meta


def test_desc_owner_injection_idempotent_across_runs():
    """A gap-filled column gets desc-owner: upstream on run 1; run 2 sees force_inherit=True
    and skips injection — but the tag remains (additive-only)."""
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()

    def run():
        with patched(
            results={
                "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
            },
            yaml_descs={("parent", "col"): "Parent desc"},
            node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
        ):
            inherit_upstream_column_knowledge_cll(ctx, child)
        return child.columns["COL"]

    first = run()
    assert first.description == "Parent desc"
    assert first.meta.get("desc-owner") == "upstream"

    # Run 2: column now has desc-owner: upstream in meta → force_inherit=True → description
    # is overwritten from upstream, injection skipped (already present), tag remains.
    second = run()
    assert second.description == "Parent desc"
    assert second.meta.get("desc-owner") == "upstream"  # unchanged — no flip-flop


def test_preexisting_text_match_injects_desc_owner_upstream():
    """A column whose description already matches upstream but has no ownership tag yet
    (inherited before this osmosis version) gets desc-owner: upstream on the next run."""
    child = FakeNode("child", {"COL": FakeColumn("COL", description="Parent desc")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Parent desc"
    assert col.meta.get("desc-owner") == "upstream"


def test_text_match_injects_desc_owner_regardless_of_progenitor():
    """When the column's text matches the (new) upstream, desc-owner: upstream is injected.
    This covers the backfill case where a column was already carrying the right text."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Shared desc")},
    )
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="new_parent", progenitor_column="col")]
        },
        yaml_descs={("new_parent", "col"): "Shared desc"},
        node_index={
            "new_parent": FakeNode("new_parent", {"COL": FakeColumn("COL", "Shared desc")})
        },
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.meta.get("desc-owner") == "upstream"


def test_upstream_text_drift_does_not_inject_ownership():
    """When upstream text drifts away from the local (developer-improved) description,
    desc-owner: upstream is NOT injected. desc-owner: this default protects the local text."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Locally improved desc")},
        settings={("desc-owner", None): "this"},
    )
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Upstream desc"},  # different from local
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Upstream desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Locally improved desc"  # frozen by desc-owner: this
    assert "desc-owner" not in col.meta  # no injection (text diverged)


def test_authored_description_gets_no_injection():
    """A locally authored description that differs from upstream and was never inherited
    receives no desc-owner: upstream injection — the developer owns it."""
    child = FakeNode("child", {"COL": FakeColumn("COL", description="Locally authored")})
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Locally authored"  # preserved (desc-owner: this)
    assert "desc-owner" not in col.meta
    assert "desc-source" not in col.meta


def test_named_anchor_owner_gets_no_injection():
    """A column with a NAMED desc-owner anchor (e.g. 'aml') is authored/owned there — it is an
    ORIGIN — so no desc-owner: upstream injection even when its text matches upstream."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Parent desc")},
        settings={("desc-owner", None): "aml"},
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
    col = child.columns["COL"]
    assert col.description == "Parent desc"  # preserved (named anchor, not overwritten)
    assert "desc-owner" not in col.meta  # named anchor → origin → no injection
    assert "desc-source" not in col.meta


def test_named_anchor_owner_strips_legacy_desc_source():
    """A column with a NAMED desc-owner anchor gets no injection (it is an origin).
    Any pre-existing meta is left untouched by osmosis."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Parent desc")},
        settings={("desc-owner", None): "aml"},
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
    col = child.columns["COL"]
    assert "desc-owner" not in col.meta  # named anchor → no injection


# ---------------------------------------------------------------------------
# Origin-walk soft-fails (cycle / max-depth) + self-reference handling
# ---------------------------------------------------------------------------


def test_self_reference_resolves_to_own_and_records_no_soft_fail():
    """A direct self-ref (incremental {{ this }}: M.col → M.col) is short-circuited before the
    cycle guard: it resolves to the column's own description, records NO soft-fail, and does not
    inject desc-owner: upstream on itself."""
    ctx = make_context()
    clear_cll_walk_soft_fails(ctx)
    m = FakeNode("m", {"COL": FakeColumn("COL", "Self-defined desc")})
    with patched(
        results={"m": [cll("m", "col", progenitor_model="m", progenitor_column="col")]},
        node_index={"m": m},
    ):
        desc, origin = _resolve_cll_description(ctx, "m", "col")
        assert desc == "Self-defined desc"
        assert origin == "M.COL"  # it is its own origin
        assert get_cll_walk_soft_fails(ctx) == {}  # no cycle recorded

        # Through the inherit path, the column must NOT inject ownership on itself.
        inherit_upstream_column_knowledge_cll(ctx, m)
    assert "desc-owner" not in m.columns["COL"].meta
    assert "desc-source" not in m.columns["COL"].meta
    clear_cll_walk_soft_fails(ctx)


def test_multi_node_cycle_records_soft_fail():
    """A genuine multi-node lineage loop (A.col → B.col → A.col, neither owning a description)
    resolves to nothing and is recorded as a 'cycle' soft-fail for the run-end summary."""
    ctx = make_context()
    clear_cll_walk_soft_fails(ctx)
    a = FakeNode("a", {"COL": FakeColumn("COL", "")})
    b = FakeNode("b", {"COL": FakeColumn("COL", "")})
    with patched(
        results={
            "a": [cll("a", "col", progenitor_model="b", progenitor_column="col")],
            "b": [cll("b", "col", progenitor_model="a", progenitor_column="col")],
        },
        node_index={"a": a, "b": b},
    ):
        desc, origin = _resolve_cll_description(ctx, "a", "col")
        assert desc is None and origin is None
        soft_fails = get_cll_walk_soft_fails(ctx)
        assert "A.COL" in soft_fails.get("cycle", frozenset())
    clear_cll_walk_soft_fails(ctx)


def test_max_depth_records_soft_fail():
    """Exceeding the configured max origin depth is recorded as a 'max-depth' soft-fail."""
    ctx = make_context()
    clear_cll_walk_soft_fails(ctx)
    # A 3-link passthrough chain with max_depth=1 forces the depth guard to trip.
    deep = FakeNode("deep", {"COL": FakeColumn("COL", "Deep")})
    mid = FakeNode(
        "mid", {"COL": FakeColumn("COL", "")}, settings={("desc-owner", None): "upstream"}
    )
    with patched(
        results={
            "mid": [cll("mid", "col", progenitor_model="deep", progenitor_column="col")],
        },
        node_index={"mid": mid, "deep": deep},
    ):
        desc, origin = _resolve_cll_description(ctx, "mid", "col", max_depth=0)
        assert desc is None and origin is None
        soft_fails = get_cll_walk_soft_fails(ctx)
        assert soft_fails.get("max-depth")  # at least one column recorded
    clear_cll_walk_soft_fails(ctx)


# ---------------------------------------------------------------------------
# get_column_origin — computation-origin walker (annotation provenance)
#
# The annotation walker answers "where is this column COMPUTED?" and must agree
# with the desc-source walker on the model a column is born in. Both stop at the
# same computation walls (union / aggregate / window / literal / generated /
# multi-source). The annotation walker is purely structural: it passes *through*
# pure passthroughs / renames and inherited description copies, and must NOT stop
# at the first ancestor that merely carries a description.
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _no_origin_cache():
    """Run with an empty, isolated origin cache (it is a module global)."""
    with mock.patch("dbt_osmosis_cll.integration.cll._ORIGIN_CACHE", {}):
        yield


def _window_passthrough_fixture():
    """DP.prev_col ← (passthrough) UNION.prev_col ← (passthrough) IDENT.prev_col = LAG(...).

    IDENT computes prev_col with a window function; UNION carries an inherited *copy* of
    its description; DP is the layer being annotated. Returns (ctx, dp, node_index, results).
    """
    ctx = make_context()
    desc = "Previous value at the same partition (LAG)."
    ident = FakeNode("ident", {"PREV_COL": FakeColumn("PREV_COL", desc)})
    union = FakeNode("union", {"PREV_COL": FakeColumn("PREV_COL", desc)})
    dp = FakeNode("dp", {"PREV_COL": FakeColumn("PREV_COL", "")})
    results = {
        "dp": [
            cll(
                "dp",
                "prev_col",
                is_rename=True,
                progenitor_model="union",
                progenitor_column="prev_col",
            )
        ],
        "union": [
            cll(
                "union",
                "prev_col",
                is_rename=True,
                progenitor_model="ident",
                progenitor_column="prev_col",
            )
        ],
        "ident": [
            cll(
                "ident",
                "prev_col",
                is_window=True,
                is_computed=True,
                progenitor_model="src",
                progenitor_column="col",
            )
        ],
    }
    return ctx, dp, {"dp": dp, "union": union, "ident": ident}, results


def test_origin_walks_through_passthrough_to_window_computation():
    """The computation origin is the window node IDENT, not the described intermediate UNION.

    Regression: the annotation tracer stopped at the first ancestor carrying a description
    (UNION), crediting a passthrough instead of the node where the column is computed.
    """
    ctx, dp, node_index, results = _window_passthrough_fixture()
    with _no_origin_cache(), patched(results=results, node_index=node_index):
        origin = get_column_origin(ctx, dp, "PREV_COL")
    assert origin is not None
    _schema, origin_model, origin_col, _entry = origin
    assert origin_model == "IDENT"  # computed where the window lives, not UNION
    assert origin_col == ""  # "computed in MODEL" sentinel — no single source column


def test_annotation_origin_and_desc_source_agree_on_window_passthrough():
    """Computation origin (annotation) and description origin (desc-source) resolve to
    the SAME model for a window passthrough — the maintainer's documentation-quality
    invariant: the annotation points at the model the desc-source resolves to."""
    ctx, dp, node_index, results = _window_passthrough_fixture()
    with _no_origin_cache(), patched(results=results, node_index=node_index):
        origin = get_column_origin(ctx, dp, "PREV_COL")
        _desc, desc_source_ref = _resolve_cll_description(ctx, "dp", "prev_col")
    assert origin is not None and desc_source_ref is not None
    assert origin[1] == desc_source_ref.split(".")[0] == "IDENT"


def test_origin_carries_renamed_name_at_computation_wall():
    """When the chain renames the column before the computation wall, the origin's
    entry_col reports the name AT the wall (here BAR) so the annotation can append
    '(as BAR)' — letting the reader find the column in the computing model under its
    real name there. Regression guard for rename-aware computed-in annotations."""
    ctx = make_context()
    desc = "Windowed value."
    ident = FakeNode("ident", {"BAR": FakeColumn("BAR", desc)})
    union = FakeNode("union", {"BAR": FakeColumn("BAR", desc)})
    dp = FakeNode("dp", {"FOO": FakeColumn("FOO", "")})
    results = {
        "dp": [cll("dp", "foo", is_rename=True, progenitor_model="union", progenitor_column="bar")],
        "union": [
            cll("union", "bar", is_rename=True, progenitor_model="ident", progenitor_column="bar")
        ],
        "ident": [
            cll(
                "ident",
                "bar",
                is_window=True,
                is_computed=True,
                progenitor_model="src",
                progenitor_column="col",
            )
        ],
    }
    with (
        _no_origin_cache(),
        patched(results=results, node_index={"dp": dp, "union": union, "ident": ident}),
    ):
        origin = get_column_origin(ctx, dp, "FOO")
    _schema, origin_model, origin_col, entry_col = origin
    assert origin_model == "IDENT"
    assert origin_col == ""  # computed-in sentinel
    assert entry_col == "BAR"  # name at the wall — feeds the "(as BAR)" annotation


# ---------------------------------------------------------------------------
# Union anchor re-definition (desc-source origin at union nodes)
#
# Regression for the real-repo case where a union node (KWM) owned a description
# that differed from its agreeing branches (stg spelled "gueltig", KWM "gültig"):
# the union step ignored the union node's own text, so the NEXT downstream copy
# was mis-credited as the desc-source anchor. The union path must apply the same
# re-definition semantics as the single-progenitor path.
# ---------------------------------------------------------------------------


def _union_anchor_fixture(union_own_desc):
    """child.col ← mid.col ← union(kwm).col ← {stg1.col, stg2.col} (branches agree).

    ``union_own_desc`` is the text stored at the union node; mid carries a copy of it.
    Returns (ctx, child, node_index, results).
    """
    ctx = make_context()
    branch_desc = "Branch text"
    stg1 = FakeNode("stg1", {"COL": FakeColumn("COL", branch_desc)})
    stg2 = FakeNode("stg2", {"COL": FakeColumn("COL", branch_desc)})
    kwm = FakeNode("kwm", {"COL": FakeColumn("COL", union_own_desc)})
    mid = FakeNode("mid", {"COL": FakeColumn("COL", union_own_desc)})
    child = FakeNode("child", {"COL": FakeColumn("COL", "")})
    results = {
        "child": [cll("child", "col", progenitor_model="mid", progenitor_column="col")],
        "mid": [cll("mid", "col", progenitor_model="kwm", progenitor_column="col")],
        "kwm": [
            cll(
                "kwm",
                "col",
                is_union=True,
                union_branches=[("stg1", "col"), ("stg2", "col")],
            )
        ],
    }
    node_index = {"stg1": stg1, "stg2": stg2, "kwm": kwm, "mid": mid, "child": child}
    return ctx, child, node_index, results


def test_union_node_redefining_text_is_the_origin_not_its_downstream_copy():
    """A union node owning a description that DIFFERS from the branch agreement re-defines
    the text — it is the origin, and the downstream copy (mid) is walked through instead of
    being mis-credited as the anchor."""
    ctx, child, node_index, results = _union_anchor_fixture("Curated text")
    with patched(results=results, node_index=node_index):
        desc, origin = _resolve_cll_description(ctx, "mid", "col")
        assert desc == "Curated text"
        assert origin == "KWM.COL"  # the union node, NOT MID.COL

        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Curated text"
    # The union node is the origin (KWM.COL re-defines the text), child is a copy → injection.
    assert col.meta.get("desc-owner") == "upstream"


def test_union_node_matching_branch_agreement_still_yields_no_origin():
    """A union node whose own text matches the agreeing branches does NOT anchor: the text
    inherits with origin None (a union has no single origin), so no desc-owner is injected."""
    ctx, child, node_index, results = _union_anchor_fixture("Branch text")
    with patched(results=results, node_index=node_index):
        desc, origin = _resolve_cll_description(ctx, "mid", "col")
        assert desc == "Branch text"
        assert origin is None

        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Branch text"
    assert "desc-owner" not in col.meta
    assert "desc-source" not in col.meta


def test_whitespace_only_difference_is_not_a_redefinition_anchor():
    """The step-8 anchor check uses the same whitespace-robust equivalence as union
    agreement: a copy that differs from upstream only in line-wrap is walked through,
    keeping the deeper origin instead of becoming a spurious anchor."""
    ctx = make_context()
    origin_node = FakeNode("origin", {"COL": FakeColumn("COL", "Some long description text")})
    copy = FakeNode("copy", {"COL": FakeColumn("COL", "Some long\ndescription   text")})
    results = {
        "copy": [cll("copy", "col", progenitor_model="origin", progenitor_column="col")],
    }
    with patched(results=results, node_index={"origin": origin_node, "copy": copy}):
        desc, origin = _resolve_cll_description(ctx, "copy", "col")
    assert desc == "Some long description text"
    assert origin == "ORIGIN.COL"  # wrap-only difference → not a re-definition at COPY
