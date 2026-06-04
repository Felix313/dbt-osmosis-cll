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
from dbt_osmosis.core.cll import (
    clear_cll_walk_soft_fails,
    get_cll_walk_soft_fails,
)
from dbt_osmosis.core.transforms import (
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
# desc-source provenance (managed meta key, written via top-level meta)
# ---------------------------------------------------------------------------
#
# NOTE: the tag is written to the column's TOP-LEVEL ``meta`` (FakeColumn.meta). The real YAML
# sync writer then places it under ``config.meta`` (fusion) or keeps it top-level (classic);
# these unit tests call the inherit transform directly, so they assert on ``meta``.


def test_gap_fill_writes_desc_source_to_meta():
    """An empty column gap-filled via CLL records its origin in the column's meta."""
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
    assert col.meta == {
        "desc-source": "STG_EDW__AE_AML__AML_T_BPARTNER_POSITION.BPARTNER_ID"
    }


def test_desc_source_points_to_true_origin_through_same_text_copies():
    """Option 1: desc-source walks THROUGH same-text copies to the node where the text was
    first defined — not the immediate parent.

    Chain (each the progenitor of the next): src "V" → base "X" → base2 "Y" → base3 "Y".
    base and base2 re-define the text (differ from upstream) so they are origins. base3 carries
    the SAME text as base2, so it is a copy — and its desc-source must point at base2 (where "Y"
    was authored), proving the walk passed through nothing-to-stop-at and that desc-source is the
    deepest same-text node, not the immediate parent.
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

    # base3 is a same-text copy of base2 → tagged with the true origin (base2).
    with patched(**chain):
        inherit_upstream_column_knowledge_cll(ctx, base3)
    assert base3.columns["COL"].description == "Y"
    assert base3.columns["COL"].meta == {"desc-source": "BASE2.COL"}

    # base2 re-defines the text (Y != X) → it IS an origin → no tag.
    with patched(**chain):
        inherit_upstream_column_knowledge_cll(ctx, base2)
    assert base2.columns["COL"].description == "Y"
    assert "desc-source" not in base2.columns["COL"].meta


def test_force_inherit_overwrite_does_not_write_desc_source():
    """An existing description overwritten via desc-owner: upstream is owned upstream —
    no desc-source provenance is written."""
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
    assert col.description == "Parent desc"  # overwritten
    assert "desc-source" not in col.meta  # but no provenance tag


def test_no_description_resolved_writes_no_desc_source():
    """When CLL resolves no description, no gap-fill happens → no desc-source written."""
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
    assert "desc-source" not in col.meta


def test_desc_source_disabled_writes_no_key():
    """desc-source-key = '' disables writing the provenance key entirely."""
    child = FakeNode("child", {"COL": FakeColumn("COL", description="")})
    ctx = make_context()
    cfg = get_config()
    cfg.desc_source_key = ""  # disable
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "Parent desc"},
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "Parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Parent desc"  # still inherited
    assert col.meta == {}  # but no provenance key


def test_desc_source_idempotent_across_runs():
    """The provenance tag is recomputed each run: a gap-filled column keeps the same tag on a
    second run rather than flip-flopping (write run 1, strip run 2)."""
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
    assert first.meta == {"desc-source": "PARENT.COL"}

    second = run()  # rerun on the now-populated, now-tagged column
    assert second.description == "Parent desc"
    assert second.meta == {"desc-source": "PARENT.COL"}  # unchanged — no flip-flop


def test_desc_source_backfilled_for_preexisting_inherited():
    """A column whose description already matches upstream but carries no tag yet (inherited
    before this feature existed) has the tag written on the next run via content match."""
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
    assert col.meta == {"desc-source": "PARENT.COL"}


def test_desc_source_updates_when_progenitor_changes():
    """When CLL now resolves the column to a different progenitor, the tag is rewritten to
    point at the new one rather than left pointing at the stale old source."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Shared desc", meta={"desc-source": "OLD_PARENT.COL"})},
    )
    ctx = make_context()
    with patched(
        results={
            "child": [
                cll("child", "col", progenitor_model="new_parent", progenitor_column="col")
            ]
        },
        yaml_descs={("new_parent", "col"): "Shared desc"},
        node_index={"new_parent": FakeNode("new_parent", {"COL": FakeColumn("COL", "Shared desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.meta == {"desc-source": "NEW_PARENT.COL"}  # repointed, not stale


def test_desc_source_kept_when_upstream_text_drifts():
    """desc-owner: this freezes the child description. If the upstream text later drifts, the
    child keeps its (now-divergent) text AND its provenance tag — the pointer is still correct,
    so the tag must not be stripped just because the texts no longer match."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Old parent desc", meta={"desc-source": "PARENT.COL"})},
        settings={("desc-owner", None): "this"},
    )
    ctx = make_context()
    with patched(
        results={
            "child": [cll("child", "col", progenitor_model="parent", progenitor_column="col")]
        },
        yaml_descs={("parent", "col"): "New parent desc"},  # upstream text has drifted
        node_index={"parent": FakeNode("parent", {"COL": FakeColumn("COL", "New parent desc")})},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert col.description == "Old parent desc"  # frozen, not overwritten
    assert col.meta == {"desc-source": "PARENT.COL"}  # provenance kept


def test_authored_description_without_tag_keeps_no_tag():
    """A locally authored description that differs from upstream and was never inherited (no
    tag present) stays untagged. Removing the tag is how ownership is claimed."""
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
    assert "desc-source" not in col.meta


def test_force_inherit_strips_existing_tag():
    """A column switched to desc-owner: upstream is now owned upstream — any provenance tag it
    carried from a previous (desc-owner: this) state is dropped."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Local desc", meta={"desc-source": "PARENT.COL"})},
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
    assert col.description == "Parent desc"  # overwritten (force-inherit)
    assert "desc-source" not in col.meta  # owned upstream → no provenance


def test_desc_source_stripped_when_column_becomes_computed():
    """A column that previously inherited (carries a tag) but is now a multi-source/computed
    column loses its tag on the early-skip path — it no longer has a single progenitor."""
    child = FakeNode(
        "child",
        {"COL": FakeColumn("COL", description="Some desc", meta={"desc-source": "PARENT.COL"})},
    )
    ctx = make_context()
    with patched(
        results={"child": [cll("child", "col", is_computed=True, progenitor_column=None)]},
    ):
        inherit_upstream_column_knowledge_cll(ctx, child)
    col = child.columns["COL"]
    assert "desc-source" not in col.meta


# ---------------------------------------------------------------------------
# Origin-walk soft-fails (cycle / max-depth) + self-reference handling
# ---------------------------------------------------------------------------


def test_self_reference_resolves_to_own_and_records_no_soft_fail():
    """A direct self-ref (incremental {{ this }}: M.col → M.col) is short-circuited before the
    cycle guard: it resolves to the column's own description, records NO soft-fail, and does not
    tag itself as its own desc-source."""
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

        # Through the inherit path, the column must NOT cite itself as its own source.
        inherit_upstream_column_knowledge_cll(ctx, m)
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
    mid = FakeNode("mid", {"COL": FakeColumn("COL", "")}, settings={("desc-owner", None): "upstream"})
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
