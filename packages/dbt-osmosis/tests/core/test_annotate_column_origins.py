"""Behavior tests for annotate_column_origins (CLL origin annotation engine).

Mocks the CLL boundaries (get_cll_results, get_column_origin, central docs) and
asserts the annotation written to each column's description / meta, mirroring the
style of test_inheritance_cll.py. The format_* helpers and config run for real,
so assertions reference the live annotation strings from get_config().
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest
from dbt.artifacts.resources.types import NodeType

from dbt_osmosis.config import get_config, reset_config
from dbt_osmosis.core.transforms import annotate_column_origins

PROJECT = "/proj"


class FakeColumn:
    def __init__(self, name, description="", meta=None, tags=None):
        self.name = name
        self.description = description
        self.meta = dict(meta or {})
        self.tags = list(tags or [])

    def replace(self, **kwargs):
        new = FakeColumn(self.name, self.description, dict(self.meta), list(self.tags))
        for key, value in kwargs.items():
            setattr(new, key, value)
        return new


class FakeNode:
    def __init__(self, name, columns, schema="DC_STG", resource_type=NodeType.Model, settings=None):
        self.name = name
        self.columns = columns
        self.schema = schema
        self.unrendered_config = SimpleNamespace(schema=schema)
        self.resource_type = resource_type
        self.unique_id = f"model.test.{name}"
        self._settings = settings or {}


def make_context():
    return SimpleNamespace(
        placeholders=("",),
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
        "literal_value": None,
        "generated_value": None,
        "source_column": None,
    }
    base.update(kwargs)
    return SimpleNamespace(model=model, column=column, **base)


@contextlib.contextmanager
def patched(*, results=None, settings=None, origin=None, origin_desc=None):
    """Patch the CLL boundaries annotate_column_origins reads from.

    *settings* maps a setting key to its value for _get_setting_for_node (only the
    annotate-column-origin-infos mode matters here).
    """
    results = results or {}
    settings = settings or {}

    def fake_get_setting(key, node, col_name=None, fallback=None):
        return settings.get(key, fallback)

    def fake_get_cll_results(_ctx, node):
        return results.get(node.name, [])

    stack = contextlib.ExitStack()
    stack.enter_context(mock.patch("dbt_osmosis.core.cll.get_cll_results", fake_get_cll_results))
    stack.enter_context(
        mock.patch("dbt_osmosis.core.introspection._get_setting_for_node", fake_get_setting)
    )
    stack.enter_context(mock.patch("dbt_osmosis.config.get_column_docs", dict))
    stack.enter_context(
        mock.patch("dbt_osmosis.core.cll.get_column_origin", lambda *a, **k: origin)
    )
    stack.enter_context(
        mock.patch(
            "dbt_osmosis.core.cll.get_origin_source_description", lambda *a, **k: origin_desc
        )
    )
    with stack:
        yield


@pytest.fixture(autouse=True)
def _reset_config():
    reset_config()
    yield
    reset_config()


def _annotate(node, **kwargs):
    with patched(**kwargs):
        annotate_column_origins(make_context(), node)


def test_source_node_is_skipped():
    node = FakeNode("s", {"COL": FakeColumn("COL", "raw desc")}, resource_type=NodeType.Source)
    _annotate(
        node,
        results={"s": [cll("s", "col", is_aggregate=True)]},
        settings={"annotate-column-origin-infos": "always"},
    )
    assert node.columns["COL"].description == "raw desc"


def test_aggregate_annotation_appended_when_if_altered():
    cfg = get_config()
    node = FakeNode("m", {"CNT": FakeColumn("CNT", "")})
    _annotate(
        node,
        results={
            "m": [
                cll(
                    "m",
                    "cnt",
                    is_aggregate=True,
                    progenitor_model="src",
                    progenitor_column="amount",
                )
            ]
        },
        settings={"annotate-column-origin-infos": "if_altered"},
    )
    desc = node.columns["CNT"].description
    assert cfg.annotation_separator in desc
    assert cfg.annotation_aggregate_from in desc


def test_union_annotation_appended():
    cfg = get_config()
    node = FakeNode("m", {"C": FakeColumn("C", "")})
    _annotate(
        node,
        results={"m": [cll("m", "c", is_union=True)]},
        settings={"annotate-column-origin-infos": "always"},
    )
    assert cfg.annotation_union in node.columns["C"].description


def test_literal_annotation_appended():
    cfg = get_config()
    node = FakeNode("m", {"SRC": FakeColumn("SRC", "")})
    _annotate(
        node,
        results={"m": [cll("m", "src", is_literal=True, literal_value="'SAP'")]},
        settings={"annotate-column-origin-infos": "if_altered"},
    )
    assert cfg.annotation_literal in node.columns["SRC"].description


def test_multi_source_computed_annotation_appended():
    cfg = get_config()
    node = FakeNode("m", {"KPI": FakeColumn("KPI", "")})
    _annotate(
        node,
        results={"m": [cll("m", "kpi", is_computed=True, progenitor_column=None)]},
        settings={"annotate-column-origin-infos": "if_altered"},
    )
    desc = node.columns["KPI"].description
    assert cfg.annotation_computed in desc
    assert "DC_STG.M" in desc


def test_never_mode_strips_stale_tags_without_annotating():
    cfg = get_config()
    stale = f"Real desc\n{cfg.annotation_separator}\n{cfg.annotation_namespace} -> {cfg.annotation_aggregate_from} x in: y"
    node = FakeNode("m", {"CNT": FakeColumn("CNT", stale)})
    _annotate(
        node,
        results={"m": [cll("m", "cnt", is_aggregate=True)]},
        settings={"annotate-column-origin-infos": "never"},
    )
    desc = node.columns["CNT"].description
    assert desc == "Real desc"
    assert cfg.annotation_separator not in desc


def test_no_cll_result_strips_stale_managed_meta():
    cfg = get_config()
    managed_key = cfg.meta_key_renamed_from
    node = FakeNode("m", {"COL": FakeColumn("COL", "desc", meta={managed_key: "x.y", "keep": "v"})})
    # CLL returns nothing for this column.
    _annotate(node, results={"m": []}, settings={"annotate-column-origin-infos": "if_altered"})
    meta = node.columns["COL"].meta
    assert managed_key not in meta  # stale managed key removed
    assert meta.get("keep") == "v"  # unmanaged meta preserved


def test_rename_writes_meta_when_write_cll_tags_enabled():
    from dbt_osmosis.config import OsmosisConfig

    cfg = OsmosisConfig(write_cll_tags_to_meta=True)
    node = FakeNode("m", {"ORDER_ID": FakeColumn("ORDER_ID", "")})
    with mock.patch("dbt_osmosis.core.transforms.get_config", lambda: cfg):
        _annotate(
            node,
            results={
                "m": [
                    cll(
                        "m",
                        "order_id",
                        is_rename=True,
                        progenitor_model="src",
                        progenitor_column="id",
                    )
                ]
            },
            settings={"annotate-column-origin-infos": "if_altered"},
            origin=("DC_STG", "SRC", "ID", "ID"),
        )
    meta = node.columns["ORDER_ID"].meta
    assert meta.get(cfg.meta_key_renamed_from) == "SRC.ID"
