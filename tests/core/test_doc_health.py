"""Tests for the documentation-health report (core/doc_health.py)."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest
from dbt.artifacts.resources.types import NodeType

from dbt_osmosis_cll.config import get_config, reset_config
from dbt_osmosis_cll.osmosis_propagation.commands.doc_health import (
    compute_doc_health,
    format_report,
)


class FakeColumn:
    def __init__(self, name, description="", meta=None):
        self.name = name
        self.description = description
        self.meta = meta or {}


class FakeNode:
    def __init__(self, name, columns, resource_type=NodeType.Model):
        self.name = name
        self.columns = columns
        self.resource_type = resource_type
        self.unique_id = f"{resource_type.value}.test.{name}"


def make_context():
    return SimpleNamespace(placeholders=("",))


@contextlib.contextmanager
def patched(nodes, *, cll_failures=None):
    cll_failures = cll_failures or set()
    stack = contextlib.ExitStack()
    stack.enter_context(
        mock.patch(
            "dbt_osmosis_cll.osmosis_propagation.node_filters._iter_candidate_nodes",
            lambda _ctx: ((n.unique_id, n) for n in nodes),
        )
    )
    stack.enter_context(
        mock.patch("dbt_osmosis_cll.integration.cll.get_cll_results", lambda _c, _n: [])
    )
    stack.enter_context(
        mock.patch(
            "dbt_osmosis_cll.integration.cll.get_cll_failures", lambda _c: frozenset(cll_failures)
        )
    )
    with stack:
        yield


@pytest.fixture(autouse=True)
def _reset_config():
    reset_config()
    yield
    reset_config()


def _annotation_only_description() -> str:
    cfg = get_config()
    return (
        f"{cfg.annotation_separator}\n{cfg.annotation_namespace} -> "
        f"{cfg.annotation_renamed} parent.col"
    )


def test_classifies_documented_annotation_only_and_undocumented():
    node = FakeNode(
        "child",
        {
            "A": FakeColumn("A", "Real description"),
            "B": FakeColumn("B", "Another real one"),
            "C": FakeColumn("C", _annotation_only_description()),
            "D": FakeColumn("D", ""),
        },
    )
    ctx = make_context()
    with patched([node]):
        report = compute_doc_health(ctx)

    assert len(report.nodes) == 1
    nh = report.nodes[0]
    assert nh.total == 4
    assert nh.documented == 2
    assert nh.annotation_only == 1
    assert nh.undocumented_columns == ["D"]
    assert report.coverage == 50.0


def test_empty_project_reports_full_coverage():
    ctx = make_context()
    with patched([]):
        report = compute_doc_health(ctx)
    assert report.total_columns == 0
    assert report.coverage == 100.0


def test_to_dict_shape_is_stable():
    node = FakeNode("m", {"A": FakeColumn("A", "doc"), "B": FakeColumn("B", "")})
    ctx = make_context()
    with patched([node]):
        report = compute_doc_health(ctx)
    data = report.to_dict()
    assert data["summary"]["total_columns"] == 2
    assert data["summary"]["documented_columns"] == 1
    assert data["summary"]["undocumented_columns"] == 1
    assert data["summary"]["coverage_pct"] == 50.0
    assert data["nodes"][0]["node_name"] == "m"
    assert data["nodes"][0]["coverage_pct"] == 50.0


def test_check_cll_collects_failures_and_skips_sources():
    model = FakeNode("m", {"A": FakeColumn("A", "doc")}, resource_type=NodeType.Model)
    source = FakeNode("s", {"A": FakeColumn("A", "doc")}, resource_type=NodeType.Source)
    ctx = make_context()
    cll_results = mock.Mock(return_value=[])
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch(
                "dbt_osmosis_cll.osmosis_propagation.node_filters._iter_candidate_nodes",
                lambda _ctx: iter([(model.unique_id, model), (source.unique_id, source)]),
            )
        )
        stack.enter_context(
            mock.patch("dbt_osmosis_cll.integration.cll.get_cll_results", cll_results)
        )
        stack.enter_context(
            mock.patch(
                "dbt_osmosis_cll.integration.cll.get_cll_failures", lambda _c: frozenset({"m"})
            )
        )
        report = compute_doc_health(ctx, check_cll=True)

    assert report.cll_checked is True
    assert report.cll_failures == ["m"]
    # CLL is run for the model but not for the source node.
    called_nodes = {call.args[1].name for call in cll_results.call_args_list}
    assert called_nodes == {"m"}


def test_format_report_contains_summary_and_attention_section():
    node = FakeNode("child", {"A": FakeColumn("A", "doc"), "B": FakeColumn("B", "")})
    ctx = make_context()
    with patched([node]):
        report = compute_doc_health(ctx)
    text = format_report(report, verbose=True)
    assert "Documentation health" in text
    assert "Coverage:" in text
    assert "child" in text
    assert "missing: B" in text


def test_trust_breakdown_authored_inherited_glossary():
    """Documented columns split by provenance: glossary > inherited (desc-owner: upstream) > authored."""
    node = FakeNode(
        "m",
        {
            "A": FakeColumn("A", "hand-written description"),
            "B": FakeColumn("B", "gap-filled text", meta={"desc-owner": "upstream"}),
            "C": FakeColumn("C", "glossary text"),
            "D": FakeColumn("D", ""),
        },
    )
    ctx = make_context()
    with (
        patched([node]),
        mock.patch(
            "dbt_osmosis_cll.config.get_column_docs", lambda *a, **k: {"c": "glossary text"}
        ),
    ):
        report = compute_doc_health(ctx)

    nh = report.nodes[0]
    assert nh.documented == 3
    assert nh.authored == 1
    assert nh.inherited == 1
    assert nh.glossary == 1
    assert nh.authored + nh.inherited + nh.glossary == nh.documented

    data = report.to_dict()
    assert data["summary"]["authored_columns"] == 1
    assert data["summary"]["inherited_columns"] == 1
    assert data["summary"]["glossary_columns"] == 1
    assert data["nodes"][0]["authored"] == 1
    assert data["nodes"][0]["inherited"] == 1
    assert data["nodes"][0]["glossary"] == 1


def test_trust_breakdown_reads_desc_owner_from_config_meta():
    """Fusion mode writes desc-owner: upstream to config.meta — inherited must be detected there."""
    col = FakeColumn("B", "gap-filled text")
    col.config = {"meta": {"desc-owner": "upstream"}}
    node = FakeNode("m", {"B": col})
    ctx = make_context()
    with patched([node]):
        report = compute_doc_health(ctx)
    assert report.nodes[0].inherited == 1
    assert report.nodes[0].authored == 0


def test_trust_breakdown_in_text_report():
    node = FakeNode("m", {"A": FakeColumn("A", "authored text")})
    ctx = make_context()
    with patched([node]):
        report = compute_doc_health(ctx)
    text = format_report(report)
    assert "authored:" in text
    assert "inherited:" in text
    assert "glossary:" in text
