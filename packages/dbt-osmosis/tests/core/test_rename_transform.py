# pyright: reportPrivateImportUsage=false, reportPrivateUsage=false, reportUnknownParameterType=false, reportMissingParameterType=false, reportAny=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportArgumentType=false, reportFunctionMemberAccess=false, reportUnknownVariableType=false, reportUnusedParameter=false
"""Tests for the enrich_rename_descriptions transform.

All tests use MagicMock — no DuckDB or warehouse connection required.

The transform calls:
  - dbt_osmosis.core.cll.get_cll_results       → patched to return mock CLL results
  - dbt_osmosis.core.cll.get_column_origin     → patched to return (schema, model, col) or None
  - dbt_osmosis.core.cll.get_origin_source_description → patched to return desc or None
  - dbt_osmosis.core.introspection._get_setting_for_node → patched per-test

`_LINEAGE_CACHE` lives in dbt_osmosis.core.cll and is cleared via fixture.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub dbt_column_lineage into sys.modules BEFORE importing transforms,
# so the lazy local import inside the transform body always resolves.
# ---------------------------------------------------------------------------
_stub_api = types.ModuleType("dbt_column_lineage.api")
_stub_api.get_column_lineage = MagicMock(return_value=[])  # type: ignore[attr-defined]
_stub_root = types.ModuleType("dbt_column_lineage")
sys.modules.setdefault("dbt_column_lineage", _stub_root)
sys.modules.setdefault("dbt_column_lineage.api", _stub_api)

from dbt_osmosis.core.cll import _LINEAGE_CACHE  # noqa: E402
from dbt_osmosis.core.transforms import enrich_rename_descriptions  # noqa: E402

# ---------------------------------------------------------------------------
# Patch-target constants
# ---------------------------------------------------------------------------
_SETTING = "dbt_osmosis.core.introspection._get_setting_for_node"
_GET_CLL = "dbt_osmosis.core.cll.get_cll_results"
_GET_ORIGIN = "dbt_osmosis.core.cll.get_column_origin"
_GET_ORIGIN_DESC = "dbt_osmosis.core.cll.get_origin_source_description"

# ---------------------------------------------------------------------------
# Shared placeholder tuple (must match the real value)
# ---------------------------------------------------------------------------
_PLACEHOLDERS = ("", "Pending further documentation", "No description for this column")


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------

def _make_cll_result(
    model: str,
    column: str,
    *,
    is_computed: bool = False,
    is_rename: bool = False,
    progenitor_model: str | None = None,
    progenitor_column: str | None = None,
) -> MagicMock:
    r = MagicMock()
    r.model = model
    r.column = column
    r.is_computed = is_computed
    r.is_rename = is_rename
    r.progenitor_model = progenitor_model
    r.progenitor_column = progenitor_column
    return r


def _make_col(desc: str, meta: dict | None = None) -> MagicMock:
    """Create a minimal ColumnInfo-like mock with a working .replace()."""
    col = MagicMock()
    col.description = desc
    col.meta = meta or {}

    def _replace(**kw: object) -> MagicMock:
        new_desc = kw.get("description", desc)
        new_meta = kw.get("meta", col.meta)
        return _make_col(str(new_desc), dict(new_meta))  # type: ignore[arg-type]

    col.replace = _replace
    return col


def _make_node(
    name: str = "my_model",
    schema: str = "DC_STG",
    resource_type: str = "model",
    columns: dict[str, str] | None = None,
) -> MagicMock:
    from dbt.artifacts.resources.types import NodeType

    node = MagicMock()
    node.unique_id = f"model.pkg.{name}"
    node.name = name
    node.schema = schema
    node.unrendered_config = MagicMock()
    node.unrendered_config.schema = schema
    node.resource_type = NodeType.Source if resource_type == "source" else NodeType.Model

    node.columns = {col_name: _make_col(desc) for col_name, desc in (columns or {}).items()}
    return node


def _make_context() -> MagicMock:
    ctx = MagicMock()
    ctx.settings.force_inherit_descriptions = False
    ctx.placeholders = _PLACEHOLDERS
    ctx.project.runtime_cfg.project_root = "/fake/project"
    ctx.project.manifest.nodes = {}
    ctx.project.manifest.sources = {}
    return ctx


# ---------------------------------------------------------------------------
# Fixture: clear the module-level lineage cache before each test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_lineage_cache():
    _LINEAGE_CACHE.clear()
    yield
    _LINEAGE_CACHE.clear()


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

@patch(_SETTING)
def test_rename_descriptions_false_skips_all(mock_setting: MagicMock) -> None:
    """add-col-origin-to-meta=False → node is not touched at all."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: (
        False if name == "add-col-origin-to-meta" else fallback
    )
    node = _make_node(columns={"CUSTOMER_ID": ""})

    with patch(_GET_CLL) as mock_cll:
        enrich_rename_descriptions(_make_context(), node)

    mock_cll.assert_not_called()
    assert node.columns["CUSTOMER_ID"].description == ""


@patch(_SETTING)
def test_source_node_skipped(mock_setting: MagicMock) -> None:
    """Source nodes are always skipped."""
    mock_setting.return_value = True
    node = _make_node(resource_type="source", columns={"ID": ""})

    with patch(_GET_CLL) as mock_cll:
        enrich_rename_descriptions(_make_context(), node)

    mock_cll.assert_not_called()


# ---------------------------------------------------------------------------
# Pure rename: "Umbenannt von:" prefix
# ---------------------------------------------------------------------------

@patch(_SETTING)
@patch(_GET_ORIGIN_DESC, return_value=None)
@patch(_GET_ORIGIN)
@patch(_GET_CLL)
def test_pure_rename_appends_umbenannt_von(
    mock_cll: MagicMock, mock_origin: MagicMock, mock_desc: MagicMock, mock_setting: MagicMock,
) -> None:
    """Pure rename (is_rename=True) → 'Umbenannt von:' appended."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: {
        "add-col-origin-to-meta": True,
        "append-col-origin-to-description": True,
    }.get(name, fallback)

    mock_cll.return_value = [_make_cll_result("my_model", "contract_id", is_rename=True)]
    mock_origin.return_value = ("DC_STG", "SRC_TABLE", "VERTRAG_NR")

    node = _make_node(columns={"contract_id": ""})
    enrich_rename_descriptions(_make_context(), node)

    desc = node.columns["contract_id"].description
    assert "__________" in desc
    assert "CBM-ODP -> Umbenannt von: SRC_TABLE -> VERTRAG_NR" in desc
    assert "Abgeleitet aus:" not in desc


@patch(_SETTING)
@patch(_GET_ORIGIN_DESC, return_value="Der Vertrag-Schlüssel")
@patch(_GET_ORIGIN)
@patch(_GET_CLL)
def test_pure_rename_includes_source_desc(
    mock_cll: MagicMock, mock_origin: MagicMock, mock_desc: MagicMock, mock_setting: MagicMock,
) -> None:
    """Pure rename with source description → included after em dash in 'Umbenannt von:'."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: {
        "add-col-origin-to-meta": True,
        "append-col-origin-to-description": True,
    }.get(name, fallback)

    mock_cll.return_value = [_make_cll_result("my_model", "contract_id", is_rename=True)]
    mock_origin.return_value = ("DC_STG", "SRC_TABLE", "VERTRAG_NR")

    node = _make_node(columns={"contract_id": ""})
    enrich_rename_descriptions(_make_context(), node)

    desc = node.columns["contract_id"].description
    assert "CBM-ODP -> Umbenannt von: SRC_TABLE -> VERTRAG_NR — Der Vertrag-Schlüssel" in desc


# ---------------------------------------------------------------------------
# Single-source computed: "Abgeleitet aus:" prefix
# ---------------------------------------------------------------------------

@patch(_SETTING)
@patch(_GET_ORIGIN_DESC, return_value=None)
@patch(_GET_ORIGIN)
@patch(_GET_CLL)
def test_computed_single_source_appends_abgeleitet(
    mock_cll: MagicMock, mock_origin: MagicMock, mock_desc: MagicMock, mock_setting: MagicMock,
) -> None:
    """Single-source computed (is_computed=True, progenitor not None) → 'Abgeleitet aus:'."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: {
        "add-col-origin-to-meta": True,
        "append-col-origin-to-description": True,
    }.get(name, fallback)

    mock_cll.return_value = [_make_cll_result(
        "my_model", "amount_eur", is_computed=True, is_rename=False, progenitor_column="AMOUNT_RAW"
    )]
    mock_origin.return_value = ("DC_STG", "SRC_TABLE", "AMOUNT_RAW")

    node = _make_node(columns={"amount_eur": ""})
    enrich_rename_descriptions(_make_context(), node)

    desc = node.columns["amount_eur"].description
    assert "CBM-ODP -> Abgeleitet aus:" in desc
    assert "Umbenannt von:" not in desc


# ---------------------------------------------------------------------------
# Multi-source derived: "Abgeleitet aus: SCHEMA.MODEL"
# ---------------------------------------------------------------------------

@patch(_SETTING)
@patch(_GET_CLL)
def test_multi_source_derived_appends_berechnet_in(
    mock_cll: MagicMock, mock_setting: MagicMock,
) -> None:
    """Multi-source (is_computed=True, progenitor_column=None) → 'Berechnet in: SCHEMA.MODEL'."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: {
        "add-col-origin-to-meta": True,
        "append-col-origin-to-description": True,
    }.get(name, fallback)

    mock_cll.return_value = [_make_cll_result(
        "my_model", "kpi_score", is_computed=True, progenitor_column=None
    )]

    node = _make_node(name="my_model", schema="DC_STG", columns={"kpi_score": ""})
    enrich_rename_descriptions(_make_context(), node)

    desc = node.columns["kpi_score"].description
    assert "CBM-ODP -> Berechnet in:" in desc
    assert "DC_STG.MY_MODEL" in desc


# ---------------------------------------------------------------------------
# Existing description preserved when append=False
# ---------------------------------------------------------------------------

@patch(_SETTING)
@patch(_GET_ORIGIN_DESC, return_value=None)
@patch(_GET_ORIGIN)
@patch(_GET_CLL)
def test_append_false_no_desc_change(
    mock_cll: MagicMock, mock_origin: MagicMock, mock_desc: MagicMock, mock_setting: MagicMock,
) -> None:
    """append-col-origin-to-description=False → meta written but description unchanged."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: {
        "add-col-origin-to-meta": True,
        "append-col-origin-to-description": False,
    }.get(name, fallback)

    mock_cll.return_value = [_make_cll_result("my_model", "contract_id", is_computed=False)]
    mock_origin.return_value = ("DC_STG", "SRC_TABLE", "VERTRAG_NR")

    node = _make_node(columns={"contract_id": "Existing desc"})
    enrich_rename_descriptions(_make_context(), node)

    assert node.columns["contract_id"].description == "Existing desc"
    assert node.columns["contract_id"].meta.get("cbm_source_name") == "DC_STG.SRC_TABLE.VERTRAG_NR"


# ---------------------------------------------------------------------------
# Passthrough (same name end-to-end) — no annotation
# ---------------------------------------------------------------------------

@patch(_SETTING)
@patch(_GET_ORIGIN_DESC, return_value=None)
@patch(_GET_ORIGIN)
@patch(_GET_CLL)
def test_passthrough_no_annotation(
    mock_cll: MagicMock, mock_origin: MagicMock, mock_desc: MagicMock, mock_setting: MagicMock,
) -> None:
    """Column with same name in origin → stale meta cleared, no annotation added."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: {
        "add-col-origin-to-meta": True,
        "append-col-origin-to-description": True,
    }.get(name, fallback)

    mock_cll.return_value = [_make_cll_result("my_model", "CONTRACT_ID", is_computed=False)]
    mock_origin.return_value = ("DC_STG", "SRC_TABLE", "CONTRACT_ID")  # same name

    node = _make_node(columns={"CONTRACT_ID": "Existing desc"})
    enrich_rename_descriptions(_make_context(), node)

    desc = node.columns["CONTRACT_ID"].description
    assert "Basiert auf:" not in desc
    assert "Abgeleitet aus:" not in desc


# ---------------------------------------------------------------------------
# Resilience: CLL returns no result for column → no change
# ---------------------------------------------------------------------------

@patch(_SETTING)
@patch(_GET_CLL)
def test_no_cll_result_for_column_unchanged(
    mock_cll: MagicMock, mock_setting: MagicMock,
) -> None:
    """Column absent from CLL results → description untouched."""
    mock_setting.side_effect = lambda name, *a, fallback=None, **kw: (
        True if name in ("add-col-origin-to-meta", "append-col-origin-to-description") else fallback
    )
    mock_cll.return_value = []  # no lineage at all

    node = _make_node(columns={"CONTRACT_ID": "Existing desc"})
    enrich_rename_descriptions(_make_context(), node)

    assert node.columns["CONTRACT_ID"].description == "Existing desc"


