"""Resolving a CLL progenitor back to the dbt node it names.

``ColumnLineageResult.progenitor_model`` is read out of compiled SQL, so it is a
*relation* name. The manifest index it is resolved against used to hold dbt *node*
names only, so any model whose ``alias`` differs from its name — including every
versioned model — failed to resolve, and the origin walk returned None without
saying anything.

Guarantees under test:
- a progenitor naming an aliased model's relation resolves to that node;
- lookups by plain model name keep working;
- versioned models resolve to the version owning the bare relation;
- node kinds that cannot appear in a FROM clause never shadow a real model;
- a progenitor that is genuinely no dbt node is reported as a soft-fail instead
  of vanishing.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dbt_osmosis_cll.integration import cll as cll_mod

PROJECT = "/proj"


@pytest.fixture(autouse=True)
def _clear_indexes():
    for cache in (
        cll_mod._NODE_INDEX,
        cll_mod._SOURCE_INDEX,
        cll_mod._SOURCE_REVERSE_INDEX,
        cll_mod._ORIGIN_CACHE,
        cll_mod._CLL_WALK_SOFT_FAILS,
    ):
        cache.clear()
    yield
    for cache in (
        cll_mod._NODE_INDEX,
        cll_mod._SOURCE_INDEX,
        cll_mod._SOURCE_REVERSE_INDEX,
        cll_mod._ORIGIN_CACHE,
        cll_mod._CLL_WALK_SOFT_FAILS,
    ):
        cache.clear()


def _node(name, alias=None, resource_type="model", schema="DC_STG", columns=None, version=None):
    uid = f"{resource_type}.pkg.{name}" + (f".v{version}" if version else "")
    return SimpleNamespace(
        name=name,
        alias=alias if alias is not None else name,
        resource_type=resource_type,
        schema=schema,
        unique_id=uid,
        columns=columns or {},
        unrendered_config=SimpleNamespace(schema=schema),
    )


def _context(nodes, sources=()):
    return SimpleNamespace(
        placeholders=("",),
        project=SimpleNamespace(
            runtime_cfg=SimpleNamespace(project_root=PROJECT),
            manifest=SimpleNamespace(
                nodes={n.unique_id: n for n in nodes},
                sources={s.unique_id: s for s in sources},
            ),
        ),
    )


def _row(model, column, **kwargs):
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
        "unique_id": None,
    }
    base.update(kwargs)
    return SimpleNamespace(model=model, column=column, **base)


def _patch_results(monkeypatch, rows_by_model):
    """Serve CLL rows per dbt node name, as get_cll_results does."""

    def fake(context, node):
        return rows_by_model.get(node.name, [])

    monkeypatch.setattr(cll_mod, "get_cll_results", fake)


# ---------------------------------------------------------------------------
# Index construction
# ---------------------------------------------------------------------------


def test_model_is_indexed_by_alias_and_by_name():
    ctx = _context([_node("stg_orders", alias="physical_orders")])
    cll_mod._ensure_manifest_index(ctx)
    index = cll_mod._NODE_INDEX[PROJECT]

    # The alias is what compiled SQL references, so it must resolve...
    assert index["physical_orders"].name == "stg_orders"
    # ...and the plain model name stays usable for every other caller.
    assert index["stg_orders"].name == "stg_orders"


def test_versioned_models_resolve_to_the_version_owning_the_bare_relation():
    ctx = _context([
        _node("stg_customers", alias="stg_customers_v2", version=2),
        _node("stg_customers", alias="stg_customers", version=1),
    ])
    cll_mod._ensure_manifest_index(ctx)
    index = cll_mod._NODE_INDEX[PROJECT]

    assert index["stg_customers"].unique_id == "model.pkg.stg_customers.v1"
    assert index["stg_customers_v2"].unique_id == "model.pkg.stg_customers.v2"


def test_version_order_in_the_manifest_does_not_decide_the_winner():
    """The alias entry must win over the name fallback regardless of iteration order."""
    ctx = _context([
        _node("stg_customers", alias="stg_customers", version=1),
        _node("stg_customers", alias="stg_customers_v2", version=2),
    ])
    cll_mod._ensure_manifest_index(ctx)
    assert cll_mod._NODE_INDEX[PROJECT]["stg_customers"].unique_id == "model.pkg.stg_customers.v1"


def test_non_relation_node_kinds_never_shadow_a_model():
    """A test node carries an alias too; indexing it could hide a real relation."""
    ctx = _context([
        _node("orders", resource_type="model"),
        _node("orders", alias="orders", resource_type="test"),
        _node("raw_countries", resource_type="seed"),
        _node("orders_snap", resource_type="snapshot"),
    ])
    cll_mod._ensure_manifest_index(ctx)
    index = cll_mod._NODE_INDEX[PROJECT]

    assert index["orders"].resource_type == "model"
    assert "raw_countries" in index
    assert "orders_snap" in index


def test_sources_are_still_indexed_by_identifier_and_name():
    source = SimpleNamespace(
        name="orders",
        identifier="raw_orders",
        schema="RAW",
        database="DB",
        unique_id="source.pkg.a",
    )
    ctx = _context([], sources=[source])
    cll_mod._ensure_manifest_index(ctx)

    assert cll_mod._SOURCE_INDEX[PROJECT]["raw_orders"] is source
    assert cll_mod._SOURCE_INDEX[PROJECT]["orders"] is source


# ---------------------------------------------------------------------------
# The origin walk
# ---------------------------------------------------------------------------


def test_origin_walk_traces_through_an_aliased_model(monkeypatch):
    """The chain used to stop dead at the aliased relation."""
    stg = _node("stg_orders", alias="physical_orders")
    mart = _node("mart_orders", schema="DC_MART")
    ctx = _context([stg, mart])

    _patch_results(
        monkeypatch,
        {
            # mart_orders.order_id comes from the relation physical_orders...
            "mart_orders": [
                _row(
                    "mart_orders",
                    "order_id",
                    progenitor_model="physical_orders",
                    progenitor_column="order_id",
                    is_rename=True,
                )
            ],
            # ...which is stg_orders, where the column originates.
            "stg_orders": [_row("stg_orders", "order_id", is_first_in_chain=True)],
        },
    )

    origin = cll_mod.get_column_origin(ctx, mart, "order_id")
    assert origin == ("DC_STG", "STG_ORDERS", "ORDER_ID", "ORDER_ID")


def test_origin_walk_still_works_when_alias_equals_name(monkeypatch):
    stg = _node("stg_orders")
    mart = _node("mart_orders", schema="DC_MART")
    ctx = _context([stg, mart])

    _patch_results(
        monkeypatch,
        {
            "mart_orders": [
                _row(
                    "mart_orders",
                    "order_id",
                    progenitor_model="stg_orders",
                    progenitor_column="order_id",
                    is_rename=True,
                )
            ],
            "stg_orders": [_row("stg_orders", "order_id", is_first_in_chain=True)],
        },
    )

    assert cll_mod.get_column_origin(ctx, mart, "order_id") == (
        "DC_STG",
        "STG_ORDERS",
        "ORDER_ID",
        "ORDER_ID",
    )


def test_source_progenitor_still_terminates_the_walk(monkeypatch):
    source = SimpleNamespace(
        name="orders",
        identifier="raw_orders",
        schema="RAW",
        database="DB",
        unique_id="source.pkg.a",
    )
    stg = _node("stg_orders")
    ctx = _context([stg], sources=[source])

    _patch_results(
        monkeypatch,
        {
            "stg_orders": [
                _row(
                    "stg_orders",
                    "order_id",
                    progenitor_model="raw_orders",
                    progenitor_column="id",
                    is_rename=True,
                )
            ]
        },
    )

    assert cll_mod.get_column_origin(ctx, stg, "order_id") == ("RAW", "RAW_ORDERS", "ID", "ID")


# ---------------------------------------------------------------------------
# Reporting instead of silence
# ---------------------------------------------------------------------------


def test_unresolvable_progenitor_is_reported_as_a_soft_fail(monkeypatch):
    """A relation that is no dbt node is a legitimate dead end — but a named one."""
    stg = _node("stg_orders")
    ctx = _context([stg])

    _patch_results(
        monkeypatch,
        {
            "stg_orders": [
                _row(
                    "stg_orders",
                    "order_id",
                    progenitor_model="some_raw_table",
                    progenitor_column="id",
                    is_rename=True,
                )
            ]
        },
    )

    assert cll_mod.get_column_origin(ctx, stg, "order_id") is None

    soft_fails = cll_mod.get_cll_walk_soft_fails(ctx)
    assert "unresolved-progenitor" in soft_fails
    assert soft_fails["unresolved-progenitor"] == frozenset({
        "stg_orders.order_id → some_raw_table"
    })


def test_resolvable_progenitor_records_no_soft_fail(monkeypatch):
    stg = _node("stg_orders", alias="physical_orders")
    mart = _node("mart_orders")
    ctx = _context([stg, mart])

    _patch_results(
        monkeypatch,
        {
            "mart_orders": [
                _row(
                    "mart_orders",
                    "order_id",
                    progenitor_model="physical_orders",
                    progenitor_column="order_id",
                    is_rename=True,
                )
            ],
            "stg_orders": [_row("stg_orders", "order_id", is_first_in_chain=True)],
        },
    )

    cll_mod.get_column_origin(ctx, mart, "order_id")
    assert (
        cll_mod.get_cll_walk_soft_fails(ctx).get("unresolved-progenitor", frozenset())
        == frozenset()
    )


# ---------------------------------------------------------------------------
# Description lookup uses the same index
# ---------------------------------------------------------------------------


def test_origin_description_is_found_for_an_aliased_model():
    col = SimpleNamespace(description="The order's primary key")
    stg = _node("stg_orders", alias="physical_orders", columns={"order_id": col})
    ctx = _context([stg])

    assert (
        cll_mod.get_origin_source_description(ctx, "DC_STG", "physical_orders", "order_id")
        == "The order's primary key"
    )
    # The model name resolves too, so callers holding either form succeed.
    assert (
        cll_mod.get_origin_source_description(ctx, "DC_STG", "stg_orders", "order_id")
        == "The order's primary key"
    )


# ---------------------------------------------------------------------------
# End-of-run summary
# ---------------------------------------------------------------------------


def test_summary_names_the_unresolved_relation():
    from dbt_osmosis_cll.osmosis_propagation.transforms import format_soft_fail_summary

    messages = format_soft_fail_summary({
        "unresolved-progenitor": {"stg_orders.order_id → some_raw_table"}
    })
    assert len(messages) == 1
    assert "is no dbt node" in messages[0]
    assert "stg_orders.order_id → some_raw_table" in messages[0]
    assert "1 column(s)" in messages[0]


def test_summary_truncates_long_lists_but_keeps_the_count():
    """A repo addressing raw tables directly can produce thousands of these."""
    from dbt_osmosis_cll.osmosis_propagation.transforms import format_soft_fail_summary

    refs = {f"m.col_{i:03d} → raw_{i}" for i in range(50)}
    message = format_soft_fail_summary({"unresolved-progenitor": refs}, limit=20)[0]

    assert "50 column(s)" in message
    assert "... and 30 more" in message
    assert message.count("\n  ") == 21  # 20 refs + the "and more" line


def test_summary_skips_reasons_with_no_refs():
    from dbt_osmosis_cll.osmosis_propagation.transforms import format_soft_fail_summary

    assert format_soft_fail_summary({"cycle": set(), "max-depth": frozenset()}) == []


def test_summary_covers_every_recorded_reason():
    from dbt_osmosis_cll.osmosis_propagation.transforms import format_soft_fail_summary

    messages = format_soft_fail_summary({
        "cycle": {"a.col"},
        "max-depth": {"b.col"},
        "unresolved-progenitor": {"c.col → t"},
    })
    assert len(messages) == 3
    # An unknown reason still reports rather than being swallowed.
    assert "brand-new-reason" in format_soft_fail_summary({"brand-new-reason": {"x.y"}})[0]


# ---------------------------------------------------------------------------
# Caches must not collapse two versions of one model
# ---------------------------------------------------------------------------


def test_origin_cache_does_not_confuse_two_versions_of_a_model(monkeypatch):
    """Both versions carry the same name; a name-keyed cache would answer for the wrong one."""
    v1 = _node("stg_customers", alias="stg_customers", version=1, schema="DC_V1")
    v2 = _node("stg_customers", alias="stg_customers_v2", version=2, schema="DC_V2")
    ctx = _context([v1, v2])

    def fake(context, node):
        # Rows are served per node identity, as get_cll_results does after its
        # unique_id filter — both sets claim model name "stg_customers".
        return [_row("stg_customers", "col", is_first_in_chain=True)]

    monkeypatch.setattr(cll_mod, "get_cll_results", fake)

    assert cll_mod.get_column_origin(ctx, v1, "col") == ("DC_V1", "STG_CUSTOMERS", "COL", "COL")
    # Without unique_id in the cache key this returns v1's answer.
    assert cll_mod.get_column_origin(ctx, v2, "col") == ("DC_V2", "STG_CUSTOMERS", "COL", "COL")
