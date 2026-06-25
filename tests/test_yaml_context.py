from unittest import mock

from dbt_osmosis_cll.osmosis_propagation.config import _reload_manifest
from dbt_osmosis_cll.osmosis_propagation.introspection import _COLUMN_LIST_CACHE, get_columns
from dbt_osmosis_cll.osmosis_propagation.path_management import create_missing_source_yamls
from dbt_osmosis_cll.osmosis_propagation.commands.restructuring import (
    apply_restructure_plan,
    draft_restructure_delta_plan,
)
from dbt_osmosis_cll.osmosis_propagation.settings import YamlRefactorContext
from dbt_osmosis_cll.osmosis_propagation.transforms import inherit_upstream_column_knowledge

# Note: The yaml_context fixture is defined in conftest.py


# Sanity tests


def test_reload_manifest(yaml_context: YamlRefactorContext):
    _reload_manifest(yaml_context.project)


def test_create_missing_source_yamls(yaml_context: YamlRefactorContext):
    create_missing_source_yamls(yaml_context)


def test_draft_restructure_delta_plan(yaml_context: YamlRefactorContext):
    assert draft_restructure_delta_plan(yaml_context) is not None


def test_apply_restructure_plan(yaml_context: YamlRefactorContext):
    plan = draft_restructure_delta_plan(yaml_context)
    apply_restructure_plan(yaml_context, plan, confirm=False)


def test_inherit_upstream_column_knowledge(yaml_context: YamlRefactorContext):
    inherit_upstream_column_knowledge(yaml_context)


# Column type + settings tests


def _customer_column_types(yaml_context: YamlRefactorContext) -> dict[str, str]:
    node = next(n for n in yaml_context.project.manifest.nodes.values() if n.name == "customers")
    assert node

    columns = get_columns(yaml_context, node)
    assert columns

    column_types = dict({name: meta.type for name, meta in columns.items()})
    assert column_types
    return column_types


def test_get_columns_meta(yaml_context: YamlRefactorContext):
    with mock.patch.dict(_COLUMN_LIST_CACHE, {}, clear=True):
        assert _customer_column_types(yaml_context) == {
            # in DuckDB decimals always have presision and scale
            "customer_average_value": "DECIMAL(18,3)",
            "customer_id": "INTEGER",
            "customer_lifetime_value": "DOUBLE",
            "first_name": "VARCHAR",
            "first_order": "DATE",
            "last_name": "VARCHAR",
            "most_recent_order": "DATE",
            "number_of_orders": "BIGINT",
        }


def test_get_columns_meta_char_length(yaml_context: YamlRefactorContext):
    """Test string_length setting includes the VARCHAR length in the type.

    Column types come from live DuckDB introspection (no catalog file is used — see
    the yaml_context fixture). With string_length=True, VARCHAR columns therefore
    report their length as ``character varying(256)``; with it off they are bare
    ``VARCHAR`` (see test_get_columns_meta). Only the two VARCHAR columns differ.
    """
    # Update the context settings for this test
    yaml_context.settings.string_length = True
    with mock.patch.dict(_COLUMN_LIST_CACHE, {}, clear=True):
        assert _customer_column_types(yaml_context) == {
            "customer_average_value": "DECIMAL(18,3)",
            "customer_id": "INTEGER",
            "customer_lifetime_value": "DOUBLE",
            "first_name": "character varying(256)",  # length included by string_length
            "first_order": "DATE",
            "last_name": "character varying(256)",  # length included by string_length
            "most_recent_order": "DATE",
            "number_of_orders": "BIGINT",
        }


def test_get_columns_meta_numeric_precision(yaml_context: YamlRefactorContext):
    """Test numeric_precision_and_scale setting."""
    yaml_context.settings.numeric_precision_and_scale = True
    with mock.patch.dict(_COLUMN_LIST_CACHE, {}, clear=True):
        assert _customer_column_types(yaml_context) == {
            # in DuckDB decimals always have presision and scale
            "customer_average_value": "DECIMAL(18,3)",
            "customer_id": "INTEGER",
            "customer_lifetime_value": "DOUBLE",
            "first_name": "VARCHAR",
            "first_order": "DATE",
            "last_name": "VARCHAR",
            "most_recent_order": "DATE",
            "number_of_orders": "BIGINT",
        }
