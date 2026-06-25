# pyright: reportUnknownVariableType=false, reportPrivateImportUsage=false, reportUnknownMemberType=false
# ruff: noqa: E402,F401
"""Public compatibility facade for dbt-osmosis core APIs."""

from __future__ import annotations

# Import SqlCompileRunner for test compatibility
from dbt.task.sql import SqlCompileRunner

# Core configuration and project management
from dbt_osmosis_cll.osmosis_propagation.config import (
    DbtConfiguration,
    DbtProjectContext,
    _reload_manifest,
    config_to_namespace,
    create_dbt_project_context,
    discover_profiles_dir,
    discover_project_dir,
)

# Inheritance functionality
from dbt_osmosis_cll.osmosis_propagation.inheritance import (
    _build_column_knowledge_graph,
    _build_node_ancestor_tree,
    _get_node_yaml,
)

# Introspection utilities
from dbt_osmosis_cll.osmosis_propagation.introspection import (
    _COLUMN_LIST_CACHE,
    PropertyAccessor,
    SettingsResolver,
    _find_first,
    _get_setting_for_node,
    _maybe_use_precise_dtype,
    get_columns,
    prefetch_columns,
    normalize_column_name,
)

# Schema diff functionality
from dbt_osmosis_cll.osmosis_propagation.commands.diff import (
    ChangeCategory,
    ChangeSeverity,
    ColumnAdded,
    ColumnRemoved,
    ColumnRenamed,
    ColumnTypeChanged,
    SchemaChange,
    SchemaDiff,
    SchemaDiffResult,
)

# Node filtering and sorting
from dbt_osmosis_cll.osmosis_propagation.node_filters import (
    _topological_sort,
)

# Path management
from dbt_osmosis_cll.osmosis_propagation.path_management import (
    MissingOsmosisConfig,
    _get_yaml_path_template,
    build_yaml_file_mapping,
    create_missing_source_yamls,
    get_current_yaml_path,
    get_target_yaml_path,
)

# Plugin system
from dbt_osmosis_cll.osmosis_propagation.plugins import (
    FuzzyCaseMatching,
    FuzzyPrefixMatching,
    get_plugin_manager,
)

# Restructuring operations
from dbt_osmosis_cll.osmosis_propagation.commands.restructuring import (
    RestructureDeltaPlan,
    RestructureOperation,
    apply_restructure_plan,
    draft_restructure_delta_plan,
    pretty_print_plan,
)

# External formatter integration
from dbt_osmosis_cll.osmosis_propagation.formatting import (
    run_external_formatter as run_external_formatter,
)  # noqa: F401

# Schema parsing and writing
from dbt_osmosis_cll.osmosis_propagation.schema.parser import (
    create_yaml_instance,
)
from dbt_osmosis_cll.osmosis_propagation.schema.reader import (
    _YAML_BUFFER_CACHE,
)
from dbt_osmosis_cll.osmosis_propagation.schema.writer import (
    commit_yamls as _commit_yamls_impl,
)

# Settings and context
from dbt_osmosis_cll.osmosis_propagation.settings import (
    EMPTY_STRING,
    YamlRefactorContext,
    YamlRefactorSettings,
)

# SQL operations
from dbt_osmosis_cll.osmosis_propagation.commands.sql_operations import (
    compile_sql_code,
    execute_sql_code,
)

# SQL linting
from dbt_osmosis_cll.osmosis_propagation.commands.sql_lint import (
    KeywordCapitalizationRule,
    LintLevel,
    LintResult,
    LintRule,
    LintViolation,
    LineLengthRule,
    QuotedIdentifierRule,
    SQLLinter,
    SelectStarRule,
    TableAliasRule,
    lint_sql_code,
)

# Staging generation (deterministic, via dbt-core-interface)
from dbt_osmosis_cll.osmosis_propagation.commands.generators import (
    StagingGenerationResult,
    generate_staging_from_source,
)

# Sync operations
from dbt_osmosis_cll.osmosis_propagation.sync_operations import (
    sync_node_to_yaml,
)

from dbt_osmosis_cll.osmosis_propagation.commands.test_suggestions import (
    AITestSuggester,
    ModelTestAnalysis,
    TestPatternExtractor,
    TestSuggester,
    TestSuggestion,
    suggest_tests_for_model,
    suggest_tests_for_project,
)


# Transform operations
from dbt_osmosis_cll.osmosis_propagation.transforms import (
    inherit_upstream_column_knowledge,
    inherit_upstream_column_knowledge_cll,
    inject_missing_columns,
    remove_columns_not_in_database,
    sort_columns_alphabetically,
    sort_columns_as_configured,
    sort_columns_as_in_database,
    synchronize_data_types,
)

# Note: process_node is imported in sql_operations.py where it's used


# Backwards compatibility wrapper for commit_yamls
def commit_yamls(context: YamlRefactorContext) -> None:
    """Backwards compatible wrapper for commit_yamls that accepts only a context."""
    _commit_yamls_impl(
        yaml_handler=context.yaml_handler,
        yaml_handler_lock=context.yaml_handler_lock,
        dry_run=context.settings.dry_run,
        mutation_tracker=context.register_mutations,
        strip_eof_blank_lines=context.settings.strip_eof_blank_lines,
        written_file_tracker=getattr(context, "register_written_file", None),
    )


# Backwards compatibility exports
__all__ = list(
    dict.fromkeys([
        "discover_project_dir",
        "discover_profiles_dir",
        "DbtConfiguration",
        "DbtProjectContext",
        "create_dbt_project_context",
        "create_yaml_instance",
        "YamlRefactorSettings",
        "YamlRefactorContext",
        "EMPTY_STRING",
        "compile_sql_code",
        "execute_sql_code",
        "normalize_column_name",
        "get_columns",
        "create_missing_source_yamls",
        "get_current_yaml_path",
        "get_target_yaml_path",
        "build_yaml_file_mapping",
        "commit_yamls",
        "draft_restructure_delta_plan",
        "pretty_print_plan",
        "sync_node_to_yaml",
        "apply_restructure_plan",
        "inherit_upstream_column_knowledge",
        "inherit_upstream_column_knowledge_cll",
        "inject_missing_columns",
        "remove_columns_not_in_database",
        "sort_columns_as_in_database",
        "sort_columns_alphabetically",
        "sort_columns_as_configured",
        "synchronize_data_types",
        "config_to_namespace",
        "_reload_manifest",
        "_find_first",
        "SettingsResolver",
        "PropertyAccessor",
        "_get_setting_for_node",
        "_maybe_use_precise_dtype",
        "_topological_sort",
        "MissingOsmosisConfig",
        "_get_yaml_path_template",
        "RestructureOperation",
        "RestructureDeltaPlan",
        "get_plugin_manager",
        "FuzzyCaseMatching",
        "FuzzyPrefixMatching",
        "_build_node_ancestor_tree",
        "_get_node_yaml",
        "_build_column_knowledge_graph",
        "_COLUMN_LIST_CACHE",
        "_YAML_BUFFER_CACHE",
        "DbtConfiguration",
        "DbtProjectContext",
        "EMPTY_STRING",
        "FuzzyCaseMatching",
        "FuzzyPrefixMatching",
        "MissingOsmosisConfig",
        "PropertyAccessor",
        "RestructureDeltaPlan",
        "RestructureOperation",
        "SettingsResolver",
        "SqlCompileRunner",
        "StagingGenerationResult",
        "TestPatternExtractor",
        "TestSuggester",
        "TestSuggestion",
        "YamlRefactorContext",
        "YamlRefactorSettings",
        "_build_column_knowledge_graph",
        "_build_node_ancestor_tree",
        "_find_first",
        "_get_node_yaml",
        "_get_setting_for_node",
        "_get_yaml_path_template",
        "_maybe_use_precise_dtype",
        "_reload_manifest",
        "_topological_sort",
        "AITestSuggester",
        "ModelTestAnalysis",
        "apply_restructure_plan",
        "build_yaml_file_mapping",
        "commit_yamls",
        "compile_sql_code",
        "config_to_namespace",
        "create_dbt_project_context",
        "create_missing_source_yamls",
        "create_yaml_instance",
        "discover_profiles_dir",
        "discover_project_dir",
        "draft_restructure_delta_plan",
        "execute_sql_code",
        "generate_staging_from_source",
        "get_columns",
        "prefetch_columns",
        "get_current_yaml_path",
        "get_plugin_manager",
        "get_target_yaml_path",
        "inherit_upstream_column_knowledge",
        "inject_missing_columns",
        "normalize_column_name",
        "pretty_print_plan",
        "remove_columns_not_in_database",
        "sort_columns_alphabetically",
        "sort_columns_as_configured",
        "sort_columns_as_in_database",
        "suggest_tests_for_model",
        "suggest_tests_for_project",
        "sync_node_to_yaml",
        "synchronize_data_types",
        # Schema diff functionality
        "ChangeCategory",
        "ChangeSeverity",
        "ColumnAdded",
        "ColumnRemoved",
        "ColumnRenamed",
        "ColumnTypeChanged",
        "SchemaChange",
        "SchemaDiff",
        "SchemaDiffResult",
        # SQL linting
        "LintLevel",
        "LintViolation",
        "LintResult",
        "LintRule",
        "SQLLinter",
        "lint_sql_code",
        "KeywordCapitalizationRule",
        "LineLengthRule",
        "SelectStarRule",
        "TableAliasRule",
        "QuotedIdentifierRule",
    ])
)
