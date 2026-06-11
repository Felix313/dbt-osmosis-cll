"""Wrapper module for dbt-core-interface generators and documentation checker.

This module provides a unified interface to dbt-core-interface's source generation,
staging model generation, and documentation checking capabilities, adapted for
use in dbt-osmosis.
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass
from pathlib import Path

from dbt.contracts.graph.nodes import SourceDefinition

import dbt_osmosis_cll.osmosis_propagation.logger as logger
from dbt_osmosis_cll.osmosis_propagation.config import DbtProjectContext

__all__ = [
    "DocumentationCheckResult",
    "StagingGenerationResult",
    "check_documentation",
    "generate_sources_from_database",
    "generate_staging_from_source",
]


@dataclass
class StagingGenerationResult:
    """Result of staging model generation.

    Attributes:
        source_name: Name of the source table
        staging_name: Name of the generated staging model
        sql_content: Generated SQL content
        yaml_content: Generated YAML content
        sql_path: Path where the SQL file will be written
        yaml_path: Path where the YAML file will be written
        error: Any error that occurred during generation
    """

    source_name: str
    staging_name: str | None = None
    sql_content: str | None = None
    yaml_content: str | None = None
    sql_path: Path | None = None
    yaml_path: Path | None = None
    error: Exception | None = None


@dataclass
class SourceGenerationResult:
    """Result of source generation.

    Attributes:
        source_name: Name of the source generated
        table_count: Number of tables discovered
        yaml_content: Generated YAML content
        yaml_path: Path where YAML should be written
        error: Any error that occurred during generation
    """

    source_name: str
    table_count: int
    yaml_content: str
    yaml_path: Path
    error: Exception | None = None


@dataclass
class DocumentationCheckResult:
    """Result of documentation completeness check.

    Attributes:
        total_models: Total number of models checked
        models_with_descriptions: Number of models with descriptions
        models_without_descriptions: Number of models without descriptions
        total_columns: Total number of columns checked
        documented_columns: Number of columns with descriptions
        undocumented_columns: Number of columns without descriptions
        gaps: List of documentation gaps found
    """

    total_models: int
    models_with_descriptions: int
    models_without_descriptions: int
    total_columns: int
    documented_columns: int
    undocumented_columns: int
    gaps: list[t.Any]  # DocumentationGap from dbt-core-interface


def _resolve_staging_output_paths(
    context: DbtProjectContext,
    staging_name: str,
    staging_path: Path | None,
) -> tuple[Path, Path]:
    """Resolve output paths for generated staging artifacts without writing them."""
    if staging_path is None:
        project_root = Path(context.config.project_dir)
        staging_path = project_root / "models" / "staging"

    return staging_path / f"{staging_name}.sql", staging_path / f"{staging_name}.yml"


def generate_sources_from_database(
    context: DbtProjectContext,
    source_name: str = "raw",
    schema_name: str | None = None,
    exclude_schemas: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    quote_identifiers: bool = False,
    output_path: Path | None = None,
) -> SourceGenerationResult:
    """Generate source definitions from database introspection.

    This function uses dbt-core-interface's SourceGenerator to discover
    tables in the database and generate dbt source YAML definitions.

    Args:
        context: The dbt project context
        source_name: Name for the source (default: "raw")
        schema_name: Specific schema to scan (None = all schemas in database)
        exclude_schemas: Schemas to exclude from scanning
        exclude_tables: Tables to exclude from generation
        quote_identifiers: Whether to quote identifiers in generated YAML
        output_path: Path where YAML file should be written (default: models/sources/{source_name}.yml)

    Returns:
        SourceGenerationResult with generated YAML and metadata

    Raises:
        Exception: If source generation fails
    """
    from dbt_core_interface.source_generator import (
        SourceGenerationOptions,
        SourceGenerationStrategy,
        SourceGenerator,
        to_yaml,
    )

    logger.info("Generating sources from database introspection...")

    try:
        # Create source generator
        assert context._project is not None, "DbtProjectContext not initialized"
        source_gen = SourceGenerator(project=context._project)

        # Configure generation options
        options = SourceGenerationOptions(
            strategy=SourceGenerationStrategy.SPECIFIC_SCHEMA
            if schema_name
            else SourceGenerationStrategy.ALL_SCHEMAS,
            schema_name=schema_name,
            source_name=source_name,
            include_descriptions=True,
            infer_descriptions=True,
            exclude_schemas=exclude_schemas or [],
            exclude_tables=exclude_tables or [],
            quote_identifiers=quote_identifiers,
        )

        # Generate sources
        source_defs = source_gen.generate_sources(options=options)

        if not source_defs:
            logger.warning("No sources found with given configuration")
            return SourceGenerationResult(
                source_name=source_name,
                table_count=0,
                yaml_content="",
                yaml_path=output_path or Path.cwd() / "models" / "sources" / f"{source_name}.yml",
            )

        # Generate YAML content
        yaml_content = to_yaml(source_defs=source_defs, quote_identifiers=quote_identifiers)

        # Determine output path
        if output_path is None:
            project_root = Path(context.config.project_dir)
            output_path = project_root / "models" / "sources" / f"{source_name}.yml"
        else:
            output_path = Path(output_path)

        total_tables = sum(len(source_def.tables) for source_def in source_defs)

        logger.info(
            ":white_check_mark: Generated source '%s' with %d tables",
            source_name,
            total_tables,
        )

        return SourceGenerationResult(
            source_name=source_name,
            table_count=total_tables,
            yaml_content=yaml_content,
            yaml_path=output_path,
        )

    except Exception as e:
        logger.error("Error generating sources: %s", e)
        raise


def generate_staging_from_source(
    context: DbtProjectContext,
    source_name: str,
    table_name: str,
    staging_path: Path | None = None,
) -> StagingGenerationResult:
    """Generate a staging model from a source table.

    Uses dbt-core-interface's StagingGenerator for deterministic generation.

    Args:
        context: The dbt project context
        source_name: Name of the source (e.g., "raw")
        table_name: Name of the table in the source
        staging_path: Directory where staging models should be written
                     (default: models/staging/)

    Returns:
        StagingGenerationResult with generated SQL and YAML

    Raises:
        Exception: If staging generation fails
    """
    from dbt_core_interface.staging_generator import (
        NamingConvention,
        StagingModelConfig,
        generate_staging_model_from_source,
    )

    logger.info("Generating staging model for %s.%s...", source_name, table_name)

    try:
        # Get source definition from manifest
        source_def = _get_source_definition(context, source_name, table_name)
        if source_def is None:
            raise ValueError(f"Source {source_name}.{table_name} not found in manifest")

        config = StagingModelConfig(
            source_name=source_name,
            table_name=table_name,
            materialization="view",
            naming_convention=NamingConvention.SNAKE_CASE,
            generate_tests=False,  # Don't auto-generate tests
            generate_documentation=True,
        )

        result_dict = generate_staging_model_from_source(
            source=source_def,
            manifest=context.manifest,
            config=config,
        )

        staging_name_value = result_dict.get("staging_name")
        staging_name = (
            staging_name_value
            if isinstance(staging_name_value, str) and staging_name_value
            else f"stg_{table_name}"
        )
        sql_content = result_dict.get("sql")
        yaml_content = result_dict.get("yaml")
        sql_path, yaml_path = _resolve_staging_output_paths(context, staging_name, staging_path)

        logger.info(
            ":white_check_mark: Generated staging model %s (interface-based, no files written yet)",
            staging_name,
        )

        return StagingGenerationResult(
            source_name=f"{source_name}.{table_name}",
            staging_name=staging_name,
            sql_content=sql_content if isinstance(sql_content, str) else "",
            yaml_content=yaml_content if isinstance(yaml_content, str) else "",
            sql_path=sql_path,
            yaml_path=yaml_path,
        )

    except Exception as e:
        logger.error("Error generating staging model: %s", e)
        raise


def check_documentation(
    context: DbtProjectContext,
    model_filter: str | None = None,
    min_model_length: int = 10,
    min_column_length: int = 5,
) -> DocumentationCheckResult:
    """Check documentation completeness across the dbt project.

    This function uses dbt-core-interface's DocumentationChecker to analyze
    model and column documentation, identifying gaps and completeness.

    Args:
        context: The dbt project context
        model_filter: Optional model name filter (None = check all models)
        min_model_length: Minimum length for model descriptions
        min_column_length: Minimum length for column descriptions

    Returns:
        DocumentationCheckResult with coverage statistics and gaps

    Raises:
        Exception: If documentation check fails
    """
    from dbt_core_interface.doc_checker import DocumentationChecker

    logger.info("Checking documentation completeness...")

    try:
        # Create documentation checker
        doc_checker = DocumentationChecker(
            min_model_description_length=min_model_length,
            min_column_description_length=min_column_length,
        )

        assert context._project is not None, "DbtProjectContext not initialized"
        # Run check
        report = doc_checker.check_project(
            manifest=context.manifest,
            project_name=context._project.project_name,
            model_name_filter=model_filter,
        )

        # Convert to simplified result
        result = DocumentationCheckResult(
            total_models=report.total_models,
            models_with_descriptions=report.models_with_descriptions,
            models_without_descriptions=report.models_without_descriptions,
            total_columns=report.total_columns,
            documented_columns=report.documented_columns,
            undocumented_columns=report.undocumented_columns,
            gaps=report.all_gaps,
        )

        coverage_percent = (
            (result.documented_columns / result.total_columns * 100)
            if result.total_columns > 0
            else 0.0
        )

        logger.info(
            ":white_check_mark: Documentation check complete: %.1f%% coverage (%d/%d columns)",
            coverage_percent,
            result.documented_columns,
            result.total_columns,
        )

        return result

    except Exception as e:
        logger.error("Error checking documentation: %s", e)
        raise


def _get_source_definition(
    context: DbtProjectContext,
    source_name: str,
    table_name: str,
) -> SourceDefinition | None:
    """Get a source definition from the manifest.

    Args:
        context: The dbt project context
        source_name: Name of the source
        table_name: Name of the table

    Returns:
        SourceDefinition if found, None otherwise
    """
    manifest = context.manifest

    for source in manifest.sources.values():
        if source.source_name == source_name and source.name == table_name:
            return source

    return None
