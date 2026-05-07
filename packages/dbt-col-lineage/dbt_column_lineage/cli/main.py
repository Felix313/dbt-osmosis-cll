import sys
from pathlib import Path
import click
import logging
from typing import Optional

from dbt_column_lineage.lineage.display import TextDisplay, DotDisplay
from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.lineage.service import LineageService, LineageSelector
from dbt_column_lineage.lineage.display.base import LineageStaticDisplay


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

@click.command()
@click.option(
    '--select',
    help="Select models/columns to generate lineage for. Format: [+]model_name[.column_name][+]\n"
         "Examples:\n"
         "  stg_accounts.account_id+  (downstream lineage)\n"
         "  +stg_accounts.account_id  (upstream lineage)\n"
         "  stg_accounts.account_id   (both directions)"
)
@click.option(
    '--explore',
    is_flag=True,
    help="Start an interactive HTML server for exploring model and column lineage"
)
@click.option(
    '--catalog',
    type=click.Path(),
    default=None,
    help="Path to the dbt catalog file (mutually exclusive with --live-db)"
)
@click.option(
    '--manifest',
    type=click.Path(exists=True),
    default="target/manifest.json",
    help="Path to the dbt manifest file"
)
@click.option(
    '--live-db',
    is_flag=True,
    default=False,
    help="Query the live database for column schemas instead of --catalog (requires --profiles-dir)"
)
@click.option(
    '--profiles-dir',
    default=".",
    help="Directory containing profiles.yml (used with --live-db)"
)
@click.option(
    '--project-dir',
    default=".",
    help="dbt project root directory (used with --live-db)"
)
@click.option(
    '--target',
    default=None,
    help="dbt target profile name (used with --live-db)"
)
@click.option('--format', '-f',
              type=click.Choice(['text', 'dot']),
              default='text',
              help='Output format (text or dot graph)')
@click.option('--output', '-o', default='lineage',
              help='Output file name for dot format (without extension)')
@click.option('--port', '-p',
              default=8000,
              help='Port to run the HTML server (only used with --explore)')
@click.option('--adapter',
              help='Override sqlglot dialect (e.g., tsql, snowflake, bigquery). If set, ignores adapter from manifest.')
def cli(
    select: str,
    explore: bool,
    catalog: Optional[str],
    manifest: str,
    live_db: bool,
    profiles_dir: str,
    project_dir: str,
    target: Optional[str],
    format: str,
    output: str,
    port: int,
    adapter: Optional[str],
) -> None:
    """DBT Column Lineage - Generate column-level lineage for DBT models."""
    if not select and not explore:
        click.echo("Error: Either --select or --explore must be specified", err=True)
        sys.exit(1)

    if select and explore:
        click.echo("Error: Cannot use both --select and --explore at the same time", err=True)
        sys.exit(1)

    if live_db and catalog:
        click.echo("Error: --live-db and --catalog are mutually exclusive", err=True)
        sys.exit(1)

    if not live_db and not catalog:
        # Fall back to default catalog path when neither flag is given
        catalog = "target/catalog.json"

    try:
        if live_db:
            from dbt_column_lineage.artifacts.live_db import LiveDbCatalogReader
            from dbt_column_lineage.artifacts.registry import ModelRegistry

            catalog_reader = LiveDbCatalogReader(
                manifest_path=manifest,
                project_dir=project_dir,
                profiles_dir=profiles_dir,
                target=target,
            )
            registry_obj = ModelRegistry(
                catalog_path=None,
                manifest_path=manifest,
                adapter_override=adapter,
                _catalog_reader_override=catalog_reader,
            )
            registry_obj.load()

            class _ServiceShim:
                """Minimal shim so the rest of the CLI works unchanged."""
                def __init__(self, reg):
                    self.registry = reg

                def get_model_info(self, selector):
                    model = self.registry.get_model(selector.model)
                    return {
                        "name": model.name,
                        "schema": model.schema_name,
                        "database": model.database,
                        "columns": list(model.columns.keys()),
                        "upstream": list(model.upstream) if selector.upstream else [],
                        "downstream": list(model.downstream) if selector.downstream else [],
                    }

                def _get_upstream_lineage(self, model, column):
                    from dbt_column_lineage.lineage.service import LineageService
                    raise NotImplementedError("Recursive upstream not available in shim")

                def _get_downstream_lineage(self, model, column):
                    raise NotImplementedError("Recursive downstream not available in shim")

            service = _ServiceShim(registry_obj)
        else:
            service = LineageService(Path(catalog), Path(manifest), adapter=adapter)
        
        if explore:
            click.echo(f"Starting explore mode server on port {port}...")
            lineage_explorer = LineageExplorer(port=port)
            lineage_explorer.set_lineage_service(service)
            lineage_explorer.start()
            return
            
        selector = LineageSelector.from_string(select)
        model = service.registry.get_model(selector.model)
    
        if selector.column:
            if selector.column in model.columns:
                column = model.columns[selector.column]
                
                display: LineageStaticDisplay
                if format == 'dot':
                    display = DotDisplay(output, registry=service.registry)
                    display.main_model = selector.model
                    display.main_column = selector.column
                else:
                    display = TextDisplay()

                display.display_column_info(column)

                if selector.upstream:
                    upstream_refs = service._get_upstream_lineage(selector.model, selector.column)
                    display.display_upstream(upstream_refs)

                if selector.downstream:
                    downstream_refs = service._get_downstream_lineage(selector.model, selector.column)
                    display.display_downstream(downstream_refs)

                if format == 'dot':
                    display.save()
            else:
                available_columns = ", ".join(model.columns.keys())
                click.echo(f"Error: Column '{selector.column}' not found in model '{selector.model}'", err=True)
                sys.exit(1)
        else:
            model_info = service.get_model_info(selector)
            click.echo(f"\nModel: {model_info['name']}")
            click.echo(f"Schema: {model_info['schema']}")
            click.echo(f"Database: {model_info['database']}")
            click.echo(f"Columns: {', '.join(model_info['columns'])}")
            
            if model_info['upstream']:
                click.echo("\nUpstream dependencies:")
                for upstream in model_info['upstream']:
                    click.echo(f"  {upstream}")
                
            if model_info['downstream']:
                click.echo("\nDownstream dependencies:")
                for downstream in model_info['downstream']:
                    click.echo(f"  {downstream}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

def main() -> None:
    cli()

if __name__ == "__main__":
    main()