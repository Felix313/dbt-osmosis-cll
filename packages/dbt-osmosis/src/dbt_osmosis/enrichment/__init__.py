"""
dbt-osmosis enrichment API.

Provides a plugin-style interface for enriching dbt YAML column descriptions
from any external metadata source (data dictionaries, BI layers, SAP tables,
Confluence, etc.).

Quickstart::

    from dbt_osmosis.enrichment import DescriptionFetcher, enrich_yaml_files
    import re

    class MyFetcher(DescriptionFetcher):
        def fetch(self, column_names: list[str]) -> dict[str, str]:
            # query your data source — return {COL_NAME_UPPER: description}
            return my_metadata_db.lookup(column_names)

    enrich_yaml_files(
        yml_paths=[Path("models/staging/my_model.yml")],
        fetcher=MyFetcher(),
        # Mark enriched columns as anchored so osmosis propagation won't overwrite
        # them. desc-owner is the unified ownership key: any value other than
        # "upstream" anchors the description at this model.
        anchor_meta_key="desc-owner",
        # Optional: regex matching osmosis-propagated descriptions that are
        # safe to replace (fullmatch against existing description text).
        replaceable_pattern=re.compile(r"See parent:.*", re.DOTALL),
        dry_run=False,
        verbose=True,
    )
"""

from ._engine import enrich_yaml_files
from ._merge import DescriptionFetcher, merge_description
from ._yaml import render_model_yml

__all__ = [
    "DescriptionFetcher",
    "enrich_yaml_files",
    "merge_description",
    "render_model_yml",
]
