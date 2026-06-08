"""Manifest-based catalog reader for dbt-column-lineage.

Reads column definitions from dbt's manifest.json instead of querying the
live database. This eliminates the Snowflake connection from the hot path —
source column lists come from source YMLs (already in the manifest), and
model column lists from their YAML definitions.

Prerequisite: source YMLs must be kept up-to-date via periodic osmosis source
refresh (e.g. osmosis_sources_scoped.sh). When source YMLs are accurate, this
reader is strictly more correct than LiveDbCatalogReader for the osmosis use
case because it reflects the documented contract, not transient DB state.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from dbt_osmosis_cll.cll_generator.models.schema import Model


class ManifestCatalogReader:
    """Catalog reader backed by manifest.json.

    Implements the same ``load`` / ``get_models_nodes`` interface as
    ``CatalogReader`` and ``LiveDbCatalogReader`` so it can be passed as
    ``_catalog_reader_override`` to ``get_column_lineage``.
    """

    def __init__(self, manifest_path: str) -> None:
        self.manifest_path = Path(manifest_path)
        self.manifest: Dict[str, Any] = {}

    def load(self) -> None:
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {self.manifest_path}")
        with open(self.manifest_path, "r", encoding="utf-8") as f:
            self.manifest = json.load(f)

    def get_models_nodes(self) -> Dict[str, Model]:
        models: Dict[str, Model] = {}

        for node_id, node_data in self.manifest.get("nodes", {}).items():
            resource_type = node_id.split(".")[0]
            if resource_type not in ("model", "seed"):
                continue
            model_name = (node_data.get("name") or node_id.split(".")[-1]).lower()
            columns: Dict[str, Any] = {}
            for col_name, col_data in node_data.get("columns", {}).items():
                ncol = col_name.lower()
                columns[ncol] = {
                    "name": ncol,
                    "model_name": model_name,
                    "description": col_data.get("description"),
                    "data_type": col_data.get("data_type") or col_data.get("type"),
                    "lineage": [],
                }
            models[model_name] = Model(
                **{
                    "name": model_name,
                    "schema": node_data.get("schema") or "main",
                    "database": node_data.get("database") or "main",
                    "columns": columns,
                    "resource_type": resource_type,
                }
            )

        for source_id, source_data in self.manifest.get("sources", {}).items():
            # identifier = actual DB table name (what CLL uses for lookup)
            source_identifier = (
                source_data.get("identifier")
                or source_data.get("name")
                or source_id.split(".")[-1]
            ).lower()
            table_name = (source_data.get("name") or source_id.split(".")[-1]).lower()
            source_name = (source_data.get("source_name") or "").lower()

            columns = {}
            for col_name, col_data in source_data.get("columns", {}).items():
                ncol = col_name.lower()
                columns[ncol] = {
                    "name": ncol,
                    "model_name": source_identifier,
                    "description": col_data.get("description"),
                    "data_type": col_data.get("data_type") or col_data.get("type"),
                    "lineage": [],
                }
            model = Model(
                **{
                    "name": table_name,
                    "schema": source_data.get("schema") or "main",
                    "database": source_data.get("database") or "main",
                    "columns": columns,
                    "resource_type": "source",
                    "source_identifier": source_identifier or None,
                    "source_name": source_name or None,
                }
            )
            models[source_identifier] = model
            for col in model.columns.values():
                col.model_name = source_identifier

        return models
