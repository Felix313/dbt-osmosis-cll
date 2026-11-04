"""Column lists from several sources, in priority order.

Lineage quality depends on knowing every relation's columns: ``SELECT *`` cannot be
expanded and unqualified references in joins cannot be attributed without them. The
manifest only knows the columns documented in YAML — complete in some projects,
absent in others (a source declared with just its table name).

:class:`LayeredCatalogReader` gives every relation the columns of the first configured
source that knows it, in that source's order:

``live``      ``target/cll-columns.json`` written by ``compile-cll``; without that file
              the warehouse, queried only for relations still without a warehouse answer
              (and, when *live_relations* is given, only for those) — needs a profile
``catalog``   ``catalog.json`` from ``dbt docs generate``
``manifest``  YAML columns: documentation, possibly incomplete. The last resort unless
              configured earlier

Which source answered for which relation is kept in :attr:`column_origin`, so a run
can report the relations that stayed without columns.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from dbt_osmosis_cll.cll_generator.artifacts.column_snapshot import snapshot_path
from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
from dbt_osmosis_cll.cll_generator.models.schema import Column, Model

logger = logging.getLogger(__name__)

VALID_SOURCES = ("manifest", "catalog", "live")


def _lookup_name(model: Model) -> str:
    return ((model.source_identifier or model.alias or model.name) or "").lower()


class LayeredCatalogReader:
    """Catalog reader combining manifest, catalog.json and (opt-in) live columns."""

    def __init__(
        self,
        manifest_path: str,
        sources: Iterable[str] = ("manifest",),
        catalog_path: str | None = None,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        target: str | None = None,
        live_relations: set[str] | None = None,
    ) -> None:
        self._sources = [s.lower() for s in sources if s]
        unknown = set(self._sources) - set(VALID_SOURCES)
        if unknown:
            raise ValueError(
                f"Unknown column source(s): {', '.join(sorted(unknown))} "
                f"(valid: {', '.join(VALID_SOURCES)})"
            )
        if "manifest" not in self._sources:
            self._sources.append("manifest")
        self._manifest_reader = ManifestCatalogReader(manifest_path)
        self._manifest_path = manifest_path
        self._catalog_path = catalog_path
        self._project_dir = project_dir
        self._profiles_dir = profiles_dir
        self._target = target
        self._live_relations = {r.lower() for r in live_relations} if live_relations else None
        self._catalog_models: dict[str, Model] = {}
        self._snapshot_models: dict[str, Model] = {}
        self.column_origin: dict[str, str | None] = {}
        """unique_id → ``manifest`` / ``catalog`` / ``live`` / None (no columns)."""

    @property
    def manifest(self) -> dict[str, Any]:
        return self._manifest_reader.manifest

    def load(self) -> None:
        self._manifest_reader.load()
        if "catalog" in self._sources:
            path = Path(self._catalog_path) if self._catalog_path else None
            if path is None or not path.exists():
                logger.warning(
                    "Column source 'catalog' requested but no catalog at %s — run "
                    "'dbt docs generate' or point cll-catalog-path at it.",
                    path,
                )
            else:
                from dbt_osmosis_cll.cll_generator.artifacts.catalog import CatalogReader

                reader = CatalogReader(str(path))
                reader.load()
                self._catalog_models = reader.get_models_nodes()
        if "live" in self._sources:
            snapshot = snapshot_path(Path(self._project_dir or "."))
            if snapshot.exists():
                from dbt_osmosis_cll.cll_generator.artifacts.catalog import CatalogReader

                reader = CatalogReader(str(snapshot))
                reader.load()
                self._snapshot_models = reader.get_models_nodes()
                logger.info("Column source 'live': using %s", snapshot)

    def get_models_nodes(self) -> dict[str, Model]:
        models = self._manifest_reader.get_models_nodes()
        by_source = {"catalog": self._catalog_models, "live": self._snapshot_models}
        for uid, model in models.items():
            documented = bool(model.columns)
            origin = None
            for source in self._sources:
                if source == "manifest":
                    if documented:
                        origin = "manifest"
                        break
                    continue
                found = by_source[source].get(uid)
                if found is not None and found.columns:
                    self._replace(model, {c: col.data_type for c, col in found.columns.items()})
                    origin = source
                    break
            self.column_origin[uid] = origin

        if "live" in self._sources and not self._snapshot_models:
            missing = [
                (uid, m)
                for uid, m in sorted(models.items())
                if self.column_origin.get(uid) in (None, "manifest")
                and (self._live_relations is None or uid.lower() in self._live_relations)
            ]
            if missing:
                self._fill_live(missing)
        return models

    def _replace(self, model: Model, columns: dict[str, str | None]) -> None:
        """Make *columns* the model's columns, in their order; YAML metadata stays on match."""
        owner = _lookup_name(model)
        existing = model.columns
        model.columns = {}
        for name, dtype in columns.items():
            key = name.lower()
            column = existing.get(key) or Column(name=key, model_name=owner)
            if dtype and not column.data_type:
                column.data_type = dtype
            model.columns[key] = column

    def _fill(self, model: Model, columns: dict[str, str | None]) -> None:
        owner = _lookup_name(model)
        for name, dtype in sorted(columns.items()):
            key = name.lower()
            model.columns[key] = Column(name=key, model_name=owner, data_type=dtype)

    def _fill_live(self, missing: list) -> None:
        from dbt_osmosis_cll.cll_generator.artifacts.live_db import LiveDbCatalogReader

        live = LiveDbCatalogReader(
            manifest_path=self._manifest_path,
            project_dir=self._project_dir,
            profiles_dir=self._profiles_dir,
            target=self._target,
        )
        try:
            live.load()
        except Exception as exc:  # noqa: BLE001 — live is best effort, report and continue
            logger.warning("Column source 'live' unavailable: %s", exc)
            return
        logger.info("Querying the warehouse for columns of %d relation(s)…", len(missing))
        for uid, model in missing:
            identifier = model.source_identifier or model.alias or model.name
            cols = live._get_live_columns(model.database, model.schema_name, identifier)
            if cols:
                self._fill(model, cols)
                self.column_origin[uid] = "live"
