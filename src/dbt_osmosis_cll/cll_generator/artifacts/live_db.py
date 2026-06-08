from __future__ import annotations

import logging
import os
from argparse import Namespace
from typing import Any, Dict, Optional

from dbt_osmosis_cll.cll_generator.models.schema import Column, Model

logger = logging.getLogger(__name__)


def _bootstrap_dbt_adapter(
    project_dir: str,
    profiles_dir: str,
    target: Optional[str],
) -> Any:
    """Return a live dbt adapter instance for column introspection.

    Compatible with dbt-core 1.8–1.10. Wraps version-specific bootstrap
    differences and raises RuntimeError with a helpful message on failure.
    """
    try:
        from dbt.adapters.factory import get_adapter, register_adapter
        from dbt.config.project import Project
        from dbt.config.profile import Profile
        from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
        from dbt.config.runtime import RuntimeConfig
    except ImportError as exc:
        raise ImportError(
            "dbt-core is required for --live-db. "
            "Install the appropriate dbt adapter (e.g. dbt-snowflake)."
        ) from exc

    args = Namespace(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        target=target,
        profile=None,
        vars={},
        cli_vars={},
        threads=1,
        which="run",
        quiet=True,
        no_print=True,
        single_threaded=True,
    )

    try:
        import yaml
        from pathlib import Path

        profiles_path = Path(profiles_dir) / "profiles.yml"
        raw_profiles: Dict[str, Any] = yaml.safe_load(profiles_path.read_text()) or {}

        # Derive profile name from dbt_project.yml when not overridden
        dbt_project_path = Path(project_dir) / "dbt_project.yml"
        dbt_project_raw: Dict[str, Any] = yaml.safe_load(dbt_project_path.read_text()) or {}
        profile_name: str = dbt_project_raw.get("profile", next(iter(raw_profiles)))

        # dbt-core ≥1.9 requires the invocation context and global flags to be
        # initialised before any Jinja rendering (env_var calls in packages.yml etc).
        try:
            import os as _os
            from dbt_common.context import set_invocation_context
            set_invocation_context(_os.environ)
        except Exception:
            pass
        try:
            import dbt.flags as _dbt_flags
            if hasattr(_dbt_flags, "set_from_args"):
                _dbt_flags.set_from_args(args, None)
        except Exception:
            pass

        profile_renderer = ProfileRenderer({})

        # dbt-core ≥1.9 removed Profile.render_from_args; use from_raw_profiles instead.
        if hasattr(Profile, "from_raw_profiles"):
            profile = Profile.from_raw_profiles(
                raw_profiles=raw_profiles,
                profile_name=profile_name,
                renderer=profile_renderer,
                target_override=target,
            )
        else:
            # Fallback for dbt-core <1.9
            profile = Profile.render_from_args(args, profile_renderer)  # type: ignore[attr-defined]

        resolved_target = target or profile.target_name

        project_renderer = DbtProjectYamlRenderer({}, resolved_target)
        project = Project.from_project_root(project_dir, project_renderer)

        config = RuntimeConfig.from_parts(project=project, profile=profile, args=args)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load dbt project/profile from '{project_dir}' / '{profiles_dir}': {exc}"
        ) from exc

    try:
        try:
            from dbt.mp_context import get_mp_context

            register_adapter(config, get_mp_context())
        except (ImportError, TypeError):
            register_adapter(config)  # older dbt-core compat

        return get_adapter(config)
    except Exception as exc:
        raise RuntimeError(f"Failed to register dbt adapter: {exc}") from exc


class LiveDbCatalogReader:
    """Resolve column schemas by querying the live database via the dbt adapter.

    Presents the same ``get_models_nodes()`` interface as ``CatalogReader`` so
    ``ModelRegistry`` can accept either without change.

    Results are cached per relation to avoid repeated warehouse round-trips.
    """

    def __init__(
        self,
        manifest_path: str,
        project_dir: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
    ) -> None:
        import json
        from pathlib import Path

        self._project_dir = str(project_dir or ".")
        self._profiles_dir = str(profiles_dir or ".")
        self._target = target
        self._column_cache: Dict[str, Dict[str, str]] = {}
        self._adapter: Any = None
        self._original_cwd = os.getcwd()

        with open(manifest_path) as f:
            self._manifest: Dict[str, Any] = json.load(f)

    # ------------------------------------------------------------------
    # Public interface (mirrors CatalogReader)
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Eagerly bootstrap the adapter. No-op if already loaded."""
        if self._adapter is None:
            self._adapter = _bootstrap_dbt_adapter(
                self._project_dir,
                self._profiles_dir,
                self._target,
            )

    def get_models_nodes(self) -> Dict[str, Model]:
        """Build a model dict from manifest metadata + live column types."""
        if self._adapter is None:
            self.load()

        models: Dict[str, Model] = {}

        for node_id, node in self._manifest.get("nodes", {}).items():
            resource_type = node_id.split(".")[0]
            if resource_type not in ("model", "seed"):
                continue
            if node.get("language") == "python":
                continue

            model_name = (node.get("name") or node_id.split(".")[-1]).lower()
            database = (node.get("database") or "").lower()
            schema = (node.get("schema") or "").lower()

            column_types = self._get_live_columns(database, schema, model_name)

            columns: Dict[str, Column] = {}
            for col_name, col_type in column_types.items():
                columns[col_name] = Column(
                    name=col_name,
                    model_name=model_name,
                    data_type=col_type,
                )

            # Fall back to manifest column list when DB returns nothing (model not materialized)
            if not columns:
                for col_name in node.get("columns", {}):
                    normalized = col_name.lower()
                    columns[normalized] = Column(name=normalized, model_name=model_name)

            models[model_name] = Model(
                name=model_name,
                schema=schema or "main",
                database=database or "main",
                columns=columns,
                resource_type=resource_type,
            )

        for source_id, source in self._manifest.get("sources", {}).items():
            source_identifier = (source.get("identifier") or source.get("name") or "").lower()
            database = (source.get("database") or "").lower()
            schema = (source.get("schema") or "").lower()
            source_name_val = source.get("source_name", "").lower()

            column_types = self._get_live_columns(database, schema, source_identifier)

            columns = {}
            for col_name, col_type in column_types.items():
                columns[col_name] = Column(
                    name=col_name,
                    model_name=source_identifier,
                    data_type=col_type,
                )

            if not columns:
                for col_name in source.get("columns", {}):
                    normalized = col_name.lower()
                    columns[normalized] = Column(name=normalized, model_name=source_identifier)

            model = Model(
                name=source_identifier,
                schema=schema or "main",
                database=database or "main",
                columns=columns,
                resource_type="source",
                source_identifier=source_identifier,
                source_name=source_name_val or None,
            )
            key = source_identifier or model.name
            models[key] = model
            for col in model.columns.values():
                col.model_name = key

        return models

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_live_columns(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        cache_key = f"{database}.{schema}.{identifier}".lower()
        if cache_key in self._column_cache:
            return self._column_cache[cache_key]

        result: Dict[str, str] = {}
        try:
            relation = self._adapter.Relation.create(
                database=database or None,
                schema=schema or None,
                identifier=identifier,
            )
            with self._adapter.connection_named("live_db_catalog"):
                columns = self._adapter.get_columns_in_relation(relation)
            result = {col.column.lower(): col.dtype for col in columns}
        except Exception as exc:
            logger.debug(
                "Could not retrieve live columns for %s.%s.%s: %s",
                database,
                schema,
                identifier,
                exc,
            )

        self._column_cache[cache_key] = result
        return result
