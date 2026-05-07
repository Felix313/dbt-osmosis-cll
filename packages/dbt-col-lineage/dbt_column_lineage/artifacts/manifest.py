import json
from typing import Dict, Optional, Set, Any
from pathlib import Path

from dbt_column_lineage.artifacts.adapter_mapping import normalize_adapter


class ManifestReader:
    def __init__(self, manifest_path: Optional[str] = None):
        self.manifest_path = Path(manifest_path) if manifest_path else None
        self.manifest: Dict[str, Any] = {}

    def load(self) -> None:
        if not self.manifest_path or not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")
        with open(self.manifest_path, "r") as f:
            self.manifest = json.load(f)

    def get_adapter(self) -> Optional[str]:
        adapter_name = self.manifest.get("metadata", {}).get("adapter_type")
        return normalize_adapter(adapter_name)

    def _find_node(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Find a node in the manifest by model name."""
        if not self.manifest:
            return None
        model_name_lower = model_name.lower()
        for _, node in self.manifest.get("nodes", {}).items():
            if node.get("name", "").lower() == model_name_lower:
                return dict(node)
        return None

    def get_model_dependencies(self) -> Dict[str, Set[str]]:
        """Return a dictionary of model dependencies with full model names.

        Returns:
            Dict[str, Set[str]]: Key is full model name, value is set of full dependency names
        """
        dependencies = {}
        for model_id, model_data in self.manifest.get("nodes", {}).items():
            depends_on = set(
                f"{dep['alias']}.{dep['alias']}"
                for dep in model_data.get("depends_on", {}).get("nodes", [])
            )
            dependencies[model_id] = depends_on
        return dependencies

    def get_model_upstream(self) -> Dict[str, Set[str]]:
        """Get upstream dependencies for each model."""
        upstream: Dict[str, Set[str]] = {}

        for _, node in self.manifest.get("nodes", {}).items():
            resource_type = node.get("resource_type")
            if resource_type in ("model", "snapshot"):
                model_name = node.get("name")
                if not model_name:
                    continue

                model_name = model_name.lower()
                upstream[model_name] = set()

                depends_on = node.get("depends_on", {})
                for dep_id in depends_on.get("nodes", []):
                    parts = dep_id.split(".")
                    if parts[0] == "model":
                        dep_name = parts[-1].lower()
                        upstream[model_name].add(dep_name)
                    elif parts[0] == "source":
                        source_node = self.manifest.get("sources", {}).get(dep_id, {})
                        source_identifier = source_node.get("identifier")
                        if source_identifier:
                            upstream[model_name].add(source_identifier.lower())
                        else:
                            # Fallback to source name if identifier not found
                            source_name = parts[-1].lower()
                            upstream[model_name].add(source_name)
                    elif parts[0] == "snapshot":
                        dep_name = parts[-1].lower()
                        upstream[model_name].add(dep_name)

        return upstream

    def get_model_downstream(self) -> Dict[str, Set[str]]:
        """Return a dictionary of model downstream dependencies."""
        downstream: Dict[str, Set[str]] = {}

        upstream_deps = self.get_model_upstream()

        for model_name, upstream_models in upstream_deps.items():
            for upstream_model in upstream_models:
                if upstream_model not in downstream:
                    downstream[upstream_model] = set()
                downstream[upstream_model].add(model_name)

        return downstream

    def get_compiled_sql(self, model_name: str) -> Optional[str]:
        """Return compiled SQL embedded in the manifest node, or None.

        ``dbt compile`` and ``dbt run`` embed compiled SQL directly in every
        manifest node (``compiled_code`` for dbt ≥ 1.3, ``compiled_sql`` for
        older versions).  ``dbt parse`` does **not** — this method returns
        ``None`` in that case.  Callers that need a fallback should call
        :meth:`get_compiled_sql_from_disk` explicitly.
        """
        node = self._find_node(model_name)
        if not node:
            return None
        return node.get("compiled_sql") or node.get("compiled_code") or None

    def get_compiled_sql_from_disk(self, model_name: str) -> Optional[str]:
        """Return compiled SQL from ``target/compiled/`` on disk, or None.

        This is an *explicit* fallback for when the manifest has no inline
        compiled SQL (i.e. the manifest was produced by ``dbt parse``).

        .. warning::
            The files under ``target/compiled/`` may be **stale** — they
            reflect the last ``dbt compile`` run, which may pre-date recent
            model changes.  Use this method only when you have verified that
            the compiled files are up-to-date, or when approximate lineage is
            acceptable.
        """
        node = self._find_node(model_name)
        if not node:
            return None
        compiled_path = self._compiled_sql_path(node)
        if compiled_path and compiled_path.exists():
            try:
                return compiled_path.read_text(encoding="utf-8")
            except OSError:
                return None
        return None

    def has_inline_compiled_sql(self) -> bool:
        """Return True if at least one model node has inline compiled SQL.

        A quick pre-flight check: if the manifest was produced by ``dbt parse``
        none of the nodes will have ``compiled_code``.
        """
        for node in self.manifest.get("nodes", {}).values():
            if node.get("resource_type") == "model":
                if node.get("compiled_sql") or node.get("compiled_code"):
                    return True
        return False

    def _compiled_sql_path(self, node: Dict[str, Any]) -> Optional[Path]:
        """Construct the on-disk path for a compiled SQL file.

        Pattern: <target_dir>/compiled/<package_name>/<original_file_path>
        where <target_dir> is the directory that contains manifest.json.
        """
        if not self.manifest_path:
            return None

        original_file_path = node.get("original_file_path")
        unique_id = node.get("unique_id", "")
        parts = unique_id.split(".")
        if not original_file_path or len(parts) < 2:
            return None

        package_name = parts[1]
        target_dir = self.manifest_path.parent  # e.g. .../target/
        return target_dir / "compiled" / package_name / Path(original_file_path)

    def get_model_path(self, model_name: str) -> Optional[str]:
        """Get the path to the model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None

        return node.get("path")

    def get_model_language(self, model_name: str) -> Optional[str]:
        """Get the language of a model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None
        return node.get("language")

    def get_model_resource_path(self, model_name: str) -> Optional[str]:
        """Get the original file path of a model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None
        return node.get("original_file_path")

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        node = self.manifest.get("nodes", {}).get(node_id)
        if node is None:
            return None
        return dict(node)

    def get_exposures(self) -> Dict[str, Dict[str, Any]]:
        """Get all exposures from the manifest.

        Returns:
            Dict[str, Dict[str, Any]]: Key is exposure unique_id, value is exposure data
        """
        return self.manifest.get("exposures", {})

    def get_exposure_dependencies(self) -> Dict[str, Set[str]]:
        """Get model dependencies for each exposure.

        Returns:
            Dict[str, Set[str]]: Key is exposure name, value is set of model names it depends on
        """
        exposure_deps: Dict[str, Set[str]] = {}

        for exposure_id, exposure_data in self.manifest.get("exposures", {}).items():
            exposure_name = exposure_data.get("name")
            if not exposure_name:
                continue

            exposure_deps[exposure_name] = set()

            depends_on = exposure_data.get("depends_on", {})
            for dep_id in depends_on.get("nodes", []):
                parts = dep_id.split(".")
                if parts[0] == "model":
                    dep_name = parts[-1].lower()
                    exposure_deps[exposure_name].add(dep_name)
                elif parts[0] == "source":
                    source_node = self.manifest.get("sources", {}).get(dep_id, {})
                    source_identifier = source_node.get("identifier")
                    if source_identifier:
                        exposure_deps[exposure_name].add(source_identifier.lower())
                    else:
                        source_name = parts[-1].lower()
                        exposure_deps[exposure_name].add(source_name)
                elif parts[0] == "snapshot":
                    dep_name = parts[-1].lower()
                    exposure_deps[exposure_name].add(dep_name)

        return exposure_deps

    def get_model_exposures(self) -> Dict[str, Set[str]]:
        """Get exposures that depend on each model.

        Returns:
            Dict[str, Set[str]]: Key is model name, value is set of exposure names that depend on it
        """
        model_exposures: Dict[str, Set[str]] = {}

        exposure_deps = self.get_exposure_dependencies()

        for exposure_name, model_names in exposure_deps.items():
            for model_name in model_names:
                if model_name not in model_exposures:
                    model_exposures[model_name] = set()
                model_exposures[model_name].add(exposure_name)

        return model_exposures
