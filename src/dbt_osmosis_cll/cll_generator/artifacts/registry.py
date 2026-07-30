from typing import Dict, Iterable, List, Mapping, Optional, Set
from dataclasses import dataclass, field
import logging
import re
import threading
import time

from sqlglot import exp, parse_one


def _extract_source_select(sql: str, dialect: Optional[str] = None) -> str:
    """For DML statements (MERGE, INSERT INTO ... SELECT), extract just the source SELECT.

    dbt incremental models compile {{ this }} into the DML target — the model's own
    table. The parser should never see that self-reference; only the source SELECT
    (MERGE USING clause / INSERT body) carries actual column lineage.
    Returns the original SQL unchanged for plain SELECT statements.
    """
    try:
        parsed = parse_one(sql, dialect=dialect)
    except Exception:
        return sql

    if isinstance(parsed, exp.Merge):
        using = parsed.args.get("using")
        if using is not None:
            select = using.find(exp.Select)
            if select is not None:
                return select.sql(dialect=dialect)

    if isinstance(parsed, exp.Insert):
        select = parsed.find(exp.Select)
        if select is not None:
            return select.sql(dialect=dialect)

    return sql


def _strip_self_referencing_union_branches(
    sql: str, model_name: str, dialect: str | None = None
) -> str:
    """Remove set-operation branches that read from the model's OWN relation.

    dbt incremental SCD2 / attribute-history models often accumulate state with
    ``SELECT ... FROM {{ this }} UNION ALL SELECT ... FROM <new source>``. In the compiled
    SQL ``{{ this }}`` is the model's own relation, so column lineage classifies the column
    as a multi-source UNION with no single origin — even though the only *real* upstream is
    the new-data branch (the ``{{ this }}`` branch just carries the accumulated history of
    the same value). Dropping the self-referencing branch(es) lets the column resolve to its
    true origin instead of stopping at the model itself.

    A branch is "self" when its primary FROM resolves — directly, or through a CTE whose
    primary FROM does — to a relation whose identifier matches *model_name*. Self-references
    inside scalar / WHERE subqueries (e.g. an incremental high-watermark
    ``SELECT MAX(...) FROM {{ this }}``) are not branches and are left untouched. Branches
    are removed only when at least one non-self branch remains. Any parse/transform failure
    returns the SQL unchanged (best-effort preprocessing).

    Relation matching is by name, consistent with the rest of the resolver (which keys
    models by name, not by a custom dbt ``alias``).
    """
    self_name = model_name.lower()
    try:
        tree = parse_one(sql, dialect=dialect)
    except Exception:  # noqa: BLE001 — best-effort preprocessing; never block parsing
        return sql

    def _primary_from(select: exp.Select) -> str | None:
        frm = select.args.get("from") or select.args.get("from_")
        if frm is None:
            return None
        table = frm.this
        return table.name.lower() if isinstance(table, exp.Table) else None

    # CTEs whose primary FROM is the self relation — transitively (a CTE reading from
    # another self-CTE is itself self).
    ctes = {c.alias_or_name.lower(): c for c in tree.find_all(exp.CTE)}
    self_ctes: set[str] = set()
    changed = True
    while changed:
        changed = False
        for name, cte in ctes.items():
            if name in self_ctes:
                continue
            body = cte.this
            if isinstance(body, exp.Select):
                pf = _primary_from(body)
                if pf is not None and (pf == self_name or pf in self_ctes):
                    self_ctes.add(name)
                    changed = True

    def _is_self_branch(node: exp.Expression) -> bool:
        if not isinstance(node, exp.Select):
            return False
        pf = _primary_from(node)
        return pf is not None and (pf == self_name or pf in self_ctes)

    def _flatten(union: exp.Union) -> list:
        branches: list = []
        for side in (union.this, union.expression):
            # Recurse only into pure UNION / UNION ALL — never EXCEPT / INTERSECT, whose
            # branches are not interchangeable and must not be dropped.
            if type(side) is exp.Union:
                branches.extend(_flatten(side))
            else:
                branches.append(side)
        return branches

    modified = False
    for union in [
        u
        for u in tree.find_all(exp.Union)
        if type(u) is exp.Union and not isinstance(u.parent, exp.Union)
    ]:
        branches = _flatten(union)
        kept = [b for b in branches if not _is_self_branch(b)]
        if kept and len(kept) < len(branches):
            new_node: exp.Expression = kept[0]
            for branch in kept[1:]:
                new_node = exp.Union(
                    this=new_node, expression=branch, distinct=union.args.get("distinct", False)
                )
            if union is tree:  # root-level set operation has no parent to swap into
                tree = new_node
            else:
                union.replace(new_node)
            modified = True

    if not modified:
        return sql
    try:
        return tree.sql(dialect=dialect)
    except Exception:  # noqa: BLE001
        return sql


def _replace_placeholder(match: re.Match) -> str:
    """Return TRUE for custom placeholders; leave dbt-internal tokens untouched.

    dbt always prefixes its internal CTE references with ``__dbt__``, so any
    match starting with that sequence is passed through unchanged — this exclusion
    is enforced at the tool level regardless of what pattern the user configured.
    """
    return match.group(0) if match.group(0).startswith("__dbt__") else "TRUE"


from dbt_osmosis_cll.cll_generator.artifacts.catalog import CatalogReader  # noqa: E402
from dbt_osmosis_cll.cll_generator.artifacts.manifest import ManifestReader  # noqa: E402
from dbt_osmosis_cll.cll_generator.models.schema import (  # noqa: E402
    Column,
    Model,
    SQLParseResult,
    ColumnLineage,
    Exposure,
)
from dbt_osmosis_cll.cll_generator.artifacts.exceptions import (  # noqa: E402
    ModelNotFoundError,
    RegistryNotLoadedError,
    RegistryError,
)
from dbt_osmosis_cll.cll_generator.parser import SQLColumnParser  # noqa: E402

logger = logging.getLogger(__name__)


@dataclass
class RegistryState:
    """Immutable state of the registry.

    ``models`` is keyed by manifest ``unique_id`` so name collisions
    (cross-package models, model vs source identifier) never drop nodes.
    ``name_alias`` maps the SQL relation lookup name (lowercase model name, or
    source identifier for sources) to the winning unique_id; collisions resolve
    last-wins in reader insertion order — nodes first, then sources — which is
    exactly the overwrite order of the old name-keyed dict.
    """

    models: Dict[str, Model]
    exposures: Dict[str, Exposure]
    is_loaded: bool = False
    name_alias: Dict[str, str] = field(default_factory=dict)


class ModelRegistry:
    def __init__(
        self,
        catalog_path: Optional[str],
        manifest_path: str,
        adapter_override: Optional[str] = None,
        _catalog_reader_override: Optional[CatalogReader] = None,
        use_target_dir_fallback: bool = False,
        stop_at_ephemeral: bool = False,
        placeholder_patterns: Optional[list] = None,
        lazy_lineage: bool = False,
    ):
        if _catalog_reader_override is not None:
            self._catalog_reader = _catalog_reader_override
        else:
            self._catalog_reader = CatalogReader(catalog_path)  # type: ignore[arg-type]
        self._manifest_reader = ManifestReader(manifest_path)
        self._state = RegistryState(models={}, exposures={}, is_loaded=False)
        self._sql_parser: Optional[SQLColumnParser] = None
        self._dialect: Optional[str] = None
        self._adapter_override: Optional[str] = adapter_override
        self._use_target_dir_fallback: bool = use_target_dir_fallback
        self._stop_at_ephemeral: bool = stop_at_ephemeral
        self._ephemeral_lineage: Dict[str, Dict] = {}
        # Lazy lineage mode: load() skips the whole-project SQL parse; each model's
        # compiled SQL is parsed on first access via ensure_lineage()/ensure_all_lineage().
        # Keeps cold-start cost proportional to the models actually requested.
        self._lazy_lineage: bool = lazy_lineage
        self._lineage_done: Set[str] = set()
        self._parse_lock = threading.RLock()
        # Build combined placeholder regex from caller-supplied patterns.
        # No default — placeholder replacement is repo-specific and opt-in via .osmosis.
        if placeholder_patterns:
            combined = "|".join(f"(?:{p})" for p in placeholder_patterns)
            self._placeholder_re: Optional[re.Pattern] = re.compile(combined)
        else:
            self._placeholder_re = None

    @property
    def is_loaded(self) -> bool:
        return self._state.is_loaded

    @staticmethod
    def _lookup_name(model: Model) -> str:
        """SQL relation lookup name: source identifier for sources, else model name."""
        return ((model.source_identifier or model.name) or "").lower()

    @staticmethod
    def _normalize_models(raw: Dict[str, Model]) -> tuple[Dict[str, Model], Dict[str, str]]:
        """Normalize a reader's output into (models_by_unique_id, name_alias).

        Readers return unique_id-keyed dicts; older/custom readers that still key
        by name are handled by falling back to the dict key as identity. The alias
        map is built last-wins in insertion order, replicating the overwrite
        semantics of the historical name-keyed dict.
        """
        by_id: Dict[str, Model] = {}
        alias: Dict[str, str] = {}
        for key, model in raw.items():
            uid = model.unique_id or key
            by_id[uid] = model
            alias[ModelRegistry._lookup_name(model) or key.lower()] = uid
        return by_id, alias

    def _resolve_model(self, name: str) -> Optional[Model]:
        """Resolve a SQL relation name (or a unique_id) to a Model, or None."""
        uid = self._state.name_alias.get(name.lower())
        if uid is not None:
            return self._state.models.get(uid)
        return self._state.models.get(name)

    def get_ephemeral_lineage(self) -> Dict[str, Dict]:
        """Return collected ephemeral CTE lineage (only populated when stop_at_ephemeral=True).

        Returns a dict keyed by lowercased __dbt__cte__ name, with column → ColumnLineage values.
        """
        return self._ephemeral_lineage

    def _initialize_models(self) -> Dict[str, Model]:
        """Initialize base model information from catalog."""
        try:
            models = self._catalog_reader.get_models_nodes()
            if not models:
                raise RegistryError("No models found in catalog")
            return models
        except Exception as e:
            raise RegistryError(f"Failed to initialize models: {e}")

    def _apply_dependencies(self, models: Dict[str, Model]) -> None:
        """Apply upstream and downstream dependencies to models."""
        try:
            upstream_deps = self._manifest_reader.get_model_upstream()
            downstream_deps = self._manifest_reader.get_model_downstream()
            model_exposures = self._manifest_reader.get_model_exposures()

            manifest_sources = self._manifest_reader.manifest.get("sources", {})
            for source_id, source_node in manifest_sources.items():
                source_name = source_node.get("source_name")
                source_identifier = (
                    source_node.get("identifier", "").lower()
                    if source_node.get("identifier")
                    else source_node.get("name", "").lower()
                )

                source_model = models.get(source_id) or self._resolve_model(source_identifier)
                if source_model and source_model.resource_type == "source" and source_name:
                    source_model.source_name = source_name.lower()

            for model in models.values():
                model_name = self._lookup_name(model)
                model.upstream = upstream_deps.get(model_name, set())
                model.downstream = downstream_deps.get(model_name, set())
                if model_name in model_exposures:
                    model.downstream.update(model_exposures[model_name])
                # Exact node lookup by unique_id when available; name-indexed fallback
                # for models built by readers that did not set unique_id.
                node = None
                if model.unique_id:
                    node = self._manifest_reader.get_node(model.unique_id)
                if node is None:
                    node = self._manifest_reader._find_node(model_name)
                if node:
                    model.language = node.get("language")
                    model.resource_path = node.get("original_file_path")
                    model.description = node.get("description")
                    model.tags = node.get("tags", [])
        except Exception as e:
            raise RegistryError(f"Failed to apply dependencies: {e}")

    def _load_exposures(self) -> Dict[str, Exposure]:
        """Load exposures from manifest."""
        exposures = {}
        exposure_data = self._manifest_reader.get_exposures()
        exposure_deps = self._manifest_reader.get_exposure_dependencies()

        for exposure_id, exp_data in exposure_data.items():
            exposure_name = exp_data.get("name")
            if not exposure_name:
                continue

            depends_on_models = exposure_deps.get(exposure_name, set())

            exposure = Exposure(
                name=exposure_name,
                type=exp_data.get("type", "dashboard"),
                url=exp_data.get("url"),
                description=exp_data.get("description"),
                owner=exp_data.get("owner"),
                unique_id=exposure_id,
                depends_on_models=depends_on_models,
                resource_path=exp_data.get("original_file_path"),
                metadata=exp_data.get("meta", {}),
            )
            exposures[exposure_name] = exposure

        return exposures

    def _parse_model_lineage(self, model: Model) -> str:
        """Parse one model's compiled SQL and apply its column lineage.

        Returns ``"ok"``, ``"failed"``, or ``"skipped"`` (no SQL / not a SQL model).
        """
        if self._sql_parser is None:
            raise RegistryError("SQL parser not initialized. Call load() first.")

        if model.language != "sql":
            return "skipped"

        model_name = self._lookup_name(model)
        sql = self._manifest_reader.get_compiled_sql(model_name, unique_id=model.unique_id)
        if not sql and self._use_target_dir_fallback:
            sql = self._manifest_reader.get_compiled_sql_from_disk(
                model_name, unique_id=model.unique_id
            )
        if not sql:
            return "skipped"

        # For DML statements (MERGE / INSERT INTO), extract just the source
        # SELECT so the parser never sees {{ this }} as a self-reference target.
        # Only DML needs this (and it costs a full parse_one), so plain SELECTs —
        # the vast majority — skip the throwaway parse via a cheap keyword check.
        if re.search(r"\b(?:MERGE|INSERT)\b", sql, re.IGNORECASE):
            sql = _extract_source_select(sql, dialect=self._adapter_override)

        # Incremental SCD2 / history models read {{ this }} as a UNION source to
        # accumulate state. Drop that self-referencing branch so the column resolves to
        # its real upstream instead of a multi-source UNION with no origin. Cheap gate
        # (UNION present AND the model's own name appears) avoids the parse for the
        # vast majority of models.
        if re.search(r"\bUNION\b", sql, re.IGNORECASE) and model_name.lower() in sql.lower():
            sql = _strip_self_referencing_union_branches(
                sql, model_name, dialect=self._adapter_override
            )

        # Replace unresolved custom-materialization placeholders (e.g.
        # __PERIOD_FILTER__) with TRUE so the SQL parser sees valid syntax.
        if self._placeholder_re is not None:
            sql = self._placeholder_re.sub(_replace_placeholder, sql)

        try:
            parse_result = self._sql_parser.parse_column_lineage(
                sql, stop_at_ephemeral=self._stop_at_ephemeral
            )
            self._apply_column_lineage(model, parse_result)
            # Collect ephemeral CTE lineage from this model's parse result
            for cte_name, cte_cols in parse_result.ephemeral_cte_lineage.items():
                self._ephemeral_lineage.setdefault(cte_name, {}).update(cte_cols)
            return "ok"
        except Exception as e:
            logger.warning(
                f"Failed to process lineage for model {model_name}: {type(e).__name__}: {str(e)}"
            )
            return "failed"

    def _process_lineage(self, models: Dict[str, Model]) -> None:
        """Process and apply column lineage to models (eager, whole set)."""
        if self._sql_parser is None:
            raise RegistryError("SQL parser not initialized. Call load() first.")

        sql_models = [m for m in models.values() if m.language == "sql"]
        total = len(sql_models)
        logger.info(
            "Parsing compiled SQL of %d models for column lineage — this runs once per "
            "process (results are cached).",
            total,
        )

        successful_parses = 0
        failed_parses = 0
        skipped_models = 0
        failed_model_names = []

        # First pass: Process explicit column references
        last_progress = time.monotonic()
        for i, model in enumerate(sql_models, 1):
            status = self._parse_model_lineage(model)
            if status == "ok":
                successful_parses += 1
            elif status == "failed":
                failed_parses += 1
                failed_model_names.append(self._lookup_name(model))
            else:
                skipped_models += 1
            now = time.monotonic()
            if now - last_progress >= 2.0:
                logger.info("Column lineage parse progress: %d/%d models...", i, total)
                last_progress = now

        logger.info(
            f"SQL parsing summary: {successful_parses} successful, "
            f"{failed_parses} failed, {skipped_models} skipped (no SQL)"
        )

        if failed_model_names:
            logger.info(
                f"Failed models ({len(failed_model_names)}): {', '.join(failed_model_names)}"
            )

        # Second pass: Process star references
        try:
            self._process_star_references(models)
        except Exception as e:
            logger.error(f"Failed to process star references: {e}", exc_info=True)

    def _apply_column_lineage(self, model: Model, parse_result: SQLParseResult) -> None:
        """Apply parsed lineage to model columns.

        Columns discovered by the SQL parser that are not yet in the model's column dict
        (i.e. the YAML has no documented columns for a new/undocumented model) are created
        as stub Column entries so that CLL can still return lineage results for them.
        Without this, new models with empty YAMLs would always return [] from
        get_column_lineage() because the api.py loop iterates over model.columns.
        """
        for col_name, lineage in parse_result.column_lineage.items():
            if col_name not in model.columns:
                model.columns[col_name] = Column(name=col_name, model_name=model.name)
            model.columns[col_name].lineage = lineage

        if parse_result.star_sources:
            model.metadata = model.metadata or {}
            model.metadata["star_sources"] = list(parse_result.star_sources)

    def _process_star_references(self, models: Dict[str, Model]) -> None:
        """Process star references between models."""
        for model in models.values():
            self._process_star_references_for(model)

    def _process_star_references_for(self, model: Model) -> None:
        """Apply *model*'s ``select *`` references (recorded as ``star_sources`` metadata
        during its SQL parse) as direct column lineage from the referenced relations."""
        if not model.metadata or "star_sources" not in model.metadata:
            return

        for source_name in model.metadata["star_sources"]:
            star_source = self._resolve_model(source_name)
            if star_source is not None:
                self._apply_star_columns(model, source_name, star_source)
            elif source_name in self._ephemeral_lineage:
                # Ephemeral model: source_name is __dbt__cte__<model> — emit
                # child.col ← __dbt__cte__<model>.col so the ephemeral remains
                # visible as an intermediate node in include_ephemeral=True mode.
                self._apply_ephemeral_star_columns(
                    model, source_name, self._ephemeral_lineage[source_name]
                )

    def _apply_star_columns(self, target: Model, source_name: str, source: Model) -> None:
        """Apply star columns from source to target model."""
        for col_name, source_col in source.columns.items():
            if col_name not in target.columns:
                continue

            target_col = target.columns[col_name]
            if not target_col.lineage:
                target_col.lineage = []

            star_lineage = ColumnLineage(
                source_columns={f"{source_name}.{col_name}"},
                transformation_type="direct",
            )

            if not any(
                existing.source_columns == star_lineage.source_columns
                for existing in target_col.lineage
            ):
                target_col.lineage.append(star_lineage)

    def _apply_ephemeral_star_columns(
        self,
        target: Model,
        ephemeral_cte_name: str,
        ephemeral_cols: Dict,
    ) -> None:
        """Apply passthrough star columns from an ephemeral CTE to a child model.

        Emits child.col ← ephemeral_cte_name.col so the ephemeral remains visible
        as an intermediate node when include_ephemeral=True.
        """
        for col_name in ephemeral_cols:
            if col_name not in target.columns:
                continue
            target_col = target.columns[col_name]
            if not target_col.lineage:
                target_col.lineage = []
            star_lineage = ColumnLineage(
                source_columns={f"{ephemeral_cte_name}.{col_name}"},
                transformation_type="direct",
            )
            if not any(
                existing.source_columns == star_lineage.source_columns
                for existing in target_col.lineage
            ):
                target_col.lineage.append(star_lineage)

    def ensure_lineage(self, model: Model) -> None:
        """Guarantee *model*'s column lineage is parsed (lazy mode only).

        Eager registries parse everything at :meth:`load`, so this is a no-op there.
        In lazy mode the model's compiled SQL is parsed on first request; ``select *``
        references recursively pull in the referenced models' parses first, because
        star-column application needs the source model's parser-discovered stub
        columns to match eager-mode results. Thread-safe.
        """
        if not self._lazy_lineage:
            return
        with self._parse_lock:
            self._ensure_lineage_locked(model, in_progress=set())

    def ensure_all_lineage(self) -> None:
        """Guarantee every model's column lineage is parsed (lazy mode only).

        Equivalent to the eager whole-project parse; used when a caller genuinely
        needs project-wide results (no model filter, or ephemeral rows that can
        surface from any consuming model's SQL).
        """
        if not self._lazy_lineage:
            return
        with self._parse_lock:
            pending = [
                m
                for m in self._state.models.values()
                if (m.unique_id or self._lookup_name(m)) not in self._lineage_done
                and m.language == "sql"
            ]
            if not pending:
                return
            total = len(pending)
            logger.info(
                "Parsing compiled SQL of %d models for column lineage — this runs once per "
                "process (results are cached).",
                total,
            )
            last_progress = time.monotonic()
            for i, model in enumerate(pending, 1):
                self._ensure_lineage_locked(model, in_progress=set())
                now = time.monotonic()
                if now - last_progress >= 2.0:
                    logger.info("Column lineage parse progress: %d/%d models...", i, total)
                    last_progress = now

    def _ensure_lineage_locked(self, model: Model, in_progress: Set[str]) -> None:
        """Parse *model* and apply its star references, recursing into star sources.

        ``in_progress`` guards against star-reference cycles (e.g. mutually
        star-selecting models): a model already on the stack has necessarily been
        parsed, which is all a dependent star application needs.
        """
        uid = model.unique_id or self._lookup_name(model)
        if uid in self._lineage_done or uid in in_progress:
            return
        if model.language != "sql":
            # Sources and seeds have no SQL to trace — the eager pass filters them
            # out up front, so mark them done rather than re-entering the parser on
            # every access (a lineage walk touches its terminal sources constantly).
            self._lineage_done.add(uid)
            return
        in_progress.add(uid)

        self._parse_model_lineage(model)

        # Star references need the source models' parser-discovered stub columns
        # (undocumented models have no YAML columns), so parse those first —
        # mirroring the eager flow where ALL parses precede the star pass.
        if model.metadata and "star_sources" in model.metadata:
            for source_name in model.metadata["star_sources"]:
                star_source = self._resolve_model(source_name)
                if star_source is not None:
                    self._ensure_lineage_locked(star_source, in_progress)
            try:
                self._process_star_references_for(model)
            except Exception as e:
                logger.error(f"Failed to process star references: {e}", exc_info=True)

        self._lineage_done.add(uid)

    def load(self) -> None:
        """Load and initialize the registry.

        With ``lazy_lineage=True`` the (potentially expensive) whole-project SQL parse
        is deferred: models, dependencies, and exposures load as usual, and each
        model's lineage is parsed on first access via :meth:`ensure_lineage`.
        """
        if self.is_loaded:
            raise RegistryError("Registry has already been loaded")

        try:
            self._catalog_reader.load()
            self._manifest_reader.load()

            # Ensure the dialect is set before initializing the parser
            self._dialect = self._adapter_override or self._manifest_reader.get_adapter()

            if self._adapter_override:
                logger.info(f"Using adapter override from CLI: {self._adapter_override}")
            elif self._dialect:
                logger.info(f"Detected dialect: {self._dialect}")
            else:
                logger.warning("No dialect detected, the sql parser will be less accurate")

            models, name_alias = self._normalize_models(self._initialize_models())
            # Interim state so _resolve_model works during the load phases below.
            self._state = RegistryState(
                models=models, exposures={}, is_loaded=False, name_alias=name_alias
            )

            # Known column lists per SQL relation name (from the catalog reader) —
            # lets the parser resolve unqualified columns in joins to the table
            # that actually has the column (roadmap #4). Colliding names merge
            # their column sets; ambiguity then falls back to first-FROM-table.
            table_columns: Dict[str, set] = {}
            for m in models.values():
                lookup = self._lookup_name(m)
                if lookup and m.columns:
                    table_columns.setdefault(lookup, set()).update(m.columns.keys())
            self._sql_parser = SQLColumnParser(
                dialect=self._dialect, table_columns=table_columns or None
            )
            self._apply_dependencies(models)
            if not self._lazy_lineage:
                self._process_lineage(models)
            exposures = self._load_exposures()
            self._state = RegistryState(
                models=models, exposures=exposures, is_loaded=True, name_alias=name_alias
            )
        except Exception as e:
            raise RegistryError(f"Failed to load registry: {e}")

    def apply_cached_lineage(
        self, lineage_by_model: Mapping[str, Mapping[str, List[ColumnLineage]]]
    ) -> tuple[list[str], list[str]]:
        """Seed column lineage from precomputed results instead of parsing SQL.

        *lineage_by_model* maps a model key (manifest ``unique_id`` or SQL relation
        name) to ``{column_name: [ColumnLineage, ...]}``. Columns not yet on the model
        are created as stubs, exactly as :meth:`_apply_column_lineage` does for
        parser-discovered columns, so ``select *`` expansions cached by a previous run
        survive the round-trip.

        Each hydrated model is marked lineage-done, so a lazy registry never re-parses
        it; models absent from *lineage_by_model* stay unparsed and are handled on
        first access by :meth:`ensure_lineage`. Call after :meth:`load`.

        Returns ``(applied_keys, unknown_keys)`` — the latter are keys with no matching
        model in this manifest (stale cache entries for deleted models).
        """
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before applying cached lineage")

        applied: list[str] = []
        unknown: list[str] = []
        with self._parse_lock:
            for key, columns in lineage_by_model.items():
                model = self._resolve_model(key)
                if model is None:
                    unknown.append(key)
                    continue
                for col_name, lineage in columns.items():
                    if col_name not in model.columns:
                        model.columns[col_name] = Column(name=col_name, model_name=model.name)
                    model.columns[col_name].lineage = list(lineage)
                self._lineage_done.add(model.unique_id or self._lookup_name(model))
                applied.append(key)
        return applied, unknown

    def restrict_to(self, model_keys: Iterable[str]) -> None:
        """Drop every model outside *model_keys* from the registry.

        Keys are matched as SQL relation names or manifest ``unique_id``s. Sources
        and seeds feeding a kept model are retained so lineage graphs still terminate
        at their real origin, and exposures survive only while at least one model
        they depend on is still present.

        Used by the selector-scoped explorer mode, where the lineage artifact covers
        a deliberate subset of the project and showing the rest would advertise
        lineage the caller never asked to compute.
        """
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before restricting it")

        keep_ids: Set[str] = set()
        for key in model_keys:
            model = self._resolve_model(key)
            if model is not None:
                keep_ids.add(model.unique_id or self._lookup_name(model))

        # Sources and seeds referenced by a kept model are the graph's terminals —
        # a selector resolves to models only, so without this the chain would stop
        # one hop short of where the data actually originates.
        for uid in list(keep_ids):
            model = self._state.models.get(uid)
            if model is None:
                continue
            for upstream_name in model.upstream:
                upstream = self._resolve_model(upstream_name)
                if upstream is not None and upstream.resource_type in ("source", "seed"):
                    keep_ids.add(upstream.unique_id or self._lookup_name(upstream))

        models = {uid: m for uid, m in self._state.models.items() if uid in keep_ids}
        name_alias = {name: uid for name, uid in self._state.name_alias.items() if uid in keep_ids}
        kept_names = {self._lookup_name(m) for m in models.values()}
        exposures = {
            name: exp
            for name, exp in self._state.exposures.items()
            if any(dep.lower() in kept_names for dep in exp.depends_on_models)
        }
        self._state = RegistryState(
            models=models, exposures=exposures, is_loaded=True, name_alias=name_alias
        )

    def get_models(self) -> Dict[str, Model]:
        """Get all models keyed by SQL relation name (backwards-compatible view).

        On name collisions this view shows the alias-map winner only; use
        :meth:`get_models_by_id` for the complete, collision-safe dict.
        """
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")
        return {
            name: self._state.models[uid]
            for name, uid in self._state.name_alias.items()
            if uid in self._state.models
        }

    def get_models_by_id(self) -> Dict[str, Model]:
        """Get ALL models keyed by manifest unique_id (collision-safe)."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")
        return self._state.models

    def get_model(self, model_name: str) -> Model:
        """Get a specific model by SQL relation name or manifest unique_id."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")

        model = self._resolve_model(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found")
        return model

    def get_exposures(self) -> Dict[str, Exposure]:
        """Get all exposures in the registry."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing exposures")
        return self._state.exposures

    def get_exposure(self, exposure_name: str) -> Exposure:
        """Get a specific exposure by name."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing exposures")

        exposure = self._state.exposures.get(exposure_name)
        if exposure is None:
            raise ValueError(f"Exposure '{exposure_name}' not found")
        return exposure

    def _check_loaded(self) -> None:
        """Verify registry is loaded before operations"""
        if not self._state.models:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")

    def _find_compiled_sql(self, model_name: str) -> Optional[str]:
        """Find compiled SQL for a model from manifest or target file."""
        self._check_loaded()
        model = self._resolve_model(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")

        # Find in manifest (meaning node has been executed)
        manifest_sql = self._manifest_reader.get_compiled_sql(model_name)
        if manifest_sql:
            model.compiled_sql = manifest_sql
            return manifest_sql

        # If not in manifest, try to read from compiled target file
        compiled_path = self._manifest_reader.get_model_path(model_name)
        if compiled_path:
            try:
                with open(compiled_path, "r") as f:
                    compiled_sql = f.read()
                model.compiled_sql = compiled_sql
                return compiled_sql
            except (FileNotFoundError, IOError):
                pass

        return None

    def get_compiled_sql(self, model_name: str) -> str:
        """Get compiled SQL for a model, trying manifest first then target file."""
        self._check_loaded()
        model = self._resolve_model(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")

        if model.compiled_sql:
            return model.compiled_sql

        compiled_sql = self._find_compiled_sql(model_name)
        if compiled_sql:
            return compiled_sql

        raise ValueError(f"No compiled SQL found for model '{model_name}'")
