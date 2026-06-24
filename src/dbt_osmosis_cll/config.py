"""
Package-level configuration for dbt-osmosis-cll.

Settings are read from a ``.osmosis`` file (INI format) found by walking up
from the current working directory — the same convention as ``.sqlfluff``.

Place ``.osmosis`` in your dbt project root, next to ``dbt_project.yml``:

.. code-block:: ini

    [osmosis]
    annotation-renamed   = Renamed from:
    annotation-derived   = Derived from:
    annotation-computed  = Computed in:
    annotation-namespace = MY-ORG
    annotation-separator = __________
    cll-cache-path       = target/cll_cache.json
    cll-max-origin-depth = 100
    column-docs-path     = docs/osmosis_column_references.yml
    yaml-best-width      = 160
    compiled-sql-placeholder-patterns =
        __[A-Z][A-Z0-9_]*__

All settings are optional — defaults are used when the file or a key is absent.

Column glossary (``column-docs-path``)
---------------------------------------
A flat YAML file mapping column names to canonical descriptions.  Every column
listed there is automatically treated as CLL-ignored: no ``col-renamed-from`` /
``col-derived-from`` / ``col-computed-in`` meta tag is written for it, stale tags from prior runs are
removed, and the glossary description is authoritative — it is written on every
run, overwriting any existing description in the YAML.

.. code-block:: yaml

    # docs/osmosis_column_references.yml
    ROW_BATCH_TIMESTAMP: >-
      Zeitstempel des letzten Batch-Ladeprozesses.
    ROW_CREATE_TIMESTAMP: >-
      Zeitstempel der erstmaligen Erstellung dieses Datensatzes.
"""
from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

_CONFIG_FILENAME = ".osmosis-cll"
_SECTION = "osmosis"


@dataclass
class OsmosisConfig:
    """Resolved package configuration with defaults."""

    # ── Annotation strings (written into YAML descriptions) ──────────────────
    annotation_renamed:   str = "Renamed from:"
    """Prefix for a pure column rename (name changed, same data)."""

    annotation_derived:   str = "Derived from:"
    """Prefix for a single-source computed transform (CAST, function, etc.)."""

    annotation_computed:  str = "Computed in:"
    """Prefix for multi-source or unresolvable columns (no single parent)."""

    annotation_aggregate_from: str = "Aggregated from"
    """Prefix for single-source aggregate functions (SUM, AVG, MAX…). Column name follows."""

    annotation_aggregate_in: str = "Aggregated in:"
    """Prefix for aggregate functions with no traceable source column (COUNT(*), multi-source SUM)."""

    annotation_window_from: str = "Windowed from:"
    """Prefix for window functions with a traceable value column (SUM OVER, AVG OVER…). Column name follows."""

    annotation_window_in: str = "Windowed in:"
    """Prefix for window functions with no traceable value column (ROW_NUMBER, RANK, DENSE_RANK…)."""

    annotation_union: str = "UNION in:"
    """Prefix for columns in a top-level UNION / UNION ALL / INTERSECT / EXCEPT model.
    CLL does not distinguish UNION from UNION ALL, so one label covers both."""

    annotation_literal: str = "Literal"
    """Prefix for hardcoded constant columns. Literal value follows."""

    annotation_generated: str = "Generated"
    """Prefix for zero-argument system-function columns (CURRENT_DATE, SYSDATE, UUID_STRING…).
    Expression string follows, then ``in: SCHEMA.MODEL``."""

    annotation_namespace: str = "OSMOSIS"
    """Namespace label that appears before every annotation tag."""

    annotation_separator: str = "__________"
    """Visual separator line inserted above the annotation block."""

    legacy_strip_markers: list[str] = field(default_factory=list)
    """Project-specific legacy tag prefixes to strip from descriptions (e.g. old pre-CLL osmosis
    tags). Configured via ``legacy-strip-markers`` in .osmosis as a comma-separated list.
    Example: ``legacy-strip-markers = OLD_ORIGIN:, OLD_DERIVED_IN:``"""

    # ── CLL cache ────────────────────────────────────────────────────────────
    cll_cache_path: str = "target/cll_cache.json"
    """Path to the CLL disk cache, relative to the dbt project root."""

    # ── Depth guards ─────────────────────────────────────────────────────────
    cll_max_origin_depth: int = 60
    """Maximum number of model hops to follow when tracing a column back to its
    origin. Counts DAG model hops (each step crosses to the progenitor model), so
    the default of 60 is far beyond any realistic warehouse depth — it exists only
    as a cycle/runaway guard. Raise it only if you see "origin depth exceeded"
    warnings on legitimately deep lineage."""

    # ── SQL preprocessing ────────────────────────────────────────────────────
    compiled_sql_placeholder_patterns: list = None  # type: ignore[assignment]
    """Regex patterns (one per line) for tokens in compiled SQL that are
    custom-materialization placeholders left verbatim after ``dbt compile``.
    Each match is replaced with ``TRUE`` before CLL parses the SQL, making
    otherwise-invalid SQL parseable without affecting SELECT structure.

    Default when absent or empty: replaces ``__UPPERCASE__`` tokens
    (e.g. ``__PERIOD_FILTER__``).

    Example .osmosis entry::

        compiled-sql-placeholder-patterns =
            __[A-Z][A-Z0-9_]*__
            %%CUSTOM_PLACEHOLDER%%
    """

    # ── Central column docs ───────────────────────────────────────────────────
    column_docs_path: str = ""
    """Path (relative to dbt project root, or absolute) to a flat YAML file
    mapping column names to their canonical descriptions.

    Every column listed in the file is **automatically CLL-ignored**: osmosis
    will not write ``meta_key_renamed_from`` / ``meta_key_derived_from`` / ``meta_key_computed_in``
    meta tags for it, will strip any stale tags from prior runs, and treats the
    glossary description as authoritative — writing it on every run and
    overwriting any existing description in the YAML.

    Leave empty (default) to disable.
    """

    # ── CLL-driven inheritance (Phase 4) ────────────────────────────────────
    inherit_through_renames: bool = False
    """When ``True``, CLL-driven inheritance follows column renames across model hops
    and propagates the upstream description even when the column name changes.

    Set ``True`` for layers that only rename/cast (e.g. staging) where upstream
    descriptions remain semantically valid after a rename.  Set ``False`` (default)
    for domain/DP layers where renames signal a semantic change — inheriting the
    upstream description would be misleading (e.g. ``SUM(bpartner_id) AS cnt_bpart``
    should not inherit the bpartner_id description).

    Configured as ``inherit-through-renames`` in ``.osmosis`` or via
    ``+dbt-osmosis-options`` in ``dbt_project.yml`` for per-layer control.
    Has no effect until the CLL-driven inheritance rewrite (Phase 4) is active.
    """

    # ── YAML output ──────────────────────────────────────────────────────────
    yaml_best_width: int = 0
    """Maximum line width for YAML output (ruamel ``best_width`` setting).
    Set to 0 (default) to leave ruamel's built-in default (80) unchanged.
    Increase for wider descriptions that should not be line-wrapped, e.g. 160."""

    # ── CLL meta tags ────────────────────────────────────────────────────────
    write_cll_tags_to_meta: bool = False
    """When ``True``, write CLL origin meta tags to column YAML for all resolved columns.

    - Pure renames: writes ``meta_key_renamed_from`` with value ``TABLE.COLUMN``.
    - Single-source computed (cast, function, etc.): writes ``meta_key_derived_from`` with value ``TABLE.COLUMN``.
    - Multi-source / opaque: writes ``meta_key_computed_in`` with value ``SCHEMA.MODEL``.

    Disabled by default to keep YAMLs clean.  Enable when your team queries the dbt
    manifest programmatically for description lineage (e.g. data lineage tooling).
    """

    meta_key_renamed_from: str = "renamed_from"
    """Meta key written for a pure column rename (name changed, same data).
    Value format: ``TABLE.COLUMN``.  Aligns with ``annotation-renamed``.
    """

    meta_key_derived_from: str = "derived_from"
    """Meta key written for a single-source computed column (CAST, function, etc.).
    Value format: ``TABLE.COLUMN``.  Aligns with ``annotation-derived``.
    """

    meta_key_computed_in: str = "computed_in"
    """Meta key written for a multi-source / opaque computed column (SUM, CASE with multiple
    inputs, etc.).  Value format: ``SCHEMA.MODEL``.  Aligns with ``annotation-computed``.
    """

    # ── legacy desc-source cleanup ────────────────────────────────────────────
    desc_source_key: str = "desc-source"
    """Legacy provenance meta key written by prior osmosis versions.

    osmosis no longer writes this key; it now injects ``desc-owner: upstream`` directly when
    it verifiably traces a column's CLL origin (see ``inherit_upstream_column_knowledge_cll``).
    This field remains so the key stays in ``get_managed_meta_keys()`` — which ensures:
      • the YAML sync writer strips it from ``config.meta`` on the next write (no re-persistence)
      • it is not forwarded downstream as regular inherited meta

    Set to an empty string in ``.osmosis`` (``desc-source-key =``) to disable cleanup.
    Configured as ``desc-source-key``.
    """


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_config: OsmosisConfig | None = None
_column_docs: dict[str, str] | None = None


def get_config(start_dir: Path | None = None) -> OsmosisConfig:
    """Return the package config, loading it from ``.osmosis`` on first call.

    Walks up from *start_dir* (default: ``cwd()``) until a ``.osmosis`` file
    is found.  Falls back to all defaults if none is found.
    """
    global _config
    if _config is None:
        _config = _load_config(start_dir or Path.cwd())
    return _config


def reset_config() -> None:
    """Reset the cached config instance.  Primarily useful in tests."""
    global _config, _column_docs
    _config = None
    _column_docs = None


def get_column_docs(start_dir: Path | None = None) -> dict[str, str]:
    """Return the central column docs mapping (column_name_lower → description).

    Loads the YAML file pointed to by ``column-docs-path`` in ``.osmosis`` on
    first call and caches the result for the lifetime of the process.  Returns
    an empty dict when no path is configured or the file is absent.
    """
    global _column_docs
    if _column_docs is None:
        cfg = get_config(start_dir)
        _column_docs = _load_column_docs_file(cfg.column_docs_path, start_dir or Path.cwd())
    return _column_docs


def _load_column_docs_file(docs_path: str, project_root: Path) -> dict[str, str]:
    """Load a flat YAML file mapping column names to descriptions."""
    if not docs_path:
        return {}
    path = Path(docs_path)
    if not path.is_absolute():
        path = project_root / path
    if not path.is_file():
        logger.debug("column-docs-path '%s' not found — central docs disabled.", path)
        return {}
    try:
        import yaml  # pyyaml — always available in a dbt environment
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        result = {str(k).lower(): str(v).strip() for k, v in (data or {}).items() if v}
        logger.debug("Loaded %d central column docs from %s", len(result), path)
        return result
    except Exception as exc:
        logger.warning("Failed to load column docs from '%s': %s", path, exc)
        return {}


def _load_config(start: Path) -> OsmosisConfig:
    for directory in [start, *start.parents]:
        osmosis_file = directory / _CONFIG_FILENAME
        if not osmosis_file.is_file():
            continue

        parser = configparser.ConfigParser()
        try:
            parser.read(osmosis_file, encoding="utf-8")
        except Exception as exc:
            logger.debug("Could not parse %s: %s", osmosis_file, exc)
            continue

        if not parser.has_section(_SECTION):
            logger.debug("%s found but has no [osmosis] section — using defaults.", osmosis_file)
            return OsmosisConfig()

        section = parser[_SECTION]
        known = {f.replace("_", "-") for f in OsmosisConfig.__dataclass_fields__}
        # Add the short aliases that map to field names via custom key names in _load_config
        known |= {"col-renamed-from", "col-derived-from", "col-computed-in", "legacy-strip-markers", "desc-source-key"}
        unknown = set(section) - known
        if unknown:
            logger.warning(
                "Unknown settings in %s [osmosis]: %s — ignored.",
                osmosis_file,
                ", ".join(sorted(unknown)),
            )

        def _str(key: str, default: str) -> str:
            return section.get(key, default).strip()

        def _int(key: str, default: int) -> int:
            try:
                return section.getint(key, default)
            except ValueError:
                logger.warning(".osmosis: %s must be an integer — using default %d.", key, default)
                return default

        def _bool(key: str, default: bool) -> bool:
            try:
                return section.getboolean(key, default)
            except ValueError:
                logger.warning(".osmosis: %s must be a boolean — using default %s.", key, default)
                return default

        def _strlist(key: str, default: list[str]) -> list[str]:
            """Parse a comma-separated list of strings."""
            raw = section.get(key)
            if raw is None:
                return default
            return [s.strip() for s in raw.split(",") if s.strip()]

        def _patterns(key: str) -> list | None:
            """Parse a multi-line list of regex patterns; returns None if key absent."""
            raw = section.get(key)
            if raw is None:
                return None
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            return lines if lines else None

        cfg = OsmosisConfig(
            annotation_renamed                 = _str("annotation-renamed",          OsmosisConfig.annotation_renamed),
            annotation_derived                 = _str("annotation-derived",          OsmosisConfig.annotation_derived),
            annotation_computed                = _str("annotation-computed",         OsmosisConfig.annotation_computed),
            annotation_aggregate_from          = _str("annotation-aggregate-from",   OsmosisConfig.annotation_aggregate_from),
            annotation_aggregate_in            = _str("annotation-aggregate-in",     OsmosisConfig.annotation_aggregate_in),
            annotation_window_from             = _str("annotation-window-from",      OsmosisConfig.annotation_window_from),
            annotation_window_in               = _str("annotation-window-in",        OsmosisConfig.annotation_window_in),
            annotation_union                   = _str("annotation-union",            OsmosisConfig.annotation_union),
            annotation_literal                 = _str("annotation-literal",          OsmosisConfig.annotation_literal),
            annotation_generated               = _str("annotation-generated",        OsmosisConfig.annotation_generated),
            annotation_namespace               = _str("annotation-namespace",        OsmosisConfig.annotation_namespace),
            annotation_separator               = _str("annotation-separator",        OsmosisConfig.annotation_separator),
            cll_cache_path                     = _str("cll-cache-path",       OsmosisConfig.cll_cache_path),
            cll_max_origin_depth               = _int("cll-max-origin-depth", OsmosisConfig.cll_max_origin_depth),
            column_docs_path                   = _str("column-docs-path",     OsmosisConfig.column_docs_path),
            compiled_sql_placeholder_patterns  = _patterns("compiled-sql-placeholder-patterns"),
            inherit_through_renames            = _bool("inherit-through-renames", OsmosisConfig.inherit_through_renames),
            yaml_best_width                    = _int("yaml-best-width",      OsmosisConfig.yaml_best_width),
            write_cll_tags_to_meta             = _bool("write-cll-tags-to-meta", OsmosisConfig.write_cll_tags_to_meta),
            meta_key_renamed_from              = _str("col-renamed-from",     OsmosisConfig.meta_key_renamed_from),
            meta_key_derived_from              = _str("col-derived-from",     OsmosisConfig.meta_key_derived_from),
            meta_key_computed_in               = _str("col-computed-in",      OsmosisConfig.meta_key_computed_in),
            desc_source_key                    = _str("desc-source-key",      OsmosisConfig.desc_source_key),
            legacy_strip_markers               = _strlist("legacy-strip-markers", []),
        )
        logger.debug("Loaded dbt-osmosis-cll config from %s: %s", osmosis_file, cfg)
        return cfg

    logger.debug("No .osmosis file found — using defaults.")
    return OsmosisConfig()
