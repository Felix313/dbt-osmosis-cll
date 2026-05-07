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
    anchor-meta-key      = anchor-description
    cll-cache-path       = target/cll_cache.json
    cll-max-origin-depth = 30

All settings are optional — defaults are used when the file or a key is absent.
"""
from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_CONFIG_FILENAME = ".osmosis"
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

    annotation_namespace: str = "CBM-ODP"
    """Namespace label that appears before every annotation tag."""

    annotation_separator: str = "__________"
    """Visual separator line inserted above the annotation block."""

    # ── Enrichment ───────────────────────────────────────────────────────────
    anchor_meta_key: str = "anchor-description"
    """``meta`` key set on enriched columns to protect them from osmosis overwrite."""

    # ── CLL cache ────────────────────────────────────────────────────────────
    cll_cache_path: str = "target/cll_cache.json"
    """Path to the CLL disk cache, relative to the dbt project root."""

    # ── Depth guards ─────────────────────────────────────────────────────────
    cll_max_origin_depth: int = 30
    """Maximum recursion depth when tracing a column back to its source origin.
    Raise this only if you have legitimate lineage chains deeper than 30 hops."""


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_config: OsmosisConfig | None = None


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
    global _config
    _config = None


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

        cfg = OsmosisConfig(
            annotation_renamed   = _str("annotation-renamed",   OsmosisConfig.annotation_renamed),
            annotation_derived   = _str("annotation-derived",   OsmosisConfig.annotation_derived),
            annotation_computed  = _str("annotation-computed",  OsmosisConfig.annotation_computed),
            annotation_namespace = _str("annotation-namespace", OsmosisConfig.annotation_namespace),
            annotation_separator = _str("annotation-separator", OsmosisConfig.annotation_separator),
            anchor_meta_key      = _str("anchor-meta-key",      OsmosisConfig.anchor_meta_key),
            cll_cache_path       = _str("cll-cache-path",       OsmosisConfig.cll_cache_path),
            cll_max_origin_depth = _int("cll-max-origin-depth", OsmosisConfig.cll_max_origin_depth),
        )
        logger.debug("Loaded dbt-osmosis-cll config from %s: %s", osmosis_file, cfg)
        return cfg

    logger.debug("No .osmosis file found — using defaults.")
    return OsmosisConfig()
