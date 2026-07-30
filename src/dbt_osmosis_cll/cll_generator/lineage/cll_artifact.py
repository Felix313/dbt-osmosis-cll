"""Read precomputed CLL artifacts and turn them back into registry lineage.

Two artifacts on disk already hold everything the lineage explorer needs, so it
does not have to re-parse every model's compiled SQL just to show a graph:

``target/cll-result.json`` — written by ``dbt-osmosis-cll parse-cll``. Scoped to a
    dbt selector, carries the selector strings and the resolved model list, so the
    explorer can restrict itself to exactly that context.

``target/cll_cache.json`` — written by osmosis' ``yaml document``/``refactor`` runs
    (:mod:`dbt_osmosis_cll.integration.cll`). Covers whatever osmosis processed —
    the whole project after a full run — and carries a per-model source-SQL hash,
    which lets the explorer spot models whose SQL changed since the cache was built.

Both store the flat :class:`~dbt_osmosis_cll.cll_generator.api.ColumnLineageResult`
form (one row per model column). :func:`load_cll_artifact` normalizes either shape
into :class:`~dbt_osmosis_cll.cll_generator.models.schema.ColumnLineage` objects
keyed by model and column — the same structure the SQL parser produces — so the
registry, :class:`~dbt_osmosis_cll.cll_generator.lineage.service.LineageService`,
and every ``/api`` endpoint stay unchanged.

Rows written before schema version 5 lack ``transformation_type`` and
``sql_expression``; both are reconstructed as far as the older fields allow (the
expression is simply unavailable and the impact panel omits it).
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_osmosis_cll.cll_generator.models.schema import ColumnLineage

logger = logging.getLogger(__name__)

_VALID_TRANSFORMATION_TYPES = {
    "direct",
    "renamed",
    "derived",
    "aggregate",
    "window",
    "union",
    "literal",
    "generated",
}

# Flag → transformation kind, in precedence order. Only consulted for pre-v5 rows
# that never stored the kind explicitly; mirrors how api.py derived the flags.
_FLAG_TO_TYPE: Sequence[tuple[str, str]] = (
    ("is_union", "union"),
    ("is_aggregate", "aggregate"),
    ("is_window", "window"),
    ("is_literal", "literal"),
    ("is_generated", "generated"),
    ("is_computed", "derived"),
    ("is_rename", "renamed"),
)


class CllArtifactError(Exception):
    """Raised when a CLL artifact cannot be read or is not a recognized format."""


@dataclass
class CllArtifact:
    """A precomputed CLL artifact, normalized for registry hydration."""

    path: Path
    kind: str
    """``"selector"`` for parse-cll output, ``"project"`` for the osmosis cache."""

    lineage: dict[str, dict[str, list[ColumnLineage]]] = field(default_factory=dict)
    """``{model_name: {column_name: [ColumnLineage, ...]}}``, model names lowercased."""

    selectors: list[str] = field(default_factory=list)
    """Selector strings the artifact was built from; empty for the osmosis cache."""

    scope: list[str] | None = None
    """Model names the artifact deliberately covers, or None when it covers whatever
    osmosis happened to process. Drives the explorer's scoped mode."""

    fingerprints: dict[str, str] = field(default_factory=dict)
    """``{model_name: sha256-of-source-sql}`` where the artifact recorded one."""

    row_count: int = 0
    has_sql_expressions: bool = False
    """True when the rows carry the schema-version-5 fields. False marks an older
    artifact, where the impact panel can show no SQL expression at all."""

    @property
    def model_count(self) -> int:
        return len(self.lineage)

    def stale_models(self, fingerprints: Mapping[str, str]) -> list[str]:
        """Return cached models whose current source-SQL hash differs from the artifact's.

        Models the artifact recorded no fingerprint for cannot be checked and are
        never reported as stale — callers treat them as trusted, exactly as osmosis
        does when it reuses a cache entry.
        """
        stale = []
        for model_name, cached_hash in self.fingerprints.items():
            current = fingerprints.get(model_name)
            if current is not None and current != cached_hash:
                stale.append(model_name)
        return sorted(stale)

    def drop_models(self, model_names: Sequence[str]) -> None:
        """Remove *model_names* from the lineage map so they get parsed instead."""
        for name in model_names:
            self.lineage.pop(name.lower(), None)


def _coerce_transformation_type(row: Mapping[str, Any]) -> str:
    """Return the row's transformation kind, reconstructing it for pre-v5 rows."""
    stated = row.get("transformation_type")
    if isinstance(stated, str) and stated in _VALID_TRANSFORMATION_TYPES:
        return stated
    for flag, kind in _FLAG_TO_TYPE:
        if row.get(flag):
            return kind
    return "direct"


def _qualified_pairs(pairs: Any) -> list[str]:
    """Turn ``[[model, column], ...]`` into ``["model.column", ...]``.

    Tuple-valued fields JSON-round-trip as lists of two-element lists; anything that
    does not unpack into exactly two truthy strings is skipped rather than trusted.
    """
    out: list[str] = []
    if not isinstance(pairs, (list, tuple)):
        return out
    for pair in pairs:
        if isinstance(pair, (list, tuple)) and len(pair) == 2:
            model, column = pair
            if model and column:
                out.append(f"{str(model).lower()}.{str(column).lower()}")
    return out


def _row_to_lineage(row: Mapping[str, Any]) -> ColumnLineage | None:
    """Build a ColumnLineage from one flat result row, or None when it carries no lineage.

    ``progenitors`` is the general case — every direct ``(model, column)`` input —
    and maps straight onto ``source_columns``. Rows written before that field existed
    fall back to the single ``progenitor_model``/``progenitor_column`` pair.
    """
    ttype = _coerce_transformation_type(row)
    source_columns: set[str] = set(_qualified_pairs(row.get("progenitors")))
    if not source_columns:
        progenitor_model = row.get("progenitor_model")
        progenitor_column = row.get("progenitor_column")
        if progenitor_model and progenitor_column:
            source_columns = {f"{str(progenitor_model).lower()}.{str(progenitor_column).lower()}"}

    union_branches = _qualified_pairs(row.get("union_branches"))
    if union_branches and not source_columns:
        source_columns = set(union_branches)

    sql_expression = row.get("sql_expression")
    if not sql_expression:
        # Pre-v5 rows only kept the expression under its kind-specific alias.
        sql_expression = row.get("literal_value") or row.get("generated_value")

    if not source_columns and not sql_expression:
        # A column with neither inputs nor an expression carries no lineage worth
        # showing — the explorer renders it as a terminal node from the manifest.
        return None

    return ColumnLineage(
        source_columns=source_columns,
        transformation_type=ttype,  # type: ignore[arg-type]
        sql_expression=sql_expression,
        union_branches=union_branches,
    )


def _index_rows(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, dict[str, list[ColumnLineage]]], int, bool]:
    """Group flat rows into ``{model: {column: [ColumnLineage]}}``."""
    lineage: dict[str, dict[str, list[ColumnLineage]]] = {}
    count = 0
    has_expressions = False
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        model = row.get("model")
        column = row.get("column")
        if not model or not column:
            continue
        count += 1
        # Detect the schema by the presence of the key, not a truthy value: the
        # parser only records an expression for derived/literal/window columns, so
        # a current artifact for a project of pure passthroughs legitimately has
        # none — checking the value would report it as an outdated file.
        if "sql_expression" in row or "transformation_type" in row:
            has_expressions = True
        entry = _row_to_lineage(row)
        # Register the model even when the column has no lineage: the column set of
        # a cached model must match the post-parse state, including `select *`
        # expansions and columns that are pure terminals.
        model_columns = lineage.setdefault(str(model).lower(), {})
        if entry is not None:
            model_columns[str(column).lower()] = [entry]
    return lineage, count, has_expressions


def _load_selector_artifact(path: Path, payload: Mapping[str, Any]) -> CllArtifact:
    rows = payload.get("results") or []
    if not isinstance(rows, list):
        raise CllArtifactError(f"{path}: 'results' must be a list of lineage rows")
    lineage, count, has_expressions = _index_rows(rows)
    scope = [str(m).lower() for m in payload.get("models") or []]
    fingerprints = {
        str(k).lower(): str(v) for k, v in (payload.get("fingerprints") or {}).items() if v
    }
    return CllArtifact(
        path=path,
        kind="selector",
        lineage=lineage,
        selectors=[str(s) for s in payload.get("selectors") or []],
        scope=scope or None,
        fingerprints=fingerprints,
        row_count=count,
        has_sql_expressions=has_expressions,
    )


def _load_project_artifact(path: Path, payload: Mapping[str, Any]) -> CllArtifact:
    entries = payload.get("entries") or {}
    if not isinstance(entries, Mapping):
        raise CllArtifactError(f"{path}: 'entries' must be a mapping of model name to results")

    rows: list[Mapping[str, Any]] = []
    fingerprints: dict[str, str] = {}
    for model_name, entry in entries.items():
        if not isinstance(entry, Mapping):
            continue
        entry_rows = entry.get("results") or []
        if isinstance(entry_rows, list):
            rows.extend(r for r in entry_rows if isinstance(r, Mapping))
        sql_hash = entry.get("compiled_sql_hash")
        if sql_hash:
            fingerprints[str(model_name).lower()] = str(sql_hash)

    lineage, count, has_expressions = _index_rows(rows)
    return CllArtifact(
        path=path,
        kind="project",
        lineage=lineage,
        scope=None,
        fingerprints=fingerprints,
        row_count=count,
        has_sql_expressions=has_expressions,
    )


def load_cll_artifact(path: Path) -> CllArtifact:
    """Load *path* as either a parse-cll result or an osmosis CLL cache.

    The format is detected from the payload rather than the filename, so renamed or
    relocated artifacts still work.

    Raises:
        CllArtifactError: when the file is missing, is not valid JSON, or matches
            neither known shape.
    """
    if not path.exists():
        raise CllArtifactError(f"No CLL artifact found at {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise CllArtifactError(f"Could not read CLL artifact {path}: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise CllArtifactError(f"{path}: expected a JSON object at the top level")

    if "results" in payload:
        artifact = _load_selector_artifact(path, payload)
    elif "entries" in payload:
        artifact = _load_project_artifact(path, payload)
    else:
        raise CllArtifactError(
            f"{path}: not a recognized CLL artifact — expected 'results' (parse-cll "
            "output) or 'entries' (osmosis cll_cache.json)"
        )

    if not artifact.lineage:
        raise CllArtifactError(f"{path}: holds no lineage rows")
    return artifact


def default_artifact_path(target_dir: Path) -> Path | None:
    """Return the CLL artifact to use when the caller did not name one.

    Prefers the selector-scoped ``cll-result.json``: a caller who just ran parse-cll
    is asking about that context, whereas ``cll_cache.json`` accumulates silently in
    the background of every osmosis run.
    """
    for candidate in ("cll-result.json", "cll_cache.json"):
        path = target_dir / candidate
        if path.exists():
            return path
    return None


def source_sql_fingerprints(project_dir: Path, manifest: Mapping[str, Any]) -> dict[str, str]:
    """Hash every model's source ``.sql`` file, keyed by lowercased model name.

    Hashes the source file rather than the compiled artifact for the same reason
    :mod:`dbt_osmosis_cll.integration.cll` does: compiled SQL changes on every
    ``dbt compile`` when Jinja renders dynamic values, which would mark the whole
    cache stale on every run. Column lineage only changes when the SELECT structure
    does. Uses the same hash so the two artifacts' fingerprints are comparable.
    """
    fingerprints: dict[str, str] = {}
    for node in (manifest.get("nodes") or {}).values():
        if not isinstance(node, Mapping) or node.get("resource_type") != "model":
            continue
        name = node.get("name")
        original = node.get("original_file_path") or ""
        if not name or not str(original).endswith(".sql"):
            continue
        full = project_dir / str(original)
        try:
            fingerprints[str(name).lower()] = hashlib.sha256(full.read_bytes()).hexdigest()
        except OSError:
            continue
    return fingerprints
