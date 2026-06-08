"""Annotation-tag formatting and stripping for the propagation engine.

Renders and parses the origin-annotation blocks (``<separator>\n<namespace>
-> <verb> ...``) that osmosis writes onto column descriptions. Depends only on
the resolved config (namespace / separator / verbs).
"""
from __future__ import annotations

import re

from dbt_osmosis_cll.config import get_config


def _wrap_annotation(tag: str) -> str:
    """Wrap a raw tag string with the configured separator and namespace prefix."""
    cfg = get_config()
    return f"{cfg.annotation_separator}\n{cfg.annotation_namespace} -> {tag}"


def format_origin_tag(origin_col: str, origin_table: str, source_description: str | None) -> str:
    """Return the full annotation block for a **renamed** column."""
    base = f"{get_config().annotation_renamed} {origin_table}.{origin_col}"
    if source_description:
        base = f"{base} — {source_description}"
    return _wrap_annotation(base)


def format_computed_origin_tag(origin_col: str, origin_table: str, source_description: str | None) -> str:
    """Return the full annotation block for a **passthrough / computed** column."""
    base = f"{get_config().annotation_derived} {origin_table}.{origin_col}"
    if source_description:
        base = f"{base} — {source_description}"
    return _wrap_annotation(base)


def format_derived_tag(schema: str, model: str, entry_col: str | None = None) -> str:
    """Return the annotation block for a **multi-source / computed** column.

    When *entry_col* is supplied and differs from the queried column name, it is
    appended as ``(as ENTRY_COL)`` so the reader knows what name to search for
    in the referenced model.
    """
    tag = f"{get_config().annotation_computed} {schema}.{model}"
    if entry_col:
        tag = f"{tag} (as {entry_col})"
    return _wrap_annotation(tag)


def format_aggregate_from_tag(progenitor_col: str, progenitor_model: str) -> str:
    """Return annotation for a single-source aggregate: ``Aggregated from MODEL.COL``.

    Consistent with renamed/derived: ``from`` points to a column (MODEL.COL),
    while ``in:`` (the *_in variants) points to a model (SCHEMA.MODEL).
    """
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_aggregate_from} {progenitor_model}.{progenitor_col}")


def format_aggregate_in_tag(schema: str, model: str) -> str:
    """Return annotation for an aggregate with no traceable source: ``Aggregated here``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_aggregate_in.removesuffix('in:').rstrip()} here")


def format_window_from_tag(progenitor_col: str, progenitor_model: str) -> str:
    """Return annotation for a window function with a traceable value column: ``Windowed from: MODEL.COL``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_window_from} {progenitor_model}.{progenitor_col}")


def format_window_in_tag(schema: str, model: str) -> str:
    """Return annotation for a window function with no traceable source: ``Windowed here``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_window_in.removesuffix('in:').rstrip()} here")


def format_union_tag(schema: str, model: str) -> str:
    """Return annotation for a top-level UNION / UNION ALL column: ``UNION here``.

    CLL does not distinguish UNION from UNION ALL, so a single label covers both.
    """
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_union.removesuffix('in:').rstrip()} here")


def format_literal_tag(literal_value: str, schema: str, model: str) -> str:
    """Return annotation for a hardcoded constant: ``Literal set here``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_literal} set here")


def format_generated_tag(generated_expr: str, schema: str, model: str) -> str:
    """Return annotation for a zero-arg system function: ``Generated here``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_generated} here")


def format_computed_here_tag() -> str:
    """Return annotation for a multi-source computed column born in this model: ``Computed here``."""
    cfg = get_config()
    return _wrap_annotation(f"{cfg.annotation_computed.removesuffix('in:').rstrip()} here")


_WS_NORMALIZE_RE = re.compile(r"\s+")


def descriptions_equivalent(a: str | None, b: str | None) -> bool:
    """True when two descriptions differ only in whitespace / line-wrap.

    Database comments come back as single-line strings while YAML stores them
    wrapped at a fixed width — they describe the same content but are not
    byte-equal. Collapsing internal whitespace before compare makes the
    "is this a real change?" check robust to wrap differences and prevents
    spurious source refresh updates that would in turn flap the annotation
    layer on the very next run (idempotency bug).
    """
    if not a and not b:
        return True
    if not a or not b:
        return False
    return _WS_NORMALIZE_RE.sub(" ", a).strip() == _WS_NORMALIZE_RE.sub(" ", b).strip()


def strip_annotation_tags(description: str) -> str:
    """Remove any annotation block from a description string.

    Strips from the configured separator onwards so a fresh annotation can be
    appended on the next run.  Also handles legacy bare prefixes — both the
    current annotation verbs and any project-specific ``legacy-strip-markers``
    configured in ``.osmosis`` — for backward compatibility.
    """
    cfg = get_config()
    sep_idx = description.find(cfg.annotation_separator)
    if sep_idx != -1:
        return description[:sep_idx].rstrip()
    # Legacy bare prefixes (pre-separator format) — always strip regardless of config.
    # Namespace-prefixed variants are checked first so that old single-line annotations
    # like "NAMESPACE -> VERB TABLE.COL" strip to "" rather than leaving
    # the orphaned "NAMESPACE ->" prefix behind.
    _ns = cfg.annotation_namespace
    for marker in (
        # Namespace-prefixed forms (legacy "NAMESPACE -> VERB ..." on a single line)
        f"{_ns} -> {cfg.annotation_renamed}",
        f"{_ns} -> {cfg.annotation_derived}",
        f"{_ns} -> {cfg.annotation_computed}",
        f"{_ns} -> {cfg.annotation_aggregate_from}",
        f"{_ns} -> {cfg.annotation_aggregate_in}",
        f"{_ns} -> {cfg.annotation_window_from}",
        f"{_ns} -> {cfg.annotation_window_in}",
        f"{_ns} -> {cfg.annotation_union}",
        f"{_ns} -> {cfg.annotation_literal}",
        f"{_ns} -> {cfg.annotation_generated}",
        # Bare verb forms (even older format, or when namespace was different)
        cfg.annotation_renamed, cfg.annotation_derived, cfg.annotation_computed,
        cfg.annotation_aggregate_from, cfg.annotation_aggregate_in,
        cfg.annotation_window_from, cfg.annotation_window_in,
        cfg.annotation_union, cfg.annotation_literal, cfg.annotation_generated,
        # Project-specific legacy markers configured in .osmosis (legacy-strip-markers)
        *cfg.legacy_strip_markers,
    ):
        idx = description.find(marker)
        if idx != -1:
            description = description[:idx].rstrip()
    return description
