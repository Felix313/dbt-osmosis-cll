"""
Description merge logic for external enrichment sources.

Provides a configurable, idempotent merge that decides whether an external
description should overwrite an existing YAML column description.
"""

from __future__ import annotations

import re
import typing as t


class DescriptionFetcher(t.Protocol):
    """
    Implement this protocol to connect any external metadata source to the
    dbt-osmosis enrichment pipeline.

    The ``fetch`` method receives a list of UPPER-CASE column names and must
    return a mapping of ``COLUMN_NAME_UPPER → description string`` for those
    it was able to resolve.  Columns with no description should simply be
    absent from the returned dict.

    Example::

        class MyFetcher(DescriptionFetcher):
            def fetch(self, column_names: list[str]) -> dict[str, str]:
                rows = my_db.query(
                    "SELECT col, comment FROM metadata WHERE col IN (?)",
                    column_names,
                )
                return {r["col"].upper(): r["comment"] for r in rows}
    """

    def fetch(self, column_names: list[str]) -> dict[str, str]:
        """Return ``{COLUMN_NAME_UPPER: description}`` for the given column names."""
        ...


def merge_description(
    existing: str | None,
    new_desc: str,
    *,
    replaceable_pattern: re.Pattern[str] | None = None,
    force: bool = False,
) -> str | None:
    """
    Return the merged description string, or ``None`` if no change is needed.

    Rules (applied in order):

    A. No existing description → ``new_desc``.
    B. Existing fully matches ``replaceable_pattern`` (e.g. an osmosis-propagated
       reference) → ``new_desc`` (the external source wins over auto-generated text).
    C. ``new_desc`` is already present at the start of the existing description
       → ``None`` (idempotent, already enriched).
    D. Any other non-empty existing description:
       - ``force=False`` → ``None`` (preserve manual docs).
       - ``force=True``  → ``new_desc`` (external source is authoritative / leading).

    Args:
        existing: Current description from the YAML (may be ``None`` or empty).
        new_desc: Description coming from the external source.
        replaceable_pattern: Optional compiled regex.  When the *entire* existing
            description (stripped) matches this pattern it is treated as
            auto-generated and safe to replace.  Configure this to match whatever
            osmosis writes as provenance text in your project (e.g. the prefix
            your propagation annotations use).
        force: When ``True``, rule D overwrites instead of preserving.  Use when
            the external metadata layer is the single source of truth (e.g. AML
            for staging models).
    """
    raw = (existing or "").strip()

    if not raw:
        return new_desc  # A — nothing there yet

    if replaceable_pattern is not None and replaceable_pattern.fullmatch(raw):
        return new_desc  # B — was auto-generated, safe to replace

    if raw.startswith(new_desc.strip()):
        return None  # C — already injected (idempotent)

    if force:
        return new_desc  # D (force) — external source is leading

    return None  # D — manual / unknown content, preserve it
