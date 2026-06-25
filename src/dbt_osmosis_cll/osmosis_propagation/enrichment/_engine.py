"""
Generic YAML enrichment engine.

Orchestrates the collect → fetch → merge → write pipeline without any
knowledge of where descriptions come from.  The caller supplies a
``DescriptionFetcher`` implementation that connects to their data source.
"""

from __future__ import annotations

import fnmatch
import re
import typing as t
from pathlib import Path

import yaml

from ._merge import DescriptionFetcher, merge_description
from ._yaml import render_model_yml, _DEFAULT_MAX_LINE_WIDTH


# ── Anchor meta helpers (dbt >= 1.10: column/model meta lives under config.meta) ──
# Writes always target config.meta; reads also accept legacy top-level meta so YAMLs
# written by older runs still resolve. This keeps output dbt-1.10-correct and avoids
# the churn of osmosis later relocating a top-level meta key into config.meta.


def _read_anchor(entry: dict[str, t.Any], anchor_meta_key: str) -> t.Any:
    """Read the anchor value from config.meta, falling back to legacy top-level meta."""
    config = entry.get("config")
    if isinstance(config, dict):
        config_meta = config.get("meta")
        if isinstance(config_meta, dict) and anchor_meta_key in config_meta:
            return config_meta[anchor_meta_key]
    meta = entry.get("meta")
    if isinstance(meta, dict):
        return meta.get(anchor_meta_key)
    return None


def _set_anchor(entry: dict[str, t.Any], anchor_meta_key: str, anchor_value: t.Any) -> None:
    """Write the anchor under config.meta (dbt 1.10+); drop any stale legacy top-level copy."""
    config = entry.setdefault("config", {})
    config.setdefault("meta", {})[anchor_meta_key] = anchor_value
    legacy = entry.get("meta")
    if isinstance(legacy, dict) and anchor_meta_key in legacy:
        legacy.pop(anchor_meta_key)
        if not legacy:
            entry.pop("meta", None)


def _strip_anchor(entry: dict[str, t.Any], anchor_meta_key: str) -> bool:
    """Remove the anchor from both config.meta and legacy meta. True if anything changed."""
    changed = False
    config = entry.get("config")
    if isinstance(config, dict):
        config_meta = config.get("meta")
        if isinstance(config_meta, dict) and anchor_meta_key in config_meta:
            config_meta.pop(anchor_meta_key)
            changed = True
            if not config_meta:
                config.pop("meta", None)
            if not config:
                entry.pop("config", None)
    legacy = entry.get("meta")
    if isinstance(legacy, dict) and anchor_meta_key in legacy:
        legacy.pop(anchor_meta_key)
        changed = True
        if not legacy:
            entry.pop("meta", None)
    return changed


def enrich_yaml_files(
    yml_paths: list[Path],
    fetcher: DescriptionFetcher,
    *,
    anchor_meta_key: str = "desc-owner",
    anchor_value: bool | str = True,
    frozen_values: frozenset[bool | str] = frozenset({True}),
    replaceable_pattern: re.Pattern[str] | None = None,
    force: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    max_line_width: int = _DEFAULT_MAX_LINE_WIDTH,
    model_anchor_globs: list[str] | None = None,
    skip_columns: frozenset[str] | None = None,
) -> dict[Path, int]:
    """
    Enrich dbt model YAML files with descriptions from an external source.

    The pipeline:

    1. **Collect** — scan all ``yml_paths`` for columns that are enrichable.
       By default: empty description, or description fully matched by
       ``replaceable_pattern``.  With ``force=True``: all non-anchored columns,
       regardless of existing content (the external source is treated as leading).
    2. **Fetch** — call ``fetcher.fetch(column_names)`` once with all unique names
       (batching is the fetcher's responsibility).
    3. **Merge** — apply :func:`merge_description` idempotency rules per column.
    4. **Write** — update YAML files.  For models matching ``model_anchor_globs``,
       set ``config.meta.<anchor_meta_key>: <anchor_value>`` once at model level
       (not per column). For all other enriched columns, set the anchor on the
       column entry.  Anchors are written under ``config.meta`` (dbt >= 1.10);
       legacy top-level ``meta`` copies are read for back-compat and removed.

    Args:
        yml_paths: List of YAML file paths to process.
        fetcher: Provider of external descriptions.
        anchor_meta_key: ``config.meta`` key set on enriched columns to protect
            them from osmosis overwrite (e.g. ``desc-owner``).
        anchor_value: Value written to ``config.meta.<anchor_meta_key>`` for columns
            enriched by this script (e.g. ``"aml"`` or ``"psa"``).  Default
            ``True`` preserves backward-compatible behaviour.
        frozen_values: Set of ``config.meta.<anchor_meta_key>`` values that cause a
            column (or model) to be skipped entirely.  Defaults to
            ``frozenset({True})`` — only developer-frozen columns are skipped.
            Pass e.g. ``frozenset({True, "aml"})`` from a PSA script so it
            never overwrites AML-owned descriptions.
        replaceable_pattern: Regex whose ``fullmatch`` against an existing
            description marks it as auto-generated and safe to replace.
        force: When ``True``, the external source is treated as authoritative
            for all non-frozen columns.  Existing non-empty descriptions are
            overwritten (idempotent when identical).
        dry_run: Preview changes without writing files.
        verbose: Print per-column detail.
        max_line_width: Total line width for description word-wrapping.
        model_anchor_globs: Shell-style glob patterns (e.g. ``["STG_MY_SOURCE__*"]``).
            Matching models get ``config.meta.<anchor_meta_key>: <anchor_value>``
            written once at model level instead of per column — keeps YAML clean
            while blocking osmosis downstream propagation equally effectively.
        skip_columns: Uppercase column names to skip entirely — no enrichment and
            no anchor tag written.  Intended for columns managed centrally by
            osmosis (e.g. ``osmosis_column_references.yml`` entries such as
            ``ROW_BATCH_TIMESTAMP``), where an external system should never touch
            the description or set anchor flags.

    Returns:
        Mapping of ``{yml_path: number_of_columns_updated}``.
    """

    def _is_model_anchored(model_name: str) -> bool:
        """Return True if this model should receive a model-level anchor."""
        if not model_anchor_globs:
            return False
        return any(fnmatch.fnmatch(model_name, g) for g in model_anchor_globs)

    # ── Step 1: collect enrichable columns ───────────────────────────────────
    work_items: list[dict[str, t.Any]] = []
    col_names_needed: set[str] = set()
    # Track all yml files that contain model-anchor models for cleanup pass,
    # regardless of whether those models have any columns to update this run.
    model_anchor_files: dict[Path, set[str]] = {}

    for yml_path in yml_paths:
        try:
            data = yaml.safe_load(yml_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            print(f"  [WARN] Could not parse {yml_path.name}: {exc}")
            continue

        for model_entry in data.get("models", []):
            model_name = model_entry.get("name", "")
            if _is_model_anchored(model_name):
                model_anchor_files.setdefault(yml_path, set()).add(model_name)
            # Skip column collection if model-level anchor is in frozen_values —
            # e.g. True (developer-frozen) or a peer system's anchor ("aml").
            if _read_anchor(model_entry, anchor_meta_key) in frozen_values:
                if verbose:
                    print(f"  [SKIP] {model_name}: model-level anchor set, skipping all columns")
                continue
            for col_entry in model_entry.get("columns", []):
                col_name = (col_entry.get("name") or "").strip().upper()
                existing = (col_entry.get("description") or "").strip()
                # Skip columns owned by the central osmosis reference file — these
                # are managed by osmosis itself and must not receive external anchors.
                if skip_columns and col_name in skip_columns:
                    continue
                # Skip columns whose anchor is in frozen_values (developer-frozen or peer-system-owned).
                if _read_anchor(col_entry, anchor_meta_key) in frozen_values:
                    continue
                # In default mode: only enrich empty / auto-generated descriptions.
                # In force mode: enrich all non-anchored columns (external source is leading).
                if (
                    not force
                    and existing
                    and not (
                        replaceable_pattern is not None and replaceable_pattern.fullmatch(existing)
                    )
                ):
                    continue
                col_names_needed.add(col_name)
                work_items.append({
                    "yml_path": yml_path,
                    "model_name": model_name,
                    "col_name": col_name,
                    "existing": existing,
                    "model_anchor": _is_model_anchored(model_name),
                })

    print(
        f"{len(work_items)} enrichable column slot(s) across {len(col_names_needed)} unique name(s)."
    )
    if not work_items:
        print("Nothing to do.")
        return {}

    # ── Step 2: fetch descriptions (single call — batching inside fetcher) ───
    canonical = fetcher.fetch(sorted(col_names_needed))
    print(f"  Got descriptions for {len(canonical)}/{len(col_names_needed)} column name(s).")

    # ── Step 3: build update plan ─────────────────────────────────────────────
    updates: dict[Path, dict[tuple[str, str], str]] = {}
    # Track which models should receive a model-level anchor on write
    model_level_anchor_models: dict[Path, set[str]] = {}
    stats = {"updated": 0, "no_desc": 0, "skipped": 0}

    for item in work_items:
        ext_desc = canonical.get(item["col_name"])
        if not ext_desc:
            stats["no_desc"] += 1
            if verbose:
                print(f"  [SKIP] {item['col_name']}: not in fetcher result")
            continue

        new_desc = merge_description(
            item["existing"],
            ext_desc,
            replaceable_pattern=replaceable_pattern,
            force=force,
        )
        if new_desc is None:
            if force:
                # Description is identical to AML — no text change needed, but in force mode
                # AML is the authoritative source, so still anchor the column to prevent
                # osmosis from overwriting it with an upstream description.
                new_desc = item["existing"]
                if verbose:
                    print(
                        f"  [ANCHOR] {item['model_name']}.{item['col_name']}: desc unchanged, anchoring as AML-owned"
                    )
            else:
                stats["skipped"] += 1
                if verbose:
                    print(
                        f"  [SKIP] {item['model_name']}.{item['col_name']}: no change / manual docs preserved"
                    )
                continue

        if verbose:
            print(f"  [UPDATE] {item['model_name']}.{item['col_name']}")

        updates.setdefault(item["yml_path"], {})[(item["model_name"], item["col_name"])] = new_desc
        if item["model_anchor"]:
            model_level_anchor_models.setdefault(item["yml_path"], set()).add(item["model_name"])
        stats["updated"] += 1

    print(f"\n  -> {stats['updated']} column(s) to update")
    print(f"  -> {stats['no_desc']} skipped (not found in external source)")
    print(f"  -> {stats['skipped']} skipped (no change / manual docs preserved)")

    if not updates:
        print("Nothing to write.")
        return {}

    # ── Step 4: apply updates ─────────────────────────────────────────────────
    results: dict[Path, int] = {}

    for yml_path, col_updates in sorted(updates.items()):
        data = yaml.safe_load(yml_path.read_text(encoding="utf-8")) or {}
        anchored_models = model_level_anchor_models.get(yml_path, set())

        for model_entry in data.get("models", []):
            model_name = model_entry.get("name", "")
            use_model_anchor = model_name in anchored_models

            for col_entry in model_entry.get("columns", []):
                col_name = (col_entry.get("name") or "").strip().upper()
                new_desc = col_updates.get((model_name, col_name))
                if new_desc is not None:
                    if dry_run:
                        old = col_entry.get("description", "<none>")
                        anchor_note = (
                            "model-level"
                            if use_model_anchor
                            else f"column config.meta.{anchor_meta_key}"
                        )
                        print(f"\n[DRY RUN] {yml_path.name}  ->  {col_name}")
                        print(f"  OLD: {old!r}")
                        print(f"  NEW: {new_desc!r}")
                        print(f"  ANCHOR: {anchor_note}")
                    else:
                        col_entry["description"] = new_desc
                        if not use_model_anchor:
                            # Per-column anchor — marks this column as owned by the enrichment source
                            _set_anchor(col_entry, anchor_meta_key, anchor_value)

                # When the model gets a model-level anchor, strip per-column anchor
                # from ALL columns (not just ones updated this run) — keeps YAML clean
                # and avoids stale per-column anchors from previous runs accumulating.
                if use_model_anchor and not dry_run:
                    _strip_anchor(col_entry, anchor_meta_key)

            # Write model-level ownership anchor once for models that warrant it
            if use_model_anchor and not dry_run:
                _set_anchor(model_entry, anchor_meta_key, anchor_value)

        if not dry_run:
            yml_path.write_text(
                render_model_yml(data, max_line_width=max_line_width),
                encoding="utf-8",
            )
            print(f"  Wrote {yml_path.name} ({len(col_updates)} column(s) updated)")
            results[yml_path] = len(col_updates)

    # ── Step 4b: cleanup pass for model-anchor files not written above ────────
    # Handles models that had all columns anchored per-column in a previous run
    # (so nothing was collected in Step 1) but still need per-column anchors
    # stripped and model-level anchor set.
    already_written = set(updates.keys())
    for yml_path, anchored_model_names in sorted(model_anchor_files.items()):
        if yml_path in already_written:
            continue  # already cleaned in Step 4 above
        try:
            data = yaml.safe_load(yml_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            print(f"  [WARN] Could not parse {yml_path.name} for cleanup: {exc}")
            continue
        changed = False
        for model_entry in data.get("models", []):
            if model_entry.get("name", "") not in anchored_model_names:
                continue
            # Ensure model-level ownership anchor is present (also upgrades legacy True → anchor_value)
            if _read_anchor(model_entry, anchor_meta_key) != anchor_value:
                _set_anchor(model_entry, anchor_meta_key, anchor_value)
                changed = True
            # Strip stale per-column anchors — model-level anchor supersedes them
            for col_entry in model_entry.get("columns", []):
                if _strip_anchor(col_entry, anchor_meta_key):
                    changed = True
        if changed and not dry_run:
            yml_path.write_text(
                render_model_yml(data, max_line_width=max_line_width),
                encoding="utf-8",
            )
            print(f"  Cleaned {yml_path.name} (model-level anchor, per-column anchors removed)")

    if dry_run:
        print("\n[DRY RUN] No files written. Remove --dry-run to apply.")
    else:
        print(f"\nDone. Updated {len(results)} file(s).")

    return results
