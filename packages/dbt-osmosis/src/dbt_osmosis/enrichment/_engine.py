"""
Generic YAML enrichment engine.

Orchestrates the collect → fetch → merge → write pipeline without any
knowledge of where descriptions come from.  The caller supplies a
``DescriptionFetcher`` implementation that connects to their data source.
"""
from __future__ import annotations

import re
import typing as t
from pathlib import Path

import yaml

from ._merge import DescriptionFetcher, merge_description
from ._yaml import render_model_yml, _DEFAULT_MAX_LINE_WIDTH


def enrich_yaml_files(
    yml_paths: list[Path],
    fetcher: DescriptionFetcher,
    *,
    anchor_meta_key: str = "anchor-description",
    replaceable_pattern: re.Pattern[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    max_line_width: int = _DEFAULT_MAX_LINE_WIDTH,
) -> dict[Path, int]:
    """
    Enrich dbt model YAML files with descriptions from an external source.

    The pipeline:

    1. **Collect** — scan all ``yml_paths`` for columns that are enrichable
       (empty description, or description fully matched by ``replaceable_pattern``).
    2. **Fetch** — call ``fetcher.fetch(column_names)`` once with all unique names
       (batching is the fetcher's responsibility).
    3. **Merge** — apply :func:`merge_description` idempotency rules per column.
    4. **Write** — update YAML files and set ``meta.<anchor_meta_key>: true`` on
       enriched columns so osmosis won't overwrite them during propagation.

    Args:
        yml_paths: List of YAML file paths to process.
        fetcher: Provider of external descriptions.
        anchor_meta_key: ``meta`` key set on enriched columns to protect them
            from osmosis overwrite.  Must match the ``protected-meta-keys``
            setting in your ``dbt_project.yml`` (or equivalent osmosis config).
        replaceable_pattern: Regex whose ``fullmatch`` against an existing
            description marks it as auto-generated and safe to replace.
        dry_run: Preview changes without writing files.
        verbose: Print per-column detail.
        max_line_width: Total line width for description word-wrapping.

    Returns:
        Mapping of ``{yml_path: number_of_columns_updated}``.
    """
    # ── Step 1: collect enrichable columns ───────────────────────────────────
    work_items: list[dict[str, t.Any]] = []
    col_names_needed: set[str] = set()

    for yml_path in yml_paths:
        try:
            data = yaml.safe_load(yml_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            print(f"  [WARN] Could not parse {yml_path.name}: {exc}")
            continue

        for model_entry in data.get("models", []):
            model_name = model_entry.get("name", "")
            for col_entry in model_entry.get("columns", []):
                col_name = (col_entry.get("name") or "").strip().upper()
                existing = (col_entry.get("description") or "").strip()
                # Skip if already anchored (protected by a previous enrichment run)
                if col_entry.get("meta", {}).get(anchor_meta_key):
                    continue
                # Skip if there's manual content that we can't auto-replace
                if existing and not (
                    replaceable_pattern is not None
                    and replaceable_pattern.fullmatch(existing)
                ):
                    continue
                col_names_needed.add(col_name)
                work_items.append({
                    "yml_path":   yml_path,
                    "model_name": model_name,
                    "col_name":   col_name,
                    "existing":   existing,
                })

    print(f"{len(work_items)} enrichable column slot(s) across {len(col_names_needed)} unique name(s).")
    if not work_items:
        print("Nothing to do.")
        return {}

    # ── Step 2: fetch descriptions (single call — batching inside fetcher) ───
    canonical = fetcher.fetch(sorted(col_names_needed))
    print(f"  Got descriptions for {len(canonical)}/{len(col_names_needed)} column name(s).")

    # ── Step 3: build update plan ─────────────────────────────────────────────
    updates: dict[Path, dict[tuple[str, str], str]] = {}
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
        )
        if new_desc is None:
            stats["skipped"] += 1
            if verbose:
                print(f"  [SKIP] {item['model_name']}.{item['col_name']}: no change / manual docs preserved")
            continue

        if verbose:
            print(f"  [UPDATE] {item['model_name']}.{item['col_name']}")

        updates.setdefault(item["yml_path"], {})[(item["model_name"], item["col_name"])] = new_desc
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

        for model_entry in data.get("models", []):
            model_name = model_entry.get("name", "")
            for col_entry in model_entry.get("columns", []):
                col_name = (col_entry.get("name") or "").strip().upper()
                new_desc = col_updates.get((model_name, col_name))
                if new_desc is not None:
                    if dry_run:
                        old = col_entry.get("description", "<none>")
                        print(f"\n[DRY RUN] {yml_path.name}  ->  {col_name}")
                        print(f"  OLD: {old!r}")
                        print(f"  NEW: {new_desc!r}")
                        print(f"  META: {anchor_meta_key}: true")
                    else:
                        col_entry["description"] = new_desc
                        col_entry.setdefault("meta", {})[anchor_meta_key] = True

        if not dry_run:
            yml_path.write_text(
                render_model_yml(data, max_line_width=max_line_width),
                encoding="utf-8",
            )
            print(f"  Wrote {yml_path.name} ({len(col_updates)} column(s) updated)")
            results[yml_path] = len(col_updates)

    if dry_run:
        print(f"\n[DRY RUN] No files written. Remove --dry-run to apply.")
    else:
        print(f"\nDone. Updated {len(results)} file(s).")

    return results
