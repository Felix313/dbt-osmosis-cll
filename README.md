# dbt-osmosis-cll

An internal monorepo that fuses two dbt tooling forks into a single,
lineage-aware documentation engine: **dbt-osmosis** (YAML stability) drives the
workflow, and an embedded **dbt-column-lineage** (CLL) gives it true
column-level lineage to propagate and annotate documentation from.

| Package | Forked from | Role here |
|---------|-------------|-----------|
| [`packages/dbt-osmosis`](packages/dbt-osmosis) | [z3z1ma/dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis) | Orchestrates dbt YAML: injects columns, inherits/owns descriptions, annotates origins, sorts columns, syncs data types |
| [`packages/dbt-col-lineage`](packages/dbt-col-lineage) | [Fszta/dbt-column-lineage](https://github.com/Fszta/dbt-column-lineage) | Resolves column-level lineage (CLL) from compiled SQL |

## How the two work together

On its own, osmosis propagates documentation *structurally* — parent → child by
matching column name. This fork makes propagation **lineage-aware** by embedding
CLL into the pipeline:

1. **Lineage resolution** — CLL parses each model's compiled SQL to trace every
   output column to its source column(s) and classify it: a pure rename, a
   single-source derivation, a multi-source computation, an
   aggregate/window/union/literal, or a generated value. A `ManifestCatalogReader`
   lets CLL read column lists straight from `manifest.json` (YAML-documented
   columns only) — no `catalog.json` or warehouse round-trip required.
2. **Description inheritance** — descriptions flow along the *real* lineage chain,
   not just by name. `desc-owner` decides who owns a column's text: `upstream`
   force-inherits; any other (anchor) value protects a locally authored
   description and only gap-fills downstream.
3. **Origin annotation** — a column can be tagged with where its value came from
   (renamed / derived-from / computed-here) under a **configurable annotation
   namespace**, controlled per layer (`if_altered`, `always`, or off).
4. **Central column glossary** — `column-docs-path` defines canonical descriptions
   for audit/technical columns that recur in every model. Listed columns are
   CLL-ignored, and the glossary description is **authoritative**: osmosis writes
   it on every run, overwriting any existing text. Edit once, propagate everywhere.

The single-pass refactor pipeline:

```
inject_missing_columns → remove_extra_columns
  → inherit_upstream_column_knowledge (CLL) → annotate_column_origins
  → sort_columns → synchronize_data_types
```

## Setup

```bash
# From this repo root — installs both packages in editable mode
uv sync

# Or into an existing venv
pip install -e packages/dbt-osmosis -e packages/dbt-col-lineage
```

Per-package usage and the full configuration reference live in
[`packages/dbt-osmosis/README.md`](packages/dbt-osmosis/README.md).

## Structure

```
packages/
  dbt-osmosis/       → dbt_osmosis package (src/ layout); CLL integration + config live here
  dbt-col-lineage/   → dbt_column_lineage package (flat layout); the lineage resolver
```

The repo root is a `uv` workspace (`[tool.uv.workspace]`) and is not itself an
installable package.

## Relationship to upstream

Both packages are forks, vendored here as one workspace so they version and ship
together. The significant divergences from upstream:

- **dbt-osmosis** — column-level-lineage integration (`core/cll.py`,
  `ManifestCatalogReader` wiring), lineage-aware inheritance and origin annotation
  (`core/inheritance.py`, `core/transforms.py`), the authoritative column glossary,
  and assorted cache / idempotency fixes.
- **dbt-column-lineage** — `ManifestCatalogReader`, which sources column lists from
  `manifest.json` so lineage resolves without a warehouse catalog.
