# dbt-osmosis-cll

A single, installable, **lineage-aware dbt YAML documentation engine**. It fuses
two upstream forks into one package: **dbt-osmosis** (YAML stability) drives the
workflow, and an embedded **column-level-lineage** resolver gives it true
column-level lineage to propagate and annotate documentation from. Tested against
Snowflake.

| Fused from | Upstream | Lives in |
|---|---|---|
| dbt-osmosis | [z3z1ma/dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis) | `osmosis_propagation/` + `integration/` |
| dbt-column-lineage | [Fszta/dbt-column-lineage](https://github.com/Fszta/dbt-column-lineage) | `cll_generator/` (vendored) |

> Not a drop-in replacement for upstream `dbt-osmosis`: the config (`.osmosis-cll`),
> CLI (`dbt-osmosis-cll`), and behavior have diverged. Attribution + licenses are in
> [`NOTICE`](NOTICE) and [`licenses/`](licenses).

## How it works

On its own, osmosis propagates documentation *structurally* — parent → child by
matching column name. This package makes propagation **lineage-aware** by embedding
a CLL resolver into the pipeline:

1. **Lineage resolution** (`cll_generator/`) — parses each model's compiled SQL to
   trace every output column to its source column(s) and classify it: a pure
   rename, a single-source derivation, a multi-source computation, an
   aggregate/window/union/literal, or a generated value.
2. **Integration** (`integration/`) — a manifest-only `ManifestCatalogReader` (reads
   column lists from `manifest.json`, no `catalog.json`/warehouse round-trip), a
   compiled-SQL hash cache, and the bridge that feeds lineage to the engine.
3. **Propagation & annotation** (`osmosis_propagation/`) —
   - descriptions flow along the *real* lineage chain, not just by name
     (`desc-owner`: `upstream` force-inherits; any anchor value protects a local
     description and only gap-fills downstream);
   - origins are tagged (renamed / derived-from / computed-here) under a
     **configurable annotation namespace**, per layer (`if_altered`, `always`, off);
   - a central **column glossary** (`column-docs-path`) gives audit/technical
     columns an **authoritative** description — written on every run, overwriting
     existing text. Edit once, propagate everywhere.

The single-pass refactor pipeline:

```
inject_missing_columns → remove_extra_columns
  → inherit_upstream_column_knowledge (CLL) → annotate_column_origins
  → sort_columns → synchronize_data_types
```

## Setup

```bash
# Install the tool (and its console script `dbt-osmosis-cll`)
pip install git+https://github.com/Felix313/dbt-osmosis-cll.git

# Or for local development
uv sync          # or: pip install -e .
```

Full usage and the configuration reference live in [`docs/USAGE.md`](docs/USAGE.md).

## Structure

```
src/dbt_osmosis_cll/
  cli/                 # entry point (the `dbt-osmosis-cll` command)
  config.py            # .osmosis-cll settings loader
  cll_generator/       # PILLAR 1 — generate column-level lineage (vendored resolver)
  integration/         # PILLAR 2 — cll cache/bridge + SQL proxy
  osmosis_propagation/ # PILLAR 3 — inherit, annotate, glossary, schema I/O, enrichment
                       #            (+ commands/ for generate, diff, lint, workbench, …)
```

The repo root is the installable package (src-layout, `hatchling`).

## Relationship to upstream

Both upstreams are fused here as one package so they version and ship together.
Significant divergences:

- **dbt-osmosis** — column-level-lineage integration (`integration/cll.py`,
  `ManifestCatalogReader` wiring), lineage-aware inheritance and origin annotation
  (`osmosis_propagation/inheritance.py`, `osmosis_propagation/transforms.py`), the
  authoritative column glossary, and assorted cache / idempotency fixes.
- **dbt-column-lineage** — `ManifestCatalogReader`, which sources column lists from
  `manifest.json` so lineage resolves without a warehouse catalog; vendored under
  `cll_generator/` with namespace changes only.
