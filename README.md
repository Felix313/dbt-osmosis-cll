# dbt-osmosis-cll

Internal monorepo combining two forks:

| Package | Source | Description |
|---------|--------|-------------|
| `dbt-osmosis` | [z3z1ma/dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis) | dbt YAML management utility |
| `dbt-col-lineage` | [Fszta/dbt-column-lineage](https://github.com/Fszta/dbt-column-lineage) | Column-level lineage for dbt |

## Custom changes vs upstream

**dbt-osmosis (`yaml-stability` branch):**
- `src/dbt_osmosis/core/cll.py` — CLL integration, ManifestCatalogReader, cache poisoning fix
- `src/dbt_osmosis/core/inheritance.py` — CLL-first column propagation, CBM-ODP annotations
- `src/dbt_osmosis/core/column_level_lineage.py` — CLL API server management

**dbt-col-lineage:**
- `dbt_column_lineage/artifacts/manifest_catalog.py` — `ManifestCatalogReader`: reads column lists from `manifest.json` (YAML-documented columns only, no catalog.json required)

## Setup

```bash
# From this repo root — installs both packages in editable mode
uv sync

# Or install into an existing venv
pip install -e packages/dbt-osmosis -e packages/dbt-col-lineage
```

## Structure

```
packages/
  dbt-osmosis/       → dbt_osmosis Python package (src/ layout)
  dbt-col-lineage/   → dbt_column_lineage Python package (flat layout)
```
