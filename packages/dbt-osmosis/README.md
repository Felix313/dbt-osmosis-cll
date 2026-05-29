# dbt-osmosis

![PyPI](https://img.shields.io/pypi/v/dbt-osmosis)
[![Downloads](https://static.pepy.tech/badge/dbt-osmosis)](https://pepy.tech/project/dbt-osmosis)
![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green.svg)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://dbt-osmosis-playground.streamlit.app/)

`dbt-osmosis` is a Python CLI and package for dbt development workflows.

It centers on four primary surfaces:

- schema YAML management (`yaml organize`, `yaml document`, `yaml refactor`)
- column-level documentation inheritance across dbt lineage
- ad-hoc SQL compile/run helpers
- an optional Streamlit workbench for interactive dbt SQL development

The repository also ships additional command families for generation, natural-language helpers, schema diffing, SQL linting, and test suggestions.

The Docusaurus site is the canonical reference for the current CLI, configuration model, support matrix, and workflow guides:

- Docs site: https://z3z1ma.github.io/dbt-osmosis/
- CLI reference: https://z3z1ma.github.io/dbt-osmosis/docs/reference/cli
- Configuration guide: https://z3z1ma.github.io/dbt-osmosis/docs/tutorial-yaml/configuration
- Migration guide: https://z3z1ma.github.io/dbt-osmosis/docs/migrating

[![dbt-osmosis](/screenshots/docs_site.png)](https://z3z1ma.github.io/dbt-osmosis/)

## Supported runtime

`dbt-osmosis` currently targets:

- Python 3.10-3.13
- dbt Core 1.8+
- a dbt adapter version compatible with the dbt Core runtime in that environment

Repository-managed DuckDB fixture coverage is explicitly exercised through the published DuckDB-backed matrix in CI today (1.8-1.10) plus a latest-core compatibility job that runs basedpyright, `dbt parse`, and the full pytest suite under `dbt-core` 1.11 with the latest published `dbt-duckdb` adapter (currently 1.10.1). Package metadata and install paths are not capped at dbt Core 1.10.

Optional extras:

- `dbt-osmosis[workbench]` for the Streamlit workbench and related UI dependencies
- `dbt-osmosis[openai]` for LLM-assisted synthesis and natural-language generation features

## Install

With `uv`:

```bash
uv tool install --with="dbt-<adapter>" dbt-osmosis
```

With `pip`:

```bash
pip install "dbt-osmosis" "dbt-<adapter>"
```

Replace `<adapter>` with your dbt adapter package, for example `duckdb`, `snowflake`, `bigquery`, `postgres`, or `redshift`.

## Quick start

1. Configure YAML routing in `dbt_project.yml`:

```yaml title="dbt_project.yml"
models:
  your_project_name:
    +dbt-osmosis: "_{model}.yml"
```

2. Optionally set per-folder behavior with `+dbt-osmosis-options` and a repo-level YAML formatter in `dbt-osmosis.yml`:

```yaml title="dbt-osmosis.yml"
formatter: "prettier --write"
```

3. Preview changes safely:

```bash
dbt-osmosis yaml refactor --dry-run --check
```

4. Apply the update once the diff looks right:

```bash
dbt-osmosis yaml refactor --auto-apply
```

## CLI surface

Top-level commands currently exposed by `dbt-osmosis --help`:

- `yaml` — manage schema YAML files and documentation inheritance
- `sql` — compile or run ad-hoc SQL in dbt context
- `workbench` — launch the Streamlit workbench
- `generate` — generate sources, staging models, models, and SQL
- `nl` — natural-language query/model helpers
- `test` — suggest dbt tests
- `test-llm` — validate LLM client configuration
- `diff` — report schema drift between YAML and the database
- `lint` — lint SQL strings, models, or a whole project

For command-by-command flags and examples, use the docs-site CLI reference rather than relying on this landing page.

## Developer tooling

Local development in this repository is built around `uv`, `task`, and Ruff.

Common workflows:

```bash
task format
task lint
task test
```

Notes:

- Ruff is the active formatter, linter, and import sorter.
- `task` is not just verification; the default task formats, lints, runs tests, and then ensures the dev environment is synced.
- Repository test fixtures are DuckDB-only today; contributor examples use `demo_duckdb`, and targeted core tests may need `uv run dbt parse --project-dir demo_duckdb --profiles-dir demo_duckdb -t test` to refresh `demo_duckdb/target/manifest.json`.
- Docs-site commands use the Node toolchain under `docs/`:

```bash
npm --prefix docs run start
npm --prefix docs run build
npm --prefix docs run serve
```

## Workbench

The optional workbench is a Streamlit app for interactive dbt SQL development.

Install the extra and launch it with:

```bash
pip install "dbt-osmosis[workbench]"
dbt-osmosis workbench
```

The hosted demo is linked from the badge at the top of this README.

## Pre-commit hook

You can run `dbt-osmosis yaml refactor -C` as a pre-commit hook:

```yaml title=".pre-commit-config.yaml"
repos:
  - repo: https://github.com/z3z1ma/dbt-osmosis
    rev: v1.3.0
    hooks:
      - id: dbt-osmosis
        files: ^models/
        args: [--target=prod]
        additional_dependencies: [dbt-<adapter>]
```

That hook keeps schema YAML changes visible in the commit that introduced them.

## Column-level lineage (CLL) configuration

This fork extends dbt-osmosis with column-level lineage tracing and a project-level `.osmosis` configuration file.

### `.osmosis` — project config file

Place a `.osmosis` file in your dbt project root (next to `dbt_project.yml`). **All settings are optional** — every key defaults to the value shown below, so you only need to list the ones you want to override.

```ini
[osmosis]
# Only override what differs from the defaults below.
annotation-namespace = MY-ORG
column-docs-path     = docs/osmosis_column_references.yml
yaml-best-width      = 150
```

#### Package-level options (`.osmosis [osmosis]`)

These apply globally across the project.

| Option | Default | Description |
|---|---|---|
| `annotation-renamed` | `Renamed from:` | Prefix for a pure rename (followed by `MODEL.COL`). |
| `annotation-derived` | `Derived from:` | Single-source computed expression (CAST, function…). |
| `annotation-computed` | `Computed in:` | Multi-source / opaque expression (no single source). |
| `annotation-aggregate-from` | `Aggregated from` | Single-source aggregate (followed by `MODEL.COL`). |
| `annotation-aggregate-in` | `Aggregated in:` | Aggregate with no single source (`COUNT(*)`, `SUM(a+b)`). |
| `annotation-window-from` | `Window from` | Window over one traceable column (followed by `MODEL.COL`). |
| `annotation-window-in` | `Window in:` | Window with no traceable column (`ROW_NUMBER()`…). |
| `annotation-union` | `UNION in:` | Top-level UNION / UNION ALL / INTERSECT / EXCEPT column. |
| `annotation-literal` | `Literal` | Hard-coded constant column. |
| `annotation-generated` | `Generated` | Zero-arg system function (`CURRENT_DATE`, `UUID_STRING`…). |
| `annotation-namespace` | `OSMOSIS` | Namespace label in the annotation block header. |
| `annotation-separator` | `__________` | Visual separator line above the annotation block. |
| `legacy-strip-markers` | _(empty)_ | Comma-separated legacy tag prefixes to strip from descriptions. |
| `anchor-meta-key` | `anchor-description` | Meta key that pins a column description (skip propagation). |
| `cll-cache-path` | `target/cll_cache.json` | On-disk CLL cache, relative to the project root. |
| `cll-max-origin-depth` | `60` | Max model hops when tracing a column to its origin (cycle guard). |
| `column-docs-path` | _(empty)_ | Flat YAML mapping `COLUMN → description`, auto-ignored by CLL (see below). |
| `compiled-sql-placeholder-patterns` | _(none)_ | Newline-separated regexes for verbatim SQL placeholders, replaced with `TRUE` before parsing (`__dbt__cte__*` is always excluded). |
| `inherit-through-renames` | `false` | Follow renames when inheriting descriptions (set `true` for staging). |
| `yaml-best-width` | `0` | Max YAML line width (`0` = ruamel default, 80). |
| `write-cll-tags-to-meta` | `false` | Also write machine-readable origin meta tags (see below). |
| `col-renamed-from` | `renamed_from` | Meta key for pure renames (`TABLE.COLUMN`). |
| `col-derived-from` | `derived_from` | Meta key for single-source computed columns (`TABLE.COLUMN`). |
| `col-computed-in` | `computed_in` | Meta key for multi-source / opaque columns (`SCHEMA.MODEL`). |

#### Node-level options (`dbt_project.yml` → `+dbt-osmosis-options`)

These can vary per model/source path via dbt config inheritance.

| Option | Default | Description |
|---|---|---|
| `output-to-upper` / `output-to-lower` | `false` | Force column names and types to upper/lowercase. |
| `numeric-precision-and-scale` | `false` | Include numeric precision/scale in data types. |
| `string-length` | `false` | Include character length in data types. |
| `skip-add-columns` | `false` | Do not add missing columns to any YAML. |
| `skip-add-source-columns` | `false` | Do not add missing columns to source YAMLs. |
| `skip-add-data-types` | `false` | Do not write data types to column entries. |
| `skip-add-tags` | `false` | Do not append upstream tags during inheritance. |
| `skip-merge-meta` | `false` | Do not merge upstream meta fields. |
| `force-inherit-descriptions` | `false` | Overwrite existing descriptions with upstream. |
| `desc-owner` | `this` | Who owns the description: `upstream` always overwrites; any other value fills gaps only. |
| `anchor-description` | `false` | Protect all columns in this node from inheritance (also settable per column). |
| `use-unrendered-descriptions` | `false` | Preserve `{{ doc(...) }}` Jinja refs as-is. |
| `prefer-yaml-values` | `false` | Keep existing YAML values; never overwrite. |
| `add-inheritance-for-specified-keys` | `[]` | Extra meta/config keys to propagate. |
| `annotate-column-origin-infos` | _(absent)_ | `if_altered` = annotate renamed/derived/computed; `always` = also passthrough (DP layers); `never`/absent = none. |
| `annotation-include-source-description` | `true` | Inject source description into the annotation when it differs (staging); `false` = origin ref only (DP layers). |
| `inherit-through-renames` | `false` | Per-layer override of the package-level setting. |
| `introspect-sources-only` | `true` | Restrict DB queries to source nodes (recommended). |
| `db-fallback-on-cll-failure` | `false` | Fall back to DB introspection if CLL fails. |
| `compile-on-cll-failure` | `true` | Auto-run `dbt compile` if compiled SQL is missing. |
| `scaffold-empty-configs` | `false` | Write empty/placeholder fields (e.g. `description: ""`). |
| `include-external` | `false` | Include models/sources from dbt packages. |
| `fusion-compat` | `null` | Fusion-compatible YAML output (`null` = auto-detect). |
| `formatter` | `null` | External formatter command run after writing YAML. |
| `catalog-path` | `null` | Read columns from `catalog.json` instead of the live DB. |

### Annotation format

Origin annotations follow a consistent scheme: **`from` points to a column, `in:` points to a model.**

```
__________
OSMOSIS -> Renamed from: STG_ORDERS.ORDER_ID
OSMOSIS -> Aggregated from STG_ORDERS.AMOUNT      # single-source aggregate
OSMOSIS -> Aggregated in: DC_MART.FCT_ORDERS      # COUNT(*), SUM(a+b) — no single source
OSMOSIS -> Computed in: DC_MART.FCT_ORDERS        # multi-source expression
```

CLL does not distinguish `UNION` from `UNION ALL`; both use `annotation-union`.

### Machine-readable lineage (`write-cll-tags-to-meta`)

Off by default. When `true`, osmosis also writes the origin to column `meta` using the
`col-renamed-from` / `col-derived-from` / `col-computed-in` keys, so downstream tooling
(data catalogs, lineage UIs, AI agents) can read lineage programmatically from the dbt
manifest instead of parsing the annotation text. Trade-off: larger YAML and more diff
churn, since the same information is already in the annotation block.

### Column reference (`column-docs-path`)

A flat YAML file that maps column names (case-insensitive) to canonical descriptions. Every column listed there is **automatically CLL-ignored**:

- No `col-renamed-from` / `col-derived-from` / `col-computed-in` meta tags are written.
- Stale tags from previous runs are stripped on the next osmosis run.
- The description is injected whenever the column has no existing description in the YAML.

This is the right place for audit/technical columns that are computed in every model (e.g. `ROW_BATCH_TIMESTAMP`) — define them once, osmosis handles them everywhere. No separate `cll-ignore-columns` setting is needed.

```yaml
# docs/osmosis_column_references.yml
ROW_BATCH_TIMESTAMP: >-
  Timestamp of the last batch load process that inserted or updated this record.
ROW_CREATE_TIMESTAMP: >-
  Timestamp when this record was first created in the database.
```

