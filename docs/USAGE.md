# dbt-osmosis

![PyPI](https://img.shields.io/pypi/v/dbt-osmosis)
[![Downloads](https://static.pepy.tech/badge/dbt-osmosis)](https://pepy.tech/project/dbt-osmosis)
![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green.svg)

`dbt-osmosis` is a Python CLI and package for dbt development workflows.

It centers on three primary surfaces:

- schema YAML management (`yaml organize`, `yaml document`, `yaml refactor`)
- column-level documentation inheritance across dbt lineage
- ad-hoc SQL compile/run helpers

The repository also ships additional command families for generation, schema diffing, SQL linting, and test suggestions.

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
- `generate` — generate sources and staging models
- `test` — suggest dbt tests
- `diff` — report schema drift between YAML and the database
- `lint` — lint SQL strings, models, or a whole project
- `lineage` — serve the read-only interactive lineage explorer (`lineage explore`, optionally fed from a precomputed CLL artifact via `--from-cll`)
- `parse-cll` — build column-level lineage for a dbt selector and write it to a standalone JSON file

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

## Using with coding agents (Claude Code, etc.)

dbt-osmosis-cll deliberately ships no LLM client. When AI-generated documentation is wanted,
run a coding agent directly in the repo and let it drive the deterministic surfaces:

1. Find the documentation gaps: `dbt-osmosis-cll yaml doc-health --format json` reports,
   per model, which columns lack real authored descriptions (as opposed to inherited or
   annotation-only ones).
2. Author descriptions at the **origin** of each column (the model or source where the
   column first appears, or the central glossary configured via `column-docs-path`). The
   agent has full repo context — SQL, upstream YAML, business docs — which produces far
   better descriptions than one-shot prompt synthesis ever did.
3. Propagate: `dbt-osmosis-cll yaml document` inherits the new origin descriptions across
   the lineage via CLL, records provenance (`desc-source`), and rewrites annotations.
4. Gate in CI with `yaml doc-health --min-coverage <pct>`.

This keeps generated text out of non-origin layers, so provenance tracking stays truthful.

The doc-health JSON additionally breaks documented columns down by **trust** — per node
and in the summary: `authored` (human-written at this node), `inherited` (gap-filled from
the CLL origin, carries `desc-source`), and `glossary` (owned by the central glossary).
"Documented" alone does not mean "trustworthy": a drop in `inherited` together with a rise
in `undocumented` between runs means CLL propagation silently stopped reaching columns it
used to reach — combine with `--check-cll` (which lists models whose lineage extraction
failed) to catch such regressions in CI.

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

### Standalone CLL build (`parse-cll`)

`parse-cll` computes column-level lineage for a dbt selector and writes it to a dedicated JSON file — no YAML is touched, no warehouse connection is needed, and osmosis' own `target/cll_cache.json` is never read or written.

```bash
# Model plus ALL upstream models, up to (but not including) sources
dbt-osmosis-cll parse-cll -s +my_model

# Depth-limited: my_model plus one generation of direct parents
dbt-osmosis-cll parse-cll -s 1+my_model

# Downstream: my_model plus everything built on top of it (impact analysis
# toward endpoints / data products); depth-limited with my_model+N
dbt-osmosis-cll parse-cll -s my_model+

# Just the model itself; multiple selectors are unioned
dbt-osmosis-cll parse-cll -s my_model -s "+other_model, 2+third_model, +both_ways+"
```

Selector syntax follows dbt's graph operators on a named model: `model`, `+model` / `N+model` (upstream), `model+` / `model+N` (downstream), and combinations like `+model+`. Method selectors (`tag:`, `path:`, `@model`) are rejected with a clear error.

Results land in `<project-dir>/target/cll-result.json` by default (override with `-o/--output`). The payload contains the resolved selector (`models`), a `fingerprints` map of per-model source-SQL hashes, and one row per `(model, column)` pair with the full `ColumnLineageResult` fields (`progenitor_model`, `progenitor_column`, `is_rename`, `is_computed`, `transformation_type`, `sql_expression`, `union_branches`, `progenitors`, …). Source tables appear as progenitors inside model rows — CLL rows themselves exist only for models, so `+model` stops naturally at the source layer.

Like `lineage explore`, the command is manifest-only: column lists come from the manifest, compiled SQL from inline `compiled_code` or `target/compiled/`. Run `dbt compile` first so lineage has SQL to trace.

**Cold-start cost tracks the selector, not the repo.** Lineage parsing is lazy: only the models the selector resolves to are SQL-parsed (`yaml refactor`/`document` likewise parse only the models a run actually touches, plus their upstream walks). A first `parse-cll -s +one_model` on a large repo therefore takes seconds, not minutes.

#### Join / filter / qualify columns (`usages`)

Projected lineage alone misses every column a model *needs* without outputting it. Since schema version 3, `cll-result.json` carries a separate `usages` list — never merged into `results`, because whether a column that only filters is still needed after a migration is a business decision the tool cannot make. One row per (model, usage, source column, scope, predicate):

| `usage` | Where the column appears |
| --- | --- |
| `join_key` | `JOIN … ON` conjunct relating two relations (`a.k = b.k`, range joins, `x BETWEEN b.from AND b.to`) — both sides are reported |
| `join_filter` | `JOIN … ON` conjunct constraining one side only (`AND b.flg = 'Y'`) |
| `filter` | `WHERE`, including the projection of a filtering subquery (`x IN (SELECT y …)`) |
| `qualify` | `QUALIFY`, window specification included (`detail`: `partition_by` / `order_by` / `predicate`) |
| `window` | `PARTITION BY` / `ORDER BY` of a window function in a projection |
| `group_by` | `GROUP BY` keys; `GROUP BY ALL` expands to the non-aggregate projections, ordinals map to projections |
| `having` | `HAVING` |
| `compile_time` | read by a compile-time Jinja query such as `get_column_values(ref('cfg'), column='flag', order_by='MAX(ts) DESC')` — the column decides which SQL is generated without appearing in the compiled SQL; `dbt_vars` names the result variable, `controls_branch` says whether it drives an `{% if %}` |

Each row names the model whose SQL reads the column, the `source_model` / `source_column` it resolves to **through every CTE of that model** (a `QUALIFY` on a column a CTE never projects still lands on the base relation), the `scope` (CTE name, `<final>`, `…/subquery`, `<jinja>`), the normalized `predicate_sql`, and `resolution` — `qualified`, `single_table`, `unqualified_resolved` (unqualified reference matched through the parent models' column lists; parents without YAML columns are parsed on demand), `select_alias`, or `ambiguous` (one row per candidate instead of silently picking the first `FROM` table). `origins` / `via_models` follow the used column upstream through the projected lineage to sources, seeds, or models without compiled SQL (`--no-usage-origins` skips that walk). `is_self_reference` marks incremental `{{ this }}` joins. Predicates whose literals match a string `var` from `dbt_project.yml` list the var in `dbt_vars`.

Rows are sorted and de-duplicated; two runs over the same artifacts write byte-identical files. The same data is available programmatically via `dbt_osmosis_cll.cll_generator.api.get_column_usage()`.

`parse-cll` reads `compiled-sql-placeholder-patterns` and `compile-time-query-macros` from the project's `.osmosis-cll`, so repo-specific tokens and introspective macros are handled by configuration, not code.

#### Column sources and coverage

Star expansion and unqualified references need every relation's columns. The manifest only knows documented YAML columns; relations without them can be filled from further sources, in order:

```ini
[osmosis]
cll-column-sources = manifest, catalog, live   # default: manifest
cll-catalog-path   = target/catalog.json       # from `dbt docs generate`
```

`live` queries the warehouse (needs `--profiles-dir` / `--target`) and only for relations the selection reads that are still without columns. `--column-sources` / `--catalog` override the config per run. Every `cll-result.json` carries a `coverage` block — relations read by the selection without any known column, `SELECT *` over such relations, ambiguous / unresolved usage counts, and which source supplied each relation's columns — and the same findings are logged as warnings.

### Offline compile for lineage (`compile-cll`)

Lineage needs the SQL a model really runs. Projects that decide it at compile time — a body gated by `get_column_values(ref('load_config'), 'load_flg')`, `is_incremental()`, `dbt_utils.star(from=ref('x'))`, in-house introspective macros — cannot be compiled without a warehouse, or compile to the wrong branch. `compile-cll` compiles them offline, configured entirely in `.osmosis-cll`:

```ini
[compile]
adapter = snowflake               # adapter of the generated offline profile
target = prod                     # target.name the models see
introspection-values =            # pinned results of get_column_values-style calls
    load_config.load_flg = Y
    load_config.run_dt = 2000-01-31
is-incremental = false            # what is_incremental() returns (default false)
is-incremental-models = model_a   # models compiled with is_incremental() = true
vars = {"run_ts": "2000-01-01T00:00:00"}   # pin time-dependent vars for identical output
macro-stubs =                     # any other introspective macro: name = Jinja expression
    my_pkg.latest_loaded_ts = '2000-01-01'
relations-exist = audit_log       # adapter.get_relation(): treat as existing ...
relations-missing = tmp_backfill  # ... or as missing (default: exists iff the manifest knows it)
```

Direct adapter calls in model Jinja are answered offline too: `adapter.get_relation()` returns a relation for everything the manifest knows (override with `relations-exist` / `relations-missing`), `adapter.get_columns_in_relation()` returns the manifest's documented columns. Each answer is recorded per model (`relations_assumed_existing` / `relations_assumed_missing`).

```bash
dbt-osmosis-cll compile-cll -s "1+my_model"
dbt-osmosis-cll parse-cll   -s "1+my_model"
```

How it works: dbt parses the project in-process; in the parsed manifest — never on disk — every macro named `is_incremental`, every `get_column_values`-style macro (any package, signature-checked) and every `star` is replaced by a stub, plus the `macro-stubs` you list. The compile then runs with a generated profile whose credentials cannot reach a warehouse, without cache population or introspection; any remaining connection attempt fails with a message naming the setting to add. Nothing in the dbt project is modified.

Every assumption is logged and written to `target/cll-compile.json`, and `parse-cll` copies it per model into `cll-result.json` (`compile`): the `is_incremental()` value each model was compiled with (the full-build branch is assumed to hold the complete logic — models whose incremental branch is the only real one need `is-incremental-models`), pinned and *unpinned* compile-time values (an unpinned call returns the macro default), `star` calls over relations without known columns, and models whose compiled SQL looks like an empty shell (`LIMIT 0`, `WHERE 1 = 0` / `FALSE`, reading only their own relation).

### Serving the explorer from a precomputed CLL artifact

By default `lineage explore` parses every model's compiled SQL before it starts serving — on a large repo that is the whole cold-start cost. When the work has already been done, point the explorer at the result instead:

```bash
# Explore exactly the slice you just analysed (selector-scoped)
dbt-osmosis-cll parse-cll -s +my_model
dbt-osmosis-cll lineage explore --from-cll target/cll-result.json

# Explore the whole project using the cache an osmosis run left behind
dbt-osmosis-cll yaml document          # writes target/cll_cache.json as a side effect
dbt-osmosis-cll lineage explore --from-cll target/cll_cache.json

# Let it pick: cll-result.json if present, otherwise cll_cache.json
dbt-osmosis-cll lineage explore --from-cll
```

The format is detected from the payload, not the filename, so a relocated or renamed artifact works too.

**The two artifacts differ in scope, and the explorer honours that difference.** A `cll-result.json` states which models its selector resolved to, so the explorer restricts its model tree and graphs to exactly those (plus the sources and seeds they terminate at) — the rest of the project is deliberately absent rather than silently empty. A `cll_cache.json` makes no scope claim, so the explorer shows the full project. The sidebar carries a badge naming the source and scope, so a scoped session is never mistaken for a whole-project one.

**Nothing is served stale or half-complete.** Both artifacts record a per-model hash of the source `.sql` file. Models whose SQL changed since the artifact was written are dropped from the cached set, and any model the artifact never covered is parsed individually the first time you click it. A partial or slightly outdated cache therefore still yields a complete, current graph — it just costs a few per-model parses instead of a whole-project one.

Caches written before schema version 5 lack the `sql_expression` field; they still load, but the impact panel shows no SQL expressions until the cache is rebuilt. The explorer warns on startup when it reads one.

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
| `annotation-window-from` | `Windowed from:` | Window over one traceable column (followed by `MODEL.COL`). |
| `annotation-window-in` | `Windowed in:` | Window with no traceable column (`ROW_NUMBER()`…). |
| `annotation-union` | `UNION in:` | Top-level UNION / UNION ALL / INTERSECT / EXCEPT column. |
| `annotation-literal` | `Literal` | Hard-coded constant column. |
| `annotation-generated` | `Generated` | Zero-arg system function (`CURRENT_DATE`, `UUID_STRING`…). |
| `annotation-namespace` | `OSMOSIS` | Namespace label in the annotation block header. |
| `annotation-separator` | `__________` | Visual separator line above the annotation block. |
| `legacy-strip-markers` | _(empty)_ | Comma-separated legacy tag prefixes to strip from descriptions. |
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

Off by default. When enabled, osmosis also writes the origin to column `meta` using the
`col-renamed-from` / `col-derived-from` / `col-computed-in` keys, so downstream tooling
(data catalogs, lineage UIs, AI agents) can read lineage programmatically from the dbt
manifest instead of parsing the annotation text. Trade-off: larger YAML and more diff
churn, since the same information is already in the annotation block.

It resolves **per node**, so you can enable it for just the layers an agent consumes —
e.g. set `+dbt-osmosis-options: {write-cll-tags-to-meta: true}` on your data-product
path in `dbt_project.yml` — while leaving intermediate layers untagged. A global default
can still be set in `.osmosis`.

#### Recommended pattern: machine-readable origins on endpoint / data-product layers

Endpoint (data-product) models are what catalog tools, BI users, and agents actually
consume — and the layer where column origins are least obvious. The recommended
configuration turns on full annotation **and** machine-readable meta tags there, while
keeping intermediate layers lean:

```yaml
# dbt_project.yml
models:
  my_project:
    staging:
      +dbt-osmosis-options:
        inherit-through-renames: true        # staging renames are transparent
    intermediate: {}                         # default: annotate only if altered, no meta tags
    marts:                                   # endpoint / data-product layer
      +dbt-osmosis-options:
        annotate-column-origin-infos: always        # every column states its origin
        annotation-include-source-description: false # origin ref only, no text duplication
        write-cll-tags-to-meta: true                 # col-renamed-from / col-derived-from /
                                                     # col-computed-in in column meta
```

With this in place every endpoint column carries a human-readable origin block in its
description **and** a machine-readable pointer in its meta, so any column on the
data-product surface traces to its true origin without opening the SQL (goal 3 of the
CLL roadmap). Downstream catalogs read the meta keys straight from `manifest.json`.

### Column reference (`column-docs-path`)

A flat YAML file that maps column names (case-insensitive) to canonical descriptions. Every column listed there is **automatically CLL-ignored**:

- No `col-renamed-from` / `col-derived-from` / `col-computed-in` meta tags are written.
- Stale tags from previous runs are stripped on the next osmosis run.
- The description is authoritative: osmosis writes it on every run, overwriting any existing description in the YAML. Edit the glossary and rerun osmosis to propagate the change everywhere.

This is the right place for audit/technical columns that are computed in every model (e.g. `ROW_BATCH_TIMESTAMP`) — define them once, osmosis handles them everywhere.

```yaml
# docs/osmosis_column_references.yml
ROW_BATCH_TIMESTAMP: >-
  Timestamp of the last batch load process that inserted or updated this record.
ROW_CREATE_TIMESTAMP: >-
  Timestamp when this record was first created in the database.
```
