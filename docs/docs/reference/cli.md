---
sidebar_position: 1
---

# CLI reference

This page documents the current `dbt-osmosis` CLI surface area as exposed by `dbt-osmosis --help`.

## Global command groups

`dbt-osmosis` currently exposes these top-level commands:

- `yaml` — schema YAML management and documentation inheritance
- `sql` — compile or run ad-hoc SQL in dbt context
- `generate` — generate sources and staging models
- `test` — suggest dbt tests
- `diff` — compare YAML definitions with live database schema
- `lint` — lint SQL strings, models, or a whole project
- `lineage` — interactive column-level lineage explorer

## Shared dbt options

Most project-aware commands accept some or all of the following:

- `--project-dir`
- `--profiles-dir`
- `-t, --target`
- `--threads`
- `--profile`
- `--vars`
- `--log-level`

Project discovery defaults to the current directory and its parents. Profiles default to `DBT_PROFILES_DIR`, the current directory, the discovered project root, or `~/.dbt`.

## `dbt-osmosis yaml`

YAML commands manage schema files and column-level documentation inheritance.

### Common YAML options

- positional selectors: `dbt-osmosis yaml <command> [<selector> ...]`
- `-f, --fqn` (repeatable)
- `-d, --dry-run`
- `-C, --check`
- `--catalog-path`
- `--disable-introspection`
- `--include-external`
- `--scaffold-empty-configs/--no-scaffold-empty-configs`
- `--strip-eof-blank-lines/--keep-eof-blank-lines`
- `--fusion-compat/--no-fusion-compat`
- `--formatter <command>`

### `dbt-osmosis yaml refactor`

Runs `organize` (file placement) and then `document` (inherit docs) in one command.

Common behavior flags:

- `--auto-apply` (skip confirmation for file moves)
- `-F, --force-inherit-descriptions`
- `--use-unrendered-descriptions` (propagate `{{ doc(...) }}` descriptions)
- `--prefer-yaml-values` (preserve unrendered templates for all fields)
- `--skip-merge-meta`
- `--skip-add-tags`
- `--skip-add-columns`
- `--skip-add-source-columns`
- `--skip-add-data-types`
- `--add-progenitor-to-meta`
- `--add-inheritance-for-specified-keys <key>` (repeatable)
- `--numeric-precision-and-scale`
- `--string-length`
- `--output-to-lower`
- `--output-to-upper`
- `--include-external`

Fusion compatibility:

- `--fusion-compat/--no-fusion-compat` outputs Fusion-compatible YAML with `meta` and `tags` nested under `config`. If unspecified, dbt-osmosis auto-detects from Fusion manifest evidence or dbt Core >= 1.9.6.

External formatting:

- `--formatter "prettier --write"` or another CLI formatter command. dbt-osmosis appends written file paths and runs the formatter once after successful writes.

Example:

```bash
dbt-osmosis yaml refactor models/staging --dry-run --check
```

### `dbt-osmosis yaml organize`

Ensures YAML files exist and are placed according to your `+dbt-osmosis` routing rules.

Additional flag:

- `--auto-apply`

### `dbt-osmosis yaml document`

Applies column documentation inheritance and optional column injection/sorting.

It supports the same inheritance, output, and formatting flags as `yaml refactor`, except `--auto-apply`.

## `dbt-osmosis sql`

Executes or compiles ad-hoc SQL, including dbt Jinja, against your project.

- `dbt-osmosis sql run "select ..."`
- `dbt-osmosis sql compile "select ..."`

## `dbt-osmosis generate`

Generates dbt artifacts deterministically (no LLM involved).

- `dbt-osmosis generate sources`
  - options: `--source-name`, `--schema-name`, `--exclude-schemas`, `--exclude-tables`, `--quote-identifiers`, `--output-path`, `--dry-run`
- `dbt-osmosis generate staging <source_name> <table_name>`
  - options: `--staging-path`, `--dry-run`

## `dbt-osmosis test`

Test suggestion helpers.

Currently exposed subcommand:

- `dbt-osmosis test suggest [<model> ...]`

Important options:

- `-f, --fqn`
- `-o, --output`
- `--format [json|yaml|table]`

Suggestions are deterministic, derived from test conventions already present in the project.

## `dbt-osmosis diff`

Schema diff helpers.

Currently exposed subcommand:

- `dbt-osmosis diff schema`

Important options:

- YAML selection flags (`[MODELS]`, `-f/--fqn`, `--include-external`)
- `--output-format [text|json|markdown]`
- `--severity [safe|moderate|breaking|all]`
- `--fuzzy-match-threshold`
- `--detect-column-renames/--no-detect-column-renames`

This command compares YAML definitions with live database schema and reports additions, removals, type changes, and fuzzy-matched renames.

## `dbt-osmosis lineage`

Interactive column-level lineage visualization (requires the `lineage-ui` extra:
`pip install 'dbt-osmosis-cll[lineage-ui]'`).

- `dbt-osmosis-cll lineage explore`
  - options: `--project-dir`, `--manifest`, `--host`, `-p/--port`, `--dialect`

Serves the HTML lineage explorer for the whole project. Manifest-only: column lists
come from source/model YAMLs already in the manifest, compiled SQL from inline
`compiled_code` or `target/compiled/` — no `catalog.json` and no warehouse
connection. Run `dbt compile` first so lineage has SQL to trace.

## `dbt-osmosis lint`

SQL lint helpers.

Exposed subcommands:

- `dbt-osmosis lint file <sql-or-path>`
- `dbt-osmosis lint model <model_name>`
- `dbt-osmosis lint project`

Important options across lint commands:

- `--rules`
- `--disable-rules`
- `--dialect`
- `-f, --fqn` on `lint project`

These commands exit non-zero when errors or warnings are reported.
