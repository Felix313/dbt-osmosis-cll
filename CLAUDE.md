# CLAUDE.md

## Issue Tracking

This project uses **bd (beads)** for issue tracking (initialized 2026-06-11; installed
via `npm install -g @beads/bd` — NOT the unrelated PyPI "beads" package). See the
managed Beads block at the bottom of this file for the quick reference, and run
`bd prime` for full workflow context. `docs/plans/` holds longer-form design notes
and the executed Phase-2 roadmap; new follow-ups go into beads.

## Repository Overview

**dbt-osmosis-cll** is a CLI tool that enhances the dbt developer experience through automated YAML schema management and column-level documentation inheritance driven by an embedded column-level-lineage (CLL) resolver. The tool operates as both a dbt utility and standalone Python package. It deliberately ships no LLM client and no in-package doc-synthesis UI: AI-assisted documentation is done by coding agents working in the repo against `yaml doc-health --format json` and `yaml document` (see "Using with coding agents" in docs/USAGE.md). The only UI is the read-only lineage explorer (`lineage explore`, optional `lineage-ui` extra).

## Development Commands

### Environment Setup
```bash
# Install task runner (see https://taskfile.dev/installation/)
# Then run default task (format, lint, dev setup, test)
task

# Setup dev environment only
task dev

# Create virtual environment
task venv
```

### Code Quality
```bash
# Format code (auto-fix imports + ruff format)
task format

# Lint code
task lint

# Manual ruff commands
uvx ruff check
uvx ruff format --preview
uvx ruff check --fix --select I  # Fix imports only
```

### Testing
```bash
# Run the local Taskfile test matrix
# (Python 3.10-3.12 × dbt 1.8-1.9, plus Python 3.10-3.13 × dbt 1.10.0)
task test

# Run tests for current environment
uv run pytest

# Run specific test file
uv run pytest tests/core/test_introspection.py

# Parse demo project (useful for debugging)
uv run dbt parse --project-dir demo_duckdb --profiles-dir demo_duckdb -t test
```

### Package Management
This project uses **uv** for dependency management:
```bash
# Sync dependencies
uv sync

# Sync with extras
uv sync --extra dev

# Install package in editable mode
uv pip install -e .

# Add dependency
uv add <package>
```

### Running dbt-osmosis-cll Commands
```bash
# Main YAML refactor command (organize + document)
uv run dbt-osmosis-cll yaml refactor --project-dir <path> --profiles-dir <path>

# Organize YAML files only (no documentation)
uv run dbt-osmosis-cll yaml organize --project-dir <path> --profiles-dir <path>

# Document models only (inherit upstream docs)
uv run dbt-osmosis-cll yaml document --project-dir <path> --profiles-dir <path>

# Run with external YAML formatter (prettier, yamlfmt, yq, etc.)
uv run dbt-osmosis-cll yaml refactor --formatter "prettier --write" --project-dir <path> --profiles-dir <path>

# Compile SQL
uv run dbt-osmosis-cll sql compile "SELECT * FROM {{ ref('my_model') }}"

# Execute SQL
uv run dbt-osmosis-cll sql run "SELECT 1"

# Documentation coverage report (the JSON shape is the agent/CI contract)
uv run dbt-osmosis-cll yaml doc-health --format json --project-dir <path> --profiles-dir <path>
```

### Demo Project
The `demo_duckdb/` directory contains a test dbt project based on jaffle_shop:
```bash
cd demo_duckdb
dbt run --profiles-dir . --target test
dbt test --profiles-dir . --target test
```

## Code Architecture

### Entry Points
- **CLI**: `src/dbt_osmosis_cll/cli/main.py` - Click-based CLI with `yaml`, `sql`, `generate`, `diff`, `lint`, and `test` command families
- **Core API**: `src/dbt_osmosis_cll/osmosis_propagation/osmosis.py` - Re-exports all public APIs for backwards compatibility

### Core Module Structure (`src/dbt_osmosis_cll/osmosis_propagation/`)

The core functionality is split into specialized modules:

- **config.py**: dbt project initialization, manifest loading, profiles/project discovery
- **settings.py**: `YamlRefactorSettings` and `YamlRefactorContext` dataclasses that configure behavior
- **osmosis.py**: Main API re-export layer for backwards compatibility with imports

#### YAML Management Pipeline
1. **path_management.py**: Determines where YAML files should live based on `dbt_project.yml` configuration (e.g., `+dbt-osmosis: "{node.schema}/{node.name}.yml"`)
2. **restructuring.py**: Creates move/delete plans for YAML reorganization
3. **schema/reader.py**: Reads and caches YAML files
4. **schema/parser.py**: Parses YAML using ruamel.yaml with custom formatting
5. **schema/writer.py**: Writes YAML back to disk with formatting preservation

#### Documentation Inheritance Pipeline
1. **introspection.py**: Queries database for column schema, caches results, **provides SettingsResolver and PropertyAccessor APIs**
2. **inheritance.py**: Builds column knowledge graph from upstream models
3. **transforms.py**: Pipeline of transforms that can be composed with `>>` operator:
   - `inject_missing_columns`: Adds columns from database not in YAML
   - `remove_columns_not_in_database`: Removes stale columns
   - `inherit_upstream_column_knowledge`: Propagates docs/tags/meta from upstream
   - `sort_columns_as_configured`: Orders columns
   - `synchronize_data_types`: Updates data types from database

### Configuration Resolution System

**dbt-osmosis-cll** provides a unified configuration resolution system through two main APIs:

#### SettingsResolver (`dbt_osmosis_cll.osmosis_propagation.introspection.SettingsResolver`)

The `SettingsResolver` class provides a clean, testable interface for retrieving configuration values from multiple sources with defined precedence rules.

**Configuration Precedence (highest to lowest):**
1. **Column-level meta** (e.g., `columns.name.meta.dbt-osmosis-<key>`)
2. **Node-level meta** (e.g., `models.project.model.meta.dbt-osmosis-<key>`)
3. **Node-level config.extra** (e.g., `{{ config(dbt_osmosis_<key>=value) }}`)
4. **Node-level config.meta** (dbt 1.10+, e.g., `{{ config(meta={'dbt-osmosis-<key>': value}) }}`)
5. **Node-level unrendered_config** (dbt 1.10+)
6. **Project-level vars** (e.g., `dbt_project.yml` vars.dbt-osmosis-cll.<key>)
7. **Supplementary file** (`dbt-osmosis-cll.yml` in project root)
8. **Fallback value** (default if not found)

**Public API:**
```python
from dbt_osmosis_cll.osmosis_propagation.osmosis import SettingsResolver

resolver = SettingsResolver()

# Resolve a setting value
value = resolver.resolve(
    "output-to-lower",           # Setting name (kebab-case or snake_case)
    node,                        # dbt node (model, source, etc.)
    column_name="user_id",       # Optional: check column-level settings
    fallback=False               # Optional: default value if not found
)

# Check if a setting exists
has_setting = resolver.has(
    "output-to-lower",
    node,
    column_name="user_id"        # Optional
)

# Get full precedence chain (for debugging)
chain = resolver.get_precedence_chain(
    "output-to-lower",
    node,
    column_name="user_id"        # Optional
)
# Returns: [('column_meta', value), ('node_meta', value), ...]
```

**Key Features:**
- **Key normalization**: Supports both kebab-case (`output-to-lower`) and snake_case (`output_to_lower`)
- **Prefix handling**: Recognizes `dbt-osmosis-<key>`, `dbt_osmosis_<key>`, and direct `<key>` variants
- **Options object**: Supports nested `dbt-osmosis-options.<key>` syntax
- **Cross-version compatibility**: Gracefully handles dbt 1.8-1.11+ differences
- **Column overrides**: Column-level settings take precedence over node-level settings

**Example Usage:**
```python
# In a transform function
from dbt_osmosis_cll.osmosis_propagation.osmosis import SettingsResolver

def my_transform(context: YamlRefactorContext) -> YamlRefactorContext:
    resolver = SettingsResolver()

    for node in context.nodes:
        # Check if node has a specific setting
        if resolver.has("skip-add-tags", node):
            continue

        # Get setting with fallback
        output_lower = resolver.resolve("output-to-lower", node, fallback=False)

        # Get column-specific setting
        for column_name in node.columns:
            skip_meta = resolver.resolve(
                "skip-meta-merge",
                node,
                column_name=column_name,
                fallback=False
            )
            if skip_meta:
                # Skip meta merge for this column
                pass

    return context
```

#### PropertyAccessor (`dbt_osmosis_cll.osmosis_propagation.introspection.PropertyAccessor`)

The `PropertyAccessor` class provides a unified interface for accessing model properties (descriptions, tags, meta, data types) from multiple sources with support for unrendered jinja templates.

**Property Sources:**
- **`manifest`**: Rendered jinja values (pre-compiled by dbt, default)
- **`yaml`**: Unrendered jinja templates (raw `{{ doc(...) }}` syntax)
- **`auto`**: Automatically detects and prefers YAML if unrendered jinja is present

**Public API:**
```python
from dbt_osmosis_cll.osmosis_propagation.osmosis import PropertyAccessor, PropertySource

accessor = PropertyAccessor(context=context)

# Get any property from a specific source
description = accessor.get(
    "description",                # Property name
    node,                         # dbt node
    column_name="user_id",        # Optional: column-level property
    source=PropertySource.MANIFEST # or "yaml", "auto"
)

# Convenience method for descriptions
description = accessor.get_description(
    node,
    column_name="user_id",        # Optional
    source="manifest"             # or "yaml", "auto"
)

# Convenience method for metadata
metadata = accessor.get_meta(
    node,
    column_name="user_id",        # Optional
    source="manifest",            # or "yaml", "auto"
    meta_key="pii"                # Optional: get specific key from meta dict
)

# Check if a property exists
has_desc = accessor.has_property(
    "description",
    node,
    column_name="user_id"         # Optional
)
```

**Key Features:**
- **Unrendered jinja preservation**: Preserves `{{ doc('block_name') }}` templates when using `source="yaml"`
- **Auto-detection**: Automatically chooses YAML when unrendered jinja is detected (`source="auto"`)
- **Graceful fallback**: Falls back to manifest when YAML is unavailable
- **Column-level access**: Supports both node-level and column-level properties
- **Multiple property types**: Supports description, tags, meta, data_type, name, and custom properties

**Example Usage:**
```python
# In a transform that preserves doc blocks
from dbt_osmosis_cll.osmosis_propagation.osmosis import PropertyAccessor, PropertySource

def inherit_docs_preserving_blocks(context: YamlRefactorContext) -> YamlRefactorContext:
    accessor = PropertyAccessor(context=context)

    for node in context.nodes:
        # Get unrendered description if it contains doc blocks
        description = accessor.get_description(
            node,
            source="auto"  # Preserves {{ doc(...) }} if present
        )

        # Get column descriptions
        for column_name in node.columns:
            col_desc = accessor.get_description(
                node,
                column_name=column_name,
                source="yaml"  # Force YAML to preserve templates
            )

            # Get metadata
            is_pii = accessor.get_meta(
                node,
                column_name=column_name,
                meta_key="pii",
                source="manifest"
            )

    return context
```

#### Backward Compatibility

**For existing code using `_get_setting_for_node`:**

The legacy `_get_setting_for_node()` function is now a backward compatibility wrapper that delegates to `SettingsResolver`. All existing code continues to work without changes:

```python
# Old way (still works)
from dbt_osmosis_cll.osmosis_propagation.introspection import _get_setting_for_node

value = _get_setting_for_node(
    "output-to-lower",
    node,
    col="column_name",           # Optional column
    fallback=False               # Optional fallback
)

# New recommended way
from dbt_osmosis_cll.osmosis_propagation.osmosis import SettingsResolver

resolver = SettingsResolver()
value = resolver.resolve(
    "output-to-lower",
    node,
    column_name="column_name",   # Optional column
    fallback=False               # Optional fallback
)
```

**Migration guide:**
- Replace `_get_setting_for_node(opt, node, col, fallback=X)` with `resolver.resolve(opt, node, column_name=col, fallback=X)`
- Use `resolver.has()` instead of checking if result is not None
- Use `PropertyAccessor` for accessing model properties (descriptions, tags, meta)
- Use `source="auto"` or `source="yaml"` to preserve unrendered jinja templates

**Why migrate?**
- **Better API**: Clearer parameter names and method names
- **More features**: `has()`, `get_precedence_chain()`, PropertyAccessor
- **Type safety**: Full type hints for better IDE support
- **Testability**: Easier to test and mock
- **Future-proof**: New features will only be added to the new APIs

#### Other Key Modules
- **node_filters.py**: Filters dbt nodes by FQN/path, topological sorting
- **sync_operations.py**: Syncs individual nodes to YAML
- **sql_operations.py**: Compiles and executes dbt SQL via dbt's internal APIs
- **commands/doc_health.py**: Documentation coverage report with a stable JSON shape for agents/CI
- **plugins.py**: Pluggy-based plugin system for fuzzy matching (FuzzyCaseMatching, FuzzyPrefixMatching)

### Transform Pipeline Pattern

dbt-osmosis-cll uses a functional pipeline pattern with the `>>` operator:
```python
transform = (
    inject_missing_columns
    >> remove_columns_not_in_database
    >> inherit_upstream_column_knowledge
    >> sort_columns_as_configured
    >> synchronize_data_types
)
result = transform(context=context)
```

Each transform function takes a `YamlRefactorContext` and returns it for chaining.

### Configuration in dbt_project.yml
Users configure YAML organization via node properties:
```yaml
models:
  my_project:
    +dbt-osmosis: "{node.schema}/{node.name}.yml"  # Template for YAML paths

vars:
  dbt-osmosis-cll:
    yaml_settings:
      map_indent: 2
      sequence_indent: 4
      brace_single_entry_mapping_in_flow_sequence: true
      explicit_start: true
```

### Column-Level Configuration Override (User Story 4)
dbt-osmosis-cll supports column-level configuration overrides, allowing individual columns to have different settings than the node-level default. This is useful when you want specific columns to behave differently.

**Precedence Order** (highest to lowest):
1. Column-level meta (highest precedence)
2. Node-level meta
3. Node-level config.extra
4. Node-level config.meta (dbt 1.10+)
5. Node-level unrendered_config (dbt 1.10+)
6. Project vars
7. Supplementary file (dbt-osmosis-cll.yml)
8. Fallback defaults

**Example: Column-level overrides in schema.yml**
```yaml
version: 2
models:
  - name: orders
    description: "Orders table with column-specific configurations"

    # Node-level default: all columns use uppercase output
    meta:
      dbt-osmosis-output-to-upper: true

    columns:
      - name: order_id
        description: "Unique order identifier"
        # Column-level override: force this column to lowercase
        meta:
          dbt-osmosis-output-to-lower: true

      - name: customer_id
        description: "Foreign key to customers"
        # No override - inherits node-level (output-to-upper: true)

      - name: amount
        description: "Order total amount"
        # Different setting: enable string-length for this column only
        meta:
          dbt-osmosis-string-length: true
```

**Supported key formats** in column meta:
- Direct keys: `output-to-lower: true`
- Prefixed kebab: `dbt-osmosis-output-to-lower: true`
- Prefixed snake: `dbt_osmosis_output_to_lower: true`
- Options object: `dbt-osmosis-options: {output-to-lower: true}`

**Using SettingsResolver programmatically**:
```python
from dbt_osmosis_cll.osmosis_propagation.introspection import SettingsResolver

resolver = SettingsResolver()

# Resolve setting for a specific column
output_lower = resolver.resolve(
    "output-to-lower",
    node=orders_node,
    column_name="order_id"
)

# Check if a column has a setting
has_setting = resolver.has(
    "string-length",
    node=orders_node,
    column_name="amount"
)

# Get full precedence chain for debugging
chain = resolver.get_precedence_chain(
    "output-to-lower",
    node=orders_node,
    column_name="order_id"
)
# Returns: [(ConfigSourceName.COLUMN_META, True), (ConfigSourceName.NODE_META, False), ...]
```

### Testing Approach
- Tests live in `tests/core/` mirroring `src/dbt_osmosis_cll/osmosis_propagation/`
- Uses pytest with demo_duckdb project as test fixture
- Local Taskfile matrix covers Python 3.10-3.12 with dbt-core 1.8-1.9 plus Python 3.10-3.13 with dbt-core 1.10.0; CI covers additional dbt versions
- Run `dbt parse` before tests to generate manifest.json

## Important Implementation Details

### dbt Integration
- dbt-osmosis-cll loads dbt projects via `dbt.cli.main.dbtRunner` and `dbt.cli.main.dbtRunnerResult`
- Accesses parsed manifest at `target/manifest.json` via `dbt.contracts.graph.manifest.Manifest`
- Uses dbt's internal SQL compilation via `dbt.task.sql.SqlCompileRunner`

### YAML Formatting
- Uses `ruamel.yaml` (NOT PyYAML) to preserve formatting, comments, and anchors
- YAML settings in `dbt_project.yml` under `vars.dbt-osmosis-cll.yaml_settings` control output formatting
- Custom `create_yaml_instance()` in `schema/parser.py` configures ruamel.yaml

### Column Knowledge Graph
- `inheritance.py` builds a directed graph of column lineage across models
- Documentation/tags/meta inherit from nearest documented upstream column
- Handles multiple inheritance paths (chooses first documented source)
- Uses `_build_node_ancestor_tree()` for topological traversal

### Caching Strategy
- Column lists cached in `introspection._COLUMN_LIST_CACHE` (thread-safe)
- YAML buffers cached in `schema/reader._YAML_BUFFER_CACHE`
- Manifest reloaded via `config._reload_manifest()` when YAML changes

### Plugin System
- Uses `pluggy` for plugin discovery
- Built-in plugins: `FuzzyCaseMatching`, `FuzzyPrefixMatching`
- Hooks defined in `plugins.py` via `dbt_osmosis_hookspec`

### External Formatter Integration
dbt-osmosis-cll supports running an external YAML formatter on all files it writes, reducing the need for a separate formatting step in CI:

**CLI usage:**
```bash
# With prettier
dbt-osmosis-cll yaml refactor --formatter "prettier --write" --project-dir . --profiles-dir .

# With yamlfmt
dbt-osmosis-cll yaml refactor --formatter "yamlfmt" --project-dir . --profiles-dir .

# With yq (in-place normalization)
dbt-osmosis-cll yaml refactor --formatter "yq -i '.'" --project-dir . --profiles-dir .
```

**Project-level config** (`dbt-osmosis-cll.yml` in project root):
```yaml
formatter: prettier --write
```

**Configuration precedence** (highest to lowest):
1. CLI flag `--formatter "cmd"`
2. `formatter` key in `dbt-osmosis-cll.yml`
3. None (default)

**How it works:**
1. Osmosis writes YAML files as usual (restructure + transforms)
2. Each successful write registers the file path in `YamlRefactorContext._written_files`
3. After all operations complete, osmosis invokes the formatter once with all written file paths
4. Formatter failure is **non-fatal**: osmosis logs a warning but exits 0

**Key implementation files:**
- `src/dbt_osmosis_cll/osmosis_propagation/formatting.py` — `run_external_formatter()` function
- `src/dbt_osmosis_cll/osmosis_propagation/settings.py` — `formatter` field on `YamlRefactorSettings`, `_written_files` tracking and `resolved_formatter` property on `YamlRefactorContext`
- `src/dbt_osmosis_cll/osmosis_propagation/schema/writer.py` — `written_file_tracker` callback parameter
- `src/dbt_osmosis_cll/cli/main.py` — `--formatter` option and `_run_formatter_if_configured()` hook

### Pre-commit Integration
Users can add dbt-osmosis-cll as a pre-commit hook:
```yaml
repos:
  - repo: https://github.com/z3z1ma/dbt-osmosis
    rev: v1.1.17
    hooks:
      - id: dbt-osmosis-cll
        files: ^models/
        args: [--target=prod]
        additional_dependencies: [dbt-duckdb]
```

## Code Style

- **Formatter**: Ruff with `--preview` mode
- **Line Length**: 100 characters
- **Python Version**: 3.10+ (uses modern typing)
- **Import Style**: Auto-sorted with ruff's isort rules
- Type hints: Uses `from __future__ import annotations` for forward references
- Pyright: Some modules have pyright suppressions (see `# pyright: reportX=false`)

## Key Files Reference

- **CLI Entry**: src/dbt_osmosis_cll/cli/main.py:48 (`cli()` function)
- **Transform Pipeline**: src/dbt_osmosis_cll/osmosis_propagation/transforms.py
- **YAML Path Logic**: src/dbt_osmosis_cll/osmosis_propagation/path_management.py:45 (`get_target_yaml_path()`)
- **Column Inheritance**: src/dbt_osmosis_cll/osmosis_propagation/inheritance.py:22 (`_build_column_knowledge_graph()`)
- **Database Introspection**: src/dbt_osmosis_cll/osmosis_propagation/introspection.py:33 (`get_columns()`)
- **External Formatter**: src/dbt_osmosis_cll/osmosis_propagation/formatting.py (`run_external_formatter()`)
- **Configuration Resolution**: src/dbt_osmosis_cll/osmosis_propagation/introspection.py:533 (`SettingsResolver`)
- **Property Access**: src/dbt_osmosis_cll/osmosis_propagation/introspection.py:1153 (`PropertyAccessor`)
- **Public API Exports**: src/dbt_osmosis_cll/osmosis_propagation/osmosis.py (re-exports all public APIs)

## Documentation and Resources

- **Official Docs**: https://z3z1ma.github.io/dbt-osmosis-cll/
- **Migration Guide**: https://z3z1ma.github.io/dbt-osmosis-cll/docs/migrating (for 0.x.x → 1.x.x)
- **Quickstart Guide**: `specs/001-unified-config-resolution/quickstart.md` - Developer quickstart for the unified configuration resolution system

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **Record remaining work** - File beads (`bd create`) for anything needing follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - `bd close` finished work, update in-progress beads
4. **PUSH TO REMOTE** - This is MANDATORY (repo policy; overrides the conservative
   default in the managed Beads block below). Beads syncs itself via the installed
   git hooks (`refs/dolt/data` on push) — there is no `bd sync` command in bd 1.x:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Active Technologies
- Python 3.10-3.13 (as specified in pyproject.toml) (001-unified-config-resolution)

## Recent Changes
- 001-unified-config-resolution: Added Python 3.10-3.13 support in pyproject and local/CI validation matrices


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:6cd5cc61 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->
