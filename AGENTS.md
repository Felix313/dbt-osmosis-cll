# Repository Guidelines

## Project Overview

`dbt-osmosis-cll` is a Python CLI and package for dbt development workflows. The repo centers on four surfaces:

- schema YAML management (`yaml organize`, `yaml document`, `yaml refactor`)
- column-level documentation inheritance across dbt lineage
- ad-hoc SQL compile/run helpers

Other CLI families (`diff`, `lint`, `test`, `generate`) reuse the same project/bootstrap spine rather than defining separate runtimes. There is intentionally no in-package LLM client: developers run coding agents (e.g. Claude Code) in the repo for doc authoring, and the package supplies the deterministic surfaces those agents need (`yaml doc-health --format json`, `yaml document`).

Primary entrypoint: `src/dbt_osmosis_cll/cli/main.py`
Package entrypoint: `src/dbt_osmosis_cll/__main__.py`

## Architecture & Data Flow

### Main execution spine
1. Click commands in `src/dbt_osmosis_cll/cli/main.py` parse flags and build `DbtConfiguration`.
2. `src/dbt_osmosis_cll/osmosis_propagation/config.py:create_dbt_project_context()` loads the dbt project, adapter, and manifest.
3. YAML commands create `YamlRefactorContext` from `src/dbt_osmosis_cll/osmosis_propagation/settings.py`.
4. Candidate nodes are filtered in `src/dbt_osmosis_cll/osmosis_propagation/node_filters.py`.
5. Transform chains in `src/dbt_osmosis_cll/osmosis_propagation/transforms.py` mutate model/source metadata.
6. YAML is read and written through `src/dbt_osmosis_cll/osmosis_propagation/schema/reader.py` and `writer.py`.
7. `src/dbt_osmosis_cll/osmosis_propagation/sync_operations.py` merges manifest-backed truth back into schema files.

### Key architectural boundaries
- `src/dbt_osmosis_cll/osmosis_propagation/introspection.py` is the configuration and property-resolution center. Prefer `SettingsResolver` and `PropertyAccessor` over ad hoc config lookups.
- `src/dbt_osmosis_cll/osmosis_propagation/path_management.py` owns YAML routing and project-root safety checks.
- `src/dbt_osmosis_cll/osmosis_propagation/inheritance.py` builds the column knowledge graph used for documentation inheritance.
- `src/dbt_osmosis_cll/osmosis_propagation/commands/sql_operations.py` is the shared SQL compile/execute path used by CLI and proxy code.
- `src/dbt_osmosis_cll/osmosis_propagation/schema/parser.py`, `reader.py`, and `writer.py` split YAML concerns deliberately: filter dbt-osmosis-owned sections, cache reads, then restore preserved sections on atomic write.

### Public vs. internal surfaces
- `src/dbt_osmosis_cll/osmosis_propagation/osmosis.py` is the compatibility/public facade. `src/dbt_osmosis_cll/osmosis_propagation/__init__.py` is no longer a re-export surface; internal code should import concrete submodules directly.
- Deep edits under `src/dbt_osmosis_cll/osmosis_propagation/` must also follow `src/dbt_osmosis_cll/osmosis_propagation/AGENTS.md`.

## Key Directories

- `src/dbt_osmosis_cll/cli/` — Click command groups and user-facing entrypoints
- `src/dbt_osmosis_cll/osmosis_propagation/` — dbt context setup, config resolution, transforms, YAML I/O, inheritance, plugins
- `src/dbt_osmosis_cll/osmosis_propagation/schema/` — round-trip YAML parsing, caching, writing, validation
- `src/dbt_osmosis_cll/integration/` — SQL proxy and related helpers
- `tests/` — pytest suite; `tests/core/` mirrors core modules, root tests cover higher-level YAML behavior
- `demo_duckdb/` — canonical dbt fixture project used by tests and examples
- `docs/` — Docusaurus docs site; actual content lives under `docs/docs/`
- `specs/001-unified-config-resolution/` — detailed spec/plan/quickstart for config-resolution work
- `_deps/` — vendored dbt packages; avoid editing unless the task explicitly targets vendored code

## Important Files

| Path | Why it matters |
| --- | --- |
| `pyproject.toml` | Source of truth for Python support, dependencies, console script, Ruff, pytest, pyright |
| `Taskfile.yml` | Canonical developer workflow (`task format`, `task lint`, `task test`, `task dev`) |
| `.pre-commit-config.yaml` / `.pre-commit-hooks.yaml` | Repo hygiene policy plus packaged `dbt-osmosis-cll yaml refactor -C` pre-commit hook contract |
| `src/dbt_osmosis_cll/cli/main.py` | Complete CLI surface: `yaml`, `sql`, `generate`, `test`, `lint`, `diff` |
| `docs/package.json` / `docs/docusaurus.config.js` | Source of truth for docs-site tooling and Docusaurus 3 configuration |
| `demo_duckdb/dbt_project.yml` / `demo_duckdb/dbt-osmosis-cll.yml` | Best concrete examples of routing rules, config precedence, and YAML formatting defaults |
| `src/dbt_osmosis_cll/osmosis_propagation/config.py` | dbt project/bootstrap and manifest loading |
| `src/dbt_osmosis_cll/osmosis_propagation/settings.py` | `YamlRefactorContext`, formatter settings, catalog handling |
| `src/dbt_osmosis_cll/osmosis_propagation/introspection.py` | `SettingsResolver`, `PropertyAccessor`, caches, config precedence |
| `src/dbt_osmosis_cll/osmosis_propagation/schema/parser.py` / `reader.py` / `writer.py` | Canonical round-trip YAML filter/cache/preserve pipeline |
| `src/dbt_osmosis_cll/osmosis_propagation/transforms.py` | `TransformPipeline` and main YAML mutation operations |
| `src/dbt_osmosis_cll/osmosis_propagation/inheritance.py` | column lineage and inheritance logic |
| `src/dbt_osmosis_cll/osmosis_propagation/commands/sql_operations.py` | Shared SQL compile/execute helpers used outside just the CLI |
| `src/dbt_osmosis_cll/osmosis_propagation/path_management.py` | YAML routing, source YAML bootstrapping, root-path validation |
| `tests/conftest.py` | expensive shared dbt fixture builders and `yaml_context` |
| `tests/core/conftest.py` | ensures `demo_duckdb/target/manifest.json` exists before core tests |
| `demo_duckdb/integration_tests.sh` | integration smoke sequence; resets fixture files with `git checkout`/`git clean` |

## Development Commands

Prefer `task` and `uv`; avoid ad hoc environment management.

```bash
# Setup / full local flow
task dev
task

# Formatting and linting
task format
task lint
pre-commit run --all-files

# Tests
uv run dbt parse --project-dir demo_duckdb --profiles-dir demo_duckdb -t test
uv run pytest
task test

# Focused test runs
uv run pytest tests/core/test_cli.py
uv run pytest tests/test_yaml_inheritance.py

# CLI examples
uv run dbt-osmosis-cll yaml refactor --project-dir demo_duckdb --profiles-dir demo_duckdb
uv run dbt-osmosis-cll sql compile "select 1"
uv run dbt-osmosis-cll yaml doc-health --format json --project-dir demo_duckdb --profiles-dir demo_duckdb
```

Docs site commands use the separate Node toolchain in `docs/`:

```bash
npm --prefix docs run start
npm --prefix docs run build
npm --prefix docs run serve
```

## Runtime & Tooling Preferences

- Python: `>=3.10,<3.14`; local default is `.python-version` = `3.12`
- Package manager / venv: `uv`
- Build backend: `hatchling`
- Formatter/linter/import sorter: Ruff is canonical, even though Black/isort config still exists in `pyproject.toml`
- Test runner: `pytest`
- Type checking: pyright only covers `src/dbt_osmosis_cll/osmosis_propagation` and `src/dbt_osmosis_cll/cli`
- Docs toolchain: Docusaurus 3 in `docs/`, Node `>=18`

Important nuance: `task` is not a pure verification command; it formats, lints, tests, and defers `task dev`.

## Code Conventions & Common Patterns

### YAML and schema handling
- Use `ruamel.yaml` round-trip machinery in `src/dbt_osmosis_cll/osmosis_propagation/schema/`. Do not introduce new PyYAML-based schema editing.
- Read/write schema files through the schema helpers, not manual file I/O. The reader/writer preserve non-osmosis sections and clear caches safely.
- Atomic write behavior and preserved sections are part of the contract; bypassing them can silently lose YAML content.
- Keep the parser/reader/writer split intact: parsing filters owned top-level sections, reads cache both filtered and original content, and writes merge preserved sections back before atomic replace.

### Configuration resolution
- New config logic should flow through `SettingsResolver.resolve()`.
- Do not add new call sites for deprecated `_get_setting_for_node()`.
- Respect the established precedence model documented in code and demo config:
  - column meta
  - node meta / dbt-osmosis-cll options
  - `config.extra`
  - supplementary `dbt-osmosis-cll.yml`
  - `vars`
  - fallback defaults

### Transform and inheritance flow
- YAML refactor behavior is pipeline-based; compose operations with `TransformPipeline` and the `>>` operator.
- Column documentation inheritance belongs in `core/inheritance.py` and `core/transforms.py`, not in CLI glue.
- Node selection and ordering should stay in `core/node_filters.py`, not scattered across callers.

### Caching and concurrency
- `_COLUMN_LIST_CACHE` and `_YAML_BUFFER_CACHE` are shared caches with lock/ownership expectations.
- Do not bypass cache helpers or mutate cache state casually in production code.
- Tests explicitly reset caches; keep new tests isolated when touching cache-sensitive code.

### No in-package LLM client
- The Streamlit workbench and the OpenAI/LLM synthesis layer (`commands/llm.py`, `--synthesize`, `nl`/`generate model`/`generate query`, voice learning) were removed deliberately. Do not reintroduce API-key-driven LLM calls; agent-assisted documentation flows through `yaml doc-health --format json` + authored origin descriptions + `yaml document`.

## Testing & QA

### Test layout
- `tests/core/` mirrors `src/dbt_osmosis_cll/osmosis_propagation/` for focused unit coverage.
- Root-level `tests/test_yaml_*.py` files exercise higher-level YAML, manifest, and inheritance behavior against a real dbt fixture.
- CLI tests use `click.testing.CliRunner` and mostly validate command surfaces and help text.

### Fixture expectations
- `demo_duckdb/` is the canonical integration fixture.
- Many tests require `demo_duckdb/target/manifest.json`; generate it with `dbt parse` if missing.
- `tests/conftest.py` builds temp DuckDB projects via `dbt seed`, `dbt run`, and `dbt docs generate`.
- The earlier PostgreSQL fixture branch was removed because it was unexercised; test fixture support is DuckDB-only today.

### QA cautions
- dbt-version differences change manifest shape; avoid brittle assertions when adding tests.
- Some tests mutate cwd or shared caches, so they are not automatically parallel-safe.
- `task test` is expensive; for iterative work prefer targeted pytest runs after ensuring the manifest exists.
- CI covers a broader dbt matrix than the local Taskfile.

## Documentation & Demo Surfaces

- Root `README.md` is a lightweight landing page, not the full reference.
- Canonical CLI/config docs live in `docs/docs/`, especially `docs/docs/reference/cli.md` and the YAML workflow/configuration guides.
- `docs/README.md` is boilerplate and currently stale; it still references Docusaurus 2 even though the site runs on Docusaurus 3.
- The README intentionally omits some newer CLI families; use the Docusaurus CLI reference for `generate` details.
- `screenshots/` is illustrative only.
- Generated/disposable artifacts include `docs/build/`, `demo_duckdb/target/`, `logs/`, and DuckDB database outputs.

## Common Pitfalls

- Do not document Black or isort as the active formatter; use Ruff.
- Do not edit YAML files with plain string manipulation when schema helpers already exist.
- Do not copy the few remaining PyYAML-style legacy paths into new schema-mutating code; round-trip YAML work belongs in `core/schema/`.
- Do not bypass project-root path validation in `path_management.py`.
- Do not run `demo_duckdb/integration_tests.sh` on a dirty tree you care about; it restores fixture paths with destructive git commands.
- Do not assume README command coverage is complete; newer CLI families are documented in the Docusaurus reference.
- Do not treat `core/osmosis.py` re-exports as the best place to implement new behavior.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:970c3bf2 -->
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
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
