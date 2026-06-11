# Phase 1 Analysis & Phase 2 Roadmap — dbt-osmosis-cll

**Date:** 2026-06-10
**Status:** Phase 1 complete. Roadmap below pending/partially approved — confirm order before executing.
**Origin:** Handoff from a Claude Code session that ran the Phase 1 read-only analysis. The
analysis originally started in the (since deleted) stale local clone of `dbt-column-lineage`;
all findings below were verified against THIS repo.

## Context

`dbt-osmosis-cll` is a single installable package fusing two upstream forks:
**dbt-osmosis** (z3z1ma) for YAML doc propagation and **dbt-column-lineage** (Fszta) as the
embedded column-level-lineage (CLL) resolver. Targets dbt Core (manifest.json artifacts;
`ManifestCatalogReader` removes the catalog.json/warehouse dependency). Snowflake-tested.
Upstreams are donors, not parents — no intent to merge back (attribution in `NOTICE`/`licenses/`).

**The four goals:**
1. Automate description inheritance as fully as possible.
2. Let developers declare description ORIGINS with minimal manual overhead.
3. Annotate models so any endpoint column traces to its true origin.
4. Provide a tunable CLL engine (e.g. runtime string injection handling) that can build and
   visualize the CLL for every column in the repo.

**Three pillars:** `src/dbt_osmosis_cll/cll_generator/` (CLL engine), `integration/`
(cache/bridge), `osmosis_propagation/` (inherit, annotate, glossary, schema I/O).

## Verified state (2026-06-10)

- Full core test suite: **805 passed, 11 skipped** (`pytest tests/core`, ~6 min, run from repo root — fixtures need cwd = repo root for `demo_duckdb`).
- The stale standalone clone `C:\Users\F19942\git\github_felix313\dbt-column-lineage` was **deleted** (history safe on GitHub `Felix313/dbt-column-lineage`; its uncommitted WIP was redundant back-ports of code this repo's `cll_generator/` already had). `cll_generator/` here is now the **only** copy of the engine.
- Optional cleanup: archive the `Felix313/dbt-column-lineage` GitHub fork; fix the README claim that `cll_generator/` is vendored "with namespace changes only" (it diverged far ahead: +semantic classification, union branches, registry cache, placeholder patterns).

## Goal assessment (summary)

1. **Inheritance — largely achieved.** `inherit_upstream_column_knowledge_cll` (`osmosis_propagation/transforms.py:757`) + `_resolve_cll_description` (`transforms.py:432`): CLL-first, no name-match fallback, topological waves, idempotent single-pass (walks resolve from origins/anchors via the stable YAML buffer; copies are never laundered). Rename boundary via `inherit-through-renames`; agreement-based union inheritance; computation walls shared with the annotation walker via `integration/cll.py:531 is_computation_wall`.
2. **Origin declaration — mechanism solid, ergonomics middling.** `desc-owner` meta key (`upstream` / `this` default / named anchor), `desc-source` managed provenance key recomputed every run (`transforms.py:1020-1106`), authoritative central glossary (`column-docs-path`). Gap: no command to list the project-wide origin/anchor map.
3. **Annotation — achieved for humans, opt-in for machines.** `annotate_column_origins` (`transforms.py:1590`) + `get_column_origin` (`integration/cll.py:557`): namespaced annotation blocks (Renamed/Derived/Computed/Aggregated/Windowed/UNION/Literal/Generated), per-layer modes (`if_altered`/`always`/off), stale blocks stripped+rebuilt each run. `write-cll-tags-to-meta` off by default. Gap: multi-source computed columns don't list their inputs.
4. **Tunable CLL + visualization — tunable yes, parser has structural weaknesses, visualizer orphaned.** Tunables: `placeholder_patterns` (runtime string injection regex-replacement, `cll_generator/artifacts/registry.py` `_replace_placeholder`), dialect override, 3 compiled-SQL source modes, `include_ephemeral`, `cll-max-origin-depth`, self-union stripping. The HTML lineage explorer exists in `cll_generator/lineage/display/html/` (+ `lineage-ui` extra deps) but is NOT wired into the `dbt-osmosis-cll` CLI.

**Inheritance representation decision:** keep physical copying + recomputed `desc-source` pointer + recomputed annotations. Fusion-style pure references aren't reachable in dbt Core; idempotent recomputation already prevents copy-staleness.

## Known parser/engine weaknesses (all in `cll_generator/`, hand-rolled CTE tracer in `parser/sql_parser.py`)

1. **Multi-source collapse:** `_store_column_lineage_in_cte` keeps only `sorted(sources)[0]` per CTE column; API takes `lineage[0]` + first sorted source. `COALESCE(a.x, b.y)` through a CTE credits one arbitrary input (unions are the exception — `union_branches` is complete).
2. **Global alias map:** `get_table_aliases(parsed)` flattens aliases across ALL CTEs into one dict → same alias bound to different tables in different CTEs collides.
3. **Name-keyed registry:** models keyed by lowercase short name (`artifacts/registry.py`, `manifest_catalog.py`) → model/source-identifier and cross-package name collisions.
4. **No schema-aware resolution:** unqualified column in a join resolves to the first FROM table (`get_table_context`), even though `ManifestCatalogReader` knows the column lists. sqlglot `qualify`/scope machinery unused.
5. **O(N²) manifest scans:** `ManifestReader._find_node` (`artifacts/manifest.py`) linearly scans all nodes per lookup, several times per model at registry load (warm runs OK via process registry cache in `api.py` + disk cache `target/cll_cache.json`; cold runs pay it).

Wrong edges are **silent** and propagate wrong descriptions — highest-value fixes.

## Decision: REFACTOR (no greenfield rewrite)

Coupling is narrow (everything funnels through `integration/cll.py` → `cll_generator.api.get_column_lineage()` returning `ColumnLineageResult`); the propagation layer encodes hard-won tested knowledge (805 tests, idempotency design); the weak parts (parser core, name-keyed registry) sit behind stable contracts (`SQLParseResult`, `get_column_lineage`) and can be rebuilt in place.

## Roadmap (execution order; one item per checkpoint, tests passing before next)

| # | Item | Pain removed | Files | Effort | Status |
|---|---|---|---|---|---|
| 1 | Single source of truth for engine | hand-syncing two forks | (repo deletion done) README claim fix; optional GitHub archive | S | ~done |
| 2 | Manifest node index: `{name → node}` dict built once in `ManifestReader.load()`, kill `_find_node` linear scans | O(N²) cold-run cost | `cll_generator/artifacts/manifest.py`, `registry.py` | S | next |
| 3 | Identity by `unique_id`: registry keyed on manifest unique_id w/ name-alias map (additive `unique_id` field on `ColumnLineageResult`) | name-collision misresolution | `registry.py`, `manifest_catalog.py`, `api.py` | M | |
| 4 | Parser core hardening (in-place, behind `SQLParseResult`): per-scope alias resolution, preserve multi-source sets through CTE hops, schema-aware unqualified-column resolution via `ManifestCatalogReader` column lists; evaluate sqlglot `qualify`+scope as backbone; golden-file tests from real Snowflake repo as gate | silently wrong edges → wrong docs | `parser/sql_parser.py`, `sql_parser_utils.py`, tests | L | |
| 5 | Multi-source origins end-to-end: `progenitors: list[(model, col)]` on `ColumnLineageResult` (generalize `union_branches`); annotate "Computed in: MODEL from A.x, B.y" | computed endpoint columns don't say what feeds them | `api.py`, parser (depends #4), `annotations.py`, `transforms.py` annotate pass | M | |
| 6 | Ship visualizer: `dbt-osmosis-cll lineage explore` wiring existing HTML explorer to `ManifestCatalogReader` + CLL disk cache | goal-4 visualization not in product | `cli/main.py`, `cll_generator/lineage/display/html/`, `integration/cll.py` | M | |
| 7 | Machine-readable origins by default on endpoint/DP layers (`write-cll-tags-to-meta: true` + `annotate: always` pattern, docs) | catalog tools don't see origins | `config.py`, `docs/USAGE.md` | S | |
| 8 | Endpoint trust report: extend `doc_health` with per-model authored / inherited(origin) / glossary / annotation-only / CLL-failed breakdown, JSON for CI | "documented" ≠ "trustworthy"; CI gate vs silent CLL regressions | `commands/doc_health.py`, `cli/main.py` | S/M | |

## Out of scope (cut executed 2026-06-11)

The cut line was executed, not just flagged. Removed entirely: the Streamlit workbench
(`commands/workbench/`, CLI command, `workbench` extra), the LLM client layer
(`commands/llm.py`, `openai` extra, `test-llm`), `--synthesize` on refactor/document, the
`nl` group and `generate model`/`generate query`, AI staging (`commands/staging.py`,
`--ai` flag), voice learning (`commands/voice_learning.py`), and the AI path of test
suggestions (now pattern-only `TestSuggester`; `AITestSuggester` remains as an alias).

**Why remove rather than adapt LLM doc synthesis:** devs run coding agents (Claude Code)
directly in the repo. An agent with full repo context writes better docs than one-shot
completions seeded with a column name + truncated SQL; and in-pipeline synthesis filled
descriptions at every layer, fighting the origin/provenance architecture (generated text
at non-origin nodes would be inherited downstream as if authored). The agent-facing
surface the package keeps is deterministic: `yaml doc-health --format json` (gap
discovery), authored origin descriptions, `yaml document` (propagation + desc-source).
Documented in `docs/docs/tutorial-yaml/agents.md` and `docs/USAGE.md`.

Still flagged, do not build: SQL lint expansion (existing `lint` stays as-is).

## Phase 2 working protocol

- One roadmap item per checkpoint; `pytest tests/core` (from repo root) green before moving on.
- Don't break the `ColumnLineageResult` / `get_column_lineage` contract — extend additively.
- Keep targeting dbt Core artifacts; never propose dbt Fusion.
