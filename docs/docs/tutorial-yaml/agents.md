---
sidebar_position: 6
---

# Documenting with coding agents

dbt-osmosis deliberately ships no LLM client. When you want AI help writing column or
model descriptions, run a coding agent (Claude Code, or any terminal agent) directly in
the repository and let it drive dbt-osmosis's deterministic surfaces. An agent with full
repo context — model SQL, upstream YAML, business docs, the central glossary — writes far
better descriptions than one-shot prompt synthesis, and the package guarantees the parts
that must be exact: gap detection, provenance, and propagation.

## The workflow

1. **Find the gaps.** Ask the agent to run:

   ```bash
   dbt-osmosis yaml doc-health --format json
   ```

   The JSON shape is stable and machine-readable: per model it lists which columns lack
   real authored descriptions, distinguishing them from inherited or annotation-only ones.

2. **Author at the origin.** The agent should write descriptions where a column first
   appears — the source or model that owns it, or the central glossary configured via
   `column-docs-path`. Never paste generated text into downstream models; inheritance
   covers those.

3. **Propagate.** Run the normal pipeline:

   ```bash
   dbt-osmosis yaml document
   ```

   CLL-driven inheritance copies the new origin descriptions to every downstream column,
   records provenance (`desc-source`), and rebuilds origin annotations.

4. **Gate in CI.**

   ```bash
   dbt-osmosis yaml doc-health --min-coverage 90
   ```

## Why no built-in `--synthesize`?

Earlier versions shipped an OpenAI-backed `--synthesize` flag. It was removed because:

- a single-shot completion seeded with a column name and truncated SQL invents plausible
  but unverified text, while an in-repo agent can read everything and iterate;
- it filled missing descriptions at *every* layer, which fought the origin/provenance
  architecture — generated text at non-origin nodes was later inherited downstream as if
  a human had authored it;
- maintaining a multi-provider API client (keys, retries, prompt drift) is orthogonal to
  this package's job: deterministic YAML management and lineage-aware propagation.
