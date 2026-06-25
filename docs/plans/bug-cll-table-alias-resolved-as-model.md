# Bug: table alias resolved as model name in CLL cache

**Date:** 2026-06-17
**Severity:** Low (affects one edge type; downstream consumers can detect and filter)
**Affected component:** `cll_generator/api.py` — `_resolve_progenitor` / `source_columns` extraction

---

## Symptom

When a dbt model uses a short table alias that matches a reserved dbt Jinja variable
name (e.g. `tar` aliased as `tar`, or a CTE named `target`), the CLL cache records
a phantom progenitor like:

```json
{
  "progenitor_model": "target",
  "progenitor_column": "row_hashdiff"
}
```

`target` is not a dbt model — it is the dbt runtime object exposed via `{{ target.* }}`
Jinja context. It will never resolve to a real node in the manifest.

---

## Root cause

The compiled SQL for `EXPORT_DAT_TOP_CUSTOMER_FLG_DELTA_HIST` contains a
FULL OUTER JOIN pattern using the alias `tar` for the target table:

```sql
-- from the compiled SQL (simplified):
SELECT
    CASE WHEN tar.row_hashdiff IS NULL THEN 'I' ELSE 'U' END AS record_flg
FROM src
FULL OUTER JOIN DC_CBMDATAMART.some_table AS tar
    ON ...
WHERE (src.row_hashdiff != tar.row_hashdiff OR tar.row_hashdiff IS NULL)
```

The SQL parser extracts `tar.row_hashdiff` as a source column reference.
`_resolve_progenitor` (and the general `source_columns` loop in `api.py`) splits on
the last `.` to produce `(model="tar", column="row_hashdiff")`.

Because `tar` is not a CTE and not a model registered in the manifest, it is not
stripped by the `__dbt__cte__` prefix guard. It flows through as-is, producing
`progenitor_model = "tar"`.

**In the specific cache entry observed**, the string stored is `"target"` rather
than `"tar"`. The most likely explanation is that the model previously referenced
`{{ target.schema }}` in a way that caused `target` (the dbt Jinja namespace object)
to appear as a table alias in the compiled SQL fragment that the parser analysed.

---

## Observed cache entry

File: `target/cll_cache.json`
Model: `EXPORT_DAT_TOP_CUSTOMER_FLG_DELTA_HIST`
Column: `record_flg`

```json
{
  "model": "export_dat_top_customer_flg_delta_hist",
  "column": "record_flg",
  "is_computed": true,
  "progenitor_model": "target",
  "progenitor_column": "row_hashdiff",
  "progenitors": [["target", "row_hashdiff"]]
}
```

The alias `tar` in the SQL is for the persisted-state table (the JOIN partner in the
delta-load pattern). It is not a dbt model — it is a raw Snowflake table reference
constructed from `{{ target.schema }}.TABLE_NAME` before Jinja compilation, and the
compiled output retained `tar` (or `target`) as the table qualifier.

---

## Impact

- `progenitor_model = "target"` is a phantom node that does not exist in the manifest.
- Any consumer that follows the progenitor chain will immediately fail to resolve it
  and should treat it as a dead end.
- The column `record_flg` has a derivation that spans two sides of a FULL OUTER JOIN;
  it is genuinely computed (`is_computed = True`), so `progenitor_model = None` would
  be the semantically correct result.

---

## Suggested fix

In `_resolve_progenitor` (and the `progenitors` loop in `api.py` ~line 299), after
stripping `__dbt__cte__`, check whether the resolved model name is a known dbt
reserved identifier or is absent from `terminal_node_names` **and** not in the
manifest node set. If so, treat it as unresolvable:

```python
JINJA_RESERVED = frozenset({"target", "this", "model", "config", "var", "env_var"})

def _resolve_progenitor(lin, known_model_names=None) -> tuple[Optional[str], Optional[str]]:
    if not lin.source_columns:
        return None, None
    src = next(iter(sorted(lin.source_columns)))
    if "." not in src:
        return None, src.lower()
    parts = src.rsplit(".", 1)
    model_part = parts[0].lower()
    if model_part.startswith("__dbt__cte__"):
        model_part = model_part[len("__dbt__cte__"):]
    # Guard: reject dbt Jinja namespace names that leaked into source_columns
    if model_part in JINJA_RESERVED:
        return None, None
    # Optional: reject anything not in the known model set
    if known_model_names is not None and model_part not in known_model_names:
        return None, None
    return model_part, parts[1].lower()
```

The simpler path is the `JINJA_RESERVED` guard alone — it catches the most likely
false positives without requiring the full model set to be threaded through.

---

## Workaround (downstream consumer)

Until fixed, consumers reading `cll_cache.json` can detect phantom nodes by checking
that `progenitor_model` is present as a key in `entries`. If it is not, the entry
should be treated as `progenitor_model = None` (computed/unresolvable):

```python
CLL_PHANTOM_NODES = {'target', 'this', 'model'}  # expand as encountered

if r.get('progenitor_model', '').lower() in CLL_PHANTOM_NODES:
    # treat as computed — no traceable single-source progenitor
    lineage_status = 'cll_phantom_node'
```
