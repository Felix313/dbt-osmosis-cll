"""Golden corpus gate: parse a real dbt project's compiled SQL (roadmap #4).

Set ``DBT_OSMOSIS_CLL_GOLDEN_DIR`` to a ``target/compiled/<package>`` directory
of a real (e.g. Snowflake) dbt project to activate; the test is skipped
everywhere else. No proprietary SQL is committed to this repo — the corpus
stays local, only the parse-quality invariants live here.

Optional: ``DBT_OSMOSIS_CLL_GOLDEN_DIALECT`` (default ``snowflake``).

Invariants gated:
- the parser must not raise on ANY readable compiled file;
- at least 98% of parsed files must yield lineage (columns or star sources) —
  the remainder allows for truncated compile artifacts (files ending inside a
  CTE with no final SELECT), which legitimately produce nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from dbt_osmosis_cll.cll_generator.parser import SQLColumnParser

_GOLDEN_DIR = os.environ.get("DBT_OSMOSIS_CLL_GOLDEN_DIR", "")

pytestmark = pytest.mark.skipif(
    not _GOLDEN_DIR or not Path(_GOLDEN_DIR).is_dir(),
    reason="DBT_OSMOSIS_CLL_GOLDEN_DIR not set / not a directory",
)


def test_real_corpus_parses_without_exceptions():
    dialect = os.environ.get("DBT_OSMOSIS_CLL_GOLDEN_DIALECT", "snowflake")
    root = Path(_GOLDEN_DIR)
    files = sorted((root / "models").rglob("*.sql")) or sorted(root.rglob("*.sql"))
    assert files, f"no .sql files under {root}"

    parser = SQLColumnParser(dialect=dialect)
    exceptions: list[tuple[str, str]] = []
    no_lineage: list[str] = []
    parsed = 0

    for f in files:
        try:
            sql = f.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            continue  # unreadable stray files are not the parser's problem
        if not sql:
            continue
        rel = str(f.relative_to(root))
        try:
            result = parser.parse_column_lineage(sql)
        except Exception as exc:  # noqa: BLE001 — we report, then fail collectively
            exceptions.append((rel, f"{type(exc).__name__}: {exc}"))
            continue
        parsed += 1
        if not result.column_lineage and not result.star_sources:
            no_lineage.append(rel)

    assert not exceptions, (
        f"{len(exceptions)} files raised during parsing:\n"
        + "\n".join(f"  {r}: {e}" for r, e in exceptions[:10])
    )
    assert parsed > 0
    lineage_ratio = (parsed - len(no_lineage)) / parsed
    assert lineage_ratio >= 0.98, (
        f"only {lineage_ratio:.1%} of {parsed} files yielded lineage; "
        f"zero-lineage files:\n" + "\n".join(f"  {r}" for r in no_lineage[:10])
    )
