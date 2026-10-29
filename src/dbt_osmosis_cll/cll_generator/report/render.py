"""Write the data-product report: a self-contained HTML file plus CSV / Markdown.

The HTML embeds the lineage graph (compacted: model names as indices) and the
browser engine, so it works offline from a file share and recomputes instantly
when data products are toggled. CSV and Markdown reflect the configured selection.
"""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

from dbt_osmosis_cll.cll_generator.report.analysis import Analysis, Graph, column_status

_STATIC = Path(__file__).parent / "static"
_MAX_PREDICATE = 300


def compact_graph(graph: Graph, lang: str = "en") -> dict[str, Any]:
    """The embedded data blob read by ``static/engine.js`` (``CllEngine.expand``)."""
    names = set(graph.models)
    for model, col, _t, progs in graph.rows:
        names.add(model)
        names.update(p[0] for p in progs)
    for u in graph.usages:
        names.update((u[0], u[2]))
    for model, parents in graph.deps.items():
        names.add(model)
        names.update(parents)
    order = sorted(names)
    idx = {n: i for i, n in enumerate(order)}

    types: list[str] = []
    kinds: list[str] = []

    def code(table: list[str], value: str) -> int:
        if value not in table:
            table.append(value)
        return table.index(value)

    models = []
    mcols: dict[int, list[str]] = {}
    for name in order:
        meta = graph.models.get(name) or {"kind": "relation", "analysed": False}
        models.append([
            name,
            meta.get("kind") or "relation",
            meta.get("layer") or "",
            meta.get("materialized") or "",
            1 if meta.get("analysed") else 0,
            meta.get("path") or "",
        ])
        if not meta.get("analysed") and meta.get("columns"):
            mcols[idx[name]] = list(meta["columns"])
    return {
        "v": 1,
        "lang": lang,
        "generated": date.today().isoformat(),
        "models": models,
        "mcols": mcols,
        "types": types,
        "usageKinds": kinds,
        "rows": [
            [idx[m], c, code(types, t), [[idx[p[0]], p[1]] for p in progs]]
            for m, c, t, progs in graph.rows
        ],
        "usages": [
            [idx[u[0]], code(kinds, u[1]), idx[u[2]], u[3], u[4], (u[5] or "")[:_MAX_PREDICATE]]
            for u in graph.usages
        ],
        "deps": [[idx[m], [idx[p] for p in ps]] for m, ps in sorted(graph.deps.items())],
        "gaps": [[idx[m], c, k] for m, c, k in graph.gaps],
        "documentedOnly": [[idx[m], c] for m, c in graph.documented_only],
        "coverage": graph.coverage,
        "selectors": graph.selectors,
        "dataProducts": graph.data_products,
    }


def render_html(graph: Graph, lang: str = "en") -> str:
    blob = json.dumps(compact_graph(graph, lang), ensure_ascii=False, separators=(",", ":"))
    blob = blob.replace("</", "<\\/")
    template = (_STATIC / "report.html").read_text(encoding="utf-8")
    title = (
        "Spaltenbedarf der Datenprodukte" if lang == "de" else "Data-product column requirements"
    )
    return (
        template.replace("__LANG__", lang)
        .replace("__TITLE__", title)
        .replace("/*__ENGINE__*/", (_STATIC / "engine.js").read_text(encoding="utf-8"))
        .replace("/*__APP__*/", (_STATIC / "app.js").read_text(encoding="utf-8"))
        .replace("/*__DATA__*/null", blob)
    )


def write_columns_csv(graph: Graph, result: Analysis, path: Path) -> int:
    rows = column_status(graph, result)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "model",
            "column",
            "role",
            "layer",
            "status",
            "category",
            "usages",
            "needed_by",
        ])
        for r in rows:
            writer.writerow([
                r["model"].upper(),
                r["column"].upper(),
                r["role"],
                r["layer"],
                r["status"],
                r["category"],
                " ".join(r["usages"]),
                " ".join(d.upper() for d in r["needed_by"]),
            ])
    return len(rows)


def write_data_products_csv(result: Analysis, path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(["wave", "data_product", "depends_on", "data_columns", "logic_columns"])
        for dp in sorted(result.data_products, key=lambda d: (result.waves[d], d)):
            ups = result.anchors.get(dp) or {}
            if not ups:
                writer.writerow([result.waves[dp], dp.upper(), "", 0, 0])
            for up, cols in sorted(ups.items()):
                n_data = sum(1 for cats in cols.values() if "data" in cats)
                writer.writerow([
                    result.waves[dp],
                    dp.upper(),
                    up.upper(),
                    n_data,
                    len(cols) - n_data,
                ])


def write_markdown(graph: Graph, result: Analysis, path: Path) -> None:
    status = column_status(graph, result)
    interim = [r for r in status if r["role"] == "interim"]
    models_needed = {r["model"] for r in interim if r["status"] == "required"}
    interim_models = {r["model"] for r in interim}
    unresolved = [g for g in graph.gaps if g[2] == "unresolved"]
    lines = [
        "# Data-product column requirements",
        "",
        f"Generated {date.today().isoformat()} · scope: {' '.join(graph.selectors)}",
        "",
        "## Coverage",
        "",
        f"- columns without traced SQL: {len(unresolved)}",
        f"- references to columns the upstream model does not output: {len(result.dangling)}",
        "- relations without known columns: "
        f"{len(graph.coverage.get('relations_without_columns') or [])}",
        f"- documented in YAML but not produced by the SQL (ignored): {len(graph.documented_only)}",
        "",
        "## Build order",
        "",
        "| Wave | Data product | Reads from data products |",
        "|---:|---|---|",
    ]
    for dp in sorted(result.data_products, key=lambda d: (result.waves[d], d)):
        ups = result.anchors.get(dp) or {}
        reads = ", ".join(f"{u.upper()} ({len(c)} columns)" for u, c in sorted(ups.items()))
        lines.append(f"| {result.waves[dp]} | {dp.upper()} | {reads or '–'} |")
    if result.missing:
        lines += ["", "Not in the analysed scope: " + ", ".join(d.upper() for d in result.missing)]
    if result.dbt_only_deps:
        lines += ["", "dbt dependencies without column use:"]
        lines += [
            f"- {dp.upper()} → {', '.join(u.upper() for u in ups)}"
            for dp, ups in sorted(result.dbt_only_deps.items())
        ]
    lines += [
        "",
        "## Interim models",
        "",
        f"- needed: {len(models_needed)} of {len(interim_models)}",
        f"- columns required: {sum(1 for r in interim if r['status'] == 'required')}",
        f"- columns droppable: {sum(1 for r in interim if r['status'] == 'droppable')}",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
