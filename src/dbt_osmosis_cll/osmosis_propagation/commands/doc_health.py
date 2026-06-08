"""Documentation health report for dbt-osmosis.

Computes column-level documentation coverage for the in-scope nodes (models,
sources, seeds) without mutating anything.  Intended for two audiences:

- **Humans / CI**: a coverage summary and a list of undocumented columns, with an
  optional ``--min-coverage`` gate that fails the build when coverage drops.
- **Agents / tooling**: a stable JSON shape (``--format json``) describing exactly
  which columns are documented, annotation-only, or undocumented per node.

A column is classified as:

- ``documented``      — has a real description after annotation tags are stripped.
- ``annotation_only`` — the description is *only* a CLL annotation block (lineage
  metadata) with no human-authored content; counts against coverage.
- ``undocumented``    — empty or a configured placeholder.

This is an *in-repo* report.  Cross-repo / mesh-wide propagation is intentionally
out of scope (see project notes).
"""

from __future__ import annotations

import typing as t
from dataclasses import asdict, dataclass, field

if t.TYPE_CHECKING:
    from dbt_osmosis_cll.osmosis_propagation.dbt_protocols import YamlRefactorContextProtocol


@dataclass
class NodeHealth:
    """Per-node documentation coverage."""

    node_id: str
    node_name: str
    resource_type: str
    total: int
    documented: int
    annotation_only: int
    undocumented_columns: list[str] = field(default_factory=list)

    @property
    def undocumented(self) -> int:
        return len(self.undocumented_columns)

    @property
    def coverage(self) -> float:
        """Percentage of columns with a real (non-annotation) description."""
        return (self.documented / self.total * 100.0) if self.total else 100.0


@dataclass
class DocHealthReport:
    """Project-wide documentation coverage across all in-scope nodes."""

    nodes: list[NodeHealth] = field(default_factory=list)
    cll_failures: list[str] = field(default_factory=list)
    cll_checked: bool = False

    @property
    def total_columns(self) -> int:
        return sum(n.total for n in self.nodes)

    @property
    def documented_columns(self) -> int:
        return sum(n.documented for n in self.nodes)

    @property
    def annotation_only_columns(self) -> int:
        return sum(n.annotation_only for n in self.nodes)

    @property
    def undocumented_columns(self) -> int:
        return sum(n.undocumented for n in self.nodes)

    @property
    def coverage(self) -> float:
        total = self.total_columns
        return (self.documented_columns / total * 100.0) if total else 100.0

    def to_dict(self) -> dict[str, t.Any]:
        """Stable JSON-serialisable shape for machine consumers."""
        return {
            "summary": {
                "nodes": len(self.nodes),
                "total_columns": self.total_columns,
                "documented_columns": self.documented_columns,
                "annotation_only_columns": self.annotation_only_columns,
                "undocumented_columns": self.undocumented_columns,
                "coverage_pct": round(self.coverage, 2),
                "cll_checked": self.cll_checked,
                "cll_failures": self.cll_failures,
            },
            "nodes": [{**asdict(n), "coverage_pct": round(n.coverage, 2)} for n in self.nodes],
        }


def compute_doc_health(
    context: YamlRefactorContextProtocol,
    *,
    check_cll: bool = False,
) -> DocHealthReport:
    """Build a :class:`DocHealthReport` for the context's in-scope nodes.

    Read-only: inspects column descriptions from the manifest (which reflects the
    on-disk YAML).  When *check_cll* is True, CLL is run per model so the report
    can list models where lineage extraction failed — this requires compiled SQL
    and is therefore opt-in.
    """
    from dbt.artifacts.resources.types import NodeType

    from dbt_osmosis_cll.integration.cll import (
        get_cll_failures,
        get_cll_results,
        strip_annotation_tags,
    )
    from dbt_osmosis_cll.osmosis_propagation.node_filters import _iter_candidate_nodes

    placeholders = set(context.placeholders)
    report = DocHealthReport(cll_checked=check_cll)

    for _uid, node in _iter_candidate_nodes(context):
        total = documented = annotation_only = 0
        undocumented: list[str] = []
        for col_name, col in node.columns.items():
            total += 1
            raw = (col.description or "").strip()
            if not raw or raw in placeholders:
                undocumented.append(col_name)
                continue
            stripped = strip_annotation_tags(raw).strip()
            if not stripped or stripped in placeholders:
                annotation_only += 1
            else:
                documented += 1

        report.nodes.append(
            NodeHealth(
                node_id=node.unique_id,
                node_name=node.name,
                resource_type=str(getattr(node.resource_type, "value", node.resource_type)),
                total=total,
                documented=documented,
                annotation_only=annotation_only,
                undocumented_columns=undocumented,
            )
        )

        if check_cll and node.resource_type not in (NodeType.Source, NodeType.Seed):
            # Populates _CLL_FAILURES as a side effect; result value is unused here.
            get_cll_results(context, node)

    if check_cll:
        report.cll_failures = sorted(get_cll_failures(context))

    return report


def format_report(report: DocHealthReport, *, verbose: bool = False) -> str:
    """Render a human-readable text report."""
    lines: list[str] = []
    lines.append("Documentation health")
    lines.append("=" * 60)
    lines.append(f"Nodes:               {len(report.nodes)}")
    lines.append(f"Columns:             {report.total_columns}")
    lines.append(f"  documented:        {report.documented_columns}")
    lines.append(f"  annotation-only:   {report.annotation_only_columns}")
    lines.append(f"  undocumented:      {report.undocumented_columns}")
    lines.append(f"Coverage:            {report.coverage:.1f}%")
    if report.cll_checked:
        if report.cll_failures:
            lines.append(
                f"CLL failures ({len(report.cll_failures)}): {', '.join(report.cll_failures)}"
            )
        else:
            lines.append("CLL failures:        none")

    # Worst-covered nodes first so the report is actionable.
    incomplete = [n for n in report.nodes if n.undocumented or n.annotation_only]
    if incomplete:
        lines.append("")
        lines.append("Nodes needing attention (lowest coverage first):")
        for node in sorted(incomplete, key=lambda n: (n.coverage, n.node_name)):
            lines.append(
                f"  {node.coverage:5.1f}%  {node.node_name} "
                f"({node.documented}/{node.total} documented, "
                f"{node.annotation_only} annotation-only)"
            )
            if verbose and node.undocumented_columns:
                lines.append(f"           missing: {', '.join(node.undocumented_columns)}")

    return "\n".join(lines)
