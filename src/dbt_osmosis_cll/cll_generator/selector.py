"""dbt-style node selection for standalone CLL builds (``parse-cll``).

Supports dbt's graph-operator syntax in both directions:

- ``my_model``      — just the model itself
- ``+my_model``     — the model and ALL upstream models, up to sources
- ``2+my_model``    — the model and at most 2 generations of upstream models
- ``my_model+``     — the model and ALL downstream models, to the endpoints
- ``my_model+2``    — the model and at most 2 generations of downstream models
- ``+my_model+``    — both directions (each side may carry its own depth)

Method selectors (``tag:``, ``path:``, ``@model``) are not supported; the
resolver only ever needs the graph closure of a named anchor model.

Resolution happens over the raw ``manifest.json`` dict (``depends_on.nodes``
unique-id edges, inverted for the downstream walk), so no dbt runtime is
required. Source and seed nodes terminate the upstream walk naturally — they
are not SQL models, so CLL has no rows for them; they appear as progenitors
inside the selected models' results instead.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_SELECTOR_RE = re.compile(
    r"^(?:(?P<up>\d*)\+)?(?P<model>[A-Za-z0-9_][A-Za-z0-9_.\-]*?)(?:\+(?P<down>\d*))?$"
)


@dataclass(frozen=True)
class GraphSelector:
    """A parsed ``[N+]model_name[+M]`` selector token."""

    model: str
    """Anchor model name (as typed; matching is case-insensitive)."""

    up_depth: int | None = 0
    """Upstream generations to include: ``None`` = unlimited (bare leading ``+``),
    ``0`` = no upstream (no leading ``+`` given)."""

    down_depth: int | None = 0
    """Downstream generations to include: ``None`` = unlimited (bare trailing ``+``),
    ``0`` = no downstream (no trailing ``+`` given)."""


def parse_selector(token: str) -> GraphSelector:
    """Parse one selector token into a :class:`GraphSelector`.

    Raises:
        ValueError: On method selectors or malformed tokens.
    """
    token = token.strip()
    if not token:
        raise ValueError("Empty selector.")
    if ":" in token or token.startswith("@"):
        raise ValueError(
            f"Selector '{token}': method selectors (tag:, path:, @model) are not "
            "supported — use a model name with optional '+'/'N+' (upstream) and "
            "'+'/'+N' (downstream) operators."
        )
    match = _SELECTOR_RE.match(token)
    if not match:
        raise ValueError(f"Selector '{token}' is not valid. Expected [N+]model_name[+M].")
    up, model, down = match.group("up"), match.group("model"), match.group("down")
    return GraphSelector(
        model=model,
        up_depth=0 if up is None else (int(up) if up else None),
        down_depth=0 if down is None else (int(down) if down else None),
    )


def parse_selectors(raw_values: list[str]) -> list[GraphSelector]:
    """Split each raw ``--select`` value on whitespace/commas and parse every token."""
    selectors: list[GraphSelector] = []
    for raw in raw_values:
        for token in re.split(r"[,\s]+", raw):
            if token:
                selectors.append(parse_selector(token))
    if not selectors:
        raise ValueError("No selector given.")
    return selectors


def _bfs(
    anchor_uid: str,
    depth: int | None,
    edges: dict[str, list[str]],
    nodes: dict[str, Any],
) -> set[str]:
    """Breadth-first walk from *anchor_uid* over *edges*, at most *depth* generations
    (``None`` = unlimited). Only model/snapshot nodes are followed and returned;
    the anchor itself is not included."""
    reached: set[str] = set()
    frontier = [anchor_uid]
    seen = {anchor_uid}
    generation = 0
    while frontier and (depth is None or generation < depth):
        next_frontier: list[str] = []
        for uid in frontier:
            for neighbor_uid in edges.get(uid, []):
                if neighbor_uid in seen or neighbor_uid not in nodes:
                    continue
                if nodes[neighbor_uid].get("resource_type") not in ("model", "snapshot"):
                    continue
                seen.add(neighbor_uid)
                reached.add(neighbor_uid)
                next_frontier.append(neighbor_uid)
        frontier = next_frontier
        generation += 1
    return reached


def resolve_selected_models(
    manifest: dict[str, Any],
    selectors: list[GraphSelector],
) -> list[str]:
    """Resolve *selectors* to the set of model names whose CLL must be built.

    Walks ``depends_on.nodes`` edges breadth-first from each anchor — as-is for
    upstream, inverted for downstream — following ``model.`` and ``snapshot.``
    unique-ids (sources/seeds terminate the upstream walk). Depth counting is
    per-generation, matching dbt: ``1+model`` includes the anchor's direct
    parents only, ``model+1`` its direct children only.

    Returns:
        Sorted list of unique model names (manifest casing).

    Raises:
        KeyError: When an anchor model does not exist in the manifest.
    """
    nodes: dict[str, Any] = manifest.get("nodes", {})

    by_name: dict[str, str] = {}
    parents: dict[str, list[str]] = {}
    children: dict[str, list[str]] = {}
    for uid, node in nodes.items():
        if node.get("resource_type") in ("model", "snapshot"):
            name = (node.get("name") or "").lower()
            if name and name not in by_name:
                by_name[name] = uid
        deps = node.get("depends_on", {}).get("nodes", [])
        parents[uid] = list(deps)
        for dep_uid in deps:
            children.setdefault(dep_uid, []).append(uid)

    selected: set[str] = set()
    for sel in selectors:
        anchor_uid = by_name.get(sel.model.lower())
        if anchor_uid is None:
            raise KeyError(
                f"Model '{sel.model}' not found in the manifest. "
                "Check the name, or run 'dbt compile' to refresh target/manifest.json."
            )
        selected.add(anchor_uid)
        if sel.up_depth is None or sel.up_depth > 0:
            selected |= _bfs(anchor_uid, sel.up_depth, parents, nodes)
        if sel.down_depth is None or sel.down_depth > 0:
            selected |= _bfs(anchor_uid, sel.down_depth, children, nodes)

    return sorted({nodes[uid]["name"] for uid in selected})
