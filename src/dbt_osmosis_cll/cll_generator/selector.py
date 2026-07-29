"""dbt-style node selection for standalone CLL builds (``parse-cll``).

Supports the upstream subset of dbt's graph-operator syntax:

- ``my_model``      — just the model itself
- ``+my_model``     — the model and ALL upstream models, up to sources
- ``2+my_model``    — the model and at most 2 generations of upstream models

Downstream operators (``my_model+``) and method selectors (``tag:``, ``path:``,
``@model``) are intentionally not supported here; the resolver only ever needs
the upstream closure of an anchor model.

Resolution happens over the raw ``manifest.json`` dict (``depends_on.nodes``
unique-id edges), so no dbt runtime is required. Source and seed nodes
terminate the walk naturally — they are not SQL models, so CLL has no rows for
them; they appear as progenitors inside the selected models' results instead.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_SELECTOR_RE = re.compile(r"^(?P<depth>\d*)(?P<plus>\+)?(?P<model>[A-Za-z0-9_.\-]+)$")


@dataclass(frozen=True)
class UpstreamSelector:
    """A parsed ``[N][+]model_name`` selector token."""

    model: str
    """Anchor model name (as typed; matching is case-insensitive)."""

    depth: int | None
    """Upstream generations to include: ``None`` = unlimited (bare ``+``),
    ``0`` = the anchor model only (no ``+`` given)."""


def parse_selector(token: str) -> UpstreamSelector:
    """Parse one selector token into an :class:`UpstreamSelector`.

    Raises:
        ValueError: On downstream operators, method selectors, or malformed tokens.
    """
    token = token.strip()
    if not token:
        raise ValueError("Empty selector.")
    if token.endswith("+"):
        raise ValueError(
            f"Selector '{token}': downstream selection ('model+') is not supported — "
            "parse-cll resolves upstream lineage only. Use '+model' or 'N+model'."
        )
    if ":" in token or token.startswith("@"):
        raise ValueError(
            f"Selector '{token}': method selectors (tag:, path:, @model) are not "
            "supported — use a model name, optionally prefixed with '+' or 'N+'."
        )
    match = _SELECTOR_RE.match(token)
    if not match:
        raise ValueError(f"Selector '{token}' is not valid. Expected [N][+]model_name.")
    digits, plus, model = match.group("depth"), match.group("plus"), match.group("model")
    if digits and not plus:
        # e.g. "2stg_orders" — digits must be attached to a '+'
        raise ValueError(f"Selector '{token}' is not valid. Expected [N][+]model_name.")
    if not plus:
        return UpstreamSelector(model=model, depth=0)
    return UpstreamSelector(model=model, depth=int(digits) if digits else None)


def parse_selectors(raw_values: list[str]) -> list[UpstreamSelector]:
    """Split each raw ``--select`` value on whitespace/commas and parse every token."""
    selectors: list[UpstreamSelector] = []
    for raw in raw_values:
        for token in re.split(r"[,\s]+", raw):
            if token:
                selectors.append(parse_selector(token))
    if not selectors:
        raise ValueError("No selector given.")
    return selectors


def resolve_upstream_models(
    manifest: dict[str, Any],
    selectors: list[UpstreamSelector],
) -> list[str]:
    """Resolve *selectors* to the set of model names whose CLL must be built.

    Walks ``depends_on.nodes`` edges breadth-first from each anchor, following
    ``model.`` and ``snapshot.`` unique-ids (sources/seeds terminate the walk).
    Depth counting is per-generation, matching dbt: ``1+model`` includes the
    anchor's direct parents only.

    Returns:
        Sorted list of unique model names (manifest casing).

    Raises:
        KeyError: When an anchor model does not exist in the manifest.
    """
    nodes: dict[str, Any] = manifest.get("nodes", {})

    by_name: dict[str, str] = {}
    for uid, node in nodes.items():
        if node.get("resource_type") in ("model", "snapshot"):
            name = (node.get("name") or "").lower()
            if name and name not in by_name:
                by_name[name] = uid

    selected: dict[str, None] = {}  # uid → None (insertion-ordered set)
    for sel in selectors:
        anchor_uid = by_name.get(sel.model.lower())
        if anchor_uid is None:
            raise KeyError(
                f"Model '{sel.model}' not found in the manifest. "
                "Check the name, or run 'dbt compile' to refresh target/manifest.json."
            )
        frontier = [anchor_uid]
        seen = {anchor_uid}
        selected.setdefault(anchor_uid)
        generation = 0
        while frontier and (sel.depth is None or generation < sel.depth):
            next_frontier: list[str] = []
            for uid in frontier:
                for dep_uid in nodes.get(uid, {}).get("depends_on", {}).get("nodes", []):
                    if dep_uid in seen or dep_uid not in nodes:
                        continue
                    if nodes[dep_uid].get("resource_type") not in ("model", "snapshot"):
                        continue
                    seen.add(dep_uid)
                    selected.setdefault(dep_uid)
                    next_frontier.append(dep_uid)
            frontier = next_frontier
            generation += 1

    return sorted({nodes[uid]["name"] for uid in selected})
