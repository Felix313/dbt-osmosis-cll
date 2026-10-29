"""The ``[data-products]`` section of ``.osmosis-cll``.

.. code-block:: ini

    [data-products]
    # Models whose every column must be rebuildable; lineage stops at them.
    models =
        orders_mart
        customer_360

Names are dbt model names (case-insensitive), separated by newlines, commas or spaces.
The list preselects the data products of ``lineage report``; the HTML report can
change the selection interactively.
"""

from __future__ import annotations

import configparser
import logging
from pathlib import Path

from dbt_osmosis_cll.config import find_config_file

logger = logging.getLogger(__name__)

SECTION = "data-products"


def load_data_products(project_dir: Path | None = None) -> list[str]:
    """Data products configured in the nearest ``.osmosis-cll`` (lowercase, in order)."""
    path = find_config_file(project_dir)
    if path is None:
        return []
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(path, encoding="utf-8")
    if not parser.has_section(SECTION):
        return []
    section = parser[SECTION]
    unknown = set(section) - {"models"}
    if unknown:
        logger.warning(
            "Unknown settings in %s [%s]: %s — ignored.",
            path,
            SECTION,
            ", ".join(sorted(unknown)),
        )
    return parse_model_list(section.get("models", ""))


def parse_model_list(raw: str) -> list[str]:
    """Model names from lines / commas / whitespace; ``#`` starts a comment."""
    names: list[str] = []
    for line in raw.splitlines():
        line = line.split("#", 1)[0]
        for name in line.replace(",", " ").split():
            name = name.lower()
            if name not in names:
                names.append(name)
    return names
