"""Column lists read once from the warehouse: ``target/cll-columns.json``.

Column-level lineage needs the columns a relation really has, in their physical order:
``star``, ``get_columns_in_relation`` and ``SHOW COLUMNS`` render into the compiled SQL.
YAML documentation is no substitute, it may list only some columns in another order.

With ``live`` in ``cll-column-sources``, ``compile-cll`` reads the warehouse's
``information_schema.columns`` once per database, for the relations in the manifest,
and stores the answer in the ``catalog.json`` layout. ``parse-cll`` reuses the file, so
both commands see the same columns and need only one login. Only metadata is read.
Relations the role cannot see are simply absent; ``unavailable_databases`` names the
databases that could not be queried at all.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SNAPSHOT_NAME = "cll-columns.json"


def snapshot_path(project_dir: Path) -> Path:
    return Path(project_dir) / "target" / SNAPSHOT_NAME


def build_snapshot(
    relations: Iterable[tuple[str, str, str, str, bool]],
    rows_by_database: dict[str, list[tuple[Any, ...]]],
    unavailable: list[str],
) -> dict[str, Any]:
    """Join ``(unique_id, database, schema, identifier, is_source)`` with queried columns.

    A row is ``(table_schema, table_name, column_name, data_type, ordinal_position)``.
    """
    columns: dict[tuple[str, str, str], list[tuple[Any, ...]]] = defaultdict(list)
    for database, rows in rows_by_database.items():
        for schema, table, column, dtype, position in rows:
            key = (database.lower(), str(schema).lower(), str(table).lower())
            columns[key].append((str(column), dtype, position))
    out: dict[str, Any] = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generated_by": "dbt-osmosis-cll compile-cll",
            "unavailable_databases": sorted(unavailable),
        },
        "nodes": {},
        "sources": {},
    }
    for uid, database, schema, identifier, is_source in relations:
        found = columns.get((database.lower(), schema.lower(), identifier.lower()))
        if not found:
            continue
        out["sources" if is_source else "nodes"][uid] = {
            "unique_id": uid,
            "metadata": {"name": identifier, "schema": schema, "database": database},
            "columns": {
                name: {"name": name, "type": dtype, "index": position}
                for name, dtype, position in sorted(found, key=lambda c: c[2] or 0)
            },
        }
    return out


def fetch_snapshot(
    relations: list[tuple[str, str, str, str, bool]],
    project_dir: Path,
    profiles_dir: Path,
    target: str | None,
) -> dict[str, Any]:
    """Query the warehouse for the columns of *relations* (metadata only)."""
    from dbt.adapters.factory import reset_adapters

    from dbt_osmosis_cll.cll_generator.artifacts.live_db import _bootstrap_dbt_adapter

    adapter = _bootstrap_dbt_adapter(str(project_dir), str(profiles_dir), target)
    schemas: dict[str, set[str]] = defaultdict(set)
    for _uid, database, schema, _identifier, _is_source in relations:
        if database and schema:
            schemas[database].add(schema.lower())
    rows_by_database: dict[str, list[tuple[Any, ...]]] = {}
    unavailable: list[str] = []
    try:
        with adapter.connection_named("cll-columns"):
            for database, wanted in sorted(schemas.items()):
                in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in sorted(wanted))
                select = "select table_schema, table_name, column_name, data_type, ordinal_position"
                filter_schemas = f"lower(table_schema) in ({in_list})"
                catalog = database.replace("'", "''").lower()
                attempts = (
                    f"{select} from {adapter.quote(database)}.information_schema.columns "
                    f"where {filter_schemas}",
                    # Warehouses without database-qualified information_schema.
                    f"{select} from information_schema.columns "
                    f"where lower(table_catalog) = '{catalog}' and {filter_schemas}",
                )
                error: Exception | None = None
                for sql in attempts:
                    try:
                        _, table = adapter.execute(sql, fetch=True)
                    except Exception as exc:  # noqa: BLE001 — a database the role cannot read
                        error = error or exc
                        continue
                    rows_by_database[database] = [tuple(row) for row in table.rows]
                    break
                else:
                    logger.warning("No column metadata from %s: %s", database, str(error)[:160])
                    unavailable.append(database)
    finally:
        adapter.cleanup_connections()
        reset_adapters()
    return build_snapshot(relations, rows_by_database, unavailable)


def write_snapshot(project_dir: Path, snapshot: dict[str, Any]) -> Path:
    path = snapshot_path(project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return path
