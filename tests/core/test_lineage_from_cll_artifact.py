"""Feeding the lineage explorer from a precomputed CLL artifact.

``lineage explore --from-cll`` skips the whole-project compiled-SQL parse by
rehydrating the registry from a file that already holds the answer: either
``cll-result.json`` (parse-cll, scoped to a dbt selector) or ``cll_cache.json``
(osmosis, whole project once a full run happened).

Guarantees under test:
- **fidelity**: a service fed from the artifact answers exactly like one that
  parsed the SQL itself — including the SQL expressions the impact panel renders;
- **format detection**: both artifact shapes load from their payload, not their
  filename, and unrecognized files fail loudly;
- **backward compatibility**: pre-v5 rows (no ``transformation_type`` /
  ``sql_expression``) still reconstruct, degrading only the expression;
- **hybrid fallback**: models the artifact misses are parsed on first access, so
  a partial cache never shows a truncated graph as if it were complete;
- **staleness**: models whose source SQL changed are detected and dropped;
- **scoping**: a selector artifact restricts the registry to its models, keeping
  the sources those models terminate at.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from dbt_osmosis_cll.cll_generator.api import clear_lineage_caches, get_column_lineage
from dbt_osmosis_cll.cll_generator.artifacts.manifest_catalog import ManifestCatalogReader
from dbt_osmosis_cll.cll_generator.lineage.cll_artifact import (
    CllArtifactError,
    default_artifact_path,
    load_cll_artifact,
    source_sql_fingerprints,
)
from dbt_osmosis_cll.cll_generator.lineage.service import LineageSelector, LineageService


@pytest.fixture(autouse=True)
def _fresh_caches():
    clear_lineage_caches()
    yield
    clear_lineage_caches()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _manifest() -> dict:
    def node(name: str, sql: str, deps: list[str], columns: dict | None = None) -> dict:
        return {
            "name": name,
            "resource_type": "model",
            "language": "sql",
            "schema": "main",
            "database": "db",
            "columns": columns or {},
            "compiled_code": sql,
            "original_file_path": f"models/{name}.sql",
            "depends_on": {"nodes": deps},
        }

    return {
        "metadata": {"adapter_type": "duckdb"},
        "nodes": {
            "model.pkg.stg_orders": node(
                "stg_orders",
                "select id as order_id, amount, 'SAP' as source_system from raw_orders",
                ["source.pkg.raw.raw_orders"],
                {"order_id": {"description": "Order PK", "data_type": "INTEGER"}},
            ),
            "model.pkg.int_orders": node(
                "int_orders",
                "select order_id, amount * 2 as double_amount from stg_orders",
                ["model.pkg.stg_orders"],
            ),
            "model.pkg.mart_orders": node(
                "mart_orders",
                "select order_id, double_amount from int_orders",
                ["model.pkg.int_orders"],
            ),
            "model.pkg.unrelated": node("unrelated", "select 1 as one", []),
            "model.pkg.orders_by_country": node(
                "orders_by_country",
                "select o.order_id, c.code from mart_orders o join country_codes c on true",
                ["model.pkg.mart_orders", "seed.pkg.country_codes"],
            ),
            "seed.pkg.country_codes": {
                "name": "country_codes",
                "resource_type": "seed",
                "schema": "main",
                "database": "db",
                "columns": {"code": {}},
                "depends_on": {"nodes": []},
            },
        },
        "sources": {
            "source.pkg.raw.raw_orders": {
                "name": "raw_orders",
                "identifier": "raw_orders",
                "resource_type": "source",
                "schema": "raw",
                "database": "db",
                "columns": {"id": {"description": "Raw PK"}, "amount": {}},
            },
        },
        "exposures": {
            "exposure.pkg.orders_dashboard": {
                "name": "orders_dashboard",
                "type": "dashboard",
                "url": "https://bi.example/orders",
                "depends_on": {"nodes": ["model.pkg.mart_orders"]},
            },
        },
    }


def _write_project(tmp_path: Path) -> Path:
    """Write manifest + the source .sql files the fingerprints hash."""
    manifest = _manifest()
    target = tmp_path / "target"
    target.mkdir(parents=True, exist_ok=True)
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    for node in manifest["nodes"].values():
        # Seeds have no SQL of their own and therefore no fingerprint.
        if "original_file_path" not in node:
            continue
        (tmp_path / node["original_file_path"]).write_text(node["compiled_code"], encoding="utf-8")
    return target / "manifest.json"


def _service(manifest_path: Path, lazy: bool = False) -> LineageService:
    reader = ManifestCatalogReader(manifest_path=str(manifest_path))
    reader.load()
    return LineageService(
        catalog_path=None,
        manifest_path=manifest_path,
        catalog_reader=reader,
        use_target_dir_fallback=True,
        lazy_lineage=lazy,
    )


def _rows(manifest_path: Path, models: list[str] | None = None) -> list[dict]:
    results = get_column_lineage(
        manifest_path=str(manifest_path),
        models=models,
        compiled_sql_source="manifest",
        _catalog_reader_override=_reader(manifest_path),
    )
    return [dataclasses.asdict(r) for r in results]


def _reader(manifest_path: Path) -> ManifestCatalogReader:
    reader = ManifestCatalogReader(manifest_path=str(manifest_path))
    reader.load()
    return reader


def _selector_artifact(
    path: Path, manifest_path: Path, models: list[str], selectors: list[str]
) -> Path:
    payload = {
        "schema_version": 2,
        "selectors": selectors,
        "models": models,
        "fingerprints": source_sql_fingerprints(
            manifest_path.parent.parent, _reader(manifest_path).manifest
        ),
        "results": _rows(manifest_path, models),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _project_artifact(path: Path, manifest_path: Path) -> Path:
    rows = _rows(manifest_path)
    fingerprints = source_sql_fingerprints(
        manifest_path.parent.parent, _reader(manifest_path).manifest
    )
    entries: dict[str, dict] = {}
    for row in rows:
        entry = entries.setdefault(
            row["model"], {"compiled_sql_hash": fingerprints.get(row["model"], ""), "results": []}
        )
        entry["results"].append(row)
    path.write_text(json.dumps({"schema_version": 5, "entries": entries}), encoding="utf-8")
    return path


def _normalize(obj):
    """Make service output comparable: ColumnLineage → dict, sets → sorted lists."""
    if hasattr(obj, "model_dump"):
        data = obj.model_dump()
        return _normalize(data)
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (set, frozenset)):
        return sorted(str(v) for v in obj)
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# Fidelity: cached service answers like a parsing one
# ---------------------------------------------------------------------------


def test_cached_service_matches_live_parse_for_upstream_and_downstream(tmp_path):
    manifest_path = _write_project(tmp_path)
    artifact_path = _project_artifact(tmp_path / "cll_cache.json", manifest_path)

    live = _service(manifest_path)
    cached = _service(manifest_path, lazy=True)
    artifact = load_cll_artifact(artifact_path)
    cached.registry.apply_cached_lineage(artifact.lineage)

    for selector in ("+mart_orders.order_id", "stg_orders.order_id+", "+int_orders.double_amount+"):
        sel = LineageSelector.from_string(selector)
        assert _normalize(cached.get_column_info(sel)) == _normalize(live.get_column_info(sel)), (
            f"cached lineage diverges from the live parse for {selector}"
        )


def test_cached_service_matches_live_impact_analysis_including_sql_expressions(tmp_path):
    manifest_path = _write_project(tmp_path)
    artifact_path = _project_artifact(tmp_path / "cll_cache.json", manifest_path)

    live = _service(manifest_path)
    cached = _service(manifest_path, lazy=True)
    cached.registry.apply_cached_lineage(load_cll_artifact(artifact_path).lineage)

    live_impact = live.get_column_impact("stg_orders", "amount")
    cached_impact = cached.get_column_impact("stg_orders", "amount")
    assert _normalize(cached_impact) == _normalize(live_impact)

    # The derived column carries the expression the impact panel renders — the
    # whole reason schema version 5 added the field.
    derived = next(c for c in cached_impact["affected_columns"] if c["column"] == "double_amount")
    assert derived["transformation_type"] == "derived"
    assert derived["sql_expression"]


def test_cached_service_skips_the_sql_parse(tmp_path, monkeypatch):
    """A fully-covered artifact must not parse a single model's compiled SQL."""
    manifest_path = _write_project(tmp_path)
    artifact_path = _project_artifact(tmp_path / "cll_cache.json", manifest_path)

    cached = _service(manifest_path, lazy=True)
    cached.registry.apply_cached_lineage(load_cll_artifact(artifact_path).lineage)

    calls: list[str] = []
    original = type(cached.registry)._parse_model_lineage

    def _spy(self, model):
        calls.append(model.name)
        return original(self, model)

    monkeypatch.setattr(type(cached.registry), "_parse_model_lineage", _spy)

    cached.get_column_info(LineageSelector.from_string("+mart_orders.order_id"))
    cached.get_column_impact("stg_orders", "order_id")
    assert calls == []


def test_missing_model_is_parsed_on_first_access(tmp_path):
    """Hybrid fallback: a partial artifact still yields a complete graph."""
    manifest_path = _write_project(tmp_path)
    artifact_path = _project_artifact(tmp_path / "cll_cache.json", manifest_path)

    artifact = load_cll_artifact(artifact_path)
    artifact.drop_models(["int_orders"])
    assert "int_orders" not in artifact.lineage

    cached = _service(manifest_path, lazy=True)
    cached.registry.apply_cached_lineage(artifact.lineage)

    live = _service(manifest_path)
    sel = LineageSelector.from_string("+mart_orders.order_id")
    assert _normalize(cached.get_column_info(sel)) == _normalize(live.get_column_info(sel))


# ---------------------------------------------------------------------------
# Artifact loading
# ---------------------------------------------------------------------------


def test_loads_selector_artifact_with_scope_and_selectors(tmp_path):
    manifest_path = _write_project(tmp_path)
    path = _selector_artifact(
        tmp_path / "cll-result.json",
        manifest_path,
        ["int_orders", "mart_orders", "stg_orders"],
        ["+mart_orders"],
    )
    artifact = load_cll_artifact(path)
    assert artifact.kind == "selector"
    assert artifact.selectors == ["+mart_orders"]
    assert artifact.scope == ["int_orders", "mart_orders", "stg_orders"]
    assert artifact.has_sql_expressions is True
    assert set(artifact.lineage) == {"int_orders", "mart_orders", "stg_orders"}


def test_loads_project_artifact_with_fingerprints(tmp_path):
    manifest_path = _write_project(tmp_path)
    path = _project_artifact(tmp_path / "cll_cache.json", manifest_path)
    artifact = load_cll_artifact(path)
    assert artifact.kind == "project"
    assert artifact.scope is None
    assert artifact.fingerprints["stg_orders"]
    assert "mart_orders" in artifact.lineage


def test_format_is_detected_from_payload_not_filename(tmp_path):
    manifest_path = _write_project(tmp_path)
    path = _project_artifact(tmp_path / "renamed-anything.json", manifest_path)
    assert load_cll_artifact(path).kind == "project"


def test_unrecognized_payload_raises(tmp_path):
    path = tmp_path / "nope.json"
    path.write_text(json.dumps({"something": "else"}), encoding="utf-8")
    with pytest.raises(CllArtifactError, match="not a recognized CLL artifact"):
        load_cll_artifact(path)


def test_missing_file_raises(tmp_path):
    with pytest.raises(CllArtifactError, match="No CLL artifact found"):
        load_cll_artifact(tmp_path / "absent.json")


def test_invalid_json_raises(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(CllArtifactError, match="Could not read"):
        load_cll_artifact(path)


def test_empty_artifact_raises(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps({"schema_version": 2, "results": []}), encoding="utf-8")
    with pytest.raises(CllArtifactError, match="no lineage rows"):
        load_cll_artifact(path)


def test_default_path_prefers_selector_result(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    (target / "cll_cache.json").write_text("{}", encoding="utf-8")
    assert default_artifact_path(target) == target / "cll_cache.json"
    (target / "cll-result.json").write_text("{}", encoding="utf-8")
    assert default_artifact_path(target) == target / "cll-result.json"


def test_default_path_is_none_when_nothing_cached(tmp_path):
    assert default_artifact_path(tmp_path) is None


# ---------------------------------------------------------------------------
# Backward compatibility with pre-v5 rows
# ---------------------------------------------------------------------------


def test_pre_v5_rows_reconstruct_transformation_type_from_flags(tmp_path):
    path = tmp_path / "old.json"
    path.write_text(
        json.dumps({
            "schema_version": 1,
            "selectors": ["+mart_orders"],
            "models": ["int_orders"],
            "results": [
                {
                    "model": "int_orders",
                    "column": "double_amount",
                    "progenitor_model": None,
                    "progenitor_column": None,
                    "is_rename": False,
                    "is_computed": True,
                    "progenitors": [["stg_orders", "amount"]],
                },
                {
                    "model": "int_orders",
                    "column": "order_id",
                    "progenitor_model": "stg_orders",
                    "progenitor_column": "order_id",
                    "is_rename": False,
                },
            ],
        }),
        encoding="utf-8",
    )
    artifact = load_cll_artifact(path)
    assert artifact.has_sql_expressions is False

    derived = artifact.lineage["int_orders"]["double_amount"][0]
    assert derived.transformation_type == "derived"
    assert derived.source_columns == {"stg_orders.amount"}
    assert derived.sql_expression is None

    # No progenitors field at all — falls back to the single progenitor pair.
    direct = artifact.lineage["int_orders"]["order_id"][0]
    assert direct.transformation_type == "direct"
    assert direct.source_columns == {"stg_orders.order_id"}


def test_v5_artifact_without_any_expression_is_not_mistaken_for_an_old_one(tmp_path):
    """A project of pure passthroughs yields no expressions — that is not staleness."""
    path = tmp_path / "current.json"
    path.write_text(
        json.dumps({
            "results": [
                {
                    "model": "m",
                    "column": "c",
                    "transformation_type": "renamed",
                    "sql_expression": None,
                    "progenitors": [["up", "col"]],
                }
            ]
        }),
        encoding="utf-8",
    )
    assert load_cll_artifact(path).has_sql_expressions is True


def test_pre_v5_literal_expression_falls_back_to_literal_value(tmp_path):
    path = tmp_path / "old.json"
    path.write_text(
        json.dumps({
            "results": [
                {
                    "model": "stg_orders",
                    "column": "source_system",
                    "is_literal": True,
                    "literal_value": "'SAP'",
                    "progenitors": [],
                }
            ]
        }),
        encoding="utf-8",
    )
    lineage = load_cll_artifact(path).lineage["stg_orders"]["source_system"][0]
    assert lineage.transformation_type == "literal"
    assert lineage.sql_expression == "'SAP'"


def test_malformed_progenitor_pairs_are_skipped(tmp_path):
    path = tmp_path / "junk.json"
    path.write_text(
        json.dumps({
            "results": [
                {
                    "model": "m",
                    "column": "c",
                    "transformation_type": "derived",
                    "sql_expression": "a + b",
                    "progenitors": [["good", "col"], ["only_one"], None, ["", "empty"]],
                }
            ]
        }),
        encoding="utf-8",
    )
    lineage = load_cll_artifact(path).lineage["m"]["c"][0]
    assert lineage.source_columns == {"good.col"}


def test_unknown_transformation_type_falls_back_to_direct(tmp_path):
    path = tmp_path / "weird.json"
    path.write_text(
        json.dumps({
            "results": [
                {
                    "model": "m",
                    "column": "c",
                    "transformation_type": "teleported",
                    "progenitors": [["up", "col"]],
                }
            ]
        }),
        encoding="utf-8",
    )
    assert load_cll_artifact(path).lineage["m"]["c"][0].transformation_type == "direct"


# ---------------------------------------------------------------------------
# Staleness
# ---------------------------------------------------------------------------


def test_changed_source_sql_is_reported_stale(tmp_path):
    manifest_path = _write_project(tmp_path)
    artifact = load_cll_artifact(_project_artifact(tmp_path / "cll_cache.json", manifest_path))

    fingerprints = source_sql_fingerprints(tmp_path, _reader(manifest_path).manifest)
    assert artifact.stale_models(fingerprints) == []

    (tmp_path / "models" / "int_orders.sql").write_text("select order_id from stg_orders", "utf-8")
    changed = source_sql_fingerprints(tmp_path, _reader(manifest_path).manifest)
    assert artifact.stale_models(changed) == ["int_orders"]


def test_models_without_a_fingerprint_are_never_stale(tmp_path):
    manifest_path = _write_project(tmp_path)
    artifact = load_cll_artifact(_project_artifact(tmp_path / "cll_cache.json", manifest_path))
    artifact.fingerprints.pop("int_orders", None)
    (tmp_path / "models" / "int_orders.sql").write_text("select 1 as x", "utf-8")
    assert "int_orders" not in artifact.stale_models(
        source_sql_fingerprints(tmp_path, _reader(manifest_path).manifest)
    )


def test_stale_model_is_reparsed_after_being_dropped(tmp_path):
    manifest_path = _write_project(tmp_path)
    artifact = load_cll_artifact(_project_artifact(tmp_path / "cll_cache.json", manifest_path))
    artifact.drop_models(["stg_orders"])

    cached = _service(manifest_path, lazy=True)
    cached.registry.apply_cached_lineage(artifact.lineage)
    info = cached.get_column_info(LineageSelector.from_string("+stg_orders.order_id"))
    assert "id" in info["upstream"]["raw_orders"]

    live = _service(manifest_path)
    live_info = live.get_column_info(LineageSelector.from_string("+stg_orders.order_id"))
    assert _normalize(info) == _normalize(live_info)


# ---------------------------------------------------------------------------
# Registry hydration and scoping
# ---------------------------------------------------------------------------


def test_apply_cached_lineage_reports_unknown_models(tmp_path):
    manifest_path = _write_project(tmp_path)
    artifact = load_cll_artifact(_project_artifact(tmp_path / "cll_cache.json", manifest_path))
    artifact.lineage["deleted_model"] = {}

    service = _service(manifest_path, lazy=True)
    applied, unknown = service.registry.apply_cached_lineage(artifact.lineage)
    assert unknown == ["deleted_model"]
    assert "mart_orders" in applied


def test_apply_cached_lineage_creates_stub_columns(tmp_path):
    """Columns the parser discovered but the YAML never declared survive the round-trip."""
    manifest_path = _write_project(tmp_path)
    service = _service(manifest_path, lazy=True)
    service.registry.apply_cached_lineage(
        load_cll_artifact(_project_artifact(tmp_path / "cll_cache.json", manifest_path)).lineage
    )
    # Only order_id is declared in the manifest YAML; amount and source_system
    # exist purely because the SQL parse found them.
    assert set(service.registry.get_model("stg_orders").columns) >= {
        "order_id",
        "amount",
        "source_system",
    }


def test_restrict_to_drops_out_of_scope_models_but_keeps_sources(tmp_path):
    manifest_path = _write_project(tmp_path)
    service = _service(manifest_path, lazy=True)
    service.registry.restrict_to(["stg_orders", "int_orders"])

    models = service.registry.get_models()
    assert "stg_orders" in models and "int_orders" in models
    assert "mart_orders" not in models
    assert "unrelated" not in models
    # raw_orders feeds stg_orders — dropping it would hide where lineage ends.
    assert "raw_orders" in models
    # The dashboard hangs off mart_orders, which is out of scope — claiming it is
    # still reachable would be a lie about the impact surface.
    assert "orders_dashboard" not in service.registry.get_exposures()


def test_restrict_to_keeps_upstream_seeds(tmp_path):
    """A seed is a terminal origin just like a source — dropping it truncates the chain."""
    manifest_path = _write_project(tmp_path)
    service = _service(manifest_path, lazy=True)
    assert "country_codes" in service.registry.get_model("orders_by_country").upstream

    service.registry.restrict_to(["orders_by_country"])
    assert "country_codes" in service.registry.get_models()


def test_restrict_to_keeps_exposures_still_reachable_in_scope(tmp_path):
    manifest_path = _write_project(tmp_path)
    service = _service(manifest_path, lazy=True)
    service.registry.restrict_to(["stg_orders", "int_orders", "mart_orders"])
    assert "orders_dashboard" in service.registry.get_exposures()


def test_restrict_to_keeps_lineage_within_scope_intact(tmp_path):
    manifest_path = _write_project(tmp_path)
    path = _selector_artifact(
        tmp_path / "cll-result.json", manifest_path, ["stg_orders", "int_orders"], ["+int_orders"]
    )
    artifact = load_cll_artifact(path)

    service = _service(manifest_path, lazy=True)
    service.registry.apply_cached_lineage(artifact.lineage)
    service.registry.restrict_to(artifact.scope or [])

    info = service.get_column_info(LineageSelector.from_string("+int_orders.order_id"))
    assert "stg_orders" in info["upstream"]
    # The scope stops at stg_orders, but its source is kept so the chain still
    # terminates at the raw table rather than dangling.
    assert "id" in info["upstream"]["raw_orders"]
