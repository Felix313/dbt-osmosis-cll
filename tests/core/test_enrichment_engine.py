"""Tests for the enrichment engine's anchor placement (dbt >= 1.10 config.meta)."""

from __future__ import annotations

from pathlib import Path

import yaml

from dbt_osmosis_cll.osmosis_propagation.enrichment import enrich_yaml_files
from dbt_osmosis_cll.osmosis_propagation.enrichment._merge import DescriptionFetcher


class StubFetcher(DescriptionFetcher):
    """Returns a fixed description for every requested column."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self._mapping = mapping

    def fetch(self, column_names: list[str]) -> dict[str, str]:
        return {c: self._mapping[c] for c in column_names if c in self._mapping}


def _write_yaml(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _col(model: dict, name: str) -> dict:
    return next(c for c in model["models"][0]["columns"] if c["name"] == name)


def test_anchor_written_to_config_meta(tmp_path: Path) -> None:
    """Per-column anchor is written under config.meta (dbt 1.10+), not top-level meta."""
    yml = tmp_path / "STG_X.yml"
    _write_yaml(
        yml,
        {"version": 2, "models": [{"name": "STG_X", "columns": [{"name": "COL_A"}]}]},
    )

    enrich_yaml_files(
        [yml],
        StubFetcher({"COL_A": "Canonical AML description"}),
        anchor_meta_key="desc-owner",
        anchor_value="aml",
        force=True,
    )

    data = _read_yaml(yml)
    col = _col(data, "COL_A")
    assert col["description"] == "Canonical AML description"
    assert col["config"]["meta"]["desc-owner"] == "aml"
    # Must NOT leave a deprecated top-level column meta key.
    assert "desc-owner" not in (col.get("meta") or {})


def test_legacy_top_level_anchor_migrated_to_config_meta(tmp_path: Path) -> None:
    """A legacy top-level meta.<key> anchor is read for back-compat and relocated to config.meta."""
    yml = tmp_path / "STG_Y.yml"
    _write_yaml(
        yml,
        {
            "version": 2,
            "models": [
                {
                    "name": "STG_Y",
                    "columns": [
                        {"name": "COL_B", "description": "old", "meta": {"desc-owner": "aml"}}
                    ],
                }
            ],
        },
    )

    enrich_yaml_files(
        [yml],
        StubFetcher({"COL_B": "New AML description"}),
        anchor_meta_key="desc-owner",
        anchor_value="aml",
        force=True,
    )

    col = _col(_read_yaml(yml), "COL_B")
    assert col["config"]["meta"]["desc-owner"] == "aml"
    assert "desc-owner" not in (col.get("meta") or {})


def test_frozen_value_in_config_meta_is_respected(tmp_path: Path) -> None:
    """A column frozen via config.meta is skipped (description preserved)."""
    yml = tmp_path / "STG_Z.yml"
    _write_yaml(
        yml,
        {
            "version": 2,
            "models": [
                {
                    "name": "STG_Z",
                    "columns": [
                        {
                            "name": "COL_C",
                            "description": "Developer-owned",
                            "config": {"meta": {"desc-owner": "frozen"}},
                        }
                    ],
                }
            ],
        },
    )

    enrich_yaml_files(
        [yml],
        StubFetcher({"COL_C": "Should NOT overwrite"}),
        anchor_meta_key="desc-owner",
        anchor_value="aml",
        frozen_values=frozenset({True, "frozen"}),
        force=True,
    )

    col = _col(_read_yaml(yml), "COL_C")
    assert col["description"] == "Developer-owned"
