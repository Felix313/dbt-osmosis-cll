"""Shared pytest fixtures for dbt-col-lineage tests."""

from __future__ import annotations

import pytest

from dbt_column_lineage.api import clear_lineage_caches


@pytest.fixture(autouse=True)
def _clear_registry_cache():
    """Isolate tests from the process-level registry cache.

    get_column_lineage() caches loaded registries per (manifest_path, params) for
    the lifetime of the process.  Tests that rebuild artifacts at reused paths must
    not see a stale cached registry, so clear it before and after each test.
    """
    clear_lineage_caches()
    yield
    clear_lineage_caches()
