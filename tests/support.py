from __future__ import annotations

import time
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol, cast

_DBT_PARSE_INPUT_SUFFIXES = {".sql", ".yml", ".yaml", ".csv", ".md"}
_DBT_PARSE_EXCLUDED_DIRS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    "dbt_packages",
    "logs",
    "target",
}


class _DbtInvocationResult(Protocol):
    success: bool
    exception: object | None
    result: object | None


def _dbt_command_label(args: Sequence[str]) -> str:
    if len(args) >= 2 and args[0] == "docs" and args[1] == "generate":
        return "docs generate"
    return args[0]


def _dbt_failure_detail(result: _DbtInvocationResult) -> str:
    exception = result.exception
    if exception is not None:
        detail = str(exception)
        if detail:
            return detail
        return repr(exception)

    invocation_result = result.result
    if invocation_result is not None:
        return repr(invocation_result)

    return "Unknown error"


def run_dbt_command(args: Sequence[str]) -> None:
    """Invoke one dbt CLI command and raise immediately on failure."""
    from dbt.cli.main import dbtRunner

    label = _dbt_command_label(args)
    start = time.time()
    try:
        result = cast(_DbtInvocationResult, cast(object, dbtRunner().invoke(list(args))))
    finally:
        # dbtRunner.invoke() runs setup_event_logger(), which opens a rotating
        # file handler on <project>/logs/dbt.log and registers it on dbt's
        # process-global event manager. Since we run dbt in-process, that handle
        # stays open for the whole pytest process and — on Windows — locks
        # dbt.log, breaking the session-teardown rmtree of the template project
        # (WinError 32) and any later dbt invocation that targets the same log.
        # cleanup_event_logger() drops the handler; gc.collect() forces the
        # FileHandler to finalize so the OS releases the file lock now.
        _release_dbt_log_handle()
    elapsed = time.time() - start

    if not result.success:
        raise RuntimeError(
            f"dbt {label} failed after {elapsed:.2f}s: {_dbt_failure_detail(result)}",
        )

    print(f"✓ dbt {label} completed successfully ({elapsed:.2f}s)")


def _release_dbt_log_handle() -> None:
    """Close dbt's in-process logs/dbt.log file handler so Windows unlocks the file."""
    import gc

    try:
        from dbt_common.events.event_manager_client import cleanup_event_logger
    except ImportError:  # pragma: no cover - dbt internal layout changed
        return
    cleanup_event_logger()
    gc.collect()


def manifest_requires_refresh(manifest_path: Path, project_dir: Path) -> bool:
    """Return True when dbt parse inputs are newer than the compiled manifest."""
    if not manifest_path.exists():
        return True

    manifest_mtime = manifest_path.stat().st_mtime
    for candidate in project_dir.rglob("*"):
        if not candidate.is_file():
            continue

        relative_path = candidate.relative_to(project_dir)
        if any(part in _DBT_PARSE_EXCLUDED_DIRS for part in relative_path.parts):
            continue
        if candidate.suffix.lower() not in _DBT_PARSE_INPUT_SUFFIXES:
            continue
        if candidate.stat().st_mtime > manifest_mtime:
            return True

    return False
