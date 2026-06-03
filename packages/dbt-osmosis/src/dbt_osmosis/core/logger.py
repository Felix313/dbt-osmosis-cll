"""Logging module for dbt-osmosis. The module itself can be used as a logger as it proxies calls to the default LOGGER instance."""

from __future__ import annotations

import logging
import re
import typing as t
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path

__all__ = [
    "LOGGER",
    "LogMethod",
    "get_logger",
    "get_rotating_log_handler",
    "set_log_level",
]

_LOG_FILE_FORMAT = "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
_LOG_CONSOLE_FORMAT = "%(asctime)s  %(levelname)-8s  %(message)s"
_LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_PATH = Path.home().absolute() / ".dbt-osmosis" / "logs"
_LOGGING_LEVEL = logging.INFO

# Strip Rich markup emoji codes (e.g. ":rocket:", ":gear:") and leftover whitespace.
_RICH_MARKUP_RE = re.compile(r":\w+:")


class _CleanFormatter(logging.Formatter):
    """Plain formatter that strips Rich emoji markup codes from log messages."""

    def format(self, record: logging.LogRecord) -> str:
        record.msg = _RICH_MARKUP_RE.sub("", str(record.msg)).strip()
        return super().format(record)


def get_rotating_log_handler(name: str, path: Path, formatter: str) -> RotatingFileHandler:
    """Writes WARNING+ logs to a rotating file in ~/.dbt-osmosis/logs/."""
    path.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        str(path / f"{name}.log"),
        maxBytes=int(1e6),
        backupCount=3,
    )
    handler.setFormatter(logging.Formatter(formatter, datefmt=_LOG_TIME_FORMAT))
    handler.setLevel(logging.WARNING)
    return handler


@lru_cache(maxsize=10)
def get_logger(
    name: str = "dbt-osmosis",
    level: int | str = _LOGGING_LEVEL,
    path: Path = _LOG_PATH,
    formatter: str = _LOG_FILE_FORMAT,
) -> logging.Logger:
    """Build and cache a logger with a clean console handler (timestamp, no emoji)."""
    if isinstance(level, str):
        level = getattr(logging, level, logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(get_rotating_log_handler(name, path, formatter))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(
        _CleanFormatter(fmt=_LOG_CONSOLE_FORMAT, datefmt=_LOG_TIME_FORMAT)
    )
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


LOGGER = get_logger()
"""Default logger for dbt-osmosis"""


def set_log_level(level: int | str) -> None:
    """Set the log level for the default logger"""
    global LOGGER
    if isinstance(level, str):
        level = getattr(logging, level, logging.INFO)
    LOGGER.setLevel(level)
    for handler in LOGGER.handlers:
        # NOTE: RotatingFileHandler is fixed at WARNING level.
        if isinstance(handler, RichHandler):
            handler.setLevel(level)


class LogMethod(t.Protocol):
    """Protocol for logger methods"""

    def __call__(self, msg: t.Any, /, *args: t.Any, **kwds: t.Any) -> t.Any: ...


def __getattr__(name: str) -> LogMethod:
    if name == "set_log_level":
        return set_log_level
    func = getattr(LOGGER, name)
    return func
