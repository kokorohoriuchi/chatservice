"""Per-process file logging."""

from __future__ import annotations

import logging
from pathlib import Path

from config import LoggingConfig


def setup_logging(name: str, config: LoggingConfig) -> logging.Logger:
    """Return a logger that writes to <config.directory>/<name>.log."""
    logger = logging.getLogger(name)
    logger.setLevel(config.level)
    logger.propagate = False

    if not logger.handlers:
        directory = Path(config.directory)
        directory.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(directory / f"{name}.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter(config.format))
        logger.addHandler(handler)

    return logger
