"""
Centralized logging configuration for the ETL pipeline.

Uses loguru for structured logging with automatic file rotation.
All pipeline stages import the `logger` object from this module.
"""

import sys
from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Resolve log directory relative to this file's location so the module works
# regardless of the working directory the caller uses.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

_LOG_FILE = _LOG_DIR / "etl.log"

# Remove the default loguru handler so we can configure our own.
logger.remove()

# Human-readable console handler — INFO and above.
logger.add(
    sys.stdout,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> — "
        "<level>{message}</level>"
    ),
    colorize=True,
)

# Rotating file handler — DEBUG and above, 10 MB cap, 7 day retention.
logger.add(
    str(_LOG_FILE),
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} — {message}",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    enqueue=True,  # thread-safe async writes
)

__all__ = ["logger"]
