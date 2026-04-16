"""
Shared utilities for Stage 3 — Silver-layer transformation.

This module is the single source of truth for:
  - Silver and Quarantine layer output paths (SILVER_DIR, QUARANTINE_DIR)
  - Reading the latest Bronze Parquet snapshot for a given table directory
  - Writing transformed DataFrames to the Silver layer with date-stamped names
  - Quarantining rejected rows with reasons and timestamps via atomic write
  - Logging a standardised transform summary with drop-rate alerting
  - Resolving the pipeline date range from environment configuration

Design constraints
------------------
- All paths are resolved via ``Path(__file__).resolve().parents[2]`` so the
  module is stable regardless of the caller's working directory.
- Atomic writes (tmp + rename) are used for quarantine output, matching the
  pattern established in ``src.extract.extract_db``.
- ``get_pipeline_date_range()`` delegates entirely to ``get_pipeline_config()``
  so date configuration stays in one place (``src.utils.db``).
"""

from __future__ import annotations

import traceback
from datetime import datetime
from pathlib import Path
from typing import Final

import pandas as pd

from src.extract.config import timestamp_suffix
from src.utils.db import get_pipeline_config
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Project root — resolved once at module load time.
# ---------------------------------------------------------------------------
_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Silver and Quarantine layer output paths
# ---------------------------------------------------------------------------

#: Root directory for all Silver-layer domain Parquet files.
SILVER_DIR: Final[Path] = _PROJECT_ROOT / "data" / "silver"

#: Root directory for rows rejected during Silver-layer transformation.
QUARANTINE_DIR: Final[Path] = _PROJECT_ROOT / "data" / "quarantine"


# ---------------------------------------------------------------------------
# Bronze reader
# ---------------------------------------------------------------------------

def read_latest_bronze_parquet(table_dir: Path) -> pd.DataFrame:
    """Read the most-recent Parquet snapshot from a Bronze table directory.

    The function globs for ``*.parquet`` files (excluding in-progress
    ``*.parquet.tmp`` files produced by atomic writes).  Because extract
    modules name files with a ``YYYYMMDD`` suffix, alphabetical sort is
    equivalent to chronological sort.

    Parameters
    ----------
    table_dir : Path
        Directory that contains one or more date-stamped Parquet files,
        e.g. ``data/bronze/db/orders/``.

    Returns
    -------
    pd.DataFrame
        Contents of the latest Parquet file.

    Raises
    ------
    FileNotFoundError
        If *table_dir* contains no ``*.parquet`` files.  The error message
        names the directory so callers can act on it without re-globbing.
    """
    # Exclude .parquet.tmp files that may be mid-write
    candidates: list[Path] = sorted(
        p for p in table_dir.glob("*.parquet") if not p.name.endswith(".parquet.tmp")
    )

    if not candidates:
        raise FileNotFoundError(
            f"No Parquet files found in '{table_dir}'. "
            "Ensure Stage 2 extraction has run for this table before calling "
            "read_latest_bronze_parquet()."
        )

    latest_file: Path = candidates[-1]
    df: pd.DataFrame = pd.read_parquet(latest_file)

    logger.info(
        "Loaded Bronze file '{}': {:,} rows",
        latest_file,
        len(df),
    )
    return df


# ---------------------------------------------------------------------------
# Silver writer
# ---------------------------------------------------------------------------

def write_silver(df: pd.DataFrame, domain: str, name: str) -> Path:
    """Write a transformed DataFrame to the Silver layer.

    The output is written to::

        data/silver/{domain}/{name}_{YYYYMMDD}.parquet

    If a file already exists for today's date it is overwritten and a
    warning is emitted so operators are aware of re-runs.

    Parameters
    ----------
    df : pd.DataFrame
        Transformed data to persist.
    domain : str
        Logical grouping sub-directory, e.g. ``"orders"`` or ``"products"``.
    name : str
        Base name for the file, e.g. ``"fact_orders"``.  The date suffix is
        appended automatically.

    Returns
    -------
    Path
        Absolute path of the written Parquet file.
    """
    date_tag: str = datetime.now().strftime("%Y%m%d")
    out_dir: Path = SILVER_DIR / domain
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path: Path = out_dir / f"{name}_{date_tag}.parquet"

    if out_path.exists():
        logger.warning(
            "Overwriting existing Silver file: {}",
            out_path,
        )

    df.to_parquet(out_path, index=False, engine="pyarrow")

    logger.info(
        "Wrote Silver file '{}': {:,} rows",
        out_path,
        len(df),
    )
    return out_path


# ---------------------------------------------------------------------------
# Quarantine writer
# ---------------------------------------------------------------------------

def quarantine_rows(
    df: pd.DataFrame,
    reasons: pd.Series,
    transform_name: str,
) -> Path | None:
    """Persist rejected rows to the Quarantine layer with rejection metadata.

    Each quarantined row is annotated with:

    * ``quarantine_reason`` — the string reason explaining why the row was
      rejected (taken from *reasons*, which must be aligned with *df*'s index).
    * ``quarantined_at``    — ISO 8601 timestamp of when quarantine occurred.

    The file is written atomically via a ``.parquet.tmp`` temporary path that
    is renamed on success, matching the pattern used in Stage 2 extractors.

    Parameters
    ----------
    df : pd.DataFrame
        Rows to quarantine.  Must be non-empty for a file to be written.
    reasons : pd.Series
        String Series aligned with *df*'s index explaining each row's
        rejection.  Typically constructed with boolean masks before calling
        this function.
    transform_name : str
        Logical name of the transform step, used as the file name stem,
        e.g. ``"clean_orders"``.  A ``YYYYMMDD_HHMMSS`` timestamp is
        appended so multiple runs on the same day do not collide.

    Returns
    -------
    Path | None
        Absolute path of the written Parquet file, or ``None`` when *df* is
        empty (nothing to quarantine).
    """
    if df.empty:
        logger.info(
            "[QUARANTINE] {}: no rows to quarantine",
            transform_name,
        )
        return None

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    ts: str = timestamp_suffix()
    out_path: Path = QUARANTINE_DIR / f"{transform_name}_{ts}.parquet"
    tmp_path: Path = out_path.with_suffix(".parquet.tmp")

    quarantined: pd.DataFrame = df.copy()
    quarantined["quarantine_reason"] = reasons.values
    quarantined["quarantined_at"] = datetime.now().isoformat()

    try:
        quarantined.to_parquet(tmp_path, index=False, engine="pyarrow")
        tmp_path.rename(out_path)
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logger.error(
            "Failed to write quarantine file for '{}': {}\n{}",
            transform_name,
            exc,
            traceback.format_exc(),
        )
        raise

    logger.info(
        "[QUARANTINE] {}: {:,} row(s) quarantined → {}",
        transform_name,
        len(quarantined),
        out_path,
    )
    return out_path


# ---------------------------------------------------------------------------
# Transform summary logger
# ---------------------------------------------------------------------------

def log_transform_summary(
    stage: str,
    before: int,
    after: int,
    quarantined: int,
) -> None:
    """Log a one-line summary of a transform stage's row-count outcomes.

    The summary is emitted at INFO level when less than 10 % of rows were
    dropped, and at WARNING level when 10 % or more were dropped so that
    operators are alerted to unexpectedly high rejection rates.

    Parameters
    ----------
    stage : str
        Human-readable name of the transform stage, e.g. ``"clean_orders"``.
    before : int
        Number of rows entering the stage.
    after : int
        Number of rows passing all quality checks and written to Silver.
    quarantined : int
        Number of rows rejected and written to the Quarantine layer.
    """
    drop_pct: float = (quarantined / before * 100) if before > 0 else 0.0

    message: str = (
        f"[TRANSFORM] {stage}: {before:,} rows in "
        f"\u2192 {after:,} kept, {quarantined:,} quarantined "
        f"({drop_pct:.1f}% drop)"
    )

    if drop_pct >= 10.0:
        logger.warning(message)
    else:
        logger.info(message)


# ---------------------------------------------------------------------------
# Pipeline date range helper
# ---------------------------------------------------------------------------

def get_pipeline_date_range() -> tuple[str, str]:
    """Return the configured pipeline processing date range.

    Reads ``PIPELINE_START_DATE`` and ``PIPELINE_END_DATE`` from the
    environment (via ``get_pipeline_config()`` in ``src.utils.db``).

    Returns
    -------
    tuple[str, str]
        ``(start_date, end_date)`` as ISO date strings,
        e.g. ``("2016-09-01", "2018-10-31")``.
    """
    config: dict[str, str] = get_pipeline_config()
    return config["start_date"], config["end_date"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Path constants
    "SILVER_DIR",
    "QUARANTINE_DIR",
    # I/O helpers
    "read_latest_bronze_parquet",
    "write_silver",
    "quarantine_rows",
    # Logging
    "log_transform_summary",
    # Configuration
    "get_pipeline_date_range",
]
