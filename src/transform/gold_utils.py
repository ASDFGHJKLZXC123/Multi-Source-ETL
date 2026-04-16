"""
Shared utilities for Stage 4 — Gold-layer transformation.

This module is the single source of truth for:
  - Gold layer output paths (GOLD_DIR, GOLD_DIMS_DIR, GOLD_FACTS_DIR)
  - Reading the latest Silver Parquet snapshot for a given domain/prefix
  - Writing Gold DataFrames with atomic, date-suffix-free semantics
    (Gold always represents current state, never a dated snapshot)
  - Referential integrity checking between fact and dimension tables
  - Assigning 1-based integer surrogate keys to dimension tables

Design constraints
------------------
- All paths are resolved via ``Path(__file__).resolve().parents[2]`` so the
  module is stable regardless of the caller's working directory.
- Atomic writes (tmp + rename) are used for every Gold output, matching the
  pattern established in ``src.extract.extract_db`` and carried through
  the Silver layer.
- ``assign_surrogate_keys`` inserts the key column as the FIRST column so
  query tools that scan left-to-right find the PK immediately.
- No UUIDs are used for surrogate keys; plain 1-based integers keep joins
  cheap on columnar stores and make the output human-readable.
"""

from __future__ import annotations

import traceback
from pathlib import Path
from typing import Final

import pandas as pd

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Project root — resolved once at module load time.
# ---------------------------------------------------------------------------
_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Silver layer path — imported from the Silver utils to stay DRY.
# We re-import the constant rather than redefine it so a single rename of
# the Silver directory propagates everywhere.
# ---------------------------------------------------------------------------
from src.transform.utils import SILVER_DIR  # noqa: E402  (after Path setup)

# ---------------------------------------------------------------------------
# Gold layer output paths
# ---------------------------------------------------------------------------

#: Root directory for all Gold-layer output files.
GOLD_DIR: Final[Path] = _PROJECT_ROOT / "data" / "gold"

#: Sub-directory for Gold dimension tables.
GOLD_DIMS_DIR: Final[Path] = GOLD_DIR / "dimensions"

#: Sub-directory for Gold fact tables.
GOLD_FACTS_DIR: Final[Path] = GOLD_DIR / "facts"


# ---------------------------------------------------------------------------
# Silver reader
# ---------------------------------------------------------------------------


def read_latest_silver(domain: str, name_prefix: str) -> pd.DataFrame:
    """Read the most-recent Silver Parquet file for a domain / name prefix.

    The function globs for ``{name_prefix}_*.parquet`` files inside
    ``SILVER_DIR / domain``, excluding in-progress ``*.parquet.tmp`` files.
    Because Silver writers stamp files with a ``YYYYMMDD`` suffix,
    alphabetical sort is equivalent to chronological sort.

    Parameters
    ----------
    domain : str
        Logical domain sub-directory under ``SILVER_DIR``,
        e.g. ``"sales"`` or ``"fx"``.
    name_prefix : str
        Base name of the file without the date suffix,
        e.g. ``"orders"`` or ``"fx_rates"``.

    Returns
    -------
    pd.DataFrame
        Contents of the latest matching Parquet file.

    Raises
    ------
    FileNotFoundError
        If no matching ``*.parquet`` files are found.  The error message
        names the search directory and pattern so callers can diagnose
        missing upstream stages without re-globbing.
    """
    search_dir: Path = SILVER_DIR / domain
    pattern: str = f"{name_prefix}_*.parquet"

    candidates: list[Path] = sorted(
        p for p in search_dir.glob(pattern) if not p.name.endswith(".parquet.tmp")
    )

    if not candidates:
        raise FileNotFoundError(
            f"No Silver Parquet files matching '{pattern}' found in "
            f"'{search_dir}'. Ensure Stage 3 transformation has run for "
            f"domain='{domain}', prefix='{name_prefix}' before calling "
            "read_latest_silver()."
        )

    latest_file: Path = candidates[-1]
    df: pd.DataFrame = pd.read_parquet(latest_file)

    logger.info(
        "Loaded Silver file '{}': {:,} rows",
        latest_file,
        len(df),
    )
    return df


# ---------------------------------------------------------------------------
# Gold writer
# ---------------------------------------------------------------------------


def write_gold(df: pd.DataFrame, subdir: str, name: str) -> Path:
    """Write a Gold DataFrame to ``data/gold/{subdir}/{name}.parquet``.

    Gold files represent the **current state** of a dimension or fact and
    carry no date suffix — each run overwrites the previous file in place.
    The write is made atomic via a ``.parquet.tmp`` temporary path that is
    renamed on success, so downstream readers never observe a partial file.

    Parameters
    ----------
    df : pd.DataFrame
        Data to persist.
    subdir : str
        Sub-directory under ``GOLD_DIR``, e.g. ``"dimensions"`` or
        ``"facts"``.
    name : str
        Output file stem without extension, e.g. ``"dim_customer"``.

    Returns
    -------
    Path
        Absolute path of the written Parquet file.
    """
    out_dir: Path = GOLD_DIR / subdir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path: Path = out_dir / f"{name}.parquet"
    tmp_path: Path = out_dir / f"{name}.parquet.tmp"

    try:
        df.to_parquet(tmp_path, index=False, engine="pyarrow")
        tmp_path.rename(out_path)
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logger.error(
            "Failed to write Gold file '{}': {}\n{}",
            out_path,
            exc,
            traceback.format_exc(),
        )
        raise

    logger.info(
        "Wrote Gold file '{}': {:,} rows",
        out_path,
        len(df),
    )
    return out_path


# ---------------------------------------------------------------------------
# Referential integrity checker
# ---------------------------------------------------------------------------


def check_referential_integrity(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    fact_fk_col: str,
    dim_pk_col: str,
    label: str,
) -> tuple[pd.DataFrame, int]:
    """Identify fact rows whose foreign key has no matching dimension record.

    This is a pure diagnostic function — it does **not** drop or quarantine
    rows.  Callers decide how to handle orphans (log, quarantine, raise).

    Parameters
    ----------
    fact_df : pd.DataFrame
        Fact table (or any DataFrame acting as the "many" side of a join).
    dim_df : pd.DataFrame
        Dimension table (or any DataFrame acting as the "one" side).
    fact_fk_col : str
        Column in *fact_df* containing the foreign key values.
    dim_pk_col : str
        Column in *dim_df* containing the primary key values to match against.
    label : str
        Human-readable description used in log messages,
        e.g. ``"fact_orders.customer_id → dim_customer.customer_id"``.

    Returns
    -------
    tuple[pd.DataFrame, int]
        ``(orphan_df, orphan_count)`` where *orphan_df* contains the
        offending rows and *orphan_count* is ``len(orphan_df)``.
    """
    valid_keys: set = set(dim_df[dim_pk_col].dropna().unique())
    orphan_mask: pd.Series = ~fact_df[fact_fk_col].isin(valid_keys)
    orphan_df: pd.DataFrame = fact_df[orphan_mask].copy()
    orphan_count: int = len(orphan_df)

    if orphan_count == 0:
        logger.info("RI check passed: {}", label)
    else:
        logger.warning(
            "{:,} orphan rows in {}",
            orphan_count,
            label,
        )

    return orphan_df, orphan_count


# ---------------------------------------------------------------------------
# Surrogate key assignment
# ---------------------------------------------------------------------------


def assign_surrogate_keys(
    df: pd.DataFrame,
    key_col: str,
    start: int = 1,
) -> pd.DataFrame:
    """Prepend a 1-based integer surrogate key column to *df*.

    The key is derived from the DataFrame's positional index after any
    upstream deduplication.  Using a deterministic sequence (rather than
    UUIDs) keeps joins cheap on columnar stores and makes the output
    human-readable.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.  Its own index is ignored; positions are
        re-numbered from *start*.
    key_col : str
        Name of the surrogate key column to create,
        e.g. ``"customer_key"``.
    start : int, optional
        First key value.  Defaults to ``1`` (standard DW convention).

    Returns
    -------
    pd.DataFrame
        A new DataFrame with *key_col* inserted as the first column.
        The original index is reset and dropped.
    """
    result: pd.DataFrame = df.reset_index(drop=True).copy()
    result.insert(0, key_col, range(start, start + len(result)))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Path constants
    "GOLD_DIR",
    "GOLD_DIMS_DIR",
    "GOLD_FACTS_DIR",
    # I/O helpers
    "read_latest_silver",
    "write_gold",
    # Quality helpers
    "check_referential_integrity",
    "assign_surrogate_keys",
]
