"""
Data quality validation utilities for the ETL pipeline.

Provides:
  - validate_dataframe()       : Structural + null-rate checks against a schema dict
  - log_data_quality_report()  : Logs a summary profile of a DataFrame
  - normalize_city_name()      : Strips accents, lowercases, trims whitespace
"""

from __future__ import annotations

import unicodedata
from typing import Any

import pandas as pd

from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Type alias: schema dict maps column name -> expected pandas dtype category
# e.g. {"order_id": "object", "price": "float64", "order_date": "datetime64"}
# ---------------------------------------------------------------------------
SchemaDict = dict[str, str]


def normalize_city_name(city: str) -> str:
    """Return a normalised city name suitable for fuzzy matching or joins.

    Transformations applied:
      1. Unicode NFC normalisation
      2. Decompose accented characters (NFD) and strip combining marks
      3. Lowercase
      4. Strip leading/trailing whitespace

    Parameters
    ----------
    city : str
        Raw city name, e.g. ``"São Paulo"`` or ``"RECIFE "``

    Returns
    -------
    str
        Cleaned city name, e.g. ``"sao paulo"`` or ``"recife"``
    """
    if not isinstance(city, str):
        return ""
    # NFC first to unify representation, then NFD to split base + combining chars
    normalised = unicodedata.normalize("NFD", city)
    without_accents = "".join(ch for ch in normalised if unicodedata.category(ch) != "Mn")
    return without_accents.lower().strip()


def validate_dataframe(
    df: pd.DataFrame,
    schema_dict: SchemaDict,
    max_null_rate: float = 0.50,
) -> list[str]:
    """Validate a DataFrame against an expected schema.

    Checks performed:
      - All expected columns are present
      - Each column's dtype is broadly compatible with the expected type
      - No single column exceeds *max_null_rate* fraction of null values

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to validate.
    schema_dict : SchemaDict
        Mapping of column name to expected dtype string.
        Accepted dtype strings: ``"object"``, ``"int"``, ``"float"``,
        ``"bool"``, ``"datetime"``.
    max_null_rate : float
        Fraction of nulls above which a warning is issued (default 0.50).

    Returns
    -------
    list[str]
        List of validation error/warning messages. Empty list means passed.
    """
    issues: list[str] = []

    # --- Column presence ---
    missing_cols = [col for col in schema_dict if col not in df.columns]
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")

    for col, expected_dtype in schema_dict.items():
        if col not in df.columns:
            continue  # already reported above

        actual_dtype = str(df[col].dtype)
        compatible = _dtype_compatible(actual_dtype, expected_dtype)
        if not compatible:
            issues.append(
                f"Column '{col}': expected dtype category '{expected_dtype}', "
                f"got '{actual_dtype}'"
            )

        # --- Null rate ---
        null_rate = df[col].isna().mean()
        if null_rate > max_null_rate:
            issues.append(
                f"Column '{col}': null rate {null_rate:.1%} exceeds threshold {max_null_rate:.1%}"
            )

    return issues


def _dtype_compatible(actual: str, expected_category: str) -> bool:
    """Return True if *actual* dtype is broadly compatible with *expected_category*."""
    category_map: dict[str, list[str]] = {
        "object": ["object", "string", "category"],
        "int": ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"],
        "float": ["float16", "float32", "float64"],
        "bool": ["bool"],
        "datetime": ["datetime64", "datetime64[ns]", "datetime64[ns, UTC]"],
    }
    allowed = category_map.get(expected_category, [expected_category])
    return any(actual.startswith(a) for a in allowed)


def log_data_quality_report(df: pd.DataFrame, name: str) -> dict[str, Any]:
    """Log a concise quality profile of a DataFrame and return it as a dict.

    Logged metrics:
      - Row and column count
      - Per-column null percentage
      - Duplicate row count
      - Min/max for all numeric columns

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to profile.
    name : str
        Human-readable label used in log messages (e.g., ``"orders"``).

    Returns
    -------
    dict[str, Any]
        Dictionary containing the same metrics for downstream use.
    """
    row_count = len(df)
    col_count = len(df.columns)
    duplicate_count = int(df.duplicated().sum())

    logger.info("[QA] '{}' — {:,} rows, {} columns, {:,} duplicate rows", name, row_count, col_count, duplicate_count)

    null_report: dict[str, float] = {}
    for col in df.columns:
        null_pct = df[col].isna().mean() * 100
        if null_pct > 0:
            logger.debug("[QA] '{}' column '{}' — {:.1f}% nulls", name, col, null_pct)
        null_report[col] = round(null_pct, 2)

    numeric_stats: dict[str, dict[str, float]] = {}
    for col in df.select_dtypes(include="number").columns:
        col_min = float(df[col].min())
        col_max = float(df[col].max())
        logger.debug("[QA] '{}' column '{}' — min={:.4g}, max={:.4g}", name, col, col_min, col_max)
        numeric_stats[col] = {"min": col_min, "max": col_max}

    report = {
        "table": name,
        "row_count": row_count,
        "col_count": col_count,
        "duplicate_rows": duplicate_count,
        "null_pct_by_column": null_report,
        "numeric_range": numeric_stats,
    }
    return report


__all__ = [
    "normalize_city_name",
    "validate_dataframe",
    "log_data_quality_report",
    "SchemaDict",
]
