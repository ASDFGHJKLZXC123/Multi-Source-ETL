"""
Shared configuration and constants for the Stage 2 extract modules.

This module is the single source of truth for:
  - Bronze layer output paths (BRONZE_DB, BRONZE_API, BRONZE_MANUAL)
  - Retry / backoff policy (RetryConfig)
  - Ordered list of source-system tables to extract (BRONZE_DB_TABLES)
  - Flat-file schema specs for validation (FLAT_FILE_SCHEMAS)
  - Utility helpers for file naming (timestamp_suffix, idempotency_key)

Design constraints
------------------
- Zero imports from src.utils.db or any other pipeline module.
  This avoids circular imports: config is a leaf node in the dependency graph.
- Only stdlib modules are imported (pathlib, datetime, dataclasses, typing).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Final

# ---------------------------------------------------------------------------
# Project root — resolved once at module load time.
# All path constants are absolute so they are stable regardless of the
# working directory the caller uses.
# ---------------------------------------------------------------------------
_PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Bronze layer output paths
# ---------------------------------------------------------------------------

#: Root directory for database table extracts written as Parquet files.
BRONZE_DB: Final[Path] = _PROJECT_ROOT / "data" / "bronze" / "db"

#: Root directory for API extracts (sub-directories: weather/, fx/).
BRONZE_API: Final[Path] = _PROJECT_ROOT / "data" / "bronze" / "api"

#: Root directory for manually sourced flat files (CSV / Parquet).
BRONZE_MANUAL: Final[Path] = _PROJECT_ROOT / "data" / "bronze" / "manual"


# ---------------------------------------------------------------------------
# Retry / backoff policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryConfig:
    """Immutable configuration for HTTP / database retry behaviour.

    Attributes
    ----------
    max_attempts : int
        Total number of attempts (including the first call).
        The number of *retries* is therefore ``max_attempts - 1``.
    backoff_base : int
        Base for the exponential backoff formula.
        Wait before attempt ``n`` = ``backoff_base ** n`` seconds.
        With defaults: 2 s, 4 s, 8 s.

    Example
    -------
    >>> cfg = RetryConfig()
    >>> [cfg.backoff_base ** n for n in range(1, cfg.max_attempts)]
    [2, 4]
    """

    max_attempts: int = field(default=3)
    backoff_base: int = field(default=2)

    def wait_seconds(self, attempt: int) -> int:
        """Return the number of seconds to sleep before *attempt*.

        Parameters
        ----------
        attempt : int
            1-based attempt number (1 = first retry, 2 = second retry, ...).

        Returns
        -------
        int
        """
        return self.backoff_base ** attempt


#: Default retry policy shared across all Stage 2 extractors.
DEFAULT_RETRY: Final[RetryConfig] = RetryConfig()


# ---------------------------------------------------------------------------
# Source-system database tables — extraction order matters
# (parent tables must arrive before children to satisfy FK constraints
# during Silver-layer loading).
# ---------------------------------------------------------------------------

BRONZE_DB_TABLES: Final[list[str]] = [
    "customers",
    "stores",
    "products",
    "orders",
    "order_items",
]


# ---------------------------------------------------------------------------
# Flat-file schema specifications
# Maps each file's stem (filename without extension) to the minimum set of
# columns that must be present for the file to be considered valid.
# extract_file.py uses this dict to gate downstream processing.
# ---------------------------------------------------------------------------

FLAT_FILE_SCHEMAS: Final[dict[str, list[str]]] = {
    "municipios": [
        "codigo_ibge",
        "nome",
        "latitude",
        "longitude",
        "capital",
        "codigo_uf",
    ],
    "olist_customers_dataset": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "olist_sellers_dataset": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "olist_products_dataset": [
        "product_id",
        "product_category_name",
        "product_weight_g",
    ],
    "olist_orders_dataset": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    ],
    "olist_order_items_dataset": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    ],
    "product_category_name_translation": [
        "product_category_name",
        "product_category_name_english",
    ],
}


# ---------------------------------------------------------------------------
# File naming helpers
# ---------------------------------------------------------------------------

def timestamp_suffix() -> str:
    """Return a sortable timestamp string suitable for use in file names.

    Format: ``YYYYMMDD_HHMMSS``

    Returns
    -------
    str
        e.g. ``"20260415_143022"``

    Example
    -------
    >>> suffix = timestamp_suffix()
    >>> len(suffix)
    15
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def idempotency_key(name: str, date_str: str) -> str:
    """Build a deterministic cache key for a named extract on a given date.

    The key is used to detect whether a particular extract has already been
    written to the Bronze layer, enabling safe re-runs without duplicate data.

    Parameters
    ----------
    name : str
        Logical name of the extract, e.g. a table name (``"orders"``) or API
        slug (``"weather_sao_paulo"``).  Spaces are replaced with underscores
        and the value is lowercased for consistency.
    date_str : str
        ISO date string representing the logical processing date,
        e.g. ``"2026-04-15"``.

    Returns
    -------
    str
        A snake_case key of the form ``"{name}__{date_str}"``, e.g.
        ``"orders__2026-04-15"``.

    Example
    -------
    >>> idempotency_key("order_items", "2018-10-31")
    'order_items__2018-10-31'
    >>> idempotency_key("Sao Paulo", "2017-06-01")
    'sao_paulo__2017-06-01'
    """
    normalised_name = name.strip().lower().replace(" ", "_")
    return f"{normalised_name}__{date_str}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Paths
    "BRONZE_DB",
    "BRONZE_API",
    "BRONZE_MANUAL",
    # Retry policy
    "RetryConfig",
    "DEFAULT_RETRY",
    # Table / schema metadata
    "BRONZE_DB_TABLES",
    "FLAT_FILE_SCHEMAS",
    # Helpers
    "timestamp_suffix",
    "idempotency_key",
]
