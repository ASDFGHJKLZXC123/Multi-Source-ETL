"""
Stage 4 — Gold layer: dimension table builders.

This module builds all five Gold dimension tables from Bronze and Silver
sources and writes them as date-suffix-free Parquet files to
``data/gold/dimensions/``.

Dimension inventory
-------------------
dim_date       — Calendar spine generated from the pipeline date range.
dim_customer   — One row per unique customer (SCD Type 1, overwrite).
dim_product    — One row per unique product (SCD Type 1, overwrite).
dim_store      — One row per unique store (SCD Type 1, overwrite).
dim_currency   — Derived from Silver orders + FX rate currency codes.

Design decisions
----------------
- SCD Type 1 is applied to all entity dimensions: on each run the full
  Bronze snapshot is consumed, deduplicated on the business key keeping
  the last occurrence, and written over the existing Gold file.  No
  history columns (eff_date, exp_date, is_current) are maintained here;
  those belong to a Type 2 variant that can be layered on top later.
- ``dim_date`` uses YYYYMMDD integer keys (e.g. 20170601) rather than a
  1-based sequence because date keys are used as partition predicates in
  most analytical queries and the integer is self-documenting.
- All other dimensions use ``assign_surrogate_keys`` (1-based integers)
  for compact, join-friendly PKs.
- Currency names fall back to the raw ISO code for any unrecognised code
  so the dimension never silently drops a currency seen in the fact data.

Entry point
-----------
Run as a script::

    python -m src.transform.build_dimensions

or import and call ``run()`` from an orchestration layer (Airflow, Prefect, …).
"""

from __future__ import annotations

import sys
import traceback
from typing import Final

import pandas as pd

from src.extract.config import BRONZE_DB
from src.transform.gold_utils import (
    assign_surrogate_keys,
    read_latest_silver,
    write_gold,
)
from src.transform.utils import (
    SILVER_DIR,  # noqa: F401 — re-exported for callers that need it
    get_pipeline_date_range,
    read_latest_bronze_parquet,
)
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Hardcoded ISO 4217 currency name lookup
# Extend this dict when new currencies appear in the source data.
# ---------------------------------------------------------------------------
_CURRENCY_NAMES: Final[dict[str, str]] = {
    "BRL": "Brazilian Real",
    "USD": "US Dollar",
    "EUR": "Euro",
    "GBP": "British Pound Sterling",
    "JPY": "Japanese Yen",
    "CAD": "Canadian Dollar",
    "AUD": "Australian Dollar",
    "CHF": "Swiss Franc",
    "CNY": "Chinese Yuan Renminbi",
    "MXN": "Mexican Peso",
    "ARS": "Argentine Peso",
    "CLP": "Chilean Peso",
    "COP": "Colombian Peso",
    "PEN": "Peruvian Sol",
}


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------


def build_dim_date(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Build and write the calendar date dimension.

    If *start_date* / *end_date* are not supplied the pipeline date range
    configured in the environment (``PIPELINE_START_DATE`` /
    ``PIPELINE_END_DATE``) is used via ``get_pipeline_date_range()``.

    The surrogate key ``date_key`` is the date formatted as an integer
    ``YYYYMMDD`` (e.g. ``20170601``).  This is the standard data warehouse
    pattern for date dimensions: the key is self-describing and can be used
    directly as a partition predicate without a lookup join.

    Parameters
    ----------
    start_date : str | None
        ISO date string for the first date in the spine, e.g. ``"2016-09-01"``.
        Falls back to pipeline config when ``None``.
    end_date : str | None
        ISO date string for the last date in the spine (inclusive),
        e.g. ``"2018-10-31"``.  Falls back to pipeline config when ``None``.

    Returns
    -------
    pd.DataFrame
        The full date dimension DataFrame that was written to Gold.
    """
    if start_date is None or end_date is None:
        cfg_start, cfg_end = get_pipeline_date_range()
        start_date = start_date or cfg_start
        end_date = end_date or cfg_end

    logger.info(
        "Building dim_date: {} → {}",
        start_date,
        end_date,
    )

    dates: pd.DatetimeIndex = pd.date_range(
        start=start_date,
        end=end_date,
        freq="D",
    )

    df: pd.DataFrame = pd.DataFrame({"date": dates})

    # Surrogate key: YYYYMMDD integer — deterministic and self-documenting.
    df.insert(0, "date_key", df["date"].dt.strftime("%Y%m%d").astype(int))

    df["year"] = df["date"].dt.year.astype(int)
    df["quarter"] = df["date"].dt.quarter.astype(int)
    df["month"] = df["date"].dt.month.astype(int)
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["day_of_month"] = df["date"].dt.day.astype(int)
    # pandas dayofweek: 0 = Monday … 6 = Sunday
    df["day_of_week"] = df["date"].dt.dayofweek.astype(int)
    df["day_name"] = df["date"].dt.day_name()
    df["month_name"] = df["date"].dt.month_name()
    df["quarter_name"] = "Q" + df["quarter"].astype(str)
    df["is_weekend"] = df["day_of_week"] >= 5
    df["is_month_end"] = df["date"].dt.is_month_end

    write_gold(df, "dimensions", "dim_date")
    logger.info("dim_date complete: {:,} rows ({} → {})", len(df), start_date, end_date)
    return df


# ---------------------------------------------------------------------------
# dim_customer
# ---------------------------------------------------------------------------


def build_dim_customer() -> pd.DataFrame:
    """Build and write the customer dimension (SCD Type 1).

    Reads the latest Bronze customer snapshot, drops records without a
    business key, deduplicates on ``customer_code`` keeping the last row
    (Type 1 overwrite semantics), assigns a 1-based surrogate key, and
    writes to Gold.

    Returns
    -------
    pd.DataFrame
        The customer dimension DataFrame that was written to Gold.
    """
    logger.info("Building dim_customer from Bronze customers snapshot")

    raw: pd.DataFrame = read_latest_bronze_parquet(BRONZE_DB / "customers")

    # Select and rename to canonical Gold schema.
    df: pd.DataFrame = raw[
        [
            "customer_id",
            "customer_code",
            "zip_code_prefix",
            "city",
            "state",
            "is_active",
        ]
    ].copy()

    # Drop rows missing the business key — these cannot be joined to facts.
    null_key_mask: pd.Series = df["customer_code"].isna()
    null_key_count: int = null_key_mask.sum()
    if null_key_count > 0:
        logger.warning(
            "dim_customer: dropping {:,} row(s) with null customer_code",
            null_key_count,
        )
        df = df[~null_key_mask]

    # SCD Type 1: keep the last occurrence of each business key.
    before: int = len(df)
    df = df.drop_duplicates(subset=["customer_code"], keep="last").reset_index(drop=True)
    dupes_dropped: int = before - len(df)
    if dupes_dropped > 0:
        logger.info(
            "dim_customer: deduplicated {:,} duplicate customer_code row(s)",
            dupes_dropped,
        )

    df = assign_surrogate_keys(df, "customer_key")

    write_gold(df, "dimensions", "dim_customer")
    logger.info("dim_customer complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# dim_product
# ---------------------------------------------------------------------------


def build_dim_product() -> pd.DataFrame:
    """Build and write the product dimension (SCD Type 1).

    Reads the latest Bronze product snapshot, applies the same business-key
    guard and Type 1 deduplication as ``build_dim_customer``, then writes to
    Gold.  Attribute values from the latest Bronze snapshot overwrite any
    previously stored values on each pipeline run.

    Returns
    -------
    pd.DataFrame
        The product dimension DataFrame that was written to Gold.
    """
    logger.info("Building dim_product from Bronze products snapshot")

    raw: pd.DataFrame = read_latest_bronze_parquet(BRONZE_DB / "products")

    df: pd.DataFrame = raw[
        [
            "product_id",
            "product_code",
            "category_name_en",
            "category_name_pt",
            "weight_g",
            "is_active",
        ]
    ].copy()

    # Ensure weight_g is float (nullable).
    df["weight_g"] = pd.to_numeric(df["weight_g"], errors="coerce")

    # Drop rows missing the business key.
    null_key_mask: pd.Series = df["product_code"].isna()
    null_key_count: int = null_key_mask.sum()
    if null_key_count > 0:
        logger.warning(
            "dim_product: dropping {:,} row(s) with null product_code",
            null_key_count,
        )
        df = df[~null_key_mask]

    # SCD Type 1: keep the last occurrence of each business key.
    before: int = len(df)
    df = df.drop_duplicates(subset=["product_code"], keep="last").reset_index(drop=True)
    dupes_dropped: int = before - len(df)
    if dupes_dropped > 0:
        logger.info(
            "dim_product: deduplicated {:,} duplicate product_code row(s)",
            dupes_dropped,
        )

    df = assign_surrogate_keys(df, "product_key")

    write_gold(df, "dimensions", "dim_product")
    logger.info("dim_product complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# dim_store
# ---------------------------------------------------------------------------


def build_dim_store() -> pd.DataFrame:
    """Build and write the store dimension (SCD Type 1).

    Reads the latest Bronze store snapshot.  Follows the same pattern as
    ``build_dim_customer``: business-key guard, Type 1 deduplication on
    ``store_code``, surrogate key assignment, Gold write.

    Returns
    -------
    pd.DataFrame
        The store dimension DataFrame that was written to Gold.
    """
    logger.info("Building dim_store from Bronze stores snapshot")

    raw: pd.DataFrame = read_latest_bronze_parquet(BRONZE_DB / "stores")

    df: pd.DataFrame = raw[
        [
            "store_id",
            "store_code",
            "zip_code_prefix",
            "city",
            "state",
            "region",
            "is_active",
        ]
    ].copy()

    # Drop rows missing the business key.
    null_key_mask: pd.Series = df["store_code"].isna()
    null_key_count: int = null_key_mask.sum()
    if null_key_count > 0:
        logger.warning(
            "dim_store: dropping {:,} row(s) with null store_code",
            null_key_count,
        )
        df = df[~null_key_mask]

    # SCD Type 1: keep the last occurrence of each business key.
    before: int = len(df)
    df = df.drop_duplicates(subset=["store_code"], keep="last").reset_index(drop=True)
    dupes_dropped: int = before - len(df)
    if dupes_dropped > 0:
        logger.info(
            "dim_store: deduplicated {:,} duplicate store_code row(s)",
            dupes_dropped,
        )

    df = assign_surrogate_keys(df, "store_key")

    write_gold(df, "dimensions", "dim_store")
    logger.info("dim_store complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# dim_currency
# ---------------------------------------------------------------------------


def build_dim_currency() -> pd.DataFrame:
    """Build and write the currency dimension derived from Silver sources.

    Because there is no dedicated Bronze currency table, the dimension is
    derived by unioning all currency codes observed in Silver:

    * ``currency_code`` from Silver orders (the transaction currency).
    * ``base_currency`` and ``quote_currency`` from Silver FX rates
      (exchange rate reference currencies).

    Unknown ISO codes receive a ``currency_name`` equal to the code itself
    so the dimension is always complete with respect to the fact data.

    Returns
    -------
    pd.DataFrame
        The currency dimension DataFrame that was written to Gold.
    """
    logger.info("Building dim_currency from Silver orders + FX rates")

    # Collect codes from Silver orders.
    orders_df: pd.DataFrame = read_latest_silver("sales", "orders")
    order_codes: set[str] = set(orders_df["currency_code"].dropna().unique())
    logger.info(
        "dim_currency: {:,} unique code(s) from Silver orders",
        len(order_codes),
    )

    # Collect codes from Silver FX rates.
    fx_df: pd.DataFrame = read_latest_silver("fx", "fx_rates")
    fx_codes: set[str] = set(fx_df["base_currency"].dropna().unique()) | set(
        fx_df["quote_currency"].dropna().unique()
    )
    logger.info(
        "dim_currency: {:,} unique code(s) from Silver FX rates",
        len(fx_codes),
    )

    # Union and sort for a stable, deterministic ordering.
    all_codes: list[str] = sorted(order_codes | fx_codes)

    if not all_codes:
        logger.warning(
            "dim_currency: no currency codes found in Silver; " "writing empty dimension"
        )

    df: pd.DataFrame = pd.DataFrame({"currency_code": all_codes})

    # Map known codes to human-readable names; fall back to the code itself.
    df["currency_name"] = df["currency_code"].map(lambda code: _CURRENCY_NAMES.get(code, code))

    df = assign_surrogate_keys(df, "currency_key")

    write_gold(df, "dimensions", "dim_currency")
    logger.info("dim_currency complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------


def run() -> dict[str, pd.DataFrame]:
    """Build all five Gold dimension tables in dependency order.

    Dimensions are built in the following order so that any downstream
    fact builder that imports them can rely on all being present:

    1. dim_date       (no upstream Gold dependency)
    2. dim_customer   (no upstream Gold dependency)
    3. dim_product    (no upstream Gold dependency)
    4. dim_store      (no upstream Gold dependency)
    5. dim_currency   (derived from Silver, logically depends on orders)

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping of dimension name to the DataFrame written to Gold, e.g.
        ``{"dim_date": df_date, "dim_customer": df_customer, ...}``.
    """
    logger.info("=== Stage 4: Building Gold dimension tables ===")

    results: dict[str, pd.DataFrame] = {}

    results["dim_date"] = build_dim_date()
    results["dim_customer"] = build_dim_customer()
    results["dim_product"] = build_dim_product()
    results["dim_store"] = build_dim_store()
    results["dim_currency"] = build_dim_currency()

    # Summary log so operators can confirm row counts at a glance.
    summary_lines: list[str] = [f"  {name}: {len(df):,} rows" for name, df in results.items()]
    logger.info(
        "=== Stage 4 complete. Dimension row counts ===\n{}",
        "\n".join(summary_lines),
    )

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        dims = run()
        sys.exit(0 if dims else 1)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "build_dimensions failed: {}\n{}",
            exc,
            traceback.format_exc(),
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "build_dim_date",
    "build_dim_customer",
    "build_dim_product",
    "build_dim_store",
    "build_dim_currency",
    "run",
]
