"""
Stage 4 — Gold layer: fact table builders.

This module builds the three Gold fact tables from Silver sources and the
Gold dimension tables produced by ``build_dimensions.py``.  All outputs are
written as date-suffix-free Parquet files to ``data/gold/facts/``.

Fact table inventory
--------------------
fact_sales
    Grain: one row per order line item.
    Sources: Silver ``sales/order_items`` joined to Silver ``sales/orders``.
    FK resolution:
      - date_key         — computed directly from ``order_date`` as YYYYMMDD int;
                           no join against dim_date required.
      - customer_key     — left-join on ``customer_id`` → dim_customer.
      - product_key      — left-join on ``product_id``  → dim_product.
      - store_key        — left-join on ``store_id``    → dim_store.
      - currency_key     — left-join on ``currency_code`` → dim_currency.

fact_weather_daily
    Grain: one row per (city, date).
    Sources: Silver ``weather/weather``.
    FK resolution:
      - date_key — computed from ``date`` column as YYYYMMDD int;
                   RI-checked against dim_date.

fact_fx_rates
    Grain: one row per (date, base_currency, quote_currency).
    Sources: Silver ``fx/fx_rates``.
    FK resolution:
      - date_key           — computed from ``date`` column as YYYYMMDD int.
      - base_currency_key  — left-join on ``base_currency``  → dim_currency.
      - quote_currency_key — left-join on ``quote_currency`` → dim_currency.

Design decisions
----------------
- ``check_referential_integrity`` is diagnostic only.  Orphan rows are logged
  as warnings but are never dropped, so downstream consumers see the full
  dataset and can apply their own filtering policy.
- Surrogate key lookups all use ``how="left"`` merges to preserve every fact
  row even when a dimension record is absent (NULL key = unresolved FK).
- ``date_key`` is computed arithmetically (``strftime("%Y%m%d").astype(int)``)
  rather than joined against dim_date; this avoids a table scan and is
  guaranteed to produce the same integer the dimension uses.

Entry point
-----------
Run as a script::

    python -m src.transform.build_facts

or import and call ``run()`` from an orchestration layer (Airflow, Prefect, …).
"""

from __future__ import annotations

import sys
import traceback

import pandas as pd

from src.transform.gold_utils import (
    GOLD_DIMS_DIR,
    check_referential_integrity,
    read_latest_silver,
    write_gold,
)
from src.utils.logger import logger


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_dim_date() -> pd.DataFrame:
    """Return the minimal dim_date slice needed for RI checks."""
    return pd.read_parquet(GOLD_DIMS_DIR / "dim_date.parquet")[["date_key"]]


def _load_dim_customer() -> pd.DataFrame:
    """Return the customer_id → customer_key lookup slice."""
    return pd.read_parquet(GOLD_DIMS_DIR / "dim_customer.parquet")[
        ["customer_id", "customer_key"]
    ]


def _load_dim_product() -> pd.DataFrame:
    """Return the product_id → product_key lookup slice."""
    return pd.read_parquet(GOLD_DIMS_DIR / "dim_product.parquet")[
        ["product_id", "product_key"]
    ]


def _load_dim_store() -> pd.DataFrame:
    """Return the store_id → store_key lookup slice."""
    return pd.read_parquet(GOLD_DIMS_DIR / "dim_store.parquet")[
        ["store_id", "store_key"]
    ]


def _load_dim_currency() -> pd.DataFrame:
    """Return the currency_code → currency_key lookup slice."""
    return pd.read_parquet(GOLD_DIMS_DIR / "dim_currency.parquet")[
        ["currency_code", "currency_key"]
    ]


# ---------------------------------------------------------------------------
# fact_sales
# ---------------------------------------------------------------------------

def build_fact_sales() -> pd.DataFrame:
    """Build and write the sales fact table (grain: order line item).

    Joins Silver order_items to Silver orders, resolves all five dimension
    surrogate keys via left-merges, runs referential integrity checks on every
    FK column (logging warnings for orphans without dropping rows), selects the
    canonical output columns, and writes the result to Gold.

    Returns
    -------
    pd.DataFrame
        The fact_sales DataFrame that was written to Gold.
    """
    logger.info("Building fact_sales from Silver order_items + orders")

    # ------------------------------------------------------------------
    # Load Silver sources
    # ------------------------------------------------------------------
    order_items: pd.DataFrame = read_latest_silver("sales", "order_items")
    orders: pd.DataFrame = read_latest_silver("sales", "orders")

    logger.info(
        "fact_sales: {:,} order_item rows, {:,} order rows",
        len(order_items),
        len(orders),
    )

    # ------------------------------------------------------------------
    # Join order_items to orders on the source-system FK
    # Keep only columns needed downstream to avoid accidental name collisions
    # ------------------------------------------------------------------
    orders_slim: pd.DataFrame = orders[[
        "order_id",
        "order_code",
        "customer_id",
        "order_status",
        "order_date",
        "source_channel",
        "currency_code",
        "delivery_days_actual",
        "delivery_days_estimated",
    ]].copy()

    df: pd.DataFrame = order_items.merge(orders_slim, on="order_id", how="inner")

    logger.info(
        "fact_sales: {:,} rows after inner-join on order_id",
        len(df),
    )

    # ------------------------------------------------------------------
    # Compute date_key directly — avoids a table scan on dim_date
    # ------------------------------------------------------------------
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)

    # ------------------------------------------------------------------
    # Load dimension lookup slices
    # ------------------------------------------------------------------
    dim_customer: pd.DataFrame = _load_dim_customer()
    dim_product: pd.DataFrame = _load_dim_product()
    dim_store: pd.DataFrame = _load_dim_store()
    dim_currency: pd.DataFrame = _load_dim_currency()
    dim_date: pd.DataFrame = _load_dim_date()

    # ------------------------------------------------------------------
    # Resolve surrogate keys via left-merges
    # ------------------------------------------------------------------
    df = df.merge(dim_customer, on="customer_id", how="left")
    df = df.merge(dim_product, on="product_id", how="left")
    df = df.merge(dim_store, on="store_id", how="left")
    df = df.merge(dim_currency, on="currency_code", how="left")

    logger.info("fact_sales: surrogate key resolution complete")

    # ------------------------------------------------------------------
    # Referential integrity checks (diagnostic only — no rows dropped)
    # ------------------------------------------------------------------
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_date,
        fact_fk_col="date_key",
        dim_pk_col="date_key",
        label="fact_sales.date_key → dim_date.date_key",
    )
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_customer,
        fact_fk_col="customer_key",
        dim_pk_col="customer_key",
        label="fact_sales.customer_key → dim_customer.customer_key",
    )
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_product,
        fact_fk_col="product_key",
        dim_pk_col="product_key",
        label="fact_sales.product_key → dim_product.product_key",
    )
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_store,
        fact_fk_col="store_key",
        dim_pk_col="store_key",
        label="fact_sales.store_key → dim_store.store_key",
    )
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_currency,
        fact_fk_col="currency_key",
        dim_pk_col="currency_key",
        label="fact_sales.currency_key → dim_currency.currency_key",
    )

    # ------------------------------------------------------------------
    # Select and order output columns
    # ------------------------------------------------------------------
    output_cols: list[str] = [
        "order_item_id",
        "order_id",
        "order_code",
        "line_number",
        "date_key",
        "customer_key",
        "product_key",
        "store_key",
        "currency_key",
        "order_status",
        "source_channel",
        "unit_price",
        "freight_value",
        "quantity",
        "delivery_days_actual",
        "delivery_days_estimated",
    ]
    df = df[output_cols].reset_index(drop=True)

    write_gold(df, "facts", "fact_sales")
    logger.info("fact_sales complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# fact_weather_daily
# ---------------------------------------------------------------------------

def build_fact_weather_daily() -> pd.DataFrame:
    """Build and write the weather daily fact table (grain: city + date).

    Reads Silver weather, computes date_key as a YYYYMMDD integer, runs an RI
    check against dim_date (logging warnings for orphans without dropping rows),
    selects the canonical output columns, and writes the result to Gold.

    Returns
    -------
    pd.DataFrame
        The fact_weather_daily DataFrame that was written to Gold.
    """
    logger.info("Building fact_weather_daily from Silver weather")

    # ------------------------------------------------------------------
    # Load Silver weather
    # ------------------------------------------------------------------
    df: pd.DataFrame = read_latest_silver("weather", "weather")

    logger.info("fact_weather_daily: {:,} rows loaded from Silver", len(df))

    # ------------------------------------------------------------------
    # Compute date_key
    # ------------------------------------------------------------------
    df["date"] = pd.to_datetime(df["date"])
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

    # ------------------------------------------------------------------
    # Referential integrity check (diagnostic only — no rows dropped)
    # ------------------------------------------------------------------
    dim_date: pd.DataFrame = _load_dim_date()

    check_referential_integrity(
        fact_df=df,
        dim_df=dim_date,
        fact_fk_col="date_key",
        dim_pk_col="date_key",
        label="fact_weather_daily.date_key → dim_date.date_key",
    )

    # ------------------------------------------------------------------
    # Select and order output columns
    # ------------------------------------------------------------------
    output_cols: list[str] = [
        "date_key",
        "city",
        "state",
        "temp_max",
        "temp_min",
        "precipitation",
        "windspeed",
        "weathercode",
    ]
    df = df[output_cols].reset_index(drop=True)

    write_gold(df, "facts", "fact_weather_daily")
    logger.info("fact_weather_daily complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# fact_fx_rates
# ---------------------------------------------------------------------------

def build_fact_fx_rates() -> pd.DataFrame:
    """Build and write the FX rates fact table (grain: date + base + quote).

    Reads Silver FX rates, computes date_key as a YYYYMMDD integer, resolves
    base_currency_key and quote_currency_key via left-merges against
    dim_currency, runs RI checks on both FK columns (logging warnings for
    orphans without dropping rows), selects the canonical output columns, and
    writes the result to Gold.

    Returns
    -------
    pd.DataFrame
        The fact_fx_rates DataFrame that was written to Gold.
    """
    logger.info("Building fact_fx_rates from Silver fx_rates")

    # ------------------------------------------------------------------
    # Load Silver FX rates
    # ------------------------------------------------------------------
    df: pd.DataFrame = read_latest_silver("fx", "fx_rates")

    logger.info("fact_fx_rates: {:,} rows loaded from Silver", len(df))

    # ------------------------------------------------------------------
    # Compute date_key
    # ------------------------------------------------------------------
    df["date"] = pd.to_datetime(df["date"])
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

    # ------------------------------------------------------------------
    # Resolve base_currency_key and quote_currency_key
    # Both join on dim_currency.currency_code but map to different columns,
    # so we rename the key column immediately after each merge to avoid
    # collisions on the second join.
    # ------------------------------------------------------------------
    dim_currency: pd.DataFrame = _load_dim_currency()

    # base_currency → base_currency_key
    dim_base: pd.DataFrame = dim_currency.rename(
        columns={"currency_code": "base_currency", "currency_key": "base_currency_key"}
    )
    df = df.merge(dim_base, on="base_currency", how="left")

    # quote_currency → quote_currency_key
    dim_quote: pd.DataFrame = dim_currency.rename(
        columns={"currency_code": "quote_currency", "currency_key": "quote_currency_key"}
    )
    df = df.merge(dim_quote, on="quote_currency", how="left")

    logger.info("fact_fx_rates: currency surrogate key resolution complete")

    # ------------------------------------------------------------------
    # Referential integrity checks (diagnostic only — no rows dropped)
    # ------------------------------------------------------------------
    dim_date: pd.DataFrame = _load_dim_date()

    check_referential_integrity(
        fact_df=df,
        dim_df=dim_date,
        fact_fk_col="date_key",
        dim_pk_col="date_key",
        label="fact_fx_rates.date_key → dim_date.date_key",
    )
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_currency.rename(columns={"currency_key": "base_currency_key"}),
        fact_fk_col="base_currency_key",
        dim_pk_col="base_currency_key",
        label="fact_fx_rates.base_currency_key → dim_currency.currency_key",
    )
    check_referential_integrity(
        fact_df=df,
        dim_df=dim_currency.rename(columns={"currency_key": "quote_currency_key"}),
        fact_fk_col="quote_currency_key",
        dim_pk_col="quote_currency_key",
        label="fact_fx_rates.quote_currency_key → dim_currency.currency_key",
    )

    # ------------------------------------------------------------------
    # Select and order output columns
    # ------------------------------------------------------------------
    output_cols: list[str] = [
        "date_key",
        "base_currency_key",
        "quote_currency_key",
        "base_currency",
        "quote_currency",
        "rate",
    ]
    df = df[output_cols].reset_index(drop=True)

    write_gold(df, "facts", "fact_fx_rates")
    logger.info("fact_fx_rates complete: {:,} rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------

def run() -> dict[str, pd.DataFrame]:
    """Build all three Gold fact tables in dependency order.

    Fact tables are built after all dimensions have been written by
    ``build_dimensions.run()``.  The order here is:

    1. fact_sales          (depends on dim_date, dim_customer, dim_product,
                            dim_store, dim_currency)
    2. fact_weather_daily  (depends on dim_date)
    3. fact_fx_rates       (depends on dim_date, dim_currency)

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping of fact name to the DataFrame written to Gold, e.g.
        ``{"fact_sales": df_sales, "fact_weather_daily": df_weather, ...}``.
    """
    logger.info("=== Stage 4: Building Gold fact tables ===")

    results: dict[str, pd.DataFrame] = {}

    results["fact_sales"] = build_fact_sales()
    results["fact_weather_daily"] = build_fact_weather_daily()
    results["fact_fx_rates"] = build_fact_fx_rates()

    # Summary log so operators can confirm row counts at a glance.
    summary_lines: list[str] = [
        f"  {name}: {len(df):,} rows"
        for name, df in results.items()
    ]
    logger.info(
        "=== Stage 4 complete. Fact row counts ===\n{}",
        "\n".join(summary_lines),
    )

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        facts = run()
        sys.exit(0 if facts else 1)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "build_facts failed: {}\n{}",
            exc,
            traceback.format_exc(),
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "build_fact_sales",
    "build_fact_weather_daily",
    "build_fact_fx_rates",
    "run",
]
