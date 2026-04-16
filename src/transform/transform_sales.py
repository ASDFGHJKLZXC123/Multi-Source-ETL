"""
src/transform/transform_sales.py
---------------------------------
Stage 3 Silver Transform — Sales domain.

Reads the latest Bronze Parquet snapshots for ``orders`` and ``order_items``,
applies quality rules (status filtering, null checks, type coercion, pandera
schema validation), routes rejected rows to the Quarantine layer, and writes
clean Silver Parquet files.

Public entry-points
-------------------
* ``transform_orders()``     — cleans and promotes the orders table.
* ``transform_order_items()``— cleans and promotes the order_items table,
                               respecting the set of order IDs already in Silver.
* ``run()``                  — orchestrates both transforms end-to-end.
"""

from __future__ import annotations

import sys
from typing import Final

import pandas as pd

from src.extract.config import BRONZE_DB
from src.transform.schemas import (
    VALID_ORDER_STATUSES,
    SilverOrderItemSchema,
    SilverOrderSchema,
    validate_silver,
)
from src.utils.logger import logger
from src.transform.utils import (
    log_transform_summary,
    quarantine_rows,
    read_latest_bronze_parquet,
    write_silver,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TIMESTAMP_COLS: Final[tuple[str, ...]] = (
    "order_timestamp",
    "approved_at",
    "estimated_delivery",
    "actual_delivery",
    "ingested_at",
)

__all__ = ["transform_orders", "transform_order_items", "run"]


# ---------------------------------------------------------------------------
# Orders transform
# ---------------------------------------------------------------------------


def transform_orders() -> tuple[pd.DataFrame, int]:
    """Read, clean, validate, and write Silver orders.

    Steps performed in order:

    1. Load latest Bronze snapshot from ``data/bronze/db/orders/``.
    2. Quarantine rows where ``order_status == 'canceled'``.
    3. Quarantine rows with a null ``order_date``.
    4. Quarantine rows with a null ``customer_id``.
    5. Cast ``order_date`` to ``datetime64``.
    6. Coerce timestamp-like columns to ``datetime64`` (errors → NaT).
    7. Quarantine rows whose ``order_status`` is not in
       ``VALID_ORDER_STATUSES``.
    8. Run pandera ``SilverOrderSchema`` validation; quarantine failures.
    9. Write valid rows to ``data/silver/sales/orders_YYYYMMDD.parquet``.

    Returns
    -------
    tuple[pd.DataFrame, int]
        ``(valid_df, total_quarantine_count)`` where *valid_df* is the
        DataFrame written to Silver.
    """
    df: pd.DataFrame = read_latest_bronze_parquet(BRONZE_DB / "orders")
    initial_count: int = len(df)

    quarantine_frames: list[pd.DataFrame] = []
    quarantine_reasons: list[pd.Series] = []

    # ------------------------------------------------------------------
    # Step 2: Remove cancellations
    # ------------------------------------------------------------------
    canceled_mask: pd.Series = df["order_status"] == "canceled"
    if canceled_mask.any():
        quarantine_frames.append(df.loc[canceled_mask])
        quarantine_reasons.append(
            pd.Series(
                "canceled order excluded from Silver",
                index=df.index[canceled_mask],
            )
        )
        df = df.loc[~canceled_mask].copy()

    # ------------------------------------------------------------------
    # Step 3: Remove null order_date
    # ------------------------------------------------------------------
    null_date_mask: pd.Series = df["order_date"].isna()
    if null_date_mask.any():
        quarantine_frames.append(df.loc[null_date_mask])
        quarantine_reasons.append(
            pd.Series(
                "null order_date",
                index=df.index[null_date_mask],
            )
        )
        df = df.loc[~null_date_mask].copy()

    # ------------------------------------------------------------------
    # Step 4: Remove null customer_id
    # ------------------------------------------------------------------
    null_cust_mask: pd.Series = df["customer_id"].isna()
    if null_cust_mask.any():
        quarantine_frames.append(df.loc[null_cust_mask])
        quarantine_reasons.append(
            pd.Series(
                "null customer_id",
                index=df.index[null_cust_mask],
            )
        )
        df = df.loc[~null_cust_mask].copy()

    # ------------------------------------------------------------------
    # Step 5: Standardize order_date to datetime
    # ------------------------------------------------------------------
    df["order_date"] = pd.to_datetime(df["order_date"])

    # ------------------------------------------------------------------
    # Step 6: Coerce timestamp columns
    # ------------------------------------------------------------------
    for col in _TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # ------------------------------------------------------------------
    # Step 7: Filter to valid statuses
    # ------------------------------------------------------------------
    df = df.reset_index(drop=True)
    invalid_status_mask: pd.Series = ~df["order_status"].isin(VALID_ORDER_STATUSES)
    if invalid_status_mask.any():
        bad_statuses_df = df.loc[invalid_status_mask]
        reasons_series = bad_statuses_df["order_status"].apply(
            lambda v: f"unknown order_status: {v}"
        )
        quarantine_frames.append(bad_statuses_df)
        quarantine_reasons.append(reasons_series)
        df = df.loc[~invalid_status_mask].copy()

    # ------------------------------------------------------------------
    # Step 8: Pandera validation
    # ------------------------------------------------------------------
    df = df.reset_index(drop=True)
    valid_df, invalid_df = validate_silver(df, SilverOrderSchema, "orders")

    if not invalid_df.empty:
        # invalid_df already has a quarantine_reason column from validate_silver;
        # extract it as a Series aligned to invalid_df's index before appending.
        schema_reason = invalid_df["quarantine_reason"]
        invalid_df_without_reason = invalid_df.drop(columns=["quarantine_reason"])
        quarantine_frames.append(invalid_df_without_reason)
        quarantine_reasons.append(schema_reason.reset_index(drop=True))

    # ------------------------------------------------------------------
    # Step 9: Write quarantine batches
    # ------------------------------------------------------------------
    total_quarantined: int = sum(len(f) for f in quarantine_frames)

    if quarantine_frames:
        combined_q = pd.concat(quarantine_frames, ignore_index=True)
        combined_reasons = pd.concat(
            [r.reset_index(drop=True) for r in quarantine_reasons],
            ignore_index=True,
        )
        quarantine_rows(combined_q, combined_reasons, "orders")

    log_transform_summary("orders", initial_count, len(valid_df), total_quarantined)

    # ------------------------------------------------------------------
    # Step 10: Write Silver
    # ------------------------------------------------------------------
    if valid_df.empty:
        logger.warning(
            "transform_orders: ALL {:,} input rows were quarantined — "
            "writing empty Silver orders file. Check quarantine for details.",
            initial_count,
        )
    write_silver(valid_df, "sales", "orders")

    return valid_df, total_quarantined


# ---------------------------------------------------------------------------
# Order items transform
# ---------------------------------------------------------------------------


def transform_order_items(
    valid_order_ids: set[int],
) -> tuple[pd.DataFrame, int]:
    """Read, clean, validate, and write Silver order_items.

    Parameters
    ----------
    valid_order_ids : set[int]
        Set of ``order_id`` values that exist in the Silver orders table.
        Rows referencing an order not in this set are quarantined as orphans.

    Returns
    -------
    tuple[pd.DataFrame, int]
        ``(valid_df, total_quarantine_count)``
    """
    df: pd.DataFrame = read_latest_bronze_parquet(BRONZE_DB / "order_items")
    initial_count: int = len(df)

    quarantine_frames: list[pd.DataFrame] = []
    quarantine_reasons: list[pd.Series] = []

    # ------------------------------------------------------------------
    # Step 2: Orphan order_id filter
    # ------------------------------------------------------------------
    orphan_mask: pd.Series = ~df["order_id"].isin(valid_order_ids)
    if orphan_mask.any():
        quarantine_frames.append(df.loc[orphan_mask])
        quarantine_reasons.append(
            pd.Series(
                "order_id not in Silver orders",
                index=df.index[orphan_mask],
            )
        )
        df = df.loc[~orphan_mask].copy()

    # ------------------------------------------------------------------
    # Step 3a: Remove null order_id (defensive — should be filtered above)
    # ------------------------------------------------------------------
    null_order_mask: pd.Series = df["order_id"].isna()
    if null_order_mask.any():
        quarantine_frames.append(df.loc[null_order_mask])
        quarantine_reasons.append(
            pd.Series("null order_id", index=df.index[null_order_mask])
        )
        df = df.loc[~null_order_mask].copy()

    # ------------------------------------------------------------------
    # Step 3b: Remove null unit_price
    # ------------------------------------------------------------------
    null_price_mask: pd.Series = df["unit_price"].isna()
    if null_price_mask.any():
        quarantine_frames.append(df.loc[null_price_mask])
        quarantine_reasons.append(
            pd.Series("null unit_price", index=df.index[null_price_mask])
        )
        df = df.loc[~null_price_mask].copy()

    # ------------------------------------------------------------------
    # Step 4: Fill nulls with sensible defaults
    # ------------------------------------------------------------------
    df = df.copy()
    df["freight_value"] = df["freight_value"].fillna(0.0)
    df["quantity"] = df["quantity"].fillna(1)

    # ------------------------------------------------------------------
    # Step 5: Coerce ingested_at timestamp
    # ------------------------------------------------------------------
    if "ingested_at" in df.columns:
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")

    # ------------------------------------------------------------------
    # Step 6: Pandera validation
    # ------------------------------------------------------------------
    df = df.reset_index(drop=True)
    valid_df, invalid_df = validate_silver(df, SilverOrderItemSchema, "order_items")

    if not invalid_df.empty:
        schema_reason = invalid_df["quarantine_reason"]
        invalid_df_without_reason = invalid_df.drop(columns=["quarantine_reason"])
        quarantine_frames.append(invalid_df_without_reason)
        quarantine_reasons.append(schema_reason.reset_index(drop=True))

    # ------------------------------------------------------------------
    # Quarantine + summary + write
    # ------------------------------------------------------------------
    total_quarantined: int = sum(len(f) for f in quarantine_frames)

    if quarantine_frames:
        combined_q = pd.concat(quarantine_frames, ignore_index=True)
        combined_reasons = pd.concat(
            [r.reset_index(drop=True) for r in quarantine_reasons],
            ignore_index=True,
        )
        quarantine_rows(combined_q, combined_reasons, "order_items")

    log_transform_summary("order_items", initial_count, len(valid_df), total_quarantined)
    write_silver(valid_df, "sales", "order_items")

    return valid_df, total_quarantined


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run() -> None:
    """Orchestrate the full Sales Silver transform (orders then order_items).

    Raises
    ------
    Exception
        Any unhandled exception from either transform step propagates so the
        CLI wrapper can exit with code 1.
    """
    logger.info("Starting Sales Silver transform")

    orders_df, orders_q = transform_orders()
    valid_order_ids: set[int] = set(orders_df["order_id"].tolist())

    items_df, items_q = transform_order_items(valid_order_ids)

    logger.info(
        "Sales Silver transform complete: "
        "{} orders ({} quarantined), {} order_items ({} quarantined)",
        len(orders_df),
        orders_q,
        len(items_df),
        items_q,
    )


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        run()
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Sales transform failed: {}", exc)
        sys.exit(1)
